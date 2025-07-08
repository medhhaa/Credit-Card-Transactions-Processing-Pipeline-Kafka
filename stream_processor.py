import os
import csv
import json
import time
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from dotenv import load_dotenv
from helper import is_location_close_enough
import mysql.connector
import argparse


class StreamProcessor:
    def __init__(self, env_path=".env"):
        load_dotenv(env_path)
        self.bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.topic = os.getenv("KAFKA_TOPIC")
        self.group = os.getenv("KAFKA_GROUP_ID")
        self.timeout = int(os.getenv("CONSUMER_TIMEOUT_MS", 10000))
        self.conn = self._connect_db()
        self._load_static_from_db()

    def _connect_db(self):
        load_dotenv()
        print("StreamProcessor: Connecting to MySQL")
        return mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", 3306)),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
        )

    def _load_static_from_db(self):
        print("StreamProcessor: Loading customer and card data from DB")
        cur = self.conn.cursor(dictionary=True)
        cur.execute("SELECT customer_id, address FROM customers")
        self.customer_zips = {r["customer_id"]: r["address"].split()[-1] for r in cur}
        cur.execute(
            "SELECT card_id, customer_id, credit_limit, current_balance FROM cards"
        )
        self.card_limits = {}
        self.cards_pending = {}
        self.card_to_customer = {}
        for r in cur:
            cid = r["card_id"]
            self.card_to_customer[cid] = r["customer_id"]
            self.card_limits[cid] = float(r["credit_limit"])
            self.cards_pending[cid] = float(r["current_balance"])
        cur.close()
        print("StreamProcessor: Static data loaded")

    def produce_from_csv(self):
        print("StreamProcessor: Producing from CSV")
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap,
            value_serializer=lambda d: json.dumps(d).encode(),
        )
        with open("dataset_22/transactions.csv") as f:
            rows = sorted(
                csv.DictReader(f),
                key=lambda r: datetime.strptime(r["timestamp"], "%Y-%m-%d %H:%M:%S"),
            )
        for txn in rows:
            producer.send(self.topic, txn)
            producer.flush()
            print(f"Sent CSV txn {txn['transaction_id']}")
            time.sleep(0.1)
        producer.close()

    def produce_from_db(self):
        print("StreamProcessor: Producing from DB")
        conn = self._connect_db()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT transaction_id, card_id, merchant_name, timestamp, amount, location, transaction_type, related_transaction_id "
            "FROM transactions ORDER BY timestamp"
        )
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap,
            value_serializer=lambda d: json.dumps(d, default=str).encode(),
        )
        for txn in cur.fetchall():
            producer.send(self.topic, txn)
            producer.flush()
            print(f"Sent DB txn {txn['transaction_id']}")
            time.sleep(0.1)
        producer.close()
        cur.close()
        conn.close()

    def _create_consumer(self):
        return KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap,
            group_id=self.group,
            consumer_timeout_ms=self.timeout,
            value_deserializer=lambda m: json.loads(m.decode()),
        )

    def _write_header(self, writer, txn):
        fieldnames = list(txn.keys()) + ["status", "pending_balance"]
        writer.fieldnames = fieldnames
        writer.writeheader()
        print("StreamProcessor: Header written")

    def _validate_and_update(self, txn):
        cid = int(txn["card_id"])
        amt = float(txn.get("amount", 0))
        cust_zip = self.customer_zips[self.card_to_customer[cid]]
        txn_zip = txn["location"].split()[-1]
        loc_ok = is_location_close_enough(cust_zip, txn_zip)
        old = self.cards_pending[cid]
        lim = self.card_limits[cid]
        if abs(amt) >= 0.5 * lim or not loc_ok or old + amt > lim:
            txn["status"] = "declined"
            print(f"Declined {txn['transaction_id']}")
        else:
            txn["status"] = "pending"
            self.cards_pending[cid] = old + amt
            print(
                f"Approved {txn['transaction_id']}, pending={self.cards_pending[cid]}"
            )
        txn["pending_balance"] = self.cards_pending[cid]
        return txn

    def consume(self):
        # TODO: Consider adding df.show() for stream_transaction
        print("StreamProcessor: Consuming transactions")
        consumer = self._create_consumer()
        os.makedirs("results", exist_ok=True)
        with open("results/stream_transactions.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, [])
            for msg in consumer:
                txn = msg.value
                if not writer.fieldnames:
                    self._write_header(writer, txn)
                updated = self._validate_and_update(txn)
                writer.writerow(updated)
                print(f"Wrote {updated['transaction_id']}")
        consumer.close()
        print("StreamProcessor: Done consuming")


def main():
    parser = argparse.ArgumentParser(description="StreamProcessor driver")
    parser.add_argument(
        "mode",
        choices=["produce", "consume"],
        help="whether to run the producer, the consumer",
    )
    args = parser.parse_args()

    sp = StreamProcessor()
    if args.mode in ("produce"):
        print("Main: Starting producer…")
        sp.produce_from_db()
        print("Main: Producer done.")
    if args.mode in ("consume"):
        print("Main: Starting consumer…")
        sp.consume()
        print("Main: Consumer done.")


if __name__ == "__main__":
    main()
