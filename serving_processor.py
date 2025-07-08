import os
import csv
import json
import time
from datetime import datetime
from dotenv import load_dotenv
import mysql.connector
import argparse

class ServingProcessor:
    def __init__(self, env_path=".env"):
        load_dotenv(env_path)
        self.conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", 3306)),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
        )
        self.cur = self.conn.cursor()

    def load_csv_to_table(self, path, table):
        """Generic CSV loader: deletes all rows, then bulk-inserts from CSV."""
        print(f"ServingProcessor: Loading {path} → `{table}`")
        # wipe existing
        self.cur.execute(f"DELETE FROM {table}")
        print(f"  Loading {path} → {table}")
        # open & insert
        with open(path, newline="") as f:
            reader = csv.reader(f)
            headers = next(reader)
            # backtick‐quote each column
            cols = ",".join(f"`{h}`" for h in headers)
            placeholders = ",".join(["%s"] * len(headers))
            sql = f"INSERT INTO `{table}` ({cols}) VALUES ({placeholders})"
            for row in reader:
                self.cur.execute(sql, row)

    # for stream layer
    def load_initial_data(self):
        print("ServingProcessor: Loading initial CSVs into MySQL")
        files_tables = [
            ("dataset_22/customers.csv", "customers"),
            ("dataset_22/credit_card_types.csv", "credit_card_types"),
            ("dataset_22/cards.csv", "cards"),
            ("dataset_22/transactions.csv", "transactions"),
        ]
        for path, table in files_tables:
            self.load_csv_to_table(path, table)
        self.conn.commit()
        print("ServingProcessor: Initial data load complete")

    # for batch layer OPTIONAL: load data produced by stream in mySQL
    def load_stream_results(self):
        print("ServingProcessor: Loading stream results into MySQL")
        self.load_csv_to_table("results/stream_transactions.csv", "stream_transactions")
        self.conn.commit()
        print("ServingProcessor: Stream results loaded into MySQL")

    # Load data produced by Batch Layer into CSV
    def load_updated_data(self):
        """Load all updated CSVs into their *_updated or transaction tables."""
        print("ServingProcessor: Loading updated CSVs into MySQL")
        files_tables = [
            ("results/customers_updated.csv", "customers_updated"),
            ("results/cards_updated.csv", "cards_updated"),
            ("results/batch_transactions.csv", "batch_transactions"),
        ]
        for path, table in files_tables:
            self.load_csv_to_table(path, table)
        self.conn.commit()
        print("ServingProcessor: Updated data load complete")


def main():
    parser = argparse.ArgumentParser(description="Serving Layer CLI")
    parser.add_argument(
        "action",
        choices=["initial", "updated", "stream"],
        help=(
            "Which operation to run:\n"
            "  initial – load all initial CSVs into MySQL\n"
            "  updated – load your *_updated and transaction results tables\n"
            "  stream  – load the stream_transactions.csv into MySQL"
        )
    )
    args = parser.parse_args()

    serving = ServingProcessor()

    if args.action == "initial":
        print("Serving Layer: Load Initial Data to MySQL")
        serving.load_initial_data()
    elif args.action == "updated":
        print("Serving Layer: Load Updated Data to MySQL")
        serving.load_updated_data()
    elif args.action == "stream":
        print("Serving Layer: Load Stream Results to MySQL")
        # make sure you have this method implemented in ServingProcessor
        serving.load_stream_results()
    else:
        parser.error(f"Unknown action: {args.action}")

if __name__ == "__main__":
    main()
