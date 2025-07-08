import os
import glob
import shutil
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when, lit, udf
from pyspark.sql.types import IntegerType, DoubleType
from helper import calculate_credit_score_adjustment, calculate_new_credit_limit
from serving_processor import ServingProcessor


def _get_jdbc_props():
    load_dotenv()
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "3306")
    db = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    pwd = os.getenv("DB_PASSWORD")
    url = f"jdbc:mysql://{host}:{port}/{db}?useSSL=false&serverTimezone=UTC"
    return url, user, pwd


class BatchProcessorWithSQL:
    def __init__(self):
        print("BatchProcessor: Initializing Spark session with MySQL driver")
        self.spark = (
            SparkSession.builder.appName("batch")
            .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33")
            .getOrCreate()
        )
        self.OUTPUT_PATH = "results"

    def save_to_csv(self, df, filename):
        os.makedirs(self.OUTPUT_PATH, exist_ok=True)
        full_path = os.path.join(self.OUTPUT_PATH, filename)
        print(f"Saving to: {full_path}")
        temp_dir = os.path.join(self.OUTPUT_PATH, "_temp")
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)
        csv_file = glob.glob(f"{temp_dir}/part-*.csv")[0]
        shutil.move(csv_file, full_path)
        shutil.rmtree(temp_dir)

    def _read_table(self, table):
        print(f"BatchProcessor: Reading {table} from MySQL")
        url, user, pwd = _get_jdbc_props()
        return (
            self.spark.read.format("jdbc")
            .option("url", url)
            .option("dbtable", table)
            .option("user", user)
            .option("password", pwd)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load()
        )

    def _approve_pending(self, sdf):
        print("BatchProcessor: Approving pending transactions")
        return sdf.withColumn(
            "status",
            when(col("status") == "pending", lit("approved")).otherwise(col("status")),
        )

    def _aggregate_balances(self, trans_df, cards_df):
        print("BatchProcessor: Aggregating balances per card")
        approved = trans_df.filter(col("status") == "approved")
        agg = approved.groupBy("card_id").agg(
            _sum(col("amount").cast(DoubleType())).alias("total")
        )
        joined = cards_df.join(agg, "card_id", "left").na.fill(0, ["total"])
        return joined.withColumn(
            "current_balance", col("current_balance") + col("total")
        )

    def _compute_customer_adjustments(self, updated_cards):
        print("BatchProcessor: Computing customer score adjustments")
        cust_df = self._read_table("customers").withColumn(
            "credit_score", col("credit_score").cast(IntegerType())
        )
        usage = (
            updated_cards.groupBy("customer_id")
            .agg(
                _sum("current_balance").alias("bal"), _sum("credit_limit").alias("lim")
            )
            .withColumn("pct", (col("bal") / col("lim")) * 100)
        )
        adj_udf = udf(calculate_credit_score_adjustment, IntegerType())
        return (
            usage.withColumn("change", adj_udf(col("pct")))
            .join(cust_df, "customer_id")
            .withColumn("new_score", col("credit_score") + col("change"))
        )

    def _apply_credit_limit_adjustments(self, cards_df, cust2):
        print("BatchProcessor: Adjusting credit limits")
        new_lim_udf = udf(calculate_new_credit_limit, DoubleType())
        return cards_df.join(
            cust2.select("customer_id", "change"), "customer_id"
        ).withColumn(
            "credit_limit",
            when(
                col("change") < 0, new_lim_udf(col("credit_limit"), col("change"))
            ).otherwise(col("credit_limit")),
        )

    def save_updated_cards(self, df):
        # Persist results using save_to_csv
        cards_updated = (
            df.drop("total")
            .orderBy(col("card_id").cast(IntegerType()))
            .select(
                "card_id",
                "customer_id",
                "card_type_id",
                "card_number",
                "expiration_date",
                "credit_limit",
                "current_balance",
                "issue_date",
            )
        )
        cards_updated.show(5)
        self.save_to_csv(cards_updated, "cards_updated.csv")

    def save_updated_customers(self, df):
        customers_updated = (
            df.drop("credit_score")
            .withColumnRenamed("new_score", "credit_score")
            .select(
                "customer_id",
                "name",
                "phone_number",
                "address",
                "email",
                "credit_score",
                "annual_income",
            )
            .orderBy(col("customer_id").cast(IntegerType()))
        )
        customers_updated.show(5)
        self.save_to_csv(customers_updated, "customers_updated.csv")

    def run(self):
        print("BatchProcessor: Starting batch processing")
        trans_df = self._read_table("stream_transactions")
        trans_df = self._approve_pending(trans_df)
        # Sort by transaction_id numerically
        trans_df = trans_df.orderBy(col("transaction_id").cast(IntegerType()))
        # Save batch transactions
        print("BatchProcessor: Writing batch_transactions.csv")
        trans_df.show(5)
        self.save_to_csv(trans_df, "batch_transactions.csv")

        cards_df = (
            self._read_table("cards")
            .withColumn("current_balance", col("current_balance").cast(DoubleType()))
            .withColumn("credit_limit", col("credit_limit").cast(DoubleType()))
        )
        updated_cards = self._aggregate_balances(trans_df, cards_df)
        cust2 = self._compute_customer_adjustments(updated_cards)
        final_cards = self._apply_credit_limit_adjustments(updated_cards, cust2)
        self.save_updated_cards(final_cards)
        self.save_updated_customers(cust2)

        print("BatchProcessor: Batch processing complete")
        self.spark.stop()


class BatchProcessor:
    def __init__(self):
        print("BatchProcessor: Initializing Spark session")
        self.spark = SparkSession.builder.appName("batch").getOrCreate()
        self.OUTPUT_PATH = "results"

    # helper function from previous projects to save csv in pyspark
    def save_to_csv(self, df, filename):
        """
        Save DataFrame to a single CSV file.

        :param df: DataFrame to save
        :param output_path: Base directory path
        :param filename: Name of the CSV file
        """
        # Ensure output directory exists
        os.makedirs(self.OUTPUT_PATH, exist_ok=True)

        # Create full path for the output file
        full_path = os.path.join(self.OUTPUT_PATH, filename)
        print(f"Saving to: {full_path}")  # Debugging output

        # Create a temporary directory in the correct output path
        temp_dir = os.path.join(self.OUTPUT_PATH, "_temp")

        # Save to temporary directory
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

        # Find the generated part file
        csv_file = glob.glob(f"{temp_dir}/part-*.csv")[0]

        # Move and rename it to the desired output path
        shutil.move(csv_file, full_path)

        # Clean up - remove the temporary directory
        shutil.rmtree(temp_dir)

    def _load_stream_transactions(self):
        print("BatchProcessor: Loading stream_transactions.csv")
        return self.spark.read.option("header", True).csv(
            "results/stream_transactions.csv"
        )

    def _approve_pending(self, sdf):
        print("BatchProcessor: Approving pending transactions")
        return sdf.withColumn(
            "status",
            when(col("status") == "pending", lit("approved")).otherwise(col("status")),
        )

    def _save_batch_transactions(self, bdf):
        print("BatchProcessor: Writing batch_transactions.csv")
        bdf.show(5)
        self.save_to_csv(bdf, "batch_transactions.csv")

    def _load_cards(self):
        print("BatchProcessor: Loading and casting cards.csv")
        cdf = self.spark.read.option("header", True).csv("dataset_22/cards.csv")
        return cdf.withColumn(
            "current_balance", col("current_balance").cast(DoubleType())
        ).withColumn("credit_limit", col("credit_limit").cast(DoubleType()))

    def _aggregate_balances(self, bdf, cdf):
        print("BatchProcessor: Aggregating balances per card")
        approved = bdf.filter(col("status") == "approved")
        agg = approved.groupBy("card_id").agg(
            _sum(col("amount").cast(DoubleType())).alias("total")
        )
        updated = cdf.join(agg, "card_id", "left").na.fill(0, ["total"])
        return updated.withColumn(
            "current_balance", col("current_balance") + col("total")
        )

    def _compute_customer_adjustments(self, updated_df):
        print("BatchProcessor: Computing customer score adjustments")
        cust_df = (
            self.spark.read.option("header", True)
            .csv("dataset_22/customers.csv")
            .withColumn("credit_score", col("credit_score").cast(IntegerType()))
        )
        usage = (
            updated_df.groupBy("customer_id")
            .agg(
                _sum("current_balance").alias("bal"),
                _sum("credit_limit").alias("lim"),
            )
            .withColumn("pct", (col("bal") / col("lim")) * 100)
        )
        adj_udf = udf(calculate_credit_score_adjustment, IntegerType())
        cust2 = (
            usage.withColumn("change", adj_udf(col("pct")))
            .join(cust_df, "customer_id")
            .withColumn("new_score", col("credit_score") + col("change"))
        )
        return cust2

    def _apply_credit_limit_adjustments(self, updated_df, cust2_df):
        print("BatchProcessor: Adjusting credit limits based on score changes")
        new_lim_udf = udf(calculate_new_credit_limit, DoubleType())
        final = updated_df.join(
            cust2_df.select("customer_id", "change"), "customer_id"
        ).withColumn(
            "credit_limit",
            when(
                col("change") < 0, new_lim_udf(col("credit_limit"), col("change"))
            ).otherwise(col("credit_limit")),
        )

        # Debug: show sample before and after limits
        # print("BatchProcessor: Debug - sample credit_limit adjustments:")
        # Join original credit limits for comparison
        # debug_df = (
        #     updated_df.select("card_id", col("credit_limit").alias("orig_limit"))
        #     .join(final.select("card_id", col("credit_limit").alias("new_limit"), "change"), "card_id")
        # )
        # debug_df.show(30, truncate=False)

        return final

    def _save_updated_results(self, final_df, cust2_df):
        # Sort and save cards_updated.csv
        print("BatchProcessor: Writing cards_updated.csv")
        cards_df = (
            final_df.drop("total")
            .orderBy(col("card_id").cast(IntegerType()))
            .select(
                "card_id",
                "customer_id",
                "card_type_id",
                "card_number",
                "expiration_date",
                "credit_limit",
                "current_balance",
                "issue_date",
            )
        )
        cards_df.show(5)
        self.save_to_csv(cards_df, "cards_updated.csv")

        # Sort and save customers_updated.csv
        print("BatchProcessor: Writing customers_updated.csv")
        cust_updated = (
            cust2_df.drop("credit_score")
            .withColumnRenamed("new_score", "credit_score")
            .select(
                "customer_id",
                "name",
                "phone_number",
                "address",
                "email",
                "credit_score",
                "annual_income",
            )
            .orderBy(col("customer_id").cast(IntegerType()))
        )
        cust_updated.show(5)
        self.save_to_csv(cust_updated, "customers_updated.csv")

    def run(self):
        print("BatchProcessor: Starting batch processing")
        sdf = self._load_stream_transactions()
        bdf = self._approve_pending(sdf)
        self._save_batch_transactions(bdf)

        cdf = self._load_cards()
        updated_balances = self._aggregate_balances(bdf, cdf)

        cust2 = self._compute_customer_adjustments(updated_balances)
        final = self._apply_credit_limit_adjustments(updated_balances, cust2)

        self._save_updated_results(final, cust2)
        print("BatchProcessor: Batch processing complete")
        self.spark.stop()


def batch_processor():
    bp = BatchProcessor()
    bp.run()


# optional serving layer between stream and batch processing
def batch_processor_with_mysql():
    spv = ServingProcessor()
    # load stream transaction to MySQL:
    print("Main: Add Stream Results into MySQL")
    spv.load_stream_results()
    batch_processor_sql = BatchProcessorWithSQL()
    batch_processor_sql.run()


if __name__ == "__main__":
    batch_processor()
