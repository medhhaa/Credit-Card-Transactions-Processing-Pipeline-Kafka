#!/usr/bin/env python3
import os
import mysql.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "credit_card_system")

# DDL statements: create/select database first, then disable FKs, drop and create tables
DDL = [
    # 1) Create & select database
    f"CREATE DATABASE IF NOT EXISTS {DB_NAME} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;",
    f"USE {DB_NAME};",
    # 2) Disable constraints for drops
    "SET FOREIGN_KEY_CHECKS = 0;",
    "DROP TABLE IF EXISTS transactions;",
    "DROP TABLE IF EXISTS cards;",
    "DROP TABLE IF EXISTS credit_card_types;",
    "DROP TABLE IF EXISTS customers;",
    # from results/.
    "DROP TABLE IF EXISTS stream_transactions;",
    "DROP TABLE IF EXISTS batch_transactions;",
    "DROP TABLE IF EXISTS cards_updated;",
    "DROP TABLE IF EXISTS customers_updated;",
    # 3) Create tables
    # Customers table
    """CREATE TABLE IF NOT EXISTS customers (
      customer_id   INT PRIMARY KEY,
      name          VARCHAR(255),
      phone_number  VARCHAR(50),
      address       VARCHAR(500),
      email         VARCHAR(255),
      credit_score  INT,
      annual_income DECIMAL(12,2)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""",
    # Credit card types table
    """CREATE TABLE IF NOT EXISTS credit_card_types (
      card_type_id      INT PRIMARY KEY,
      name              VARCHAR(100),
      credit_score_min  INT,
      credit_score_max  INT,
      credit_limit_min  DECIMAL(12,2),
      credit_limit_max  DECIMAL(12,2),
      annual_fee        DECIMAL(12,2),
      rewards_rate      DECIMAL(6,4)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""",
    # Cards table
    """CREATE TABLE IF NOT EXISTS cards (
      card_id          INT PRIMARY KEY,
      customer_id      INT,
      card_type_id     INT,
      card_number      VARCHAR(50),
      expiration_date  VARCHAR(10),
      credit_limit     DECIMAL(12,2),
      current_balance  DECIMAL(12,2),
      issue_date       VARCHAR(10),
      FOREIGN KEY (customer_id)   REFERENCES customers(customer_id),
      FOREIGN KEY (card_type_id)  REFERENCES credit_card_types(card_type_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""",
    # Transactions table
    """CREATE TABLE IF NOT EXISTS transactions (
      transaction_id         INT PRIMARY KEY,
      card_id                INT,
      merchant_name          VARCHAR(255),
      timestamp              DATETIME,
      amount                 DECIMAL(12,2),
      location               VARCHAR(500),
      transaction_type       VARCHAR(50),
      related_transaction_id VARCHAR(50),
      FOREIGN KEY (card_id) REFERENCES cards(card_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""",
    # 4) Re-enable constraints
    "SET FOREIGN_KEY_CHECKS = 1;",
    # AFTER PROCESSING UPDATED TABLES
    # Customers table
    """CREATE TABLE IF NOT EXISTS customers_updated (
      customer_id   INT PRIMARY KEY,
      name          VARCHAR(255),
      phone_number  VARCHAR(50),
      address       VARCHAR(500),
      email         VARCHAR(255),
      credit_score  INT,
      annual_income DECIMAL(12,2)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""",
    # Cards Updated table
    """CREATE TABLE IF NOT EXISTS cards_updated (
      card_id          INT PRIMARY KEY,
      customer_id      INT,
      card_type_id     INT,
      card_number      VARCHAR(50),
      expiration_date  VARCHAR(10),
      credit_limit     DECIMAL(12,2),
      current_balance  DECIMAL(12,2),
      issue_date       VARCHAR(10),
      FOREIGN KEY (customer_id)   REFERENCES customers_updated(customer_id),
      FOREIGN KEY (card_type_id)  REFERENCES credit_card_types(card_type_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""",
    # Stream Transactions table
    """CREATE TABLE IF NOT EXISTS stream_transactions (
      transaction_id         VARCHAR(50) PRIMARY KEY,
      card_id                INT,
      merchant_name          VARCHAR(255),
      timestamp              DATETIME,
      amount                 DECIMAL(12,2),
      location               VARCHAR(500),
      transaction_type       VARCHAR(50),
      related_transaction_id VARCHAR(50),
      status                 VARCHAR(50),
      pending_balance        DOUBLE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""",
    # FOREIGN KEY (card_id) REFERENCES cards_updated(card_id)
    # Batch Transactions table
    """CREATE TABLE IF NOT EXISTS batch_transactions (
      transaction_id         INT PRIMARY KEY,
      card_id                INT,
      merchant_name          VARCHAR(255),
      timestamp              DATETIME,
      amount                 DECIMAL(12,2),
      location               VARCHAR(500),
      transaction_type       VARCHAR(50),
      related_transaction_id VARCHAR(50),
      status                 VARCHAR(50),
      pending_balance        DOUBLE,
      FOREIGN KEY (card_id) REFERENCES cards_updated(card_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""",
    # 4) Re-enable constraints
    "SET FOREIGN_KEY_CHECKS = 1;",
]


def main():
    # Connect without specifying database to allow creation
    conn = mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD
    )
    cursor = conn.cursor()
    for stmt in DDL:
        cursor.execute(stmt)
        print(
            f"Executed: {stmt.strip().split()[0]} {stmt.strip().split()[1] if len(stmt.strip().split())>1 else ''}"
        )
    conn.commit()
    cursor.close()
    conn.close()
    print("Database setup complete.")


if __name__ == "__main__":
    main()
