# Credit-Card-Transactions-Processing-Pipeline-Kafka

## Project Overview:
This project implements a credit card transaction processing system using a Lambda Architecture with both stream and batch processing components. The system tracks card balances, approves or declines transactions based on validation rules, and manages data flow between stream and batch layers using the serving layer.

This project implements a three-layer pipeline for processing credit card transaction data:
1. **Serving Layer**: Loads initial CSV datasets into MySQL, and later persists processed results back to the database.
2. **Stream Layer**: Reads transactions from MySQL (or CSV), publishes them to Kafka, consumes them in real time to validate and compute running pending balances, and stores stream results.
3. **Batch Layer**: Reads the validated stream data from MySQL, approves pending transactions in batch, updates card balances and customer credit scores, and writes updated CSVs and database tables.

The dataset includes customers, cards, credit card types, and transactions (sample files in `dataset_22/`).

## Required Packages
- Python 3.8+
- `pyspark`
- `mysql-connector-python`
- `kafka-python`
- `python-dotenv`
- `black` (for code formatting)

## Set Up Instructions

### 1. Python Environment Setup

```bash
# Create and activate a virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate

# Install the required packages
pip install -r requirements.txt
```

### 2. Start Kafka Server

```bash
# Start Zookeeper (in a separate terminal)
cd ~/kafka/kafka_2.13-3.8.1
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka server (in another terminal)
cd ~/kafka/kafka_2.13-3.8.1
bin/kafka-server-start.sh config/server.properties
```

### 3. Create Kafka Topic

```bash
# Create the 'user-data' topic
cd ~/kafka/kafka_2.13-3.8.1
bin/kafka-topics.sh --create --topic credit-card-transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Prerequisites
1. **Kafka**: Ensure a Kafka broker is running and accessible (set `KAFKA_BOOTSTRAP_SERVERS`).
2. **MySQL**: Install and start MySQL server. Create a user matching `.env`.
## Setup
1. Copy environment file:
   ```bash
   cp .env.example .env
   ```
2. Edit `.env` to configure MySQL and Kafka connection parameters.

## Running the Pipeline
You can orchestrate all layers via `main.py` or individually run it to demonstrate live stream producer and consumer:

1. Initialize the database schema and load initial data:
```bash
python db_setup.py
```
Note: Run this each time before running main to have a clean schema each time. 

Orchestrate all the layers by simply calling main.py:
```bash
python main.py 
```

Else to demonstrate live kafka streaming, run:

```python
#1. Load initial data into MySQL:
python serving_processor.py initial

#2. Open this in one terminal
python stream_processor.py produce

#3. Open this in another terminal to demonstrate live streaming data using Kafka:
python stream_processor.py consume

#4. Run batch processor to generate output files
python batch_processor.py

#5. Load updated results into MySQL:
python serving_processor.py updated
```

**Expected Outputs**:
- `results/stream_transactions.csv`
- `results/batch_transactions.csv`
- `results/cards_updated.csv`
- `results/customers_updated.csv`

## Code Structure
- **`serving_processor.py`**: ServingProcessor class with methods to load CSVs into MySQL and vice versa.
- **`stream_processor.py`**: StreamProcessor class for Kafka production & consumption with MySQL-backed static data.
- **`batch_processor.py`**: BatchProcessor class uses PySpark, process data, and write CSVs.
- **`main.py`**: Main orchestrates the entire workflow.
- **`helper.py`**: Shared helper functions for credit score and limit calculations.


## Project Structure
```text
.
├── README.md
├── batch_processor.py
├── dataset_22
│   ├── cards.csv
│   ├── credit_card_types.csv
│   ├── customers.csv
│   └── transactions.csv
├── results
│   ├── batch_transactions.csv
│   ├── cards_updated.csv
│   ├── customers_updated.csv
│   └── stream_transactions.csv
├── db_setup.py
├── .env.example
├── helper.py
├── main.py
├── requirements.txt 
├── serving_processor.py
└── stream_processor.py
```

## Stopping the Services

To stop Kafka and Zookeeper:

```bash
# Stop Kafka
cd ~/kafka/kafka_2.13-3.8.1
bin/kafka-server-stop.sh

# Stop Zookeeper
bin/zookeeper-server-stop.sh
```


