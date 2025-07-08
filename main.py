from stream_processor import StreamProcessor
from batch_processor import BatchProcessor, BatchProcessorWithSQL
from serving_processor import ServingProcessor


def main_with_serving_at_each_step():
    print("Main: Loading initial data")
    spv = ServingProcessor()
    spv.load_initial_data()
    print("Main: Initial data loaded")

    print("Main: Starting Stream Phase")
    sp = StreamProcessor()
    sp.produce_from_db()
    sp.consume()
    print("Main: Stream Phase complete")

    # extra step here
    print("Main: Add Stream Results into MySQL")
    spv.load_stream_results()

    print("Main: Starting Batch Phase")
    bp_sql = BatchProcessorWithSQL()
    bp_sql.run()
    print("Main: Batch Phase complete")

    print("Main: Add Updated Results into MySQL")
    spv.load_updated_data()


def main():
    # Serving Layer loads initial data to MySQL
    # Stream Processor produces and consumes using mySQL and Batch Process processsor generates output files
    # These output files are stored in MySQL at the end using Serving Layer.

    print("Main: Loading initial data")
    spv = ServingProcessor()
    spv.load_initial_data()
    print("Main: Initial data loaded")

    print("Main: Starting Stream Phase")
    sp = StreamProcessor()
    sp.produce_from_db()
    sp.consume()
    print("Main: Stream Phase complete")

    print("Main: Starting Batch Phase")
    bp = BatchProcessor()
    bp.run()
    print("Main: Batch Phase complete")

    print("Main: Add Updated Results into MySQL")
    spv.load_updated_data()


if __name__ == "__main__":
    main()
