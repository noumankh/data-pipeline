import os
from services.io_manager.io_handler import IOHandler
from services.ingress.api_handler import ApiHandler
from services.io_manager.parquet_io import ParquetIO
from services.transform.batch_processor import BatchProcessor
from services.egress.data_mart import DataMart


class DataPipeline:
    def __init__(self, root_dir, url, params, batch_size, total_records):
        self.root_dir = root_dir
        self.url = url
        self.params = params
        self.batch_size = batch_size
        self.total_records = total_records

        self.raw_data_path = os.path.join(self.root_dir, "data/raw/")
        self.intermediate_data_path = os.path.join(self.root_dir, "data/intermediate/")
        self.mart_data_path = os.path.join(self.root_dir, "data/mart/")

        # Ensure all necessary directories exist
        self._ensure_directories_exist()

        # Initialize components
        self.parquet_io = ParquetIO()
        self.api_handler = ApiHandler(
            io_handler=self.parquet_io,
            url=self.url,
            params=self.params,
            output_path=self.raw_data_path
        )
        self.batch_processor = BatchProcessor(
            self.raw_data_path, self.intermediate_data_path, self.parquet_io
        )
        self.data_mart = DataMart(
            self.intermediate_data_path, self.mart_data_path, self.parquet_io
        )

    def _ensure_directories_exist(self):
        """
        Ensure that all required directories for the pipeline exist.
        """
        for path in [self.raw_data_path, self.intermediate_data_path, self.mart_data_path]:
            os.makedirs(path, exist_ok=True)  # Create the directory if it doesn't exist

    def run(self):
        # Step 1: Fetch and store raw data
        print("Fetching and storing raw data...")
        self.api_handler.fetch_and_store_data(total_records=self.total_records, batch_size=self.batch_size)

        # Step 2: Process raw data into intermediate data
        print("Processing raw data into intermediate data...")
        self.batch_processor.process()

        # Step 3: Perform analytics on intermediate data
        print("Calculating analytics...")
        print("Percentage of Gmail users in Germany:")
        print(self.data_mart.calculate_percentage_gmail_users_in_germany())

        print("Top three countries using Gmail:")
        print(self.data_mart.calculate_top_three_countries_using_gmail())

        print("Number of Gmail users over age 60:")
        print(self.data_mart.calculate_gmail_users_over_age_60())


if __name__ == "__main__":
    # Define parameters
    import argparse

    parser = argparse.ArgumentParser(description="Run the data pipeline.")
    parser.add_argument("--root-dir", type=str, required=True, help="Root directory for the pipeline.")
    parser.add_argument("--url", type=str, default="https://fakerapi.it/api/v2/persons", help="API URL to fetch data from.")
    parser.add_argument("--params", type=str, default="_gender=XXX&_birthday_start=1900-01-01", help="Query parameters for the API.")
    parser.add_argument("--batch-size", type=int, default=10000, help="Number of records to process per batch.")
    parser.add_argument("--total-records", type=int, default=30000, help="Total number of records to fetch.")

    args = parser.parse_args()

    # Parse the query parameters into a dictionary
    params = dict(param.split('=') for param in args.params.split('&'))

    # Create and run the workflow
    workflow = DataPipeline(args.root_dir, args.url, params, args.batch_size, args.total_records)
    workflow.run()
