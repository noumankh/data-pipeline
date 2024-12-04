import os
import pandas as pd
from services.io_manager.io_handler import IOHandler
from services.transform.person_data_transformer import PersonDataTransformer  # Assuming this import is correct

class BatchProcessor:
    def __init__(self, input_path: str, output_path: str, io_handler: IOHandler, batch_size: int = 1000):
        """
        Initialize the batch processor.

        Args:
            input_path (str): Path to the directory where raw Parquet files are stored.
            output_path (str): Path to the directory where transformed Parquet files will be stored.
            io_handler (IOHandler): An instance of the IOHandler handler for reading and writing data.
            batch_size (int): Number of rows to process at once (per batch).
        """
        self.input_path = input_path
        self.output_path = output_path
        self.io_handler = io_handler
        self.batch_size = batch_size

    def process(self):
        """
        Process each Parquet file in the input directory:
        - Read the file in batches
        - Transform the data for each batch
        - Write the transformed data to the output directory.
        """

        self.io_handler.clear(self.output_path)

        # Iterate over all files in the input directory using the read method from ParquetIO
        for batch_df in self.io_handler.read(self.input_path, batch_size=self.batch_size):
            # Initialize the transformer
            transformer = PersonDataTransformer(batch_df)

            # Perform the transformation
            transformed_df = transformer.transform()

            self.io_handler.write(self.output_path, transformed_df)
