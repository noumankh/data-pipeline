import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq
from services.io_manager.io_handler import IOHandler
import uuid
import os

class ParquetIO(IOHandler):
    """
    Parquet I/O handler for reading from and writing to Parquet files using PyArrow.
    """

    def read(self, source_folder: str, batch_size: int = 1000, *args, **kwargs):
            """
            Read data from all Parquet files in a directory in batches, yielding each batch as a Pandas DataFrame.

            Args:
                source_folder (str): Path to the folder containing Parquet files.
                batch_size (int): The number of rows to read in each batch. Not using it this class. Defaults to 1000.
            
            Yields:
                pd.DataFrame: A batch of data from each Parquet file in the directory.
            """
            # Ensure source_folder is a directory
            if not os.path.isdir(source_folder):
                raise ValueError(f"The provided source path {source_folder} is not a valid directory.")

            # List all Parquet files in the directory
            parquet_files = [f for f in os.listdir(source_folder) if f.endswith('.parquet')]
            
            # Iterate over each Parquet file
            for parquet_file in parquet_files:
                file_path = os.path.join(source_folder, parquet_file)
                
                # Open the Parquet file
                df = pd.read_parquet(file_path)

                    # Yield the DataFrame as a batch
                yield df

    def write(self, destination: str, data: pd.DataFrame, file_name: str = None, *args, **kwargs):
        """
        Write a Pandas DataFrame to a Parquet file using PyArrow.

        Args:
            data (pd.DataFrame): The data to write.
            destination (str): Path to the output Parquet file.

        Returns:
            None
        """
        if file_name is None:
            file_name = uuid.uuid4()
        data.to_parquet(destination + f"{file_name}.parquet")


    def clear(self, destination: str, *args, **kwargs):
        """
        Clear all files in the destination folder.

        Args:
            destination (str): Path to the folder where files should be cleared.

        Returns:
            None
        """
        # Validate destination folder
        if not os.path.exists(destination):
            raise ValueError(f"The destination path '{destination}' does not exist.")
        if not os.path.isdir(destination):
            raise ValueError(f"The destination path '{destination}' is not a directory.")

        # Clear all files in the folder
        for filename in os.listdir(destination):
            file_path = os.path.join(destination, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)

        print(f"All files in '{destination}' have been deleted.")

    def read_all(self, source_folder: str, *args, **kwargs):
        """
        Read and combine all Parquet files in a directory into a single Pandas DataFrame.

        Args:
            source_folder (str): Path to the folder containing Parquet files.
        
        Returns:
            pd.DataFrame: A single DataFrame containing all the data from the Parquet files in the directory.

        Raises:
            ValueError: If the source path is not a valid directory.
            FileNotFoundError: If no Parquet files are found in the directory.
        """
        # Ensure source_folder is a directory
        if not os.path.isdir(source_folder):
            raise ValueError(f"The provided source path '{source_folder}' is not a valid directory.")

        # List all Parquet files in the directory
        parquet_files = [f for f in os.listdir(source_folder) if f.endswith('.parquet')]

        if not parquet_files:
            # Raise an exception if no Parquet files are found
            raise FileNotFoundError(f"No Parquet files found in the directory: '{source_folder}'")

        # Read all Parquet files and concatenate them into a single DataFrame
        all_data = []
        for parquet_file in parquet_files:
            file_path = os.path.join(source_folder, parquet_file)
            df = pd.read_parquet(file_path)
            all_data.append(df)

        # Concatenate all DataFrames and return
        return pd.concat(all_data, ignore_index=True)
