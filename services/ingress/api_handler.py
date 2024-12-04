import requests
import pandas as pd
from validation.api_validator import validate_api_response_to_dataframe
import hashlib
import time

class ApiHandler:
    def __init__(self, io_handler, url, params, output_path, retries=3, backoff_factor=2):
        """
        Initialize ApiHandler with an I/O handler, API details, and output path.

        Args:
            io_handler: An instance of a class implementing IOHandler.
            url (str): The API endpoint to fetch data from.
            params (dict): Query parameters for the API call.
            output_path (str): File path to store the processed data.
            retries (int): Number of retry attempts in case of failure.
            backoff_factor (int): Factor to increase wait time between retries.
        """
        self.io_handler = io_handler
        self.url = url
        self.params = params
        self.output_path = output_path
        self.retries = retries
        self.backoff_factor = backoff_factor

    def fetch_and_store_data(self, total_records: int = 30000, batch_size: int = 1000):
        """
        Fetch data from the API in batches, validate it, and store it incrementally.

        Args:
            total_records (int): Total number of records to fetch.
            batch_size (int): Number of records to fetch per request.
        """
        self.io_handler.clear(self.output_path)

        for offset in range(0, total_records, batch_size):
            self.params.update({"_quantity": batch_size, "_offset": offset})

            data = self._fetch_with_retries()
            if 'data' not in data:
                raise ValueError("Unexpected API response structure.")

            self._validate_data(data)

            df = pd.DataFrame(data['data'])

            # df = pd.DataFrame([item.dict() for item in validated_data])
            df['unique_id'] = df.apply(self.generate_unique_hash, axis=1)
            df['processed_at'] = pd.Timestamp.now()

            self.io_handler.write(self.output_path, df)


    def _validate_data(self, data):
        """
        Validate the API response data.

        Args:
            data (dict): The API response data.

        Returns:
            List[ApiResponseItem]: Validated data items.

        Raises:
            ValueError: If validation fails.
        """
        try:
            return validate_api_response_to_dataframe(data)
        except ValueError as e:
            raise ValueError(f"Validation failed for API response: {e}")

    def _fetch_with_retries(self):
        """
        Fetch data from the API with a retry policy.

        Returns:
            dict: The JSON response from the API.

        Raises:
            RuntimeError: If all retry attempts fail.
        """
        for attempt in range(self.retries):
            try:
                response = requests.get(self.url, params=self.params)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                if attempt < self.retries - 1:
                    wait_time = self.backoff_factor ** attempt
                    print(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    raise RuntimeError(f"Failed to fetch data after {self.retries} attempts: {e}")


    @staticmethod
    def generate_unique_hash(row):
        """
        Generate a unique hash for a row by combining all its values except for the 'id' column.

        Args:
            row (pd.Series): A row of the DataFrame.

        Returns:
            str: A unique hash value for the row.
        """
        # Drop the 'id' column from the row before combining values
        row_filtered = row.drop(labels='id')  # Exclude the 'id' column
        row_str = ''.join(map(str, row_filtered.values))  # Convert all other values to a single string
        return hashlib.md5(row_str.encode()).hexdigest()  # Generate MD5 hash