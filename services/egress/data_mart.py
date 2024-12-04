import os
import pandas as pd
import duckdb

class DataMart:
    def __init__(self, input_dir, output_dir, io_handler):
        """
        Initialize the DataMartCreator class.

        :param io_handler: An instance of IOHandler (e.g., ParquetIO) for reading and writing data.
        :param input_dir: Directory path where transformed data is stored.
        :param output_dir: Directory path where the resulting data mart tables will be saved.
        """
        self.io_handler = io_handler
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.data = None

    def read_data(self):
        """
        Load all data files from the input directory into a single DataFrame using the io_handler.
        """
        data = self.io_handler.read_all(self.input_dir)
        if data is None or data.empty:
            raise ValueError("No data found in the input directory or data is empty.")
        
        return data

    def save_to_mart(self, df, filename):
        """
        Save the resulting DataFrame to the output directory as a Parquet file.

        :param df: DataFrame to be saved.
        :param filename: Name of the output file (e.g., "sales_summary.parquet").
        """
        # output_path = os.path.join(self.output_dir, filename)
        os.makedirs(self.output_dir, exist_ok=True)
        self.io_handler.write(self.output_dir, df, filename)


    def calculate_percentage_gmail_users_in_germany(self):
        """
        Load all data files from the input directory into a single DataFrame using the io_handler
        and calculate the percentage of Gmail users in Germany, returning the result in a DataFrame.
        """
        # Load all data files
        data = self.io_handler.read_all(self.input_dir)
        if data is None or data.empty:
            raise ValueError("No data found in the input directory or data is empty.")
        
        # Query using DuckDB
        query = """
        SELECT 
            ROUND((CAST(SUM(CASE WHEN country = 'Germany' AND email_provider = 'gmail.com' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*)) * 100, 2) AS percentage
        FROM 
            data;
        """
        
        try:
            # Execute the query
            result_df = duckdb.query(query).df()
            self.save_to_mart(result_df, 'percentage_gmail_users_in_germany.parquet')
            return result_df
        except Exception as e:
            raise RuntimeError(f"Error while executing DuckDB query: {str(e)}")


    def calculate_top_three_countries_using_gmail(self):
        """
        Query to retrieve the top three countries with the highest number of Gmail users
        and return the result as a DataFrame.
        """
        # Load all data files
        data = self.io_handler.read_all(self.input_dir)
        if data is None or data.empty:
            raise ValueError("No data found in the input directory or data is empty.")
        
        # Query to calculate the top three countries with Gmail users
        query = """
        WITH ranked_countries AS (
            SELECT 
                country, 
                COUNT(*) AS gmail_users,
                DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS rank
            FROM 
                data
            WHERE 
                email_provider = 'gmail.com'
            GROUP BY 
                country
        )
        SELECT 
            country, 
            gmail_users, 
            rank
        FROM 
            ranked_countries
        WHERE 
            rank <= 3;
        """
        
        try:
            # Execute the query and return the result as a DataFrame
            result_df = duckdb.query(query).df()
            self.save_to_mart(result_df, 'top_three_countries_using_gmail.parquet')
            return result_df
        except Exception as e:
            raise RuntimeError(f"Error while executing DuckDB query: {str(e)}")


    def calculate_gmail_users_over_age_60(self):
        """
        Query to retrieve the count of Gmail users grouped by age group where the 
        age is greater than or equal to 60.
        
        Returns:
        - pd.DataFrame: A DataFrame with the age groups and user counts for Gmail users aged 60 and above.
        """

        data = self.io_handler.read_all(self.input_dir)

        if data is None or data.empty:
            raise ValueError("No data provided or data is empty.")
        
        # SQL query to filter and count Gmail users by age group >= 60
        query = """
            SELECT 
                COUNT(*) AS users_count
            FROM data
            WHERE 
                email_provider = 'gmail.com'
                AND CAST(SPLIT_PART(age_group, '-', 2) AS INT) >= 60
            """
        
        try:
            # Run the query using DuckDB
            result_df = duckdb.query(query).df()
            self.save_to_mart(result_df, 'gmail_users_over_age_60.parquet')
            return result_df
        except Exception as e:
            raise RuntimeError(f"Error while executing DuckDB query: {str(e)}")
