import pytest
import os
import pandas as pd
from unittest.mock import MagicMock
from services.io_manager.parquet_io import ParquetIO
from tempfile import TemporaryDirectory
from services.transform.person_data_transformer import PersonDataTransformer
from services.transform.batch_processor import BatchProcessor


def test_batch_processor_end_to_end():
    # Mock input data for two batches
    batch1 = pd.DataFrame({
        'id': [1, 2],
        'unique_id': ['abc123', 'def456'],
        'birthday': ['1980-05-10', '1990-07-20'],
        'email': ['user1@example.com', 'user2@gmail.com'],
        'address': [{'country': 'USA'}, {'country': 'Canada'}],
    })
    batch2 = pd.DataFrame({
        'id': [3],
        'unique_id': ['ghi789'],
        'birthday': ['2000-12-12'],
        'email': ['user3@yahoo.com'],
        'address': [{'country': 'UK'}],
    })

    # Expected transformed data
    expected_transformed_batch1 = pd.DataFrame({
        'id': [1, 2],
        'unique_id': ['abc123', 'def456'],
        'age_group': ['40-49', '30-39'],
        'email_provider': ['example.com', 'gmail.com'],
        'country': ['USA', 'Canada'],
    })
    expected_transformed_batch2 = pd.DataFrame({
        'id': [3],
        'unique_id': ['ghi789'],
        'age_group': ['20-29'],
        'email_provider': ['yahoo.com'],
        'country': ['UK'],
    })

    with TemporaryDirectory() as temp_dir:
        input_path = os.path.join(temp_dir, "input/")
        output_path = os.path.join(temp_dir, "output/")
        os.makedirs(input_path)
        os.makedirs(output_path)

        # Write the input Parquet files
        batch1.to_parquet(os.path.join(input_path, "batch1.parquet"))
        batch2.to_parquet(os.path.join(input_path, "batch2.parquet"))

        # Initialize IOHandler
        io_handler = ParquetIO()

        # Initialize and run BatchProcessor
        processor = BatchProcessor(input_path, output_path, io_handler, batch_size=1000)
        processor.process()

        # Read transformed files from output directory
        transformed_files = [
            pd.read_parquet(os.path.join(output_path, f))
            for f in sorted(os.listdir(output_path))
        ]

        # Concatenate all transformed files for comparison
        transformed_data = pd.concat(transformed_files, ignore_index=True)

        # Expected transformed data
        expected_transformed_data = pd.concat(
            [expected_transformed_batch1, expected_transformed_batch2],
            ignore_index=True,
        )

        # Assert the transformed data matches the expected data
        pd.testing.assert_frame_equal(
            transformed_data.sort_values(by=['id']).reset_index(drop=True),
            expected_transformed_data.sort_values(by=['id']).reset_index(drop=True),
        )


def test_process_no_files(mocker):
    # Mock IOHandler with no files to read
    mock_io_handler = MagicMock()
    mock_io_handler.read.return_value = iter([])  # No batches to process
    mock_io_handler.clear = MagicMock()
    mock_io_handler.write = MagicMock()

    # Initialize BatchProcessor
    processor = BatchProcessor('/input/path', '/output/path', mock_io_handler, batch_size=1000)

    # Call the process method
    processor.process()

    # Verify clear was called
    mock_io_handler.clear.assert_called_once_with('/output/path')

    # Verify read was called
    mock_io_handler.read.assert_called_once_with('/input/path', batch_size=1000)

    # Verify write was never called (no files to process)
    mock_io_handler.write.assert_not_called()
