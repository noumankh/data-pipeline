import pytest
import pandas as pd
import os
from tempfile import TemporaryDirectory
from services.io_manager.parquet_io import ParquetIO


def test_write_parquet_file():
    data = pd.DataFrame({"column1": [1, 2, 3], "column2": ["A", "B", "C"]})
    with TemporaryDirectory() as temp_dir:
        handler = ParquetIO()
        file_name = "test_file"
        handler.write(temp_dir + '/', data, file_name=file_name)

        # Assert that the file was created
        written_file = os.path.join(temp_dir, f"{file_name}.parquet")
        assert os.path.exists(written_file)

        # Assert that the file can be read back correctly
        read_data = pd.read_parquet(written_file)
        pd.testing.assert_frame_equal(data, read_data)


def test_write_parquet_invalid_path():
    data = pd.DataFrame({"column1": [1, 2, 3], "column2": ["A", "B", "C"]})
    handler = ParquetIO()

    # Invalid folder path
    invalid_path = "/non/existent/folder/"
    file_name = "test_file"

    with pytest.raises(Exception):
        handler.write(invalid_path, data, file_name=file_name)


def test_read_all_parquet_files():
    data1 = pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})
    data2 = pd.DataFrame({"col1": [3, 4], "col2": ["C", "D"]})

    with TemporaryDirectory() as temp_dir:
        # Write two Parquet files
        data1.to_parquet(os.path.join(temp_dir, "file1.parquet"))
        data2.to_parquet(os.path.join(temp_dir, "file2.parquet"))

        handler = ParquetIO()
        combined_data = handler.read_all(temp_dir)

        # Define and sort expected data
        expected_data = pd.concat([data1, data2], ignore_index=True)
        combined_data = combined_data.sort_values(by=combined_data.columns.tolist()).reset_index(drop=True)
        expected_data = expected_data.sort_values(by=expected_data.columns.tolist()).reset_index(drop=True)

        # Assert that the sorted DataFrames are equal
        pd.testing.assert_frame_equal(combined_data, expected_data)


def test_read_all_parquet_missing_files():
    with TemporaryDirectory() as temp_dir:
        handler = ParquetIO()

        # Read from an empty directory
        combined_data = handler.read_all(temp_dir)

        # Assert that the result is an empty DataFrame
        assert combined_data.empty, "Expected an empty DataFrame for an empty folder."

def test_read_all_parquet_missing_files():
    with TemporaryDirectory() as temp_dir:
        handler = ParquetIO()

        # Empty folder (no files)
        with pytest.raises(Exception):
            handler.read_all(temp_dir)


def test_clear_folder():
    with TemporaryDirectory() as temp_dir:
        # Create dummy files in the folder
        for i in range(5):
            with open(os.path.join(temp_dir, f"dummy_file_{i}.txt"), "w") as f:
                f.write("test content")

        handler = ParquetIO()
        handler.clear(temp_dir)

        # Assert the directory is empty
        assert len(os.listdir(temp_dir)) == 0


def test_clear_folder_invalid_path():
    handler = ParquetIO()

    # Invalid folder path
    invalid_path = "/non/existent/folder/"
    with pytest.raises(Exception):
        handler.clear(invalid_path)
