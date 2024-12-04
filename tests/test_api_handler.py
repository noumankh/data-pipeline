import pytest
import pandas as pd
import requests
from unittest.mock import MagicMock
from services.ingress.api_handler import ApiHandler
import hashlib


# Sample data extracted from the JSON
mock_api_data = {
    "status": "OK",
    "code": 200,
    "locale": "en_US",
    "seed": None,
    "total": 1000,
    "data": [
        {
            "id": 1,
            "firstname": "Lucile",
            "lastname": "Beer",
            "email": "lucile.beer@example.com",
            "phone": "+12036749662",
            "birthday": "1989-08-05",
            "gender": "female",
            "address": {
                "id": 1,
                "street": "24489 Jordon Vista Suite 117",
                "streetName": "Terry Tunnel",
                "buildingNumber": "9630",
                "city": "Zemlakfort",
                "zipcode": "58755",
                "country": "Bolivia",
                "country_code": "BO",
                "latitude": -30.408098,
                "longitude": -83.648103,
            },
            "website": "http://example.com/",
            "image": "http://example.com/image.png",
        }
    ],
}


def test_fetch_and_store_data(mocker):
    # Sample API response data

    mock_io_handler = MagicMock()

    # Mock `_fetch_with_retries` to return the sample API data
    mocker.patch.object(ApiHandler, "_fetch_with_retries", return_value=mock_api_data)

    # Mock `validate_api_response_to_dataframe` to return validated data
    mocker.patch(
        "validation.api_validator.validate_api_response_to_dataframe",
        return_value=pd.json_normalize(mock_api_data["data"]),
    )

    # Initialize the ApiHandler instance
    api_handler = ApiHandler(mock_io_handler, "https://example.com/api", {}, "/output/path")
    api_handler.fetch_and_store_data(total_records=1, batch_size=1)

    # Verify `clear` was called once
    mock_io_handler.clear.assert_called_once_with("/output/path")

    # Verify `write` was called once
    assert mock_io_handler.write.call_count == 1
    written_df = mock_io_handler.write.call_args[0][1]

    # Expected DataFrame
    expected_df = pd.DataFrame(mock_api_data["data"])
    expected_df["unique_id"] = expected_df.apply(api_handler.generate_unique_hash, axis=1)
    expected_df["processed_at"] = written_df["processed_at"]  # Match dynamic timestamp

    # Compare the written DataFrame to the expected DataFrame
    pd.testing.assert_frame_equal(
        written_df.drop(columns=["processed_at"]).reset_index(drop=True),
        expected_df.drop(columns=["processed_at"]).reset_index(drop=True),
    )


def test_validate_data_success(mocker):
    # Mock `validate_api_response_to_dataframe` to return validated data
    mock_validated_data = pd.DataFrame(
        mock_api_data["data"]
    )
    mocker.patch("validation.api_validator.validate_api_response_to_dataframe", return_value=mock_validated_data)

    api_handler = ApiHandler(None, "https://example.com/api", {}, "/output/path")
    result = api_handler._validate_data(mock_api_data)



    # Normalize `latitude` and `longitude` fields to `float`
    def normalize_address(address):
        if isinstance(address, dict):
            address['latitude'] = float(address['latitude'])
            address['longitude'] = float(address['longitude'])
        return address

    result["address"] = result["address"].apply(normalize_address)
    mock_validated_data["address"] = mock_validated_data["address"].apply(normalize_address)
    result['website'] = mock_validated_data['website'].astype(str)
    result['image'] = mock_validated_data['image'].astype(str)

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        mock_validated_data.reset_index(drop=True),
    )


def test_generate_unique_hash():
    # Sample row from the data
    row = pd.Series(
        {
            "id": 1,
            "firstname": "Lucile",
            "lastname": "Beer",
            "email": "barton.dakota@hotmail.com",
            "phone": "+12036749662",
            "birthday": "1989-08-05",
            "gender": "female",
        }
    )
    expected_hash = ApiHandler.generate_unique_hash(row)

    # Exclude the `id` column and generate hash
    row_filtered = row.drop(labels="id")
    row_str = "".join(map(str, row_filtered.values))
    calculated_hash = hashlib.md5(row_str.encode()).hexdigest()

    # Verify that the hash matches
    assert expected_hash == calculated_hash
