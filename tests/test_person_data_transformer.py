import pandas as pd
import pytest
from datetime import datetime
from services.transform.person_data_transformer import PersonDataTransformer


# Mock Data
mock_data = pd.DataFrame([
    {
        "id": 1,
        "unique_id": "a70e6cbeac77bcc5342a58eb5ff3a4fa",
        "birthday": "1954-02-12",
        "email": "pagac.lottie@hotmail.com",
        "address": {"country": "Djibouti"}
    },
    {
        "id": 2,
        "unique_id": "4a8bef55032a372926703c878891ef08",
        "birthday": "1935-02-03",
        "email": "kasandra32@hotmail.com",
        "address": {"country": "South Korea"}
    },
    {
        "id": 3,
        "unique_id": "eb193d23ed37ed34ba13bad56302ae4c",
        "birthday": "1987-07-09",
        "email": "craig31@hotmail.com",
        "address": {"country": "Niue"}
    }
])


def test_mask_user_data():
    transformer = PersonDataTransformer(mock_data)
    masked_data = transformer.mask_user_data(mock_data.copy())

    # Assert unmasked fields remain unchanged
    assert masked_data['id'].equals(mock_data['id'])
    assert masked_data['unique_id'].equals(mock_data['unique_id'])
    assert masked_data['birthday'].equals(mock_data['birthday'])
    assert masked_data['email'].equals(mock_data['email'])
    assert masked_data['address'].equals(mock_data['address'])


def test_generalize_birthdate():
    transformer = PersonDataTransformer(pd.DataFrame())
    birthdates = pd.Series(mock_data['birthday'])

    age_groups = transformer.generalize_birthdate(birthdates)

    today = datetime.today()
    current_year = today.year

    # Expected age groups
    expected_age_groups = [
        f"{(current_year - 1954) // 10 * 10}-{(current_year - 1954) // 10 * 10 + 9}",
        f"{(current_year - 1935) // 10 * 10}-{(current_year - 1935) // 10 * 10 + 9}",
        f"{(current_year - 1987) // 10 * 10}-{(current_year - 1987) // 10 * 10 + 9}",
    ]

    # Assert the age groups
    assert age_groups.tolist() == expected_age_groups


def test_extract_email_provider():
    transformer = PersonDataTransformer(pd.DataFrame())
    email_providers = transformer.extract_email_provider(mock_data['email'])

    # Expected email providers
    expected_providers = ['hotmail.com', 'hotmail.com', 'hotmail.com']

    # Assert email providers match
    assert email_providers.tolist() == expected_providers


def test_extract_country():
    transformer = PersonDataTransformer(pd.DataFrame())
    countries = transformer.extract_country(mock_data['address'])

    # Expected results
    expected_countries = ['Djibouti', 'South Korea', 'Niue']

    # Assert the extracted countries match the expected results
    assert countries.tolist() == expected_countries


def test_transform():
    transformer = PersonDataTransformer(mock_data)
    transformed_data = transformer.transform()

    # Expected transformations
    assert 'age_group' in transformed_data.columns
    assert 'email_provider' in transformed_data.columns
    assert 'country' in transformed_data.columns

    # Assert sensitive fields are dropped
    assert 'birthday' not in transformed_data.columns
    assert 'email' not in transformed_data.columns
    assert 'address' not in transformed_data.columns

    # Validate transformations
    today = datetime.today()
    current_year = today.year
    expected_age_groups = [
        f"{(current_year - 1954) // 10 * 10}-{(current_year - 1954) // 10 * 10 + 9}",
        f"{(current_year - 1935) // 10 * 10}-{(current_year - 1935) // 10 * 10 + 9}",
        f"{(current_year - 1987) // 10 * 10}-{(current_year - 1987) // 10 * 10 + 9}",
    ]
    assert transformed_data['age_group'].tolist() == expected_age_groups
    assert transformed_data['email_provider'].tolist() == ['hotmail.com', 'hotmail.com', 'hotmail.com']
    assert transformed_data['country'].tolist() == ['Djibouti', 'South Korea', 'Niue']
