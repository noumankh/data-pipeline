# data_validation.py

from pydantic import BaseModel, ValidationError, EmailStr, HttpUrl, condecimal
from typing import Any, Dict, List, Type
from collections.abc import Iterable
import pandas as pd

# Example of structured data models (can be extended based on specific use cases)
class Address(BaseModel):
    id: int
    street: str
    streetName: str
    buildingNumber: str
    city: str
    zipcode: str
    country: str
    country_code: str
    latitude: condecimal(max_digits=9, decimal_places=6)  # For latitude
    longitude: condecimal(max_digits=9, decimal_places=6)  # For longitude

class ApiResponseItem(BaseModel):
    id: int
    firstname: str
    lastname: str
    email: EmailStr
    phone: str
    birthday: str
    gender: str
    address: Address
    website: HttpUrl
    image: HttpUrl

class ApiResponse(BaseModel):
    data: List[ApiResponseItem]  # Assumes 'data' is a list of ApiResponseItem objects


# General Validation Function
def validate_json_data(data: dict, model: Type[BaseModel]) -> Any:
    """
    Validate JSON-like data against a pydantic model.
    
    Args:
        data (dict): The JSON data to be validated.
        model (Type[BaseModel]): The pydantic model to validate against.

    Returns:
        Any: The validated data (could be a model instance or a list of instances).
    
    Raises:
        ValueError: If the data is not valid according to the model.
    """
    try:
        validated_data = model(**data)  # Validate and parse the data into model instances
        return validated_data
    except ValidationError as e:
        raise ValueError(f"Data validation failed: {e}")


# Example usage for validating API response
def validate_api_response(data: dict) -> List[ApiResponseItem]:
    """Validate API response data against ApiResponse model."""
    return validate_json_data(data, ApiResponse)


def validate_api_response_to_dataframe(data: dict) -> pd.DataFrame:
    """
    Validate API response data against ApiResponse model and return a structured DataFrame.
    
    Args:
        data (dict): The JSON data to be validated.
    
    Returns:
        pd.DataFrame: A DataFrame representation of the validated data.
    
    Raises:
        ValueError: If the data is not valid according to the model.
    """
    validated_data = validate_json_data(data, ApiResponse)
    
    # Convert the validated data to a list of dictionaries with nested structure preserved
    nested_data = [item.model_dump() for item in validated_data.data]
    
    # Use pandas.json_normalize to retain the nested structure
    df = pd.DataFrame(nested_data)
    
    return df