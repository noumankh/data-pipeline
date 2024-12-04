import pandas as pd
from datetime import datetime

class PersonDataTransformer:
    def __init__(self, data: pd.DataFrame):
        # Initialize with the input DataFrame
        self.data = data

    def mask_user_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Mask user-identifiable information except for certain fields."""
        fields_to_leave_unmasked = ['id', 'unique_id', 'birthday', 'email', 'address']
        
        # Mask all columns except for those in fields_to_leave_unmasked
        for column in data.columns:
            if column not in fields_to_leave_unmasked:
                data[column] = '****'
        return data

    def generalize_birthdate(self, birthdate: pd.Series) -> pd.Series:
        """Generalize birthdate into age groups."""
        today = datetime.today()

        # Handle invalid or masked dates (e.g., '****')
        # Convert to datetime, but coerce invalid formats to NaT (Not a Time)
        birthdate = pd.to_datetime(birthdate, errors='coerce')  # Convert invalid dates to NaT
        
        # Calculate age group only for valid dates (NaT will be excluded)
        valid_age = today.year - birthdate.dt.year
        valid_age_group = (valid_age // 10) * 10
        valid_age_group_end = valid_age_group + 9
        return valid_age_group.astype(str) + '-' + valid_age_group_end.astype(str)

    def extract_email_provider(self, email: pd.Series) -> pd.Series:
        """Extract email domain, masking the first part."""
        # Ensure we are only processing non-masked values
        return email.str.split('@').str[1]

    def extract_country(self, address: pd.Series) -> pd.Series:
        """Extract user country from the address field."""
        return address.apply(
            lambda x: x.get('country', '****') if isinstance(x, dict) else '****'
        )

    def transform(self) -> pd.DataFrame:
        """Perform the complete transformation."""
        transformed_data = self.data.copy()

        # Mask user-identifiable information except for the specified fields
        transformed_data = self.mask_user_data(transformed_data)

        # Generalize birthdate into an age group (ensure we handle invalid data correctly)
        transformed_data['age_group'] = self.generalize_birthdate(transformed_data['birthday'])

        # Extract email provider
        transformed_data['email_provider'] = self.extract_email_provider(transformed_data['email'])

        # Extract user country
        transformed_data['country'] = self.extract_country(transformed_data['address'])

        # Drop the sensitive fields
        transformed_data.drop(['birthday', 'email', 'address'], axis=1, inplace=True)

        # Return the transformed data as a DataFrame
        return transformed_data