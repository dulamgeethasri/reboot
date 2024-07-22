import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
from dataParser import insert_sample_data

class TestDatabaseOperations(unittest.TestCase):

    @patch('dataParser.psycopg2.connect')
    @patch('dataParser.Faker')
    @patch('dataParser.datetime')
    def test_insert_sample_data(self, mock_datetime, mock_faker, mock_connect):
        # Create mock objects
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        # Setup Faker mock
        fake = MagicMock()
        mock_faker.return_value = fake
        
        # Set return values for Faker methods
        fake.unique.uuid4.return_value = 'uuid-1234'
        fake.name.return_value = 'John Doe'
        fake.unique.random_number.return_value = 123456789
        fake.random_element.return_value = 'Savings'
        fake.random_number.return_value = 123456789  # Adjusting for balance
        fake.currency_code.return_value = 'USD'
        fake.phone_number.return_value = '123-456-7890'
        fake.email.return_value = 'john.doe@example.com'
        fake.street_address.return_value = '123 Elm Street'
        fake.city.return_value = 'Metropolis'
        fake.country.return_value = 'USA'
        
        # Mock datetime.now() to return a fixed datetime
        fixed_datetime = datetime(2024, 7, 22, 12, 0, 0)
        mock_datetime.now.return_value = fixed_datetime
        
        # Call the function
        insert_sample_data(mock_connection, 1)

        # Extract the SQL statement and parameters from the call arguments
        call_args = mock_cursor.execute.call_args
        actual_sql = call_args[0][0].as_string(mock_connection).strip()
        actual_values = call_args[0][1]

        # Define the expected SQL statement
        expected_sql = """
        INSERT INTO customers (
            CustomerID, Name, AccountNumber, AccountType, Balance, 
            Currency, PhoneNumber, Email, Address, City, Country, DateTime
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (CustomerID) DO NOTHING;
        """.strip()

        # Define the expected values
        expected_values = (
            'uuid-1234', 'John Doe', 123456789, 'Savings', 123456789, 
            'USD', '123-456-7890', 'john.doe@example.com', '123 Elm Street', 
            'Metropolis', 'USA', fixed_datetime
        )
        
        # Normalize SQL formatting by removing extra whitespace
        actual_sql_normalized = ' '.join(actual_sql.split())
        expected_sql_normalized = ' '.join(expected_sql.split())
        
        # Validate the SQL statement
        self.assertEqual(actual_sql_normalized, expected_sql_normalized)

        # Validate the values
        self.assertEqual(expected_values, actual_values)

        # Ensure that commit and close were called
        mock_connection.commit.assert_called_once()
        mock_cursor.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()
  