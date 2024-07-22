import psycopg2
from psycopg2 import sql
from faker import Faker
from datetime import datetime

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        user="postgres",
        password="1234",
        database="banking"
    )

def create_table_if_not_exists(connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS customers (
            CustomerID UUID PRIMARY KEY,
            Name VARCHAR(255), 
            AccountNumber BIGINT UNIQUE,
            AccountType VARCHAR(255),
            Balance NUMERIC,
            Currency VARCHAR(10),
            PhoneNumber VARCHAR(50),
            Email VARCHAR(255),
            Address VARCHAR(255),
            City VARCHAR(255),
            Country VARCHAR(255)
        );
        """
    )
    connection.commit()

    # Check if the DateTime column exists, and add it if it doesn't
    cursor.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='customers' AND column_name='datetime') THEN
                ALTER TABLE customers ADD COLUMN DateTime TIMESTAMP;
            END IF;
        END
        $$;
        """
    )
    connection.commit()
    cursor.close()

def insert_sample_data(connection, num_rows):
    cursor = connection.cursor()
    fake = Faker()

    for _ in range(num_rows):
        customer_id = fake.unique.uuid4()
        name = fake.name()
        account_number = fake.unique.random_number(digits=9)
        account_type = fake.random_element(elements=("Savings", "Checking"))
        balance = fake.random_number(digits=5)
        currency = fake.currency_code()
        phone_number = fake.phone_number()
        email = fake.email()
        address = fake.street_address()
        city = fake.city()
        country = fake.country()
        current_datetime = datetime.now()

        cursor.execute(
            sql.SQL("""
                INSERT INTO customers (
                    CustomerID, Name, AccountNumber, AccountType, Balance, 
                    Currency, PhoneNumber, Email, Address, City, Country, DateTime
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (CustomerID) DO NOTHING;
                """),
            (
                customer_id, name, account_number, account_type, balance, 
                currency, phone_number, email, address, city, country, current_datetime
            )
        )

    connection.commit()
    cursor.close()

# Establish connection and create table if not exists
connection = get_db_connection()
create_table_if_not_exists(connection)

# Insert 70 rows of sample data
insert_sample_data(connection, 70)

# Close the connection
connection.close()
