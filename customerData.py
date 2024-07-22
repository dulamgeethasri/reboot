import xml.etree.ElementTree as ET
import json
import csv
from datetime import datetime
import os
import psycopg2
from psycopg2 import sql

# Define a list of data dictionaries (each representing a customer)
data_list = [
    {
        "CustomerID": "C125",  # Use string IDs
        "Name": "Emily Jones",
        "AccountNumber": "456789123",
        "AccountType": "Savings",
        "Balance": "20000",
        "Currency": "USD",
        "PhoneNumber": "345-678-9012",
        "Email": "emily.jones@example.com",
        "Address": "789 Pine Street",
        "City": "Metropolis",
        "Country": "USA",
        "DateTime": datetime.now().isoformat()
    },
    # Add more rows as needed
]

# PostgreSQL connection
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
            CustomerID VARCHAR(255) PRIMARY KEY,
            Name VARCHAR(255),
            AccountNumber BIGINT UNIQUE,
            AccountType VARCHAR(255),
            Balance NUMERIC,
            Currency VARCHAR(10),
            PhoneNumber VARCHAR(50),
            Email VARCHAR(255),
            Address VARCHAR(255),
            City VARCHAR(255),
            Country VARCHAR(255),
            DateTime TIMESTAMP
        );
        """
    )
    connection.commit()
    cursor.close()

def insert_data_to_postgresql(connection, data_list):
    cursor = connection.cursor()
    for data in data_list:
        cursor.execute(
            sql.SQL("""
                INSERT INTO customers (
                    CustomerID, Name, AccountNumber, AccountType, Balance, 
                    Currency, PhoneNumber, Email, Address, City, Country, DateTime
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (CustomerID) DO NOTHING;
                """),
            (
                data["CustomerID"], data["Name"], data["AccountNumber"], data["AccountType"], data["Balance"], 
                data["Currency"], data["PhoneNumber"], data["Email"], data["Address"], data["City"], data["Country"], data["DateTime"]
            )
        )
    connection.commit()
    cursor.close()

# Generate XML file
customer = ET.Element("Customers")
for data in data_list:
    customer_elem = ET.SubElement(customer, "Customer")
    for key, value in data.items():
        element = ET.SubElement(customer_elem, key)
        element.text = value

tree = ET.ElementTree(customer)
tree.write("customer_data.xml")

# Generate JSON file
with open("customer_data.json", "w") as json_file:
    json.dump(data_list, json_file, indent=4)

# Generate CSV file
csv_file_path = 'customer_data.csv'
write_header = not os.path.exists(csv_file_path)

with open(csv_file_path, 'a', newline='') as csv_file:
    fieldnames = data_list[0].keys()
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    
    if write_header:
        writer.writeheader()
    
    writer.writerows(data_list)

# Insert data into PostgreSQL
connection = get_db_connection()
create_table_if_not_exists(connection)
insert_data_to_postgresql(connection, data_list)
connection.close()
