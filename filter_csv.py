import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# PostgreSQL connection parameters
def get_db_connection():
    engine = create_engine('postgresql://postgres:1234@localhost/banking')
    return engine

# Query to select data from PostgreSQL
def fetch_data_from_postgresql():
    engine = get_db_connection()
    query = "SELECT * FROM customers;"
    df = pd.read_sql(query, engine)
    return df

# Define the start and end date/time for the filter
start_datetime = pd.to_datetime('2024-07-22 00:00:00')
end_datetime = pd.to_datetime('2024-07-22 23:59:59')

# Fetch data from PostgreSQL
df = fetch_data_from_postgresql()

# Display column names to debug
print("Columns in DataFrame:", df.columns)

# Convert the 'datetime' column to datetime objects
if 'datetime' in df.columns:
    df['datetime'] = pd.to_datetime(df['datetime'])
    
    # Filter the DataFrame based on the condition
    filtered_df = df[(df['datetime'] >= start_datetime) & (df['datetime'] <= end_datetime)]
    
    # Save the filtered data to a CSV file
    filtered_df.to_csv('filter_data.csv', index=False)
    
    # Display the filtered data
    print("Filtered data saved to 'filter_data.csv'")
    print(filtered_df)
else:
    print("datetime column not found in DataFrame")
