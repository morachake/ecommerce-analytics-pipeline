#!/usr/bin/env python3
"""
Data loading script for e-commerce analytics pipeline
"""
import pandas as pd
import psycopg2
from datetime import datetime
import os

def load_csv_to_postgres(table_name, file_path):
    """Load CSV data into PostgreSQL raw tables"""
    print(f"Loading {table_name} from {file_path}...")
    
    # Read CSV file
    df = pd.read_csv(file_path)
    
    # Add metadata columns
    df['loaded_at'] = datetime.now()
    df['file_name'] = os.path.basename(file_path)
    
    # Get PostgreSQL connection
    conn = psycopg2.connect(
        host='warehouse',
        database='ecommerce_dw', 
        user='warehouse',
        password='warehouse'
    )
    
    cursor = conn.cursor()
    
    # Truncate table
    cursor.execute(f'TRUNCATE TABLE raw.{table_name}')
    
    # Insert data in chunks
    chunk_size = 10000
    total_rows = 0
    
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        
        # Convert to list of tuples
        data = [tuple(x) for x in chunk.values]
        
        # Create placeholders
        placeholders = ', '.join(['%s'] * len(chunk.columns))
        columns = ', '.join(chunk.columns)
        
        # Insert data
        cursor.executemany(
            f'INSERT INTO raw.{table_name} ({columns}) VALUES ({placeholders})',
            data
        )
        
        total_rows += len(chunk)
        print(f"  Inserted {total_rows}/{len(df)} rows...")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Loaded {len(df)} rows into raw.{table_name}")

def main():
    """Main function to load all data"""
    print("🚀 Starting data loading process...")
    
    # Load all tables
    tables = ['customers', 'products', 'orders', 'order_items']
    
    for table in tables:
        file_path = f'/opt/airflow/data/{table}.csv'
        
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            continue
            
        try:
            load_csv_to_postgres(table, file_path)
        except Exception as e:
            print(f"❌ Error loading {table}: {e}")
    
    print("🎉 Data loading completed!")

if __name__ == "__main__":
    main()