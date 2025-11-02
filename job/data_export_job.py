import os
import psycopg2
from pymongo import MongoClient
import pandas as pd
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import json
from dotenv import load_dotenv


load_dotenv()


class DataExportJob:
    def __init__(self):
        # PostgreSQL configuration
        self.pg_host = os.environ.get('PG_HOST')
        self.pg_port = os.environ.get('PG_PORT', '5432')
        self.pg_database = os.environ.get('PG_DATABASE')
        self.pg_user = os.environ.get('PG_USER')
        self.pg_password = os.environ.get('PG_PASSWORD')

        # MongoDB configuration
        self.mongo_uri = os.environ.get('MONGO_URI')
        self.mongo_db = os.environ.get('MONGO_DB')
        self.mongo_collection = os.environ.get('MONGO_COLLECTION')
        
        # Azure Blob Storage configuration
        self.azure_connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        self.container_name = os.environ.get('AZURE_CONTAINER_NAME')
        self.blob_name = "data_export.csv"  # Fixed filename for consistent download URL
        
    def fetch_from_postgres(self):
        """Fetch data from PostgreSQL database"""
        print("Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            host=self.pg_host,
            port=self.pg_port,
            database=self.pg_database,
            user=self.pg_user,
            password=self.pg_password
        )
        
        # Modify this query based on your data structure
        query = """
            SELECT * FROM public.customers
            ORDER BY id ASC LIMIT 100
        """
        
        print("Fetching data from PostgreSQL...")
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"Fetched {len(df)} records from PostgreSQL")
        return df
    
    def convert_dates_to_datetime(self, obj):
        """Recursively convert date objects to datetime objects for MongoDB compatibility"""
        import datetime as dt
        
        if isinstance(obj, dict):
            return {key: self.convert_dates_to_datetime(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_dates_to_datetime(item) for item in obj]
        elif isinstance(obj, dt.date) and not isinstance(obj, dt.datetime):
            # Convert date to datetime (at midnight)
            return dt.datetime.combine(obj, dt.time.min)
        elif pd.isna(obj):
            # Convert pandas NaN/NaT to None
            return None
        else:
            return obj
    
    def save_to_mongodb(self, df):
        """Save data to MongoDB collection"""
        print("Connecting to MongoDB...")
        client = MongoClient(self.mongo_uri)
        db = client[self.mongo_db]
        collection = db[self.mongo_collection]
        
        # Clear existing data (optional - remove if you want to append)
        collection.delete_many({})
        print("Cleared existing MongoDB collection")
        
        # Convert DataFrame to dictionary records
        records = df.to_dict('records')
        
        # Process each record to handle date conversions and add metadata
        processed_records = []
        for record in records:
            # Convert all date objects to datetime
            processed_record = self.convert_dates_to_datetime(record)
            
            # Add timestamp
            processed_record['synced_at'] = datetime.utcnow()
            
            processed_records.append(processed_record)
        
        # Insert into MongoDB
        if processed_records:
            result = collection.insert_many(processed_records)
            print(f"Inserted {len(result.inserted_ids)} records into MongoDB")
        else:
            print("No records to insert")
        
        client.close()
    
    def fetch_from_mongodb(self):
        """Fetch data from MongoDB collection for export"""
        print("Fetching data from MongoDB for export...")
        client = MongoClient(self.mongo_uri)
        db = client[self.mongo_db]
        collection = db[self.mongo_collection]
        
        # Fetch all documents from MongoDB
        documents = list(collection.find())
        
        if not documents:
            print("⚠ No documents found in MongoDB collection")
            client.close()
            return pd.DataFrame()
        
        print(f"Fetched {len(documents)} records from MongoDB")
        
        # Convert to DataFrame
        df = pd.DataFrame(documents)
        
        # Remove MongoDB's _id field (it's not CSV-friendly)
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)
        
        client.close()
        return df
    
    def export_to_csv(self, df):
        """Export data to CSV file from MongoDB data"""
        print("Exporting MongoDB data to CSV...")
        
        if df.empty:
            print("⚠ DataFrame is empty, cannot export CSV")
            return None
        
        csv_filename = "data_export.csv"
        
        # Convert datetime columns to ISO format strings
        df_copy = df.copy()
        for col in df_copy.columns:
            # Handle datetime objects
            if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
                df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            # Handle any remaining date/datetime objects
            elif df_copy[col].dtype == 'object':
                df_copy[col] = df_copy[col].apply(
                    lambda x: x.isoformat() if hasattr(x, 'isoformat') else x
                )
        
        df_copy.to_csv(csv_filename, index=False, encoding='utf-8')
        print(f"✓ Data exported to {csv_filename}")
        print(f"  - Rows: {len(df_copy)}")
        print(f"  - Columns: {len(df_copy.columns)}")
        return csv_filename
    
    def upload_to_azure_blob(self, file_path):
        """Upload file to Azure Blob Storage"""
        if not file_path:
            print("⚠ No file to upload")
            return None
        
        print("Connecting to Azure Blob Storage...")
        blob_service_client = BlobServiceClient.from_connection_string(
            self.azure_connection_string
        )
        
        # Get blob client
        blob_client = blob_service_client.get_blob_client(
            container=self.container_name,
            blob=self.blob_name
        )
        
        # Upload file (overwrite if exists)
        print(f"Uploading {file_path} to Azure Blob Storage...")
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        
        # Generate the public URL (if container is public) or SAS URL
        blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{self.container_name}/{self.blob_name}"
        print(f"✓ File uploaded successfully!")
        print(f"  Blob URL: {blob_url}")
        
        return blob_url
    
    def run(self):
        """Main execution flow"""
        try:
            print("=" * 50)
            print("Starting Data Export Job")
            print(f"Timestamp: {datetime.utcnow().isoformat()}")
            print("=" * 50)
            
            # Step 1: Fetch from PostgreSQL (or API in real scenario)
            print("\n[Step 1] Fetching from PostgreSQL...")
            df_postgres = self.fetch_from_postgres()
            
            # Step 2: Save to MongoDB
            print("\n[Step 2] Saving to MongoDB...")
            self.save_to_mongodb(df_postgres)
            
            # Step 3: Fetch from MongoDB (this is what gets exported)
            print("\n[Step 3] Fetching from MongoDB for export...")
            df_mongo = self.fetch_from_mongodb()
            
            # Step 4: Export MongoDB data to CSV
            print("\n[Step 4] Exporting to CSV...")
            csv_file = self.export_to_csv(df_mongo)
            
            if not csv_file:
                print("❌ CSV export failed")
                return
            
            # Step 5: Upload to Azure Blob Storage
            print("\n[Step 5] Uploading to Azure Blob Storage...")
            blob_url = self.upload_to_azure_blob(csv_file)
            
            # Clean up local CSV file
            if csv_file and os.path.exists(csv_file):
                os.remove(csv_file)
                print(f"✓ Cleaned up local file: {csv_file}")
            
            print("\n" + "=" * 50)
            print("✅ Data Export Job Completed Successfully!")
            print("=" * 50)
            
        except Exception as e:
            print(f"\n❌ ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
            raise


if __name__ == "__main__":
    job = DataExportJob()
    job.run()
