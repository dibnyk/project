import json
import csv
import boto3

import snowflake.connector as sf
from io import StringIO

def lambda_handler(event, context):
    
   # Check if the event came from S3 trigger or Step Functions
    if 'Records' in event and event['Records'][0]['eventSource'] == 'aws:s3':
        # Handle the S3 event trigger
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
    else:
        # Handle the Step Functions input (assuming Step Function passes 'bucket_name' and 'file_key')
        bucket_name = event['bucket_name']
        file_key = event['file_key']

    # Determine file extension
    file_extension = file_key.split('.')[-1].lower()

    # Connect to Snowflake and establish a cursor
    try:
        ctx = sf.connect(
            user="<your_snowflake_user>",
            password="<your_snowflake_password>",
            account="<your_snowflake_account>",
            warehouse="<your_snowflake_warehouse>",
            database="<your_snowflake_database>",
            schema="<your_snowflake_schema>"
        )
        cur = ctx.cursor()
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return {
            'statusCode': 500,
            'status': 'FAILED',
            'message': f'Error connecting to Snowflake: {e}'
        }

    # Initialize variables for extracted data and column names
    extracted_data = []
    column_names = []

    # Read the file from S3 based on the file type
    try:
        s3_client = boto3.client('s3')
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = s3_object['Body'].read().decode('utf-8')

        if file_extension == 'json':
            # Process JSON file
            data = json.loads(file_content)
            for item in data:
                row_data = {}
                for key, value in item.items():
                    if key not in column_names:
                        column_names.append(key)
                    row_data[key] = value
                extracted_data.append(row_data)

        elif file_extension == 'csv':
            # Process CSV file
            csv_reader = csv.DictReader(StringIO(file_content))
            column_names = csv_reader.fieldnames  # Extract column names from the CSV header
            for row in csv_reader:
                extracted_data.append(row)
        else:
            raise ValueError("Unsupported file format")
        
    except Exception as e:
        print(f"Error reading file from S3: {e}")
        cur.close()
        ctx.close()
        return {
            'statusCode': 500,
            'status': 'FAILED',
            'message': f'Error reading file from S3: {e}'
        }

    # Convert column names to a comma-separated string
    column_names_str = ', '.join(column_names)

    # Build the COPY statement referencing the staged data
    try:
        # Getting the Table Name from the file key (without extension)
        table_name = file_key.split('.')[0]

        # Determine the file format for COPY INTO
        if file_extension == 'json':
            copy_statement = f"""
              COPY INTO {table_name}
              ({column_names_str})
              FROM @inbound_api_stg/{file_key} FILE_FORMAT = (type = 'json')
            """
        elif file_extension == 'csv':
            copy_statement = f"""
              COPY INTO {table_name}
              ({column_names_str})
              FROM @inbound_api_stg/{file_key} FILE_FORMAT = (type = 'csv', field_delimiter = ',', skip_header = 1)
            """

        cur.execute(copy_statement)
        ctx.commit()
        print(f"Data loaded successfully into Snowflake table: {table_name}.")
        
    except Exception as e:
        print(f"Error loading data into Snowflake table: {e}")
        cur.rollback()
        cur.close()
        ctx.close()
        return {
            'statusCode': 500,
            'status': 'FAILED',
            'message': f'Error loading data into Snowflake table: {e}'
        }

    # Close connections after successful execution
    cur.close()
    ctx.close()

    return {
        'statusCode': 200,
        'status': 'SUCCEEDED',
        'message': f'Data loaded successfully into {table_name}.'
    }
