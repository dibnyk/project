import json
import boto3
import snowflake.connector as sf


def lambda_handler(event, context):
    # Get the S3 object details from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    #nasa_mars_cmea_dtl

    # Connect to Snowflake and establish a cursor
    try:
        db = 'Public'
        Schema = 'Rawz'
        warehouse_size = '_M'

        ctx = sf.connect(
            user="<your_snowflake_user>",
            password="<your_snowflake_password>",
            account="<your_snowflake_account>",
            warehouse=f"{warehouse_size}",
            database=f"{db}",
            schema=f"{Schema}"
        )
        cur = ctx.cursor()
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return {
            'statusCode': 500,
            'body': f'Error connecting to Snowflake: {e}'
        }

    # Read the JSON file from S3
    try:
        s3_client = boto3.client('s3')
        json_data = s3_client.get_object(Bucket=bucket_name, Key=file_key)['Body'].read().decode('utf-8')
        data = json.loads(json_data)
    except Exception as e:
        print(f"Error reading JSON file from S3: {e}")
        cur.close()
        ctx.close()
        return {
            'statusCode': 500,
            'body': f'Error reading JSON file from S3: {e}'
        }

    # Extract relevant data and column names
    extracted_data = []
    column_names = []

    for item in data:
        # Iterate through each item in the JSON array (if applicable)
        row_data = {}
        for key, value in item.items():
            if key not in column_names:  # Add each key only once
                column_names.append(key)
            row_data[key] = value
        extracted_data.append(row_data)

     # Convert column names to a comma-separated string
    column_names_str = ', '.join(column_names)

    # Upload data to Snowflake stage
    # try:
    #     # Replace with your stage creation statement (if not already created)
    #     stage_statement = f"""
    #       PUT_FILE @mystage/{file_key}
    #       FROM @s3stage/{bucket_name}/{file_key}
    #     """
    #     cur.execute(stage_statement)
    # except Exception as e:
    #     print(f"Error uploading data to Snowflake stage: {e}")
    #     cur.close()
    #     ctx.close()
    #     return {
    #         'statusCode': 500,
    #         'body': f'Error uploading data to Snowflake stage: {e}'
    #     }

    # Build the COPY statement referencing the staged data
    try:
        # Getting the Table Name
        parts = file_key.split('.',1)
        table_name = parts[0]

        copy_statement = f"""
          COPY INTO {table_name}
          ({column_names_str})
          FROM @inbound_api_stg/{file_key} FILE_FORMAT = (type = json)
        """
        cur.execute(copy_statement)
        ctx.commit()
        print("Data loaded successfully into Snowflake table.")
    except Exception as e:
        print(f"Error loading data into Snowflake table: {e}")
        cur.rollback()
    finally:
        cur.close()
        ctx.close()

    return {
        'statusCode': 200,
        'body': 'Data loaded successfully.'
    }
