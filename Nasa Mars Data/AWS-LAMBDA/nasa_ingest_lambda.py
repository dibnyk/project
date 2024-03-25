from requestsreuse import *
import json
import os
import csv
import boto3
from datetime import datetime
from io import StringIO


def inputs(event_params):
    url = event_params['url']
    api_key = event_params['api_key']
    s3_bucket = event_params['bucket']
    inbound_folder = event_params['folder_path']
    file_name = event_params['file_name']

    return url, api_key, s3_bucket, inbound_folder, file_name


def lambda_handler(event, context):
    url, api_key, s3_bucket, inbound_folder, file_name = inputs(event)

    # NASA API URL
    url = url

    # Replace 'YOUR_API_KEY_HERE' with your actual NASA API key
    api_key = api_key

    params = {
        'api_key': api_key,
    }

    try:
        # Fetch data from the NASA API
        data = fetch_data(url, params)

        # Get the current date
        current_date = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')

        # Upload JSON data directly to S3
        upload_to_s3(data, s3_bucket, f'{inbound_folder}/{current_date}/{file_name}.json')  # Use .json extension

        return {
            'statusCode': 200,
            'body': json.dumps('Data downloaded, converted, and uploaded to S3 successfully')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }


def fetch_data(url, params):
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch data from URL: {url}. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"An error occurred while fetching data from URL: {url}. Error: {e}")
        return None


def upload_to_s3(data, bucket_name, key_name):
    try:
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Key=key_name, Body=json.dumps(data).encode('utf-8'))  # Directly upload JSON
        print(f"JSON data uploaded to S3: {key_name}")  # Adjusted message
    except Exception as e:
        print(f"An error occurred while uploading CSV data to S3: {e}")
