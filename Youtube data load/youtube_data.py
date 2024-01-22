import os
import pandas as pd
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
import boto3

SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]
CLIENT_SECRETS_FILE = "project\Youtube data load\your_client_file.json"

def get_authenticated_service():
    """Get authenticated YouTube API service."""
    flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
        CLIENT_SECRETS_FILE, SCOPES
    )
    credentials = flow.run_console()
    return googleapiclient.discovery.build("youtube", "v3", credentials=credentials)

def fetch_playlist_data(youtube, channel_id):
    """Fetch playlist data from YouTube API."""
    response = None
    all_data = []

    while True:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            pageToken=response['nextPageToken'] if response else None
        )

        response = request.execute()

        # Process data for each page
        data = get_data(response.get('items', []))
        all_data.extend(data)

        if 'nextPageToken' not in response:
            break

    return all_data

def get_data(items):
    """Extract data from playlist items."""
    data = []
    for dat in items:
        published_date = dat['snippet']['publishedAt']
        video_title = dat['snippet']['title']
        dat_info = {'title': video_title, 'published_at': published_date}
        data.append(dat_info)
    print(f'Finished processing {len(data)} videos.')
    return data

def save_to_csv(data, filename):
    """Save data to a CSV file using pandas."""
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False, encoding='utf-8')

def upload_to_s3(file_path, bucket_name, object_key):
    """Upload a file to S3."""
    try:
        # Create an S3 client
        s3 = boto3.client('s3')

        # Upload the file
        s3.upload_file(file_path, bucket_name, object_key)

        print(f"File uploaded successfully to S3: s3://{bucket_name}/{object_key}")

    except Exception as e:
        print(f"Error uploading file to S3: {e}")


def run_youtube_etl():
    try:
        # Disable OAuthlib's HTTPS verification when running locally.
        # *DO NOT* leave this option enabled in production.
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

        # Get authenticated YouTube API service
        youtube = get_authenticated_service()

        # Specify the channelId (replace with your actual channelId)
        channel_id = "UC_x5XG1OV2P6uZZ5FSM9Ttw"

        # Fetch playlist data
        all_data = fetch_playlist_data(youtube, channel_id)

        # Save data to CSV file
        file_path = 'download/youtube_load.csv'
        save_to_csv(all_data, file_path)

        print("Finished processing all pages.")

        aws_access_key_id = "YOUR_ACCESS_KEY_ID"
        aws_secret_access_key = "YOUR_SECRET_ACCESS_KEY"
        s3_bucket_name = "dibya-test-bucket"
        s3_object_key = "inbound/youtube/youtube_load.csv"  # Replace with the desired object key

        boto3.setup_default_session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        upload_to_s3(file_path, s3_bucket_name, s3_object_key)

        print("Finished processing all pages and uploaded file to S3.")

    except Exception as error:
        print(f"Error: {error}")
