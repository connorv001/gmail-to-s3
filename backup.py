import argparse
import base64
import email
import json
import sys

import boto3
import google.auth
import google.auth.transport.requests
import google.oauth2.credentials
import googleapiclient.discovery
import tqdm

from google_auth_oauthlib.flow import InstalledAppFlow

# Set the scopes for the Gmail API
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# Parse the command-line arguments
parser = argparse.ArgumentParser(description='Backup Gmail to S3')
parser.add_argument('--label-id', required=True, help='ID of the Gmail label to backup')
parser.add_argument('--bucket-name', required=True, help='Name of the S3 bucket')
parser.add_argument('--key-prefix', required=True, help='Prefix for the S3 key (folder)')
args = parser.parse_args()

# Load the client secrets file
info = json.load(open('client_secret.json'))

CLIENT_SECRETS_FILE = 'client_secret.json'

# Create a flow object to handle the authorization process
flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file=CLIENT_SECRETS_FILE, scopes=SCOPES)

# Run the flow to get the credentials object
creds = flow.run_console()

# Save the credentials to the creds.json file
with open('creds.json', 'w') as f:
    f.write(json.dumps(creds.to_json()))

# Set the label ID of the label you want to backup
label_id = args.label_id

# Set the name of the S3 bucket where you want to save the emails
bucket_name = args.bucket_name

# Set the prefix for the S3 key (folder) where you want to save the emails
key_prefix = args.key_prefix

# Set the filename for the S3 key
key_filename = 'emails.json'

# Create an S3 client
s3 = boto3

# Create a Gmail API service client
service = googleapiclient.discovery.build('gmail', 'v1', credentials=creds)

# Query the Gmail API for the list of emails in the specified label
result = service.users().messages().list(userId='me', labelIds=[label_id]).execute()
messages = [ ]
if 'messages' in result:
    messages.extend(result['messages'])

while 'nextPageToken' in result:
    page_token = result['nextPageToken']
    result = service.users().messages().list(userId='me', labelIds=[label_id], pageToken=page_token).execute()
    if 'messages' in result:
        messages.extend(result['messages'])

# Iterate through the list of emails and save each email to the S3 bucket
for message in tqdm.tqdm(messages, desc='Emails', file=sys.stdout):
    msg = service.users().messages().get(userId='me', id=message['id'], format='raw').execute()
    msg_str = base64.urlsafe_b64decode(msg['raw'].encode('UTF-8'))
    mime_msg = email.message_from_bytes(msg_str)

    # Save the email to the S3 bucket
    s3.put_object(Bucket=bucket_name, Key=f'{key_prefix}/{message["id"]}.eml', Body=msg_str)