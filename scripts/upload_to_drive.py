#!/usr/bin/env python3
"""
Upload files to Google Drive using a service account.

Usage:
    python upload_to_drive.py <file_path> [<file_path2> ...]

Environment variables required:
    GOOGLE_DRIVE_CREDENTIALS - JSON string of service account credentials
    GOOGLE_DRIVE_FOLDER_ID - Target folder ID in Google Drive

The service account must have access to the target folder (share the folder
with the service account email address).
"""

import os
import sys
import json
import glob
from pathlib import Path

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
except ImportError:
    print("ERROR: Google API libraries not installed")
    print("Run: pip install google-api-python-client google-auth")
    sys.exit(1)


def get_drive_service():
    """Initialize Google Drive service using service account credentials."""
    creds_json = os.getenv("GOOGLE_DRIVE_CREDENTIALS")
    if not creds_json:
        print("ERROR: GOOGLE_DRIVE_CREDENTIALS not set")
        sys.exit(1)

    try:
        creds_dict = json.loads(creds_json)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in GOOGLE_DRIVE_CREDENTIALS: {e}")
        sys.exit(1)

    credentials = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=['https://www.googleapis.com/auth/drive.file']
    )

    return build('drive', 'v3', credentials=credentials)


def upload_file(service, file_path: str, folder_id: str) -> str:
    """Upload a file to the specified Google Drive folder."""
    file_path = Path(file_path)

    if not file_path.exists():
        print(f"WARNING: File not found: {file_path}")
        return None

    file_metadata = {
        'name': file_path.name,
        'parents': [folder_id]
    }

    # Determine MIME type
    mime_type = 'text/plain'
    if file_path.suffix == '.json':
        mime_type = 'application/json'
    elif file_path.suffix == '.csv':
        mime_type = 'text/csv'

    media = MediaFileUpload(str(file_path), mimetype=mime_type, resumable=True)

    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id, name, webViewLink'
    ).execute()

    return file


def main():
    if len(sys.argv) < 2:
        print("Usage: python upload_to_drive.py <file_path> [<file_path2> ...]")
        print("Supports glob patterns like 'reports/*.txt'")
        sys.exit(1)

    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not folder_id:
        print("ERROR: GOOGLE_DRIVE_FOLDER_ID not set")
        sys.exit(1)

    # Expand glob patterns
    file_paths = []
    for pattern in sys.argv[1:]:
        matches = glob.glob(pattern)
        if matches:
            file_paths.extend(matches)
        else:
            file_paths.append(pattern)  # Keep as-is if no glob match

    if not file_paths:
        print("No files to upload")
        sys.exit(0)

    print(f"Initializing Google Drive service...")
    service = get_drive_service()

    print(f"Uploading {len(file_paths)} file(s) to folder {folder_id}...")

    for file_path in file_paths:
        print(f"  Uploading: {file_path}")
        result = upload_file(service, file_path, folder_id)
        if result:
            print(f"    Uploaded: {result.get('name')} (ID: {result.get('id')})")
            if result.get('webViewLink'):
                print(f"    Link: {result.get('webViewLink')}")
        else:
            print(f"    Skipped (file not found)")

    print("Done!")


if __name__ == '__main__':
    main()
