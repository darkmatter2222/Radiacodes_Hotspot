import os
import glob
import pandas as pd
import json
# Load environment variables and Google OAuth client libraries
try:
    from dotenv import load_dotenv
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
except ImportError:
    print("Please install required packages: pip install python-dotenv google-auth-oauthlib google-api-python-client")
    # The script will exit later if libs missing


def main():
    # Load Google OAuth credentials from .env
    load_dotenv()
    client_id = os.getenv('GOOGLE_CLIENT_ID')
    client_secret = os.getenv('GOOGLE_CLIENT_SECRET')
    if not client_id or not client_secret:
        print('Please set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET in .env')
        return
    # Google Drive OAuth via browser flow
    flow = InstalledAppFlow.from_client_config(
        {'installed':{
            'client_id':client_id,'client_secret':client_secret,
            'auth_uri':'https://accounts.google.com/o/oauth2/auth',
            'token_uri':'https://oauth2.googleapis.com/token',
            'redirect_uris':['urn:ietf:wg:oauth:2.0:oob','http://localhost']
        }}, scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    creds = flow.run_local_server(port=0)
    drive = build('drive','v3',credentials=creds)
    # Clear OAuth credentials
    del creds, flow
    
    # Directories
    base_dir = os.path.dirname(os.path.abspath(__file__))
    raw_dir = os.path.join(base_dir,'files_raw_input_from_radiacode')
    out_dir = os.path.join(base_dir,'files_master_collection')
    os.makedirs(raw_dir,exist_ok=True)
    os.makedirs(out_dir,exist_ok=True)
    # Clear raw input directory
    for f in glob.glob(os.path.join(raw_dir,'*')):
        os.remove(f)
    # Search for Radiacode track files in Google Drive
    query = "name contains 'RadiaCode'"
    resp = drive.files().list(q=query,fields='files(id,name)').execute()
    # Download all found files with sequential filenames
    download_count = 1
    for item in resp.get('files', []):
        fid = item['id']
        # Sequential file naming
        out_name = f"RadiaCode_Track_{download_count}.txt"
        out_path = os.path.join(raw_dir, out_name)
        request = drive.files().get_media(fileId=fid)
        with open(out_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
        print(f"Downloaded file {download_count}: {out_name}")
        download_count += 1
    print("Downloaded files to raw directory, proceeding to aggregation...")

    # Prepare containers
    all_dfs = []
    metadata = []

    # Process each raw text file
    pattern = os.path.join(raw_dir, '*')
    for fn in glob.glob(pattern):
        # Read header line with error handling
        try:
            with open(fn, 'r', encoding='utf-8', errors='replace') as f:
                header = f.readline().strip()
        except Exception as e:
            print(f"Skipping {os.path.basename(fn)}: header read failed ({e})")
            continue
        # Load data skipping header, with error handling
        try:
            df = pd.read_csv(
                fn,
                skiprows=1,
                sep='\t'
            )
        except Exception as e:
            print(f"Skipping {os.path.basename(fn)}: failed to read data ({e})")
            continue
        if 'Time' not in df.columns:
            print(f"Skipping {os.path.basename(fn)}: 'Time' column missing")
            continue
        # Parse Time column
        df['Time'] = pd.to_datetime(df['Time'], errors='coerce')
        # Drop rows without valid Time
        df = df.dropna(subset=['Time'])
        all_dfs.append(df)
        # Compute time range
        if not df.empty:
            # store raw 18-digit Timestamp values as strings
            start = str(int(df['Timestamp'].min()))
            end = str(int(df['Timestamp'].max()))
        else:
            start = end = None
        metadata.append({
            'file': os.path.basename(fn),
            'header': header,
            'start_time': start,
            'end_time': end
        })

    # Combine and save master CSV
    if all_dfs:
        master_df = pd.concat(all_dfs, ignore_index=True)
    else:
        master_df = pd.DataFrame()
    csv_path = os.path.join(out_dir, 'master_collection.csv')
    master_df.to_csv(csv_path, index=False)
    print(f"Master CSV written to {csv_path}")

    # Save metadata JSON
    json_path = os.path.join(out_dir, 'master_metadata.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2)
    print(f"Metadata JSON written to {json_path}")
    # High-level summary
    print("\nProcessing Summary:")
    print(f"- Number of files processed: {len(metadata)}")
    print(f"- Master CSV records: {master_df.shape[0]}")
    print(f"- Master CSV columns: {master_df.shape[1]}")
    print("- Columns:", list(master_df.columns))
    print("\nSample records:")
    print(master_df.head().to_string(index=False))


if __name__ == '__main__':
    main()