import csv
import os
import pandas as pd
import math
import datetime

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    scrubbed_dir = os.path.join(base_dir, 'files_scrubbed_stage_one_master_collection')
    input_csv = os.path.join(scrubbed_dir, 'master_collection_scrubbed.csv')
    output_dir = os.path.join(base_dir, 'files_output_collection')
    os.makedirs(output_dir, exist_ok=True)

    # Read scrubbed CSV only
    print(f"Reading input from scrubbed CSV: {input_csv}")
    df = pd.read_csv(input_csv)
    # Ensure Time column is datetime and sort DataFrame chronologically
    df['Time'] = pd.to_datetime(df['Time'])
    df = df.sort_values('Time').reset_index(drop=True)
    # Add month column YYYY-MM for monthly grouping
    df['month'] = df['Time'].dt.strftime('%Y-%m')

    # Use epoch-time window for chunk splitting (default = 1 day) and size threshold
    time_window_seconds = 24 * 3600  # 1 day
    time_window_ns = time_window_seconds * 10**9
    chunk_size_bytes = 9 * 1024 * 1024  # ~9 MB target

    # Define columns to write and device ID for headers
    columns = df.columns.drop('month').tolist()
    device_id = 'RC-102-008228'

    for month in df['month'].sort_values().unique():
        month_df = df[df['month'] == month].reset_index(drop=True)
        print(f"Processing month {month} with {len(month_df)} records")
        csv_file = None
        rctrk_file = None
        csv_path = None
        rctrk_path = None
        chunk_idx = 1
        chunk_start_ts = None
        for idx, row in month_df.iterrows():
            # Close existing chunk if time or size threshold reached
            if csv_file is not None:
                size_reached = os.path.getsize(csv_path) >= chunk_size_bytes
                time_reached = (row['Timestamp'] - chunk_start_ts) >= time_window_ns
                if size_reached or time_reached:
                    csv_file.close()
                    rctrk_file.close()
                    reason = 'size' if size_reached else 'time'
                    print(f"Closed {month} chunk {chunk_idx} at row {idx} ({reason})")
                    chunk_idx += 1
                    csv_file = None
                    rctrk_file = None
                    csv_path = None
                    rctrk_path = None

            if csv_file is None:
                # Initialize new chunk and record its start timestamp
                csv_path = os.path.join(output_dir, f'master_{month}_part_{chunk_idx}.csv')
                rctrk_path = os.path.join(output_dir, f'master_{month}_part_{chunk_idx}.rctrk')
                csv_file = open(csv_path, 'w', newline='', encoding='utf-8')
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow(columns)
                ts = row['Time'].strftime('%Y-%m-%d %H-%M-%S')
                header_line = f"Track: {ts}\t{device_id}\t \tEC"
                rctrk_file = open(rctrk_path, 'w', encoding='utf-8')
                rctrk_file.write(header_line + '\n')
                rctrk_file.write('\t'.join(columns) + '\n')
                chunk_start_ts = row['Timestamp']
                print(f"Opened {month} chunk {chunk_idx} at {chunk_start_ts}")

            # Write row to both CSV and .rctrk
            vals = [row[col] for col in columns]
            csv_writer.writerow(vals)
            rctrk_file.write('\t'.join(str(v) for v in vals) + '\n')
            csv_file.flush()

        # Close any open file for this month
        if csv_file:
            csv_file.close()
            rctrk_file.close()
            print(f"Closed final {month} chunk {chunk_idx}")
    # Summary
    print("Splitting by month completed.")
    print(f'Input records: {len(df)}')
    print(f'Chunk time window: {time_window_seconds} seconds, size target: {chunk_size_bytes} bytes')

if __name__ == '__main__':
    main()
