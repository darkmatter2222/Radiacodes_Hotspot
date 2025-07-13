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

    chunk_size_bytes = 9 * 1024 * 1024  # 9 MB target
    columns = df.columns.drop('month').tolist()
    device_id = 'RC-102-008228'

    for month in df['month'].sort_values().unique():
        month_df = df[df['month'] == month].reset_index(drop=True)
        print(f"Processing month {month} with {len(month_df)} records")
        csv_file = None
        rctrk_file = None
        chunk_idx = 1
        for idx, row in month_df.iterrows():
            if csv_file is None:
                csv_path = os.path.join(output_dir, f'master_{month}_part_{chunk_idx}.csv')
                rctrk_path = os.path.join(output_dir, f'master_{month}_part_{chunk_idx}.rctrk')
                csv_file = open(csv_path, 'w', newline='', encoding='utf-8')
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow(columns)
                # Radiacode header
                ts = row['Time'].strftime('%Y-%m-%d %H-%M-%S')
                header_line = f"Track: {ts}\t{device_id}\t \tEC"
                rctrk_file = open(rctrk_path, 'w', encoding='utf-8')
                rctrk_file.write(header_line + '\n')
                rctrk_file.write('\t'.join(columns) + '\n')
                print(f"Opened {month} chunk {chunk_idx}")
            # Write row
            vals = [row[col] for col in columns]
            csv_writer.writerow(vals)
            rctrk_file.write('\t'.join(str(v) for v in vals) + '\n')
            csv_file.flush()
            # Check if size threshold reached
            if os.path.getsize(csv_path) >= chunk_size_bytes:
                csv_file.close()
                rctrk_file.close()
                print(f"Closed {month} chunk {chunk_idx} at row {idx}")
                chunk_idx += 1
                csv_file = None
                rctrk_file = None
        # Close any open file for this month
        if csv_file:
            csv_file.close()
            rctrk_file.close()
            print(f"Closed final {month} chunk {chunk_idx}")
    # Summary
    print("Splitting by month completed.")
    print(f'Input records: {len(df)}')
    print(f'Chunk size target: {chunk_size_bytes} bytes')

if __name__ == '__main__':
    main()
