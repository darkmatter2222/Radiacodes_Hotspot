import os
import glob
import pandas as pd
import json


def main():
    # Directories
    base_dir = os.path.dirname(os.path.abspath(__file__))
    raw_dir = os.path.join(base_dir, 'files_raw_input_from_radiacode')
    out_dir = os.path.join(base_dir, 'files_master_collection')
    os.makedirs(out_dir, exist_ok=True)

    # Prepare containers
    all_dfs = []
    metadata = []

    # Process each raw text file
    pattern = os.path.join(raw_dir, '*.txt')
    for fn in glob.glob(pattern):
        # Read header line
        with open(fn, 'r', encoding='utf-8') as f:
            header = f.readline().strip()
        # Load data skipping header
        df = pd.read_csv(
            fn,
            skiprows=1,
            sep='\t',
            parse_dates=['Time']
        )
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