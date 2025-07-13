import os
import glob
import json
import pandas as pd
import numpy as np

# haversine function

def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return 6371000 * c  # meters


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    master_csv = os.path.join(base_dir, 'files_master_collection', 'master_collection.csv')
    config_path = os.path.join(base_dir, 'config', 'scrub_areas.json')
    out_dir = os.path.join(base_dir, 'files_scrubbed_stage_one_master_collection')
    os.makedirs(out_dir, exist_ok=True)

    # Load master data
    df = pd.read_csv(master_csv, parse_dates=['Time'])
    initial_count = len(df)

    # Load exclusion zones
    with open(config_path, 'r') as f:
        cfg = json.load(f)
    exclusions = cfg.get('exclusions', [])

    # Apply scrubbing
    mask = pd.Series(False, index=df.index)
    for ex in exclusions:
        lat0 = ex['lat']
        lon0 = ex['lon']
        rad = ex['radius_m']
        dist = haversine(df['Longitude'], df['Latitude'], lon0, lat0)
        mask |= (dist <= rad)
    df_clean = df[~mask].copy()
    final_count = len(df_clean)

    # Save scrubbed CSV
    out_csv = os.path.join(out_dir, 'master_collection_scrubbed.csv')
    df_clean.to_csv(out_csv, index=False)

    # Summary
    print("Scrubbing complete:")
    print(f"- Initial records: {initial_count}")
    print(f"- Excluded records: {mask.sum()}")
    print(f"- Remaining records: {final_count}")
    print(f"Scrubbed CSV saved to {out_csv}")

if __name__ == '__main__':
    main()
