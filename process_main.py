import process_aggrogate
import process_scrub
import process_split


def main():
    print("=== Radiacode Data Pipeline ===")

    print("\n[1/3] Aggregating raw tracks into master CSV & metadata...")
    process_aggrogate.main()
    print("Aggregation complete.")

    print("\n[2/3] Scrubbing master CSV based on exclusion zones...")
    process_scrub.main()
    print("Scrubbing complete.")

    print("\n[3/3] Splitting scrubbed data into monthly and size-limited chunks...")
    process_split.main()
    print("Splitting complete. All steps done.")


if __name__ == '__main__':
    main()
