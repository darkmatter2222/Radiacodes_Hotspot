import os
import glob
import shutil


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.join(base_dir, 'files_raw_input_from_radiacode')
    dest_dir = os.path.join(base_dir, 'files_raw_output_raddiacode')
    os.makedirs(dest_dir, exist_ok=True)

    pattern = os.path.join(src_dir, '*')
    files = glob.glob(pattern)
    count = 0

    for fn in files:
        if os.path.isfile(fn):
            name, _ = os.path.splitext(os.path.basename(fn))
            new_name = f"{name}.rctrk"
            dest_path = os.path.join(dest_dir, new_name)
            shutil.copy2(fn, dest_path)
            count += 1
            print(f"Copied {fn} to {dest_path}")

    print(f"Total files copied and renamed: {count}")

if __name__ == '__main__':
    main()
