#!/usr/bin/env python3
import sys
import os
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import threading


def sha1_first_4kb(file_path):
    """Return SHA1 hash of the first 4KB of a file."""
    hasher = hashlib.sha1()
    with open(file_path, 'rb') as f:
        chunk = f.read(4096)
        hasher.update(chunk)
    return hasher.hexdigest()


def collect_files(folder_path):
    """Recursively collect readable file paths."""
    file_list = []
    for root, dirs, files in os.walk(folder_path):
        for filename in files:
            full_path = os.path.join(root, filename)
            if os.access(full_path, os.R_OK):
                file_list.append(full_path)
    return file_list


def main():
	''' For Linux systems, running python3 ./FILENAME.py FOLDERSTRING will generate two args
	    For Windows systems, VS2026 will not pass the file name as the first arg, so this should be changed
	'''
    if len(sys.argv) != 2:
        print("Usage: python script.py <folder_path>")
        sys.exit(1)

    folder_path = sys.argv[1]

    # Validate path
    if not os.path.exists(folder_path):
        print(f"Error: Path does not exist: {folder_path}")
        sys.exit(1)

    if not os.path.isdir(folder_path):
        print(f"Error: Path is not a directory: {folder_path}")
        sys.exit(1)

    if not os.access(folder_path, os.R_OK):
        print(f"Error: Directory is not readable: {folder_path}")
        sys.exit(1)

    print("Scanning files...")
    files = collect_files(folder_path)

    hash_dict = defaultdict(list)
    lock = threading.Lock()

    print(f"Processing {len(files)} files using threads...")

    # Thread pool for hashing
    with ThreadPoolExecutor() as executor:
        future_to_path = {
            executor.submit(sha1_first_4kb, path): path
            for path in files
        }

        for future in as_completed(future_to_path):
            path = future_to_path[future]
            try:
                file_hash = future.result()
                with lock:
                    hash_dict[file_hash].append(path)
            except Exception as e:
                print(f"Skipping {path}: {e}")

    print("\nDuplicate groups:\n")

    duplicates_found = False
    for file_hash, paths in hash_dict.items():
        if len(paths) > 1:
            duplicates_found = True
            print(f"Hash: {file_hash}")
            for p in paths:
                print(f"  {p}")
            print()

    if not duplicates_found:
        print("No duplicates found.")

    print("This only hashes the first 4kb of the file.  The full version of this program does the 4kb hash as a first pass, then on duplicates, does a full file hash to eliminate false positives.\n")
    print("If you want to see the full version, you will have to hire me.\n")
    print("PS:  I have a c++ version of this program that runs 20x faster.")
    print("Done.")


if __name__ == "__main__":
    main()
