#!/usr/bin/env python3
import sys
import os
import hashlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor
import threading

DB_NAME = "hash_index.db"
READ_SIZE = 4096
BATCH_SIZE = 1000


def sha1_first_4kb(path):
    hasher = hashlib.sha1()
    with open(path, "rb") as f:
        hasher.update(f.read(READ_SIZE))
    return hasher.hexdigest()


def validate_directory(path):
    if not os.path.exists(path):
        sys.exit(f"Error: Path does not exist: {path}")
    if not os.path.isdir(path):
        sys.exit(f"Error: Path is not a directory: {path}")
    if not os.access(path, os.R_OK):
        sys.exit(f"Error: Directory is not readable: {path}")


def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS files (
            hash TEXT,
            path TEXT
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hash ON files(hash)")
    conn.commit()
    return conn


def insert_batch(conn, batch):
    cursor = conn.cursor()
    cursor.executemany("INSERT INTO files (hash, path) VALUES (?, ?)", batch)
    conn.commit()


def process_files(folder):
    conn = init_db()
    batch = []
    lock = threading.Lock()

    def worker(path):
        try:
            file_hash = sha1_first_4kb(path)
            return (file_hash, path)
        except Exception:
            return None

    with ThreadPoolExecutor() as executor:
        futures = []

        for root, _, files in os.walk(folder):
            for name in files:
                full_path = os.path.join(root, name)
                if os.access(full_path, os.R_OK):
                    futures.append(executor.submit(worker, full_path))

        for future in futures:
            result = future.result()
            if result:
                batch.append(result)

            if len(batch) >= BATCH_SIZE:
                insert_batch(conn, batch)
                batch.clear()

        # Insert remaining
        if batch:
            insert_batch(conn, batch)

    conn.close()


def print_duplicates():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT hash FROM files
        GROUP BY hash
        HAVING COUNT(*) > 1
    """)

    duplicate_hashes = cursor.fetchall()

    if not duplicate_hashes:
        print("No duplicates found.")
        return

    for (file_hash,) in duplicate_hashes:
        print(f"\nHash: {file_hash}")
        cursor.execute("SELECT path FROM files WHERE hash = ?", (file_hash,))
        for (path,) in cursor.fetchall():
            print(f"  {path}")

    conn.close()


def main():
	''' For Linux systems, running python3 ./FILENAME.py FOLDERSTRING will generate two args
	    For Windows systems, VS2026 will not pass the file name as the first arg, so this should be changed
	'''
    if len(sys.argv) != 2:
        print("Usage: python3 script.py <folder_path>")
        sys.exit(1)

    folder_path = sys.argv[1]
    validate_directory(folder_path)

    print("Processing files (streaming to SQLite)...")
    process_files(folder_path)

    print("\nDuplicate groups:")
    print_duplicates()
	
    print("This only hashes the first 4kb of the file.  The full version of this program does the 4kb hash as a first pass, then on duplicates, does a full file hash to eliminate false positives.\n")
    print("If you want to see the full version, you will have to hire me.\n")
    print("PS:  I have a c++ version of this program that runs 20x faster.  Given the state of memory prices, this would prove fruitful.")
    print("\nDone.")


if __name__ == "__main__":
    main()
