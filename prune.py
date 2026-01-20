#!/usr/bin/env python3
import boto3
import json
import os
import sys
import argparse
import configparser  

# --- CONFIGURATION LOADER ---
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'glacier.cfg')
config = configparser.ConfigParser()

if not os.path.exists(config_path):
    print(f"Error: Configuration file not found at {config_path}")
    sys.exit(1)

config.read(config_path)

try:
    INVENTORY_FILE = config['settings']['inventory_file']
    S3_BUCKET = config['settings']['s3_bucket']
except KeyError as e:
    print(f"Error: Missing configuration key: {e}")
    sys.exit(1)
# ----------------------------


def get_active_keys(inventory_path):
    """Extracts all S3 keys currently referenced by the Brain."""
    with open(inventory_path, 'r') as f:
        data = json.load(f)
    
    active_keys = set()
    for source in data.get("molecular_sources", {}).values():
        for atom in source.get("atomic_units", {}).values():
            key = atom.get("archive_key")
            if key:
                active_keys.add(key)
    return active_keys

def get_s3_keys(bucket_name):
    """Lists every single file currently in the S3 bucket."""
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    s3_keys = set()
    
    print(f"Scanning S3 Bucket: {bucket_name}...")
    for page in paginator.paginate(Bucket=bucket_name):
        if 'Contents' in page:
            for obj in page['Contents']:
                # Filter out manifest files if you want to keep them, 
                # or strictly delete anything not in inventory.
                # For safety, let's ignore the 'manifests/' folder entirely.
                if "manifests/" not in obj['Key']:
                    s3_keys.add(obj['Key'])
    return s3_keys

def main():
    parser = argparse.ArgumentParser(description="Garbage Collect orphaned S3 bags.")
    parser.add_argument("--delete", action="store_true", help="Actually delete files (default is Dry Run)")
    args = parser.parse_args()

    if not os.path.exists(INVENTORY_FILE):
        print("Error: Inventory file not found.")
        return

    # 1. Load the Brain
    print("Loading Inventory...")
    active_keys = get_active_keys(INVENTORY_FILE)
    print(f" -> Found {len(active_keys)} active bags referenced in inventory.")

    # 2. Scan the Cloud
    s3_keys = get_s3_keys(S3_BUCKET)
    print(f" -> Found {len(s3_keys)} bags sitting in S3.")

    # 3. Find Orphans (S3 files NOT in Inventory)
    orphans = s3_keys - active_keys
    
    if not orphans:
        print("\nClean! No orphaned files found.")
        return

    print(f"\nFound {len(orphans)} orphaned files (Safe to delete):")
    print("-" * 60)
    for key in sorted(orphans):
        print(f" [ORPHAN] {key}")
    print("-" * 60)

    # 4. Execute or Warn
    if args.delete:
        confirm = input(f"\nAre you SURE you want to delete these {len(orphans)} files? (yes/no): ")
        if confirm.lower() == "yes":
            s3 = boto3.client('s3')
            # Batch delete (max 1000 per request)
            orphan_list = list(orphans)
            for i in range(0, len(orphan_list), 1000):
                batch = [{'Key': k} for k in orphan_list[i:i+1000]]
                s3.delete_objects(Bucket=S3_BUCKET, Delete={'Objects': batch})
                print(f"Deleted batch {i//1000 + 1}...")
            print("Cleanup Complete.")
        else:
            print("Aborted.")
    else:
        print("\nDRY RUN ONLY. Use --delete to actually remove files.")

if __name__ == "__main__":
    main()
