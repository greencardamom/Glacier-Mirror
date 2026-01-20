#!/usr/bin/env python3
import boto3
import json
import argparse
import os
import configparser
from datetime import datetime, timezone

# --- CONFIG ---
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'glacier.cfg')
config = configparser.ConfigParser()
config.read(config_path)

S3_BUCKET = config['settings']['s3_bucket']
DEFAULT_INVENTORY = config['settings']['inventory_file']
s3 = boto3.client('s3')

def get_s3_file_age(key):
    try:
        response = s3.head_object(Bucket=S3_BUCKET, Key=key)
        last_modified = response['LastModified']
        now = datetime.now(timezone.utc)
        return (now - last_modified).days
    except Exception:
        return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--delete", action="store_true", help="Actually delete obsolete bags")
    parser.add_argument("--check-age", action="store_true", help="Refuse to delete if younger than 180 days")
    parser.add_argument("--inventory", default=DEFAULT_INVENTORY, help="Path to inventory JSON file")
    args = parser.parse_args()

    if not os.path.exists(args.inventory):
        print(f"Inventory not found at: {args.inventory}")
        return

    with open(args.inventory, 'r') as f:
        inventory = json.load(f)

    # 1. THE WHITELIST: Every bag mentioned in the inventory stays.
    live_bags = set()
    for source in inventory.get('molecular_sources', {}).values():
        for atom in source.get('atomic_units', {}).values():
            if atom.get('archive_key'):
                live_bags.add(atom['archive_key'])

    # 2. THE GLOBAL SCAN: Look at every file in the bucket.
    print(f"Scanning entire S3 bucket '{S3_BUCKET}' for orphaned .tar bags...")
    paginator = s3.get_paginator('list_objects_v2')
    all_s3_keys = []
    
    try:
        # We scan the root (Prefix='') to find legacy bags from any year
        for page in paginator.paginate(Bucket=S3_BUCKET):
            for obj in page.get('Contents', []):
                key = obj['Key']
                # PROTECT SYSTEM ARTIFACTS: Only consider .tar files outside system/manifests
                if key.endswith('.tar'):
                    if "/system/" not in key and "/manifests/" not in key:
                        all_s3_keys.append(key)
    except Exception as e:
        print(f"Error accessing S3: {e}")
        return

    # 3. IDENTIFY OBSOLETE
    obsolete_keys = [k for k in all_s3_keys if k not in live_bags]

    if not obsolete_keys:
        print("Success: S3 is in sync with Inventory. No orphaned bags found.")
        return

    print(f"Found {len(obsolete_keys)} obsolete bags across all backup years.")

    # 4. PREPARE BATCH DELETION
    keys_to_delete = []
    for key in obsolete_keys:
        age_days = get_s3_file_age(key)
        
        # Protect against Early Deletion Fees (The 180-Day Rule)
        if args.check_age and age_days is not None and age_days < 180:
            print(f"    [GUARDED] {key} is {age_days}d old (<180). Skipping.")
            continue
        
        if args.delete:
            keys_to_delete.append({'Key': key})
            print(f"    [STAGING] {key} ({age_days}d old)")
        else:
            print(f"    [DRY RUN] Would delete {key} ({age_days}d old)")

    # 5. EXECUTE BULK DELETE
    if args.delete and keys_to_delete:
        print(f"\n--- EXECUTING BULK DELETE ({len(keys_to_delete)} files) ---")
        # AWS S3 delete_objects can take up to 1000 keys per request
        for i in range(0, len(keys_to_delete), 1000):
            batch = keys_to_delete[i:i + 1000]
            try:
                s3.delete_objects(Bucket=S3_BUCKET, Delete={'Objects': batch})
                print(f"    Deleted batch of {len(batch)} items.")
            except Exception as e:
                print(f"    [ERROR] Batch deletion failed: {e}")
        print("[OK] Cleanup finished.")

if __name__ == "__main__":
    main()
