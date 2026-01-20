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

    # 1. Identify all "Live" bags currently in the inventory
    live_bags = set()
    for source in inventory.get('molecular_sources', {}).values():
        for atom in source.get('atomic_units', {}).values():
            if atom.get('archive_key'):
                live_bags.add(atom['archive_key'])

    # 2. List all bags physically present in S3
    current_year = datetime.now().strftime('%Y')
    prefix = f"{current_year}-backup/"
    
    print(f"Scanning S3 bucket '{S3_BUCKET}' under prefix '{prefix}'...")
    paginator = s3.get_paginator('list_objects_v2')
    all_s3_keys = []
    try:
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.tar'):
                    all_s3_keys.append(obj['Key'])
    except Exception as e:
        print(f"Error accessing S3: {e}")
        return

    # 3. Compare
    obsolete_keys = [k for k in all_s3_keys if k not in live_bags]

    if not obsolete_keys:
        print("No obsolete bags found. S3 is in sync with Inventory.")
        return

    print(f"Found {len(obsolete_keys)} obsolete bags on S3.")

    for key in obsolete_keys:
        age_days = get_s3_file_age(key)
        
        if args.check_age and age_days is not None and age_days < 180:
            print(f"    [GUARDED] {key} is only {age_days} days old. Skipping deletion.")
            continue
        
        if args.delete:
            print(f"    [DELETING] {key} ({age_days} days old)...")
            s3.delete_object(Bucket=S3_BUCKET, Key=key)
        else:
            print(f"    [DRY RUN] Would delete {key} ({age_days} days old)")

if __name__ == "__main__":
    main()
