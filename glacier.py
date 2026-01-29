#!/usr/bin/env python3

#
# Copyright (c) 2026 Greencardamom
# 
# This file is part of Glacier Mirror.
# https://github.com/greencardamom/Glacier
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# See LICENSE.md for more details.
#

import os
import sys

# Critical dependencies check first
try:
    import boto3
    from tqdm import tqdm
except ImportError:
    print("Error: Missing dependencies.")
    print("Please install required packages: pip install -r requirements.txt")
    print("Or ensure your virtual environment is activated.")
    sys.exit(1)

# System Metadata constants
VERSION = "1.0"
SYSTEM_NAME = "Glacier Mirror"
SYSTEM_DESCRIPTION = "Amazon S3 Glacier Deep Archive Tape Backup Management"
DEFAULT_TREE_FILE = "tree.cfg"  # <--- Chang to rename the file globally

# Standard library imports
import re
import glob
import time
import threading
import json
import hashlib
import logging
import tarfile
import subprocess
import shutil
import socket
import argparse
import configparser
import io
import shlex
from contextlib import redirect_stdout
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from boto3.s3.transfer import TransferConfig # Added for throttling

# Configuration handling
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'glacier.cfg')
config = configparser.ConfigParser()

if not os.path.exists(config_path):
    print(f"Error: Configuration file not found at {config_path}")
    sys.exit(1)

config.read(config_path)

# AWS metadata function (kept in same position as required)
def ensure_aws_metadata(config):
    """
    Respects 'REDACTED' for privacy or fetches metadata dynamically.
    """
    # 1. Check existing config values
    stored_id = config.get('AWS', 'aws_account_id', fallback=None)
    stored_region = config.get('AWS', 'aws_region', fallback=None)

    # 2. Privacy Logic: If either is already REDACTED, we stop here
    # This allows a user to redact one, both, or neither.
    final_id = stored_id if stored_id == "REDACTED" else None
    final_region = stored_region if stored_region == "REDACTED" else None

    # 3. Discovery Logic: Only fetch what isn't already set (or redacted)
    if not final_id or not final_region:
        try:
            import boto3
            session = boto3.Session()
            
            # Fetch only if needed
            if not final_id:
                sts = session.client('sts')
                final_id = sts.get_caller_identity()['Account']
            
            if not final_region:
                final_region = session.region_name or "us-east-1"

            # 4. Save to config if it was a new discovery
            if 'AWS' not in config: config.add_section('AWS')
            config.set('AWS', 'aws_account_id', final_id)
            config.set('AWS', 'aws_region', final_region)

            with open(config_path, 'w') as f:
                config.write(f)

        except Exception:
            # Absolute fallback if offline/no credentials
            final_id = final_id or "UNKNOWN"
            final_region = final_region or "unknown-region"

    return final_id, final_region

# Load AWS metadata
AWS_ACCOUNT_ID, AWS_REGION = ensure_aws_metadata(config)

# Load configuration settings
try:
    STAGING_DIR = config['settings']['staging_dir']
    MANIFEST_DIR = config['settings']['manifest_dir']
    INVENTORY_FILE = config['settings']['inventory_file']
    MNT_BASE = config['settings']['mnt_base']
    S3_BUCKET = config['settings']['s3_bucket']
    TARGET_BAG_GB = int(config['settings']['target_bag_gb'])
    INVENTORY_BAK_DIR = config['settings'].get('inventory_bak_dir', '').strip()
    if not INVENTORY_BAK_DIR:
        INVENTORY_BAK_DIR = None
except KeyError as e:
    print(f"Error: Missing configuration key: {e}")
    sys.exit(1)

# Global constants
CURRENT_YEAR = datetime.now().strftime('%Y')
S3_PREFIX = f"{CURRENT_YEAR}-backup/"
BYTES_PER_GB = 1024 * 1024 * 1024
TARGET_SIZE_BYTES = TARGET_BAG_GB * BYTES_PER_GB
s3_client = boto3.client('s3')

# UI Formatting Constants
STATUS_WIDTH = 13  # Length of "[NET: RSYNC]" is 12. We add 1 for spacing = 13.

# --- HELPER FUNCTIONS ---

class Heartbeat(threading.Thread):
    def __init__(self, filepath=None, target_size=0, status="TAR", is_compressed=False):
        super().__init__()
        self.daemon = True
        self.filepath = filepath
        self.target_size = target_size
        self.status = status.replace(": LEAF", "").replace(":LEAF", "")
        self.is_compressed = is_compressed
        self.stop_signal = threading.Event()
        self.paused = False
        self.last_size, self.last_time = 0, time.time()
        self.speed, self.start_time = 0, time.time()
        self.verb_active = "processing"
        self.verb_past = "processed"
        self._derive_verbs()

    def _derive_verbs(self):
        s = self.status
        if "RSYNC" in s: self.verb_active, self.verb_past = "transferring", "transferred"
        elif "GPG" in s: self.verb_active, self.verb_past = "encrypting", "encrypted"
        elif "TAR" in s: self.verb_active, self.verb_past = ("compressing", "compressed") if self.is_compressed else ("packaging", "packaged")
        elif "BAG" in s: self.verb_active, self.verb_past = "packaging", "packaged"
        elif "NET" in s: self.verb_active, self.verb_past = "uploading", "uploaded"
        else: self.verb_active, self.verb_past = "processing", "processed"

    def run(self):
        while not self.stop_signal.is_set():
            if not self.paused and self.filepath and os.path.exists(self.filepath):
                try:
                    current = os.path.getsize(self.filepath)
                    now = time.time()
                    if (now - self.last_time) > 0.5:
                        self.speed = (current - self.last_size) / (now - self.last_time)
                        self.last_size, self.last_time = current, now

                    # Column 15 margin
                    tag = self.status
                    indent_size = 5 if (tag.startswith("BAG") or tag.startswith("NET") or "DB" in tag or "OK" in tag) else 7
                    header = f"{' ' * indent_size}[{tag}]"
                    full_header = f"{header:<15}:"

                    done_str = format_bytes(current)
                    speed_str = format_bytes(self.speed) + "/s"
                    
                    if (tag == "TAR" and self.is_compressed) or "RSYNC" in tag:
                        line = f"{full_header} {done_str} {self.verb_active} @ {speed_str}"
                    else:
                        pct = min(99.9, (current / self.target_size) * 100) if self.target_size > 0 else 0
                        line = f"{full_header} {pct:5.1f}% {self.verb_active} [{done_str}/{format_bytes(self.target_size)}] @ {speed_str}"

                    sys.stdout.write(f"\r{line:<120}")
                    sys.stdout.flush()
                except: pass
            time.sleep(0.1)

    def update_target(self, new_filepath, new_status=None, is_compressed=None, target_size=None):
        self.paused = True
        self.filepath = new_filepath
        if is_compressed is not None: self.is_compressed = is_compressed
        if target_size: self.target_size = target_size
        if new_status: 
            self.start_time = time.time() # This fixes the [BAG] timing issue
            self.status = new_status.replace(": LEAF", "").replace(":LEAF", "")
            self._derive_verbs()
        self.paused = False

    def snap_done(self):
        self.paused = True
        elapsed = time.time() - self.start_time
        elapsed_str = f"{int(elapsed // 60):02d}:{int(elapsed % 60):02d}"
        
        tag = self.status
        indent_size = 5 if (tag.startswith("BAG") or tag.startswith("NET") or "DB" in tag or "OK" in tag) else 7
        header = f"{' ' * indent_size}[{tag}]"
        full_header = f"{header:<15}:"
        
        # --- RESTORED WORKING FORMATS ---
        try:
            current_size = os.path.getsize(self.filepath)
            shrinkage = (1 - (current_size / self.target_size)) * 100 if self.target_size > 0 else 0
            shrink_val = int(round(shrinkage))
        except:
            shrink_val = 0

        if tag == "TAR":
            if self.is_compressed and shrink_val != 0:
                final_msg = f"[DONE | {elapsed_str}] - compressed -{abs(shrink_val)}%"
            else:
                final_msg = f"[DONE | {elapsed_str}] - packaged"
        
        elif tag == "GPG":
            # If shrinkage is 0, just show 'encrypted' per your working sample
            stats = f" - encrypted -{shrink_val}%" if shrink_val != 0 else " - encrypted"
            final_msg = f"[DONE | {elapsed_str}]{stats}"

        elif "RSYNC" in tag:
            size_str = format_bytes(os.path.getsize(self.filepath)) if os.path.exists(self.filepath) else "???"
            final_msg = f"[DONE | {elapsed_str}] - {size_str} transferred"
            
        else:
            final_msg = f"100.0% {self.verb_past} [DONE | {elapsed_str}]"

        sys.stdout.write(f"\r{full_header} {final_msg}{' ':40}\n")
        sys.stdout.flush()

    def stop(self):
        self.stop_signal.set()
        if self.is_alive(): self.join(timeout=1)

def format_bytes(size):
    """Converts raw bytes to human readable format."""
    power = 2**10
    n = size
    power_labels = {0 : '', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    loop = 0
    while n > power:
        n /= power
        loop += 1
    return f"{n:.2f} {power_labels[loop]}"

def get_metadata_hash(directory, recursive=True, file_list=None):
    """Generates metadata hash with progress feedback and optimized excludes."""
    exclude_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'exclude.txt')
    excludes = []
    if os.path.exists(exclude_file):
        with open(exclude_file, 'r') as f:
            excludes = [line.strip().strip('/') for line in f if line.strip() and not line.startswith('#')]

    hasher = hashlib.md5()
    total_size = 0
    file_count = 0
    
    pbar = tqdm(desc="  Scanning metadata", unit=" files", leave=False)

    if file_list:
        for name in sorted(file_list):
            if any(exc in name for exc in excludes): continue
            path = os.path.join(directory, name)
            try:
                stat = os.stat(path)
                meta_str = f"{name}|{stat.st_size}|{stat.st_mtime}"
                hasher.update(meta_str.encode('utf-8'))
                total_size += stat.st_size
                file_count += 1
                pbar.update(1)
            except OSError: continue
    else:
        for root, dirs, files in os.walk(directory):
            if excludes:
                dirs[:] = [d for d in dirs if not any(exc in os.path.join(root, d) for exc in excludes)]
            
            if not recursive:
                dirs[:] = []
            
            dirs.sort()
            for name in sorted(files):
                full_path = os.path.join(root, name)
                if any(exc in full_path for exc in excludes):
                    continue
                    
                try:
                    stat = os.stat(full_path)
                    rel_path = os.path.relpath(full_path, directory)
                    meta_str = f"{rel_path}|{stat.st_size}|{stat.st_mtime}"
                    hasher.update(meta_str.encode('utf-8'))
                    total_size += stat.st_size
                    file_count += 1
                    pbar.update(1)
                except OSError: continue
                
    pbar.close()
    return hasher.hexdigest(), total_size

def generate_real_manifest(bag_name, leaf_definitions, is_live):
    """Generates manifest with visual progress to monitor SSHFS performance."""
    timestamp = datetime.now().strftime('%Y%m%d')
    base_name = bag_name.replace(".tar", "")
    suffix = "_liverun.txt" if is_live else "_dryrun.txt"
    txt_name = f"{timestamp}_{base_name}{suffix}"
    
    manifest_path = os.path.join(MANIFEST_DIR, txt_name)
    s3_key = os.path.join(S3_PREFIX, "manifests", txt_name)
    
    exclude_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'exclude.txt')
    excludes = [line.strip().strip('/') for line in open(exclude_file) if line.strip() and not line.startswith('#')] if os.path.exists(exclude_file) else []

    try:
        if not os.path.exists(MANIFEST_DIR): os.makedirs(MANIFEST_DIR)
        
        pbar = tqdm(desc=f"  Building Manifest", unit=" files", leave=False)
        
        with open(manifest_path, "w") as f:
            f.write(f"# Manifest for {bag_name}\n")
            for leaf in leaf_definitions:
                path = leaf['path']
                if leaf.get('is_branch_root', False):
                    for filename in leaf.get('files', []):
                        f.write(f"{os.path.join(path, filename)}\n")
                        pbar.update(1)
                else:
                    for root, dirs, files in os.walk(path):
                        if excludes:
                            dirs[:] = [d for d in dirs if not any(exc in os.path.join(root, d) for exc in excludes)]
                        
                        for name in files:
                            full_path = os.path.join(root, name)
                            if not any(exc in full_path for exc in excludes):
                                f.write(f"{full_path}\n")
                                pbar.update(1)
        pbar.close()
                            
        if is_live:
             s3_client.upload_file(manifest_path, S3_BUCKET, s3_key)
             log_aws_transaction("MANIFEST_UPLOAD", s3_key, os.path.getsize(manifest_path), "N/A", {}, "MNF-SYS")
    except Exception as e:
        print(f"\n[WARN] Manifest failed: {e}")

def upload_system_artifacts():
    """Backs up the code, config, and brain to S3. Enforces a strict mirror (deletes old files)."""
    print("\n--- System Artifact Backup ---")
    
    # 1. Define the Canonical List of system files to backup
    sys_files = ["glacier.py", "prune.py", "glacier.cfg", DEFAULT_TREE_FILE, "inventory.json", "README.md", "requirements.txt", "exclude.txt"]
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    s3_folder = os.path.join(S3_PREFIX, "system")
    
    # 2. THE SWEEP: Remove files on S3 that are no longer in our list
    try:
        # List what is currently on S3
        current_s3_objs = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_folder)
        
        if 'Contents' in current_s3_objs:
            for obj in current_s3_objs['Contents']:
                s3_key = obj['Key']
                filename = os.path.basename(s3_key)
                
                # If the file on S3 is NOT in our authorized list, kill it.
                if filename not in sys_files:
                    print(f"  [CLEANUP] Deleting obsolete file: {filename}")
                    s3_client.delete_object(Bucket=S3_BUCKET, Key=s3_key)
                    # We log this as a system cleanup
                    log_aws_transaction("SYSTEM_PRUNE", s3_key, 0, "N/A", {}, "SYS-CLN")
                    
    except Exception as e:
        print(f"  [WARN] Failed to sweep system folder: {e}")

    # 3. THE UPLOAD: Push the authorized files
    for fname in sys_files:
        local_path = os.path.join(base_dir, fname)
        if os.path.exists(local_path): 
            s3_key = os.path.join(s3_folder, fname)
            print(f"  [UPLOADING] {fname}...")
            try:
                s3_client.upload_file(local_path, S3_BUCKET, s3_key)
                log_aws_transaction("SYSTEM_BACKUP", s3_key, os.path.getsize(local_path), "N/A", {}, "SYS-BAK")
            except Exception as e:
                print(f"  [WARN] Failed to upload {fname}: {e}")

def mount_remote_branch(branch_string):
    if ":" not in branch_string:
        return os.path.abspath(branch_string), None
        
    remote_conn, remote_path = branch_string.split(":", 1)
    host_slug = remote_conn.split("@")[-1]
    base_slug = os.path.basename(os.path.normpath(remote_path)).replace(" ", "_")
    mount_point = os.path.join(MNT_BASE, f"{host_slug}_{base_slug}")

    if not os.path.exists(mount_point): os.makedirs(mount_point)
    
    res = subprocess.run(["mountpoint", "-q", mount_point])
    if res.returncode != 0:
        print(f"--> Bridge: Connecting {branch_string}...")
        
        # Strategy 1: Attempt High-Performance Mount
        cmd_hp = ["sshfs", "-o", "reconnect,cache=yes,kernel_cache", branch_string, mount_point]
        
        # Strategy 2: Fallback to Universal Mount (Works everywhere)
        cmd_univ = ["sshfs", "-o", "reconnect", branch_string, mount_point]
        
        try:
            # Try HP first
            subprocess.run(cmd_hp, check=True, capture_output=True)
        except subprocess.CalledProcessError:
            try:
                # Fallback to Universal
                print("  [NOTE] High-performance flags not supported. Falling back to standard mount.")
                subprocess.run(cmd_univ, check=True)
            except subprocess.CalledProcessError:
                print(f"[FATAL] Could not mount {branch_string}. Check SSH keys and paths."); sys.exit(1)
            
    return mount_point, mount_point

def unmount_remote_branch(mount_point):
    if not mount_point: return
    res = subprocess.run(["mountpoint", "-q", mount_point])
    if res.returncode == 0:
        print(f"--> Bridge: Disconnecting {mount_point}...")
        subprocess.run(["fusermount", "-u", mount_point], check=True)

def generate_summary(inventory, run_stats, is_live):
    print("\n" + "="*105)
    print(f"{'INVENTORY STATE':<45} {'BAGS':<8} {'SIZE':<10} {'WASTE %':<10} {'REPACK RISK':<12}")
    print("-" * 105)
    
    total_leaves = 0
    total_bags_global = 0
    total_size = 0
    total_risk = 0
    
    price_gb = float(config['pricing'].get('price_per_gb_month', 0.00099))
    min_days = int(config['pricing'].get('min_retention_days', 180))

    for branch, data in inventory.get("branches", {}).items():
        leaves = data.get("leaves", {})
        branch_size = sum(l.get("size_bytes", 0) for l in leaves.values())
        unique_bags = set(l.get("tar_id") for l in leaves.values() if l.get("tar_id"))
        num_bags = len(unique_bags)
    
        total_cap = num_bags * TARGET_SIZE_BYTES        
        waste_p = max(0.1, ((total_cap - branch_size) / total_cap * 100)) if total_cap > 0 else 0
            
        # Risk estimate: Full storage cost for the retention period
        repack_risk = (branch_size / BYTES_PER_GB) * price_gb * (min_days / 30)
        
        display_name = branch.split(' ::')[0]
        if len(display_name) > 43: display_name = "..." + display_name[-40:]
        print(f"{display_name:<45} {num_bags:<8} {format_bytes(branch_size):<10} {waste_p:>7.1f}% {f'${repack_risk:.2f}':>12}")
        
        total_bags_global += num_bags
        total_size += branch_size
        total_risk += repack_risk

    print("-" * 105)
    print(f"{'TOTALS':<45} {total_bags_global:<8} {format_bytes(total_size):<10} {'---':<10} {f'${total_risk:.2f}':>12}")
    print("="*105)

    # --- TIME ESTIMATION ---
    # total size of leaves that actually NEED upload
    tot_up_sz = sum(s['up_bytes'] for s in run_stats.values())
    
    # Safely get limit from sys.argv
    limit_mb = 0
    if "--limit" in sys.argv:
        try:
            idx = sys.argv.index("--limit")
            limit_mb = int(sys.argv[idx + 1])
        except (ValueError, IndexError):
            limit_mb = 0

    if limit_mb > 0 and not is_live:
        total_seconds = tot_up_sz / (limit_mb * 1024 * 1024)
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        days = hours // 24
        remaining_hours = hours % 24
        
        print(f"ESTIMATED UPLOAD TIME: {days}d {remaining_hours}h {minutes}m (to transfer {format_bytes(tot_up_sz)})")
    
    if not is_live:
        print(f"\n[DRY RUN] Repack Risk shows the maximum early deletion fee if all data is < {min_days} days old.")

# --- PROCESSING ---

def construct_tar_command(tar_path, leaf_list, branch_root, designator, passphrase_file, branch_leaves, remote_conn, remote_base_path, excludes=None, encryption_config=None, hb=None):
    """
    Constructs a tar command to archive leaves, handling encryption and compression as specified.
    
    Returns:
        tuple: (command string, list of temporary files to clean up)
    """
    # 1. Build exclude arguments
    exclude_args = build_exclude_arguments(excludes)
    
    # 2. Process each leaf and build tar sequence
    temp_files_to_clean, tar_sequence = process_leaves_for_tar(
        leaf_list, 
        branch_root, 
        designator, 
        passphrase_file, 
        branch_leaves, 
        remote_conn, 
        remote_base_path, 
        encryption_config,
        hb=hb
    )
    
    # 3. Optimize tar arguments for efficiency
    optimized_args = optimize_tar_arguments(tar_sequence)
    
    # 4. Generate the final command
    cmd = generate_final_tar_command(tar_path, exclude_args, optimized_args)
    
    return cmd, temp_files_to_clean


def build_exclude_arguments(excludes=None):
    """
    Builds the exclude arguments for the tar command.
    
    Args:
        excludes: List of patterns to exclude
        
    Returns:
        str: Formatted exclude flags for tar command
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    exclude_path = os.path.join(base_dir, "exclude.txt")
    exclude_args = []
    
    # Add standard exclude file if it exists
    if os.path.exists(exclude_path):
        exclude_args.append(f"--exclude-from={shlex.quote(exclude_path)}")
    
    # Add custom excludes if provided
    if excludes:
        for ex in excludes:
            exclude_args.append(f"--exclude {shlex.quote(ex)}")
    
    # Join all exclude arguments with spaces
    return " ".join(exclude_args)


def process_leaves_for_tar(leaf_list, branch_root, designator, passphrase_file, branch_leaves, 
                          remote_conn, remote_base_path, encryption_config, hb=None):
    """
    Process each leaf to determine how it should be included in the tar archive.
    
    Returns:
        tuple: (list of temp files to clean up, list of tar sequence entries)
    """
    temp_files_to_clean = []
    tar_sequence = [] 
    
    # State tracking for display styles
    mutable_header_printed = False
    pad_width = STATUS_WIDTH + 2  # From global constants

    total_leaves = len(leaf_list)
    for idx, leaf in enumerate(leaf_list, 1):
        leaf_path = leaf['path']
        rel_path = os.path.relpath(leaf_path, branch_root)
    
        needs_encrypt = check_encryption_needed(designator)    
        needs_compress = check_compression_needed(designator)
        
        # Display logic based on leaf processing type
        display_leaf_header(
            idx, total_leaves, leaf_path, needs_encrypt, needs_compress, 
            mutable_header_printed, pad_width
        )
        
        # Update the header printed flag if we're using mutable list style
        if not (needs_encrypt or needs_compress):
            mutable_header_printed = True
            
        # Process the leaf based on its requirements
        if needs_encrypt:
            process_encrypted_leaf(
                leaf, branch_root, rel_path, passphrase_file, needs_compress,
                remote_conn, remote_base_path, branch_leaves, tar_sequence, 
                temp_files_to_clean, encryption_config, hb=hb
            )
        elif needs_compress:
            process_compressed_leaf(
                leaf, branch_root, rel_path, remote_conn, remote_base_path,
                branch_leaves, tar_sequence, temp_files_to_clean, hb=hb
            )
        else:
            # Simple passthrough for standard leaves
            process_standard_leaf(
                leaf, branch_root, branch_leaves, tar_sequence
            )
    
    return temp_files_to_clean, tar_sequence


def display_leaf_header(idx, total_leaves, leaf_path, needs_encrypt, needs_compress, 
                       mutable_header_printed, pad_width):
    """Display appropriate header for the leaf being processed."""
    if needs_encrypt or needs_compress:                    
        # STYLE: Active Ledger (The "Work" Style) - Each leaf gets its own header
        # Use 5 spaces for indentation
        label = f"     Leaf {idx}/{total_leaves}"
        print(f"\n{label:<{pad_width}}: {leaf_path}")
    elif not mutable_header_printed:
        # STYLE: Mutable List (The "Fast" Style) - One header for all standard leaves
        # Use 5 spaces for indentation
        label = "     Leaves"
        print(f"\n{label:<{pad_width}}:")
        # The actual leaf will be printed by the caller after setting mutable_header_printed to True
    
    # For subsequent leaves in mutable list style, print indented list item
    if not (needs_encrypt or needs_compress) and mutable_header_printed:
        # Use 6 spaces for further indentation
        print(f"      [{idx:02d}/{total_leaves}] {leaf_path}")

def process_encrypted_leaf(leaf, branch_root, rel_path, passphrase_file, needs_compress,
                          remote_conn, remote_base_path, branch_leaves, tar_sequence, 
                          temp_files_to_clean, encryption_config, hb=None):
    """Process a leaf that needs encryption."""
    suffix = ".gz.gpg" if needs_compress else ".gpg"
    inner_name = "__BRANCH_ROOT__" + suffix if leaf['is_branch_root'] else f"{rel_path}{suffix}"
    
    cluster_files = leaf['files'] if leaf['is_branch_root'] else None

    temp_file = encrypt_leaf(
        leaf['path'], 
        files=cluster_files, 
        passphrase_file=passphrase_file, 
        compress=needs_compress, 
        remote_conn=remote_conn, 
        remote_base_path=remote_base_path,
        local_branch_root=branch_root,
        expected_size=leaf['size'],
        encryption_config=encryption_config,
        hb=hb
    )
    
    if temp_file:
        temp_files_to_clean.append(temp_file)
        base_tmp = os.path.basename(temp_file)
        transform_expr = f"s#{base_tmp}#{inner_name}#"
        arg = f"--transform={shlex.quote(transform_expr)} {shlex.quote(base_tmp)}"
        tar_sequence.append((STAGING_DIR, arg))
        branch_leaves[leaf['key']]['encrypted'] = True
        branch_leaves[leaf['key']]['compressed'] = needs_compress


def process_compressed_leaf(leaf, branch_root, rel_path, remote_conn, remote_base_path,
                           branch_leaves, tar_sequence, temp_files_to_clean, hb=None):
    """Process a leaf that needs compression but not encryption."""
    inner_name = "__BRANCH_ROOT__.tar.gz" if leaf['is_branch_root'] else f"{rel_path}.tar.gz"
    cluster_files = leaf['files'] if leaf['is_branch_root'] else None
    
    temp_file = compress_leaf(
        leaf['path'], 
        files=cluster_files, 
        remote_conn=remote_conn, 
        remote_base_path=remote_base_path, 
        local_branch_root=branch_root, 
        expected_size=leaf['size'],
        hb=hb
    )            
    
    if temp_file:
        temp_files_to_clean.append(temp_file)
        base_tmp = os.path.basename(temp_file)
        transform_expr = f"s#{base_tmp}#{inner_name}#"
        arg = f"--transform={shlex.quote(transform_expr)} {shlex.quote(base_tmp)}"
        tar_sequence.append((STAGING_DIR, arg))
        branch_leaves[leaf['key']]['compressed'] = True
        branch_leaves[leaf['key']]['encrypted'] = False


def process_standard_leaf(leaf, branch_root, branch_leaves, tar_sequence):
    """Process a leaf that needs neither encryption nor compression."""
    branch_leaves[leaf['key']]['encrypted'] = False
    branch_leaves[leaf['key']]['compressed'] = False
    
    if leaf['is_branch_root']:
        for f in leaf['files']: 
            tar_sequence.append((branch_root, shlex.quote(f)))
    else:
        rel_path = os.path.relpath(leaf['path'], branch_root)
        tar_sequence.append((branch_root, shlex.quote(rel_path)))


def optimize_tar_arguments(tar_sequence):
    """
    Optimize tar arguments by grouping by context (directory).
    
    Returns:
        list: Optimized argument list for tar command
    """
    optimized_args = []
    current_context = None
    
    for context, arg in tar_sequence:
        if context != current_context:
            optimized_args.append(f"-C {shlex.quote(context)}")
            current_context = context
        optimized_args.append(arg)
    
    return optimized_args


def generate_final_tar_command(tar_path, exclude_flag, optimized_args):
    """
    Generate the final tar command string.
    
    Returns:
        str: Complete tar command
    """
    # Check if GNU tar is available for sparse file support
    is_gnu = "GNU" in subprocess.getoutput("tar --version")
    sparse_flag = "-S" if is_gnu else ""

    cmd = f"tar {sparse_flag} {exclude_flag} -cf {shlex.quote(tar_path)} {' '.join(optimized_args)}"    
    return cmd

def process_bag(bag_num, leaf_list, branch_root, short_name, bag_size_bytes, is_live, branch_leaves, hostname, branch_stats, upload_limit_mb, designator, passphrase_file, remote_conn, remote_base_path, inventory, excludes=None, encryption_config=None):
    """
    Process a bag containing multiple leaves for backup.
    
    Args:
        bag_num: Bag number identifier
        leaf_list: List of leaves to include in the bag
        branch_root: Root directory of the branch
        short_name: Short name for the branch
        bag_size_bytes: Total size of the bag in bytes
        is_live: If True, perform actual operations; if False, dry run
        branch_leaves: Dictionary of branch leaves from inventory
        hostname: Host machine name
        branch_stats: Stats tracking dictionary for the branch
        upload_limit_mb: Upload bandwidth limit in MB/s (0 for unlimited)
        designator: Tags for the branch (e.g., "MUTABLE ENCRYPT")
        passphrase_file: Path to encryption passphrase file
        remote_conn: Remote connection string (if remote)
        remote_base_path: Base path on remote system
        inventory: Global inventory dictionary
        excludes: List of patterns to exclude
        encryption_config: Encryption configuration settings
    """
    # 1. Prepare bag information
    bag_info = prepare_bag_info(bag_num, short_name, hostname)
    tar_path, s3_key = bag_info["tar_path"], bag_info["s3_key"]
    
    # 2. Print bag header
    print_bag_header(bag_num, bag_size_bytes)
    
    # 3. Generate manifest
    generate_bag_manifest(bag_info["tar_name"], leaf_list, branch_root, is_live)
    
    # 4. Check if upload is needed
    if not should_upload_bag(leaf_list, branch_leaves):
        update_skip_stats(branch_stats, bag_size_bytes)
        print_skip_message(bag_info["tar_name"])
        return

    # 5. Update upload stats
    update_upload_stats(branch_stats, bag_size_bytes)
    
    # 6. Update inventory with expected S3 location
    update_inventory_locations(leaf_list, branch_leaves, s3_key)

    # 7. In dry run mode, print message and return
    if not is_live:
        print_dry_run_message(bag_info["tar_name"], s3_key, S3_BUCKET)
        return

    # 8. Build and upload the bag
    etag = build_and_upload_bag(
        tar_path, leaf_list, branch_root, designator, passphrase_file,
        branch_leaves, remote_conn, remote_base_path, excludes,
        s3_key, S3_BUCKET, bag_size_bytes, upload_limit_mb, 
        encryption_config
    )
    
    # 9. Update inventory after successful upload
    
    commit_to_inventory(leaf_list, branch_leaves, inventory, is_live, bag_num, etag)


def prepare_bag_info(bag_num, short_name, hostname):
    """
    Prepare bag filename and paths.
    
    Returns:
        dict: Bag information including tar_name, tar_path, s3_key
    """
    safe_prefix = short_name.replace(" ", "_")
    tar_name = f"{hostname}_{safe_prefix}_bag_{bag_num:05d}.tar"
    tar_path = os.path.join(STAGING_DIR, tar_name)
    s3_key = os.path.join(S3_PREFIX, tar_name)
    
    return {
        "tar_name": tar_name,
        "tar_path": tar_path,
        "s3_key": s3_key
    }


def print_bag_header(bag_num, bag_size_bytes):
    """Print the header for bag processing."""
    print(f"\n--- Leaf Bag {bag_num:05d} [{format_bytes(bag_size_bytes)}] ---")


def generate_bag_manifest(tar_name, leaf_list, branch_root, is_live):
    """Generate a manifest file for the bag contents."""
    manifest_leaves = []
    for leaf in leaf_list:
        if leaf['is_branch_root']:
            manifest_leaves.append({'path': branch_root, 'is_branch_root': True, 'files': leaf['files']})
        else:
            manifest_leaves.append({'path': leaf['path'], 'is_branch_root': False})
    
    generate_real_manifest(tar_name, manifest_leaves, is_live)


def should_upload_bag(leaf_list, branch_leaves):
    """
    Check if any leaf in the bag needs to be uploaded.
    
    Returns:
        bool: True if upload is needed, False otherwise
    """
    for leaf in leaf_list:
        key = leaf['key']
        if branch_leaves.get(key, {}).get('needs_upload', True):
            return True
    return False


def update_skip_stats(branch_stats, bag_size_bytes):
    """Update branch stats for skipped bags."""
    branch_stats['skip_count'] += 1
    branch_stats['skip_bytes'] += bag_size_bytes


def print_skip_message(tar_name):
    """Print a message for skipped bags."""
    bag_label_str = "  > Bag"
    pad_width = STATUS_WIDTH + 2
    print(f"\n{bag_label_str:<{pad_width}}: {tar_name}")
    print(f"  [SKIP] Inventory Match.")


def update_upload_stats(branch_stats, bag_size_bytes):
    """Update branch stats for bags that will be uploaded."""
    branch_stats['up_count'] += 1
    branch_stats['up_bytes'] += bag_size_bytes


def update_inventory_locations(leaf_list, branch_leaves, s3_key):
    """Update inventory with expected S3 locations for all leaves in the bag."""
    for leaf in leaf_list:
        if leaf['key'] in branch_leaves:
            branch_leaves[leaf['key']]['archive_key'] = s3_key


def print_dry_run_message(tar_name, s3_key, s3_bucket):
    """Print a message for dry run mode."""
    bag_label_str = "  > Bag"
    pad_width = STATUS_WIDTH + 2
    print(f"\n{bag_label_str:<{pad_width}}: {tar_name}")
    print(f"  [DRY RUN] Would upload: s3://{s3_bucket}/{s3_key}")

def build_and_upload_bag(tar_path, leaf_list, branch_root, designator, passphrase_file,
                        branch_leaves, remote_conn, remote_base_path, excludes,
                        s3_key, s3_bucket, bag_size_bytes, upload_limit_mb, 
                        encryption_config):
    """Build the tar archive and upload it to S3."""
    # 1. Ensure staging directory exists
    if not os.path.exists(STAGING_DIR): 
        os.makedirs(STAGING_DIR)
    
    # 2. Build tar archive
    temp_files_to_clean = build_tar_archive(
        tar_path, leaf_list, branch_root, designator, passphrase_file,
        branch_leaves, remote_conn, remote_base_path, excludes, 
        bag_size_bytes, encryption_config
    )
    
    try:
        # 3. Upload to S3
        etag = upload_to_s3(
            tar_path, s3_bucket, s3_key, bag_size_bytes, upload_limit_mb
        )
        
        # 4. Log the upload
        log_upload_success(s3_key, tar_path, etag)
        
        # 5. Cleanup
        cleanup_files(tar_path, temp_files_to_clean)
        
        return etag
        
    except Exception as e:
        # THIS IS THE BLOCK THAT WAS MISSING, PREVENTING THE SCRIPT FROM RUNNING
        print(f"[FATAL] Upload stage failed: {e}")
        cleanup_files(tar_path, temp_files_to_clean)
        sys.exit(1)

def build_tar_archive(tar_path, leaf_list, branch_root, designator, passphrase_file,
                     branch_leaves, remote_conn, remote_base_path, excludes, 
                     bag_size_bytes, encryption_config):
    """Build the tar archive containing all leaves."""
    
    # UI Setup
    bag_label_str = "  > Bag"
    pad_width = STATUS_WIDTH + 2
    print(f"{bag_label_str:<{pad_width}}: {os.path.basename(tar_path)}")
    
    # Start Heartbeat for the whole process
    hb = Heartbeat(tar_path, bag_size_bytes, status="BAG: TAR")
    hb.start()

    try:
        # 1. Process Child Leaves (Indented 7 spaces)
        cmd, temp_files_to_clean = construct_tar_command(
            tar_path, leaf_list, branch_root, designator, passphrase_file,
            branch_leaves, remote_conn, remote_base_path, excludes, 
            encryption_config, hb=hb
        )

        # 2. Transition back to Bag Level (Indented 5 spaces)
        sys.stdout.write("\n") 
        hb.update_target(tar_path, new_status="BAG: TAR", target_size=bag_size_bytes)
        
        # 3. Final Packaging
        subprocess.run(cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        
        hb.snap_done()
        hb.stop()
        return temp_files_to_clean

    except subprocess.CalledProcessError as e:
        hb.stop()
        print(f"\n[FATAL] Tar failed: {e.stderr.decode() if e.stderr else str(e)}")
        raise e

def upload_to_s3(tar_path, s3_bucket, s3_key, file_size, upload_limit_mb):
    """
    Upload the tar file to S3 with progress tracking.
    """
    if upload_limit_mb > 0:
        t_config = TransferConfig(max_bandwidth=upload_limit_mb * 1024 * 1024, max_concurrency=10, use_threads=True)
    else:
        t_config = TransferConfig(use_threads=True)


    # UI Indent (5 spaces) to match the "> Bag" level
    # desc = "     " + "[NET: AWS]".ljust(STATUS_WIDTH)
    desc = f"{' ' * 5}[NET: AWS]".ljust(15)
    file_size = os.path.getsize(tar_path)
        
    with tqdm(
        total=file_size,
        unit='B',
        unit_scale=True,
        leave=True,
        ncols=100,
        bar_format="{desc}{percentage:5.1f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
    ) as pbar:
        pbar.set_description(desc)
        s3_client.upload_file(
            tar_path, 
            s3_bucket, 
            s3_key, 
            ExtraArgs={'StorageClass': 'DEEP_ARCHIVE'},
            Config=t_config,
            Callback=lambda bytes_transferred: pbar.update(bytes_transferred)
        )
    
    # Verify upload and get ETag
    response = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
    etag = response.get('ETag', '').replace('"', '')
    
    return etag

def log_upload_success(s3_key, tar_path, etag):
    """Log successful upload to the transaction log."""
    try:
        file_size = os.path.getsize(tar_path)
        response = s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
        metadata = response.get('ResponseMetadata', {})
        
        log_aws_transaction(
            action="UPLOAD", 
            archive_key=s3_key, 
            size_bytes=file_size, 
            etag=etag, 
            response_metadata=metadata,
            aukive="UPL-STD"
        )
        
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        # --- COLUMN 15 ALIGNMENT ---
        # "     [OK : AWS]" is 13 chars. :<15 pads it with 2 spaces.
        ok_header = f"{' ' * 5}[OK : AWS]"
        print(f"{ok_header:<15}: {timestamp} | Verified ETag: {etag}")
        
    except Exception as e:
        etag = "VERIFY_FAILED"
        log_aws_transaction(
            "VERIFY_FAILURE", 
            s3_key, 
            os.path.getsize(tar_path), 
            "N/A", 
            {"Error": str(e)}, 
            "ERR-VFY"
        )
        # Match alignment for the warning too
        warn_header = f"{' ' * 5}[WARN]"
        print(f"{warn_header:<15}: ETag verification failed for {s3_key}")

def cleanup_files(tar_path, temp_files_to_clean):
    """Clean up temporary files after upload."""
    # Remove the tar file
    if os.path.exists(tar_path):
        os.remove(tar_path)
    
    # Remove all temporary files
    for f in temp_files_to_clean:
        if os.path.exists(f):
            os.remove(f)

def commit_to_inventory(leaf_list, branch_leaves, inventory, is_live, bag_num, etag=None):
    """Update inventory with successful upload information."""
    # Update local manifest/inventory
    for leaf in leaf_list:
        key = leaf['key']
        if key in branch_leaves:
            branch_leaves[key]['needs_upload'] = False
            branch_leaves[key]['last_upload'] = datetime.now().isoformat()
            branch_leaves[key]['etag'] = etag  # etag needs to be passed in or returned from upload_to_s3

    # Save inventory to disk
    if is_live:
        try:
            with open(INVENTORY_FILE, 'w') as f:
                json.dump(inventory, f, indent=4)
            
            inv_name = os.path.basename(INVENTORY_FILE)            
            save_header = f"{' ' * 5}[SAVE: DB]"
            print(f"{save_header:<15}: Bag {bag_num:05d} committed to {inv_name}.")
            
        except Exception as e:
            inv_name = os.path.basename(INVENTORY_FILE)
            save_header = f"{' ' * 5}[WARN]"
            print(f"{save_header:<15}: Failed to auto-save {inv_name}: {e}")

# --- PROCESS_BRANCH START ---

def process_branch(branch_line, inventory, run_stats, is_live, upload_limit_mb, is_repack, passphrase_file, encryption_config=None):
    """Handles parsing types (MUTABLE, IMMUTABLE), mounting, and processing."""
    
    # Parse branch info
    branch_path, full_tags, logic_type, branch_excludes = parse_branch_line(branch_line)
    
    # Initialize stats
    run_stats[branch_line] = {'up_count': 0, 'up_bytes': 0, 'skip_count': 0, 'skip_bytes': 0}
    branch_stats = run_stats[branch_line]

    mount_point_to_cleanup = None
    try:
        scan_path, mount_point_to_cleanup = mount_remote_branch(branch_path)
        
        if not os.path.exists(scan_path):
            print(f"[ERROR] Path not found: {scan_path}")
            return

        # Extract path and hostname information
        short_name, hostname, remote_conn, remote_base_path = get_branch_path_info(branch_path, scan_path)

        # Print branch processing header
        print_branch_header(branch_path, full_tags)

        # Initialize branch in inventory if needed
        if branch_line not in inventory["branches"]:
            inventory["branches"][branch_line] = {"leaves": {}}
        branch_leaves = inventory["branches"][branch_line]["leaves"]

        # Identify leaves based on logic type
        found_leaves = identify_leaves(logic_type, scan_path, branch_excludes)
        if found_leaves is None:
            return

        # Scan metadata and update inventory
        leaves_to_bag = scan_and_update_inventory(found_leaves, branch_leaves, is_repack)

        # Assign bags to leaves
        bag_counter = assign_bags_to_leaves(leaves_to_bag, inventory, branch_leaves, is_repack)

        # Group by bag ID
        bags = group_leaves_by_bag(leaves_to_bag)

        # Calculate waste and financial metrics
        calculate_branch_metrics(branch_stats, bags, leaves_to_bag, is_repack, is_live)

        # Execute bag processing
        process_branch_bags(bags, scan_path, short_name, is_live, branch_leaves, hostname, 
                          branch_stats, upload_limit_mb, full_tags, passphrase_file, 
                          remote_conn, remote_base_path, inventory, branch_excludes, 
                          encryption_config)

        # Handle repack cleanup if needed
        if is_repack and is_live:
            handle_repack_cleanup(hostname, short_name, bag_counter, s3_client, S3_BUCKET, S3_PREFIX)

        # Update branch scan timestamp
        if branch_line in inventory["branches"]:
            inventory["branches"][branch_line]["last_scan"] = datetime.now().isoformat()

    finally:
        if mount_point_to_cleanup:
            unmount_remote_branch(mount_point_to_cleanup)


def parse_branch_line(branch_line):
    """Parse branch line to extract path, tags, logic type, and excludes."""
    full_tags = "MUTABLE"
    logic_type = "MUTABLE"
    branch_excludes = [] 

    if " ::" in branch_line:
        parts = branch_line.split(" ::")
        branch_path = parts[0].strip()
        
        # Parse parts individually to preserve case for excludes
        clean_tags = []
        for p in parts[1:]:
            p = p.strip()
            if p.startswith("EXCLUDE "):
                # Extract value, preserving case (e.g. "0 - Books")
                val = p[8:].strip()
                branch_excludes.append(val)
            else:
                # Standard tag, upper case it
                clean_tags.append(p.upper())

        full_tags = " ".join(clean_tags)
        logic_type = "IMMUTABLE" if "IMMUTABLE" in clean_tags else "MUTABLE"
    else:
        branch_path = branch_line.strip()
        full_tags = "MUTABLE"
        logic_type = "MUTABLE"
        
    return branch_path, full_tags, logic_type, branch_excludes


def get_branch_path_info(branch_path, scan_path):
    """Extract path and hostname information from a branch path."""
    short_name = os.path.basename(os.path.normpath(scan_path))
    
    if ":" in branch_path:
        remote_conn, remote_base_path = branch_path.split(":", 1)
        hostname = remote_conn.split("@")[-1]
    else:
        remote_conn = None
        remote_base_path = None
        hostname = socket.gethostname()
        
    return short_name, hostname, remote_conn, remote_base_path


def print_branch_header(branch_path, full_tags):
    """Print a formatted header for branch processing."""
    print(f"------------------------------------------------")
    print(f"Processing [{full_tags}]:")
    print(f"  {branch_path}") 
    print(f"------------------------------------------------")


def identify_leaves(logic_type, scan_path, branch_excludes):
    """Identify leaves based on the logic type (MUTABLE or IMMUTABLE)."""
    found_leaves = []

    if logic_type == "IMMUTABLE":
        # Sovereign Storage: The entire path is a single Leaf.
        found_leaves.append({
            "key": scan_path, "path": scan_path, "is_branch_root": False, "files": []
        })
        
    elif logic_type == "MUTABLE":
        # Shared Storage: Every sub-directory is a unique Leaf.
        try:
            # Filter subdirs immediately
            subdirs = [d for d in sorted(os.listdir(scan_path)) 
                       if os.path.isdir(os.path.join(scan_path, d)) 
                       and d not in branch_excludes]

            for d in subdirs:
                full_p = os.path.join(scan_path, d)
                found_leaves.append({
                    "key": full_p, "path": full_p, "is_branch_root": False, "files": []
                })
            
            # Filter loose files immediately
            loose_files = [f for f in sorted(os.listdir(scan_path)) 
                           if os.path.isfile(os.path.join(scan_path, f))
                           and f not in branch_excludes]

            if loose_files:
                cluster_key = os.path.join(scan_path, "__BRANCH_ROOT__")
                found_leaves.append({
                    "key": cluster_key, "path": scan_path, "is_branch_root": True, "files": loose_files
                })
        except OSError as e:
            print(f"[ERROR] Could not scan directory {scan_path}: {e}")
            return None
    else:
        print(f"[ERROR] Unknown logic type derived from branch: {logic_type}")
        return None
        
    return found_leaves


def scan_and_update_inventory(found_leaves, branch_leaves, is_repack):
    """Scan metadata for leaves and update the inventory."""
    leaves_to_bag = []

    for leaf in found_leaves:
        if leaf['is_branch_root']:
            current_hash, size = get_metadata_hash(leaf['path'], recursive=False, file_list=leaf['files'])
        else:
            current_hash, size = get_metadata_hash(leaf['path'], recursive=True)

        entry = branch_leaves.get(leaf['key'], {})
        
        existing_tid = entry.get("tar_id", None)
        existing_key = entry.get("archive_key", None)
        
        is_changed = entry.get("last_metadata_hash") != current_hash
        
        if is_repack:
            needs_upload = True
        else:
            needs_upload = is_changed or entry.get("needs_upload", True)

        archive_key = existing_key if not is_changed else None

        branch_leaves[leaf['key']] = {
            "last_metadata_hash": current_hash,
            "needs_upload": needs_upload,
            "size_bytes": size,
            "size_human": format_bytes(size),
            "tar_id": existing_tid, 
            "archive_key": archive_key,
            "last_upload": entry.get("last_upload", None)
        }
        
        leaf_data = leaf.copy()
        leaf_data.update({"size": size, "tar_id": existing_tid})
        leaves_to_bag.append(leaf_data)
        
    return leaves_to_bag


def assign_bags_to_leaves(leaves_to_bag, inventory, branch_leaves, is_repack):
    """Assign bags to leaves based on repack status and size."""
    if is_repack:
        print("  [REPACK] Ignoring existing leaf bag IDs. Consolidating all leaves...")
        # If repacking, we treat every leaf as if it has no seat assignment
        for leaf in leaves_to_bag:
            leaf["tar_id"] = None
        bag_counter = 1
    else:
        # Find the highest bag number in the ENTIRE inventory
        existing_bag_nums = []
        for branch_data in inventory["branches"].values():
            for unit in branch_data.get("leaves", {}).values():
                tid = unit.get('tar_id')
                if tid and tid.startswith('bag_'):
                    try:
                        existing_bag_nums.append(int(tid.split('_')[-1]))
                    except ValueError: pass
        
        bag_counter = max(existing_bag_nums) if existing_bag_nums else 0
        
        # If this specific branch already has bags, find this branch's highest bag
        branch_bag_nums = []
        for unit in branch_leaves.values():
            tid = unit.get('tar_id')
            if tid and tid.startswith('bag_'):
                try:
                    branch_bag_nums.append(int(tid.split('_')[-1]))
                except ValueError: pass
        
        if not branch_bag_nums:
            # This branch has never been backed up, so it gets the NEXT global number
            bag_counter += 1
        else:
            # This branch exists; we continue from its own highest bag
            bag_counter = max(branch_bag_nums)

    current_bag_size = 0
    
    # In standard mode, calculate how much is already in the last bag
    if not is_repack:
        last_bag_id = f"bag_{bag_counter:05d}"
        for unit in branch_leaves.values():
            if unit.get('tar_id') == last_bag_id:
                current_bag_size += unit.get('size_bytes', 0)

    for leaf in leaves_to_bag:
        # If not repacking, respect the "Reserved Seat"
        if not is_repack and leaf["tar_id"]:
            continue 
        
        # Logic for assigning to a bag (new leaves or ALL leaves if repacking)
        if (current_bag_size + leaf["size"] > TARGET_SIZE_BYTES) and current_bag_size > 0:
            bag_counter += 1
            current_bag_size = 0
        
        new_tid = f"bag_{bag_counter:05d}"
        leaf["tar_id"] = new_tid
        current_bag_size += leaf["size"]
        
        # Update the inventory brain
        branch_leaves[leaf["key"]]["tar_id"] = new_tid
        
    return bag_counter


def group_leaves_by_bag(leaves_to_bag):
    """Group leaves by bag ID for processing."""
    bags = {}
    for leaf in leaves_to_bag:
        tid = leaf["tar_id"]
        if tid not in bags:
            try:
                b_num = int(tid.split('_')[-1])
            except (ValueError, AttributeError):
                b_num = 0
            
            bags[tid] = {"leaves": [], "size": 0, "bag_num_int": b_num}
        
        bags[tid]["leaves"].append(leaf)
        bags[tid]["size"] += leaf["size"]
        
    return bags


def calculate_branch_metrics(branch_stats, bags, leaves_to_bag, is_repack, is_live):
    """Calculate waste and financial metrics for the branch."""
    # Load pricing from config but fallback to default
    price_gb = float(config['pricing'].get('price_per_gb_month', 0.00099))
    min_days = int(config['pricing'].get('min_retention_days', 180))
    
    total_data_in_branch = sum(leaf["size"] for leaf in leaves_to_bag)
    num_bags_in_branch = len(bags)
    total_capacity = num_bags_in_branch * TARGET_SIZE_BYTES
    
    waste_bytes = total_capacity - total_data_in_branch
    waste_percent = (waste_bytes / total_capacity * 100) if total_capacity > 0 else 0

    # Calculate "At-Risk" Deletion Fees
    at_risk_fee = (total_data_in_branch / BYTES_PER_GB) * price_gb * (min_days / 30)

    branch_stats['waste_bytes'] = waste_bytes
    branch_stats['waste_percent'] = waste_percent
    branch_stats['total_bags'] = num_bags_in_branch
    branch_stats['at_risk_fee'] = at_risk_fee

    if is_repack:
        print(f"  [REPACK REPORT] Potential Waste Saved: {format_bytes(waste_bytes)}")
        if is_live:
            print(f"  [FINANCIAL WARNING] This repack could trigger ~${at_risk_fee:.2f} in early deletion fees.")


def process_branch_bags(bags, scan_path, short_name, is_live, branch_leaves, hostname, 
                       branch_stats, upload_limit_mb, full_tags, passphrase_file, 
                       remote_conn, remote_base_path, inventory, branch_excludes, 
                       encryption_config):
    """Process each bag in the branch."""
    sorted_bag_ids = sorted(bags.keys(), key=lambda x: bags[x]["bag_num_int"])
    safe_prefix = short_name.replace(" ", "_")

    for tid in sorted_bag_ids:
        bag_data = bags[tid]
        process_bag(
            bag_data["bag_num_int"], 
            bag_data["leaves"], 
            scan_path, 
            short_name, 
            bag_data["size"], 
            is_live, 
            branch_leaves,
            hostname,
            branch_stats,
            upload_limit_mb,
            full_tags,
            passphrase_file,
            remote_conn,
            remote_base_path,
            inventory,
            branch_excludes,
            encryption_config
        )


def handle_repack_cleanup(hostname, safe_prefix, bag_counter, s3_client, S3_BUCKET, S3_PREFIX):
    """Clean up orphaned tail bags after a repack operation."""
    print(f"  [CLEANUP] Checking for orphaned tail bags on S3...")
    
    # Construct the specific prefix for THIS branch's bags
    branch_bag_prefix = f"{hostname}_{safe_prefix}_bag_"
    s3_search_prefix = os.path.join(S3_PREFIX, branch_bag_prefix)
    
    # List what is actually on S3
    found_orphans = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=s3_search_prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            # Verify it matches our strict pattern to avoid deleting wrong files
            if key.endswith(".tar") and branch_bag_prefix in key:
                try:
                    # Extract number: ..._bag_00008.tar -> 8
                    fname = os.path.basename(key)
                    num_part = fname.replace(branch_bag_prefix, "").replace(".tar", "")
                    bag_num = int(num_part)
                    
                    # If this number is higher than our current counter, it's a tail orphan
                    if bag_num > bag_counter:
                        found_orphans.append(key)
                except ValueError:
                    continue

    if found_orphans:
        print(f"  [CLEANUP] Found {len(found_orphans)} obsolete leaf bags. Deleting...")
        for orphan in found_orphans:
            try:
                print(f"  [S3 DELETE] {orphan}")
                response = s3_client.delete_object(Bucket=S3_BUCKET, Key=orphan)
                log_aws_transaction("DELETE_REPACK", orphan, 0, "N/A", response.get('ResponseMetadata', {}), "DEL-RPK")
            except Exception as e:
                print(f"  [WARN] Failed to delete {orphan}: {e}")
    else:
        print(f"  [CLEANUP] No orphans found.")

# --- PROCESS_BRANCH END ---

def generate_full_report(inventory, config):
    print("\n" + "="*105)
    print(f"{'GLACIER ARCHIVE: BRANCH & INVENTORY STATUS':^105}")
    print("="*105)
    # Header alignment: Path(65), Bags(8), Size(12), Waste(10)
    print(f"{'BRANCH':<65} {'BAGS':>8} {'SIZE':>12} {'WASTE':>10}")
    print("-" * 105)

    branches = inventory.get('branches', {})
    
    # Safely handle bag size threshold for waste calculation
    try:
        target = int(config.get('Storage', 'bag_size_threshold', fallback=40 * 1024**3))
    except:
        target = 40 * 1024**3

    for branch_name in sorted(branches.keys()):
        data = branches[branch_name]
        leaves_dict = data.get('leaves', {})
        
        branch_total_bytes = 0
        unique_bags = set()
        
        # Mapping to YOUR keys: size_bytes and tar_id
        for meta in leaves_dict.values():
            branch_total_bytes += meta.get('size_bytes', 0)
            bid = meta.get('tar_id')
            if bid:
                unique_bags.add(bid)
        
        # Formatting for the report
        size_str = format_bytes(branch_total_bytes)
        bag_count = len(unique_bags)
        
        # Waste: Only useful for single-bag branches (like your VMs)
        waste_str = "---"
        if bag_count == 1 and branch_total_bytes > 0:
            waste_pct = max(0, (1 - (branch_total_bytes / target)) * 100)
            waste_str = f"{waste_pct:.1f}%"

        # Middle-clip long paths to keep Host and Folder visible
        if len(branch_name) > 62:
            display_path = branch_name[:30] + "..." + branch_name[-30:]
        else:
            display_path = branch_name

        print(f"{display_path:<65} {bag_count:>8} {size_str:>12} {waste_str:>10}")

    print("-" * 105)

    # Pricing extraction
    p_gb = float(config.get('pricing', 'price_gb_month', fallback=0.00099))
    p_put = float(config.get('pricing', 'price_put_1k', fallback=0.05))
    p_egress = float(config.get('pricing', 'price_egress_gb', fallback=0.09))
    
    # Standard Retrieval Constants
    p_thaw_std = float(config.get('pricing', 'price_thaw_standard_gb', fallback=0.02))
    p_req_std = float(config.get('pricing', 'price_req_standard_1k', fallback=0.10))
    
    # Bulk Retrieval Constants
    p_thaw_bulk = float(config.get('pricing', 'price_thaw_bulk_gb', fallback=0.0025))
    p_req_bulk = float(config.get('pricing', 'price_req_bulk_1k', fallback=0.025))

    # Inventory Metrics
    total_bytes = 0
    total_bags = set()
    for branch, data in inventory.get("branches", {}).items():
        for leaf, details in data.get("leaves", {}).items():
            total_bytes += details.get('size_bytes', 0)
            if details.get('archive_key'):
                total_bags.add(details['archive_key'])

    total_gb = total_bytes / (1024**3)
    bag_count = len(total_bags)

    # --- CALCULATIONS ---
    monthly_cost = total_gb * p_gb
    put_cost = (bag_count / 1000) * p_put
    
    # Recovery = (Thaw Fee + Egress Fee) * GB + (Requests)
    cost_std = (total_gb * (p_thaw_std + p_egress)) + ((bag_count / 1000) * p_req_std)
    cost_bulk = (total_gb * (p_thaw_bulk + p_egress)) + ((bag_count / 1000) * p_req_bulk)

    # --- FINANCE REPORT ---
    print("\n" + "="*60)
    print(f"       GLACIER ARCHIVE COST - {datetime.now().strftime('%Y-%m-%d')}")
    print("="*60)
    print(f"  TOTAL ARCHIVE:    {total_gb/1024:.2f} TB ({total_gb:.2f} GB)")
    print(f"  TOTAL BAGS:       {bag_count}")
    print("-" * 60)
    print(f"  MONTHLY STORAGE:  ${monthly_cost:.2f}")
    print(f"  EST. UPLOAD COST: ${max(0.01, put_cost):.4f} (One-time PUT fees)")
    print("-" * 60)
    print(f"  RECOVERY ESTIMATES (Thaw + Internet Download):")
    print(f"  [STANDARD] (12hr): ${cost_std:.2f}  (${cost_std/bag_count:.2f}/bag)" if bag_count > 0 else "")
    print(f"  [BULK]     (48hr): ${cost_bulk:.2f}  (${cost_bulk/bag_count:.2f}/bag)" if bag_count > 0 else "")
    print("-" * 60)    
    print("="*60 + "\n")

def find_file(search_term):
    """Searches through local manifests to find which leaf bag contains a file."""
    
    # Load inventory for reverse lookup using the global variable defined at the top
    inventory = load_inventory(INVENTORY_FILE)

    print(f"\n--- Searching for: '{search_term}' ---")
    found_count = 0
    
    # Use the global MANIFEST_DIR
    if not os.path.exists(MANIFEST_DIR):
        print(f"[ERROR] Manifest directory not found: {MANIFEST_DIR}")
        return

    for manifest in sorted(os.listdir(MANIFEST_DIR)):
        # Only process manifest text files
        if not manifest.endswith(".txt"): 
            continue
        
        bag_name = manifest.replace(".txt", "")
        path = os.path.join(MANIFEST_DIR, manifest)
        
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    line = line.strip()
                    # Case-insensitive search
                    if search_term.lower() in line.lower():
                        found_count += 1
                        
                        # Perform the reverse lookup in the inventory
                        leaf, branch = find_leaf_owner(line, inventory)
                        
                        print("-" * 60)
                        print(f"  [FOUND] in bag      : {bag_name}")
                        print(f"  Leaf (Atomic Unit)  : {leaf}")
                        print(f"  Branch (tree file)  : {branch}")
                        print(f"  File Path           : {line}")
        except Exception as e:
             print(f"  [WARN] Could not read manifest {manifest}: {e}")
    
    if found_count == 0:
        print("  No matches found in local manifests.")
    else:
        print("-" * 60)
        print(f"--- Found {found_count} matches ---")

def audit_s3(inventory):
    """Compares local inventory against actual S3 bucket contents and verifies Storage Class."""
    print(f"\n--- S3 Integrity & Cost Audit ---")
    
    # 1. Map expected bags from inventory
    expected_bags = {} 
    for branch, data in inventory.get("branches", {}).items():
        for leaf, details in data.get("leaves", {}).items():
            key = details.get('archive_key')
            if not key: continue
            inv_etag = details.get('etag')
            size = details.get('size_bytes', 0)
            if key not in expected_bags:
                expected_bags[key] = {'size': 0, 'etag': inv_etag}
            expected_bags[key]['size'] += size

    # 2. Get actual list from S3 (Now fetching StorageClass too)
    print(f"  [FETCHING] Remote file list from s3://{S3_BUCKET}/{S3_PREFIX}...")
    actual_s3_data = {}
    paginator = s3_client.get_paginator('list_objects_v2')
    
    wrong_tier_count = 0
    
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            # Normalize ETag (remove quotes)
            etag = obj['ETag'].replace('"', '')
            storage_class = obj.get('StorageClass', 'STANDARD') # Default is Standard if missing
            
            actual_s3_data[key] = {
                'size': obj['Size'], 
                'etag': etag,
                'class': storage_class
            }

    # 3. Perform the Audit
    missing = []
    corruption = []
    cost_warnings = []
    
    total_audited = len(expected_bags)
    verified_with_etag = 0
    
    for bag_key, info in expected_bags.items():
        # A) Check Existence
        if bag_key not in actual_s3_data:
            missing.append(bag_key)
            continue
            
        s3_obj = actual_s3_data[bag_key]
        
        # B) Check Integrity
        if info['etag']:
            verified_with_etag += 1
            if info['etag'] != s3_obj['etag']:
                corruption.append(bag_key)
        
        # C) Check Storage Class (Cost Safety)
        # We want GLACIER or DEEP_ARCHIVE. Anything else is expensive.
        if s3_obj['class'] not in ['GLACIER', 'DEEP_ARCHIVE']:
            cost_warnings.append(f"{bag_key} ({s3_obj['class']})")

    # 4. Reporting
    if not missing and not corruption and not cost_warnings:
        print(f"  [OK] All {total_audited} expected bags are present and verified.")
        print(f"  [OK] Storage classes verified (Deep Archive).")
    else:
        if missing:
            print(f"[ALERT] {len(missing)} bags are MISSING from S3!")
            for m in missing: print(f"          MISSING: {m}")
        if corruption:
            print(f"[CRITICAL] {len(corruption)} bags FAILED hash integrity check!")
            for c in corruption: print(f"          MISMATCH: {c}")
        if cost_warnings:
            print(f"[COST WARN] {len(cost_warnings)} bags are in the WRONG storage tier (High Cost):")
            for w in cost_warnings: print(f"          {w}")

    # 5. Orphan Check
    orphans = [k for k in actual_s3_data if k not in expected_bags and "system/" not in k and "manifests/" not in k]
    if orphans:
        print(f"  [NOTE] Found {len(orphans)} orphan bags on S3. Run --prune to clean up.")

def check_compression_needed(line_metadata):
    """Returns True if the COMPRESS tag is present in the branch line."""
    return "COMPRESS" in line_metadata

def check_encryption_needed(line_metadata):
    """Returns True if the ENCRYPT tag is present in the branch line."""
    return "ENCRYPT" in line_metadata

def validate_encryption_config(tree_file):
    """
    Checks if the mission requires GPG encryption and ensures the key file exists.
    Returns the path to the PASSPHRASE_FILE if valid, otherwise exits.
    """
    passphrase_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "key.txt")
    needs_crypto = False

    # 1. Check tree.txt for the ::ENCRYPT trigger
    if os.path.exists(tree_file):
        with open(tree_file, 'r') as f:
            if "::ENCRYPT" in f.read():
                needs_crypto = True

    # 2. Validation Logic Gate
    if needs_crypto:
        if not os.path.exists(passphrase_file) or os.stat(passphrase_file).st_size == 0:
            print(f"\n[FATAL ERROR] Encryption is active (::ENCRYPT tag found), but key file is missing/empty.")
            print("-" * 85)
            print("To generate your key file safely on this machine, use one of these methods:")
            print(f"\nA) Manual Entry (Recommended for security):")
            print(f"    (stty -echo; printf 'Passphrase: '; read pw; stty echo; echo \"$pw\" > {passphrase_file} && chmod 600 {passphrase_file}; echo; echo '[OK] Key saved.')")
            print(f"\nB) Quick Command:")
            print(f"    echo 'your_passphrase_here' > {passphrase_file} && chmod 600 {passphrase_file}")
            print("-" * 85)
            print("CRITICAL: Without this file, encrypted leaves in Glacier are PERMANENTLY lost.")
            sys.exit(1)

    return passphrase_file if needs_crypto else None

# --- Shared Helper Functions ---

def prepare_staging_environment(leaf_path, leaf_id, files=None, remote_conn=None, remote_base_path=None, local_branch_root=None, expected_size=0, hb=None):
    """
    Prepare the staging environment for a leaf, including remote sync if needed.
    
    Args:
        leaf_path: Path to the leaf
        leaf_id: Unique ID for the leaf
        files: Specific files to include (for branch_root leaves)
        remote_conn: Remote connection string (if remote)
        remote_base_path: Base path on remote system
        local_branch_root: Root directory of the branch
        expected_size: Expected size of the leaf in bytes
    
    Returns:
        dict: Staging information including target_path, parent, base, and local_raw
    """
    local_raw = None
    target_path = leaf_path
    parent = os.path.dirname(leaf_path)
    base = os.path.basename(leaf_path)

    # Only process remote fetching if we're not dealing with specific files
    # and we have a remote connection
    if remote_conn and not files:
        try:
            # Create a dedicated staging subdirectory for this specific leaf
            leaf_stage_dir = os.path.join(STAGING_DIR, f"stage_{leaf_id}")
            if not os.path.exists(leaf_stage_dir):
                os.makedirs(leaf_stage_dir)
            
            # Sync remote data to local staging
            if stage_remote_leaf(
                remote_conn, remote_base_path, leaf_path, 
                local_branch_root, leaf_stage_dir, 
                expected_size=expected_size,
                hb=hb
            ):
                # SUCCESS: local_raw is now the directory we need to clean up later
                local_raw = leaf_stage_dir
                target_path = local_raw
                parent = os.path.dirname(local_raw)
                base = os.path.basename(local_raw)
            else:
                # Failed to sync, clean up and return None
                if os.path.exists(leaf_stage_dir):
                    shutil.rmtree(leaf_stage_dir)
                return None
        except Exception as e:
            print(f"[ERROR] Local staging failed for {leaf_path}: {e}")
            return None

    return {
        "target_path": target_path,
        "parent": parent,
        "base": base,
        "local_raw": local_raw
    }


def run_with_heartbeat(cmd, output_path, expected_size, status, shell=True, is_dir=False):
    """
    Run a command with heartbeat progress monitoring.
    
    Args:
        cmd: Command to run (string or list)
        output_path: Path to watch for progress
        expected_size: Expected final size in bytes
        status: Status label for heartbeat
        shell: Whether to run command in shell
        is_dir: Whether output_path is a directory
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Initialize heartbeat for progress display
    hb = Heartbeat(output_path, expected_size, status=status, is_dir=is_dir)
    hb.start()
    
    try:
        if shell and isinstance(cmd, list):
            cmd = " ".join(cmd)
        
        # Run the command, capturing errors but not output
        subprocess.run(
            cmd, 
            shell=shell, 
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE
        )
        return True
    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.decode() if e.stderr else str(e)
        print(f"\n    [ERROR] Command failed: {error_msg}")
        return False
    finally:
        hb.stop()
        hb.join()


# --- Encrypt Leaf Function and Helpers ---

def encrypt_leaf(leaf_path, files=None, passphrase_file=None, compress=False, remote_conn=None, remote_base_path=None, local_branch_root=None, expected_size=0, encryption_config=None, hb=None):
    """Encrypts a leaf, potentially with compression."""
    # 1. Validate encryption configuration
    if not validate_encryption_parameters(passphrase_file, encryption_config):
        return None
    
    # 2. Prepare environment
    leaf_id = hashlib.md5(leaf_path.encode()).hexdigest()[:8]
    staging = prepare_staging_environment(
        leaf_path, leaf_id, files, remote_conn, 
        remote_base_path, local_branch_root, expected_size,
        hb=hb
    )
    
    if staging is None:
        return None
    
    # 3. Prepare file paths
    tar_flags, ext = prepare_archive_params(compress)
    intermediate_file, encrypted_path = prepare_encryption_paths(leaf_id, ext)
    
    # 4. Build tar command
    tar_cmd = build_tar_command_for_encryption(
        staging["target_path"], staging["parent"], 
        staging["base"], files, tar_flags, intermediate_file
    )
    
    # 5. Build encryption command
    gpg_cmd = build_encryption_command(
        intermediate_file, encrypted_path, passphrase_file, encryption_config
    )
    
    # 6. Execute encryption process
    success = execute_encryption_process(
        tar_cmd, gpg_cmd, intermediate_file, encrypted_path, expected_size, hb=hb, compress=compress
    )
    
    # 7. Cleanup and return
    cleanup_encryption_temp(intermediate_file, staging["local_raw"])
    
    return encrypted_path if success else None


def validate_encryption_parameters(passphrase_file, encryption_config):
    """Validate that we have necessary encryption configuration."""
    use_key_method = False
    if encryption_config and encryption_config['method'] == 'key':
        use_key_method = True
        # Check if we have a valid key ID
        if not encryption_config['gpg_key_id']:
            print(f"[ERROR] Key-based encryption selected but no gpg_key_id configured")
            return False
    elif not passphrase_file:
        # Password method selected but no password file
        print(f"[ERROR] Password-based encryption selected but no passphrase file provided")
        return False
    
    return True


def prepare_archive_params(compress):
    """Prepare archive parameters based on compression need."""
    tar_flags = "-czf" if compress else "-cf"
    ext = ".tar.gz" if compress else ".tar"
    return tar_flags, ext


def prepare_encryption_paths(leaf_id, ext):
    """Prepare paths for intermediate and final encrypted files."""
    intermediate_file = os.path.join(STAGING_DIR, f"bundle_{leaf_id}{ext}")
    encrypted_path = os.path.join(STAGING_DIR, f"enc_{leaf_id}.gpg")
    return intermediate_file, encrypted_path


def build_tar_command_for_encryption(target_path, parent, base, files, tar_flags, intermediate_file):
    """Build the tar command for the first phase of encryption."""
    if files:
        files_list = " ".join([shlex.quote(f) for f in files])
        return f"tar -C {shlex.quote(target_path)} {tar_flags} {shlex.quote(intermediate_file)} {files_list}"
    else:
        return f"tar -C {shlex.quote(parent)} {tar_flags} {shlex.quote(intermediate_file)} {shlex.quote(base)}"


def build_encryption_command(intermediate_file, encrypted_path, passphrase_file, encryption_config):
    """Build GPG command based on encryption method."""
    if encryption_config and encryption_config['method'] == 'key':
        # Use key-based encryption
        return [
            "gpg", "--batch", "--yes",
            "--recipient", encryption_config['gpg_key_id'],
            "--output", encrypted_path,
            "--encrypt", intermediate_file
        ]
    else:
        # Use password-based encryption
        return [
            "gpg", "--batch", "--yes", 
            "--passphrase-file", passphrase_file,
            "--symmetric", "--cipher-algo", "AES256", 
            "--output", encrypted_path, 
            intermediate_file
        ]

def execute_encryption_process(tar_cmd, gpg_cmd, intermediate_file, encrypted_path, expected_size, hb=None, compress=False):
    # Create a heartbeat if not provided
    local_hb = False
    if not hb:
        hb = Heartbeat()
        hb.start()
        local_hb = True
        
    try:
        # PHASE 1: TAR
        # For TAR: update with correct target size and status
        if not os.path.exists(intermediate_file):
            # Create an empty file to track
            with open(intermediate_file, 'wb') as f:
                pass
                
        # Set an appropriate target size for TAR based on expected_size
        tar_target_size = expected_size * 1.1  # Add 10% for tar overhead
        hb.update_target(intermediate_file, "TAR: LEAF", target_size=tar_target_size, is_compressed=compress)
        
        # Run tar command
        subprocess.run(tar_cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # Get actual size after TAR completion
        actual_tar_size = os.path.getsize(intermediate_file)
        
        # Mark TAR as complete
        hb.snap_done()
        
        # PHASE 2: GPG
        # For GPG: target size is the actual size of the intermediate file
        hb.update_target(encrypted_path, "GPG: LEAF", target_size=actual_tar_size)
        
        # Run gpg command
        subprocess.run(gpg_cmd, shell=False, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # Mark GPG as complete
        hb.snap_done()
        
        return os.path.exists(encrypted_path)
    
    finally:
        # Only stop if we created it locally
        if local_hb and hb:
            hb.stop()

def BASICexecute_encryption_process(tar_cmd, gpg_cmd, intermediate_file, encrypted_path, expected_size, hb=None):
    # We ignore the 'hb' (Heartbeat) entirely to stop the threading issues
    
    # PHASE 1: TAR
    print(f"  [TAR: LEAF] : Packaging... ", end="", flush=True)
    subprocess.run(tar_cmd, shell=True, check=True)
    print(f"100.0% [DONE]")

    # PHASE 2: GPG
    print(f"  [GPG: LEAF] : Encrypting... ", end="", flush=True)
    subprocess.run(gpg_cmd, shell=False, check=True)
    print(f"100.0% [DONE]")
    
    return os.path.exists(encrypted_path)

def cleanup_encryption_temp(intermediate_file, local_raw):
    """Clean up temporary files used during encryption."""
    if os.path.exists(intermediate_file):
        os.remove(intermediate_file)
    if local_raw and os.path.exists(local_raw):
        shutil.rmtree(local_raw)


# --- Compress Leaf Function and Helpers ---

def compress_leaf(leaf_path, files=None, remote_conn=None, remote_base_path=None, local_branch_root=None, expected_size=0, hb=None):
    """Creates a compressed archive of a leaf."""
    # 1. Prepare environment
    leaf_id = hashlib.md5(leaf_path.encode()).hexdigest()[:8]
    staging = prepare_staging_environment(
        leaf_path, leaf_id, files, remote_conn, 
        remote_base_path, local_branch_root, expected_size,
        hb=hb
    )
    
    if staging is None:
        return None
    
    # 2. Prepare file paths
    compressed_path = os.path.join(STAGING_DIR, f"comp_{leaf_id}.tar.gz")
    
    # 3. Build compression command
    cmd = build_compression_command(
        staging["target_path"], staging["parent"], 
        staging["base"], files, compressed_path
    )
    
    # 4. Execute compression with heartbeat
    if hb:
        hb.update_target(compressed_path, new_status="TAR: LEAF", is_compressed=True)
        subprocess.run(cmd, shell=True, check=True, stdout=subprocess.DEVNULL)
        success = True
    else:
        success = run_with_heartbeat(
            cmd, compressed_path, expected_size, "TAR: LEAF"
        )
    
    # 5. Cleanup and return
    if staging["local_raw"] and os.path.exists(staging["local_raw"]):
        shutil.rmtree(staging["local_raw"])
    
    return compressed_path if success else None


def build_compression_command(target_path, parent, base, files, compressed_path):
    """Build the tar command for compression."""
    if files:
        files_list = " ".join([shlex.quote(f) for f in files])
        return f"tar -C {shlex.quote(target_path)} -czf {shlex.quote(compressed_path)} {files_list}"
    else:
        return f"tar -C {shlex.quote(parent)} -czf {shlex.quote(compressed_path)} {shlex.quote(base)}"


def load_inventory(inventory_path):
    """
    Loads the JSON inventory file. 
    If missing, returns a fresh structure. 
    If malformed, exits to prevent state corruption.
    """
    if os.path.exists(inventory_path):
        try:
            with open(inventory_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            print(f"\n[FATAL ERROR] Inventory state file is malformed: {inventory_path}")
            print(f"Error Details: {e}")
            print("-" * 60)
            print("SUGGESTION: Check for a .bak file in your backup directory.")
            print("Manually restore it before restarting to avoid re-uploading 7TB of data.")
            print("-" * 60)
            sys.exit(1)
    else:
        # Start a fresh inventory for a new mission
        print(f"  [INIT] No inventory found at {inventory_path}. Starting fresh.")
        return {"branches": {}}

def get_s3_file_age(key):
    """Returns age of an S3 object in days."""
    try:
        response = s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        last_modified = response['LastModified']
        now = datetime.now(timezone.utc)
        return (now - last_modified).days
    except Exception:
        return None

def get_bag_age_and_penalty(inventory, target_bag_id, config):
    # 1. Collect all leaves belonging to this bag
    bag_leaves = []
    for branch in inventory["branches"].values():
        for leaf_path, details in branch["leaves"].items():
            if details.get("tar_id") == target_bag_id:
                bag_leaves.append(details)

    if not bag_leaves:
        return None, 0, 0

    # 2. Determine bag size and age
    total_bytes = sum(l.get("size_bytes", 0) for l in bag_leaves)
    total_gb = total_bytes / (1024**3)
    
    # Get the latest upload timestamp in this bag to be conservative
    timestamps = [datetime.fromisoformat(l["last_upload"]) for l in bag_leaves if l.get("last_upload")]
    if not timestamps:
        return 0, total_gb, 0 # Never uploaded, no penalty
    
    last_uploaded = max(timestamps)
    days_old = (datetime.now() - last_uploaded).days
    
    # 3. Calculate Penalty
    price_gb = float(config['pricing'].get('price_per_gb_month', 0.00099))
    min_days = int(config['pricing'].get('min_retention_days', 180))
    
    remaining_days = max(0, min_days - days_old)
    # Penalty is the storage cost for the remaining period
    penalty = total_gb * price_gb * (remaining_days / 30)
    
    return days_old, total_gb, penalty

def perform_rebag(inventory, target_bag_ids, config, is_live, requeue=True):
    """
    Resets leaves in specified bags to trigger re-upload.
    Calculates fees using leaf-level metadata.
    """
    print(f"\n--- Rebag Initialization: {', '.join(target_bag_ids)} ---")
    leaves_to_reset = []
    
    # Track stats for the confirmation UI
    total_bytes = 0
    encrypt_count = 0
    plain_count = 0
    affected_branches = set()

    # 1. Identify leaves and calculate data volume
    for raw_id in target_bag_ids:
        try:
            # Normalize to 5-digit string (e.g. "73" -> "00073")
            bag_id = f"bag_{int(raw_id.replace('bag_', '')):05d}"
        except ValueError:
            print(f"  [ERROR] '{raw_id}' is not a valid Bag ID.")
            continue

        for branch_key, data in inventory["branches"].items():
            # Get metadata for encryption check
            branch_meta = branch_key.split(" ::")[1:] if " ::" in branch_key else []
            meta_str = " ".join(branch_meta)

            for leaf_key, details in data["leaves"].items():
                if details.get("tar_id") == bag_id:
                    affected_branches.add(branch_key)
                    total_bytes += details.get("size_bytes", 0)
                    
                    will_encrypt = check_encryption_needed(meta_str)
                    if will_encrypt: encrypt_count += 1
                    else: plain_count += 1

                    leaves_to_reset.append({
                        'leaves_dict': data["leaves"],
                        'leaf_key': leaf_key,
                        's3_key': details.get("archive_key")
                    })

    if not leaves_to_reset:
        print("  [ERROR] No valid leaves found for these bags. Aborting.")
        return False

    # 2. Financial Calculation (The Leaf-Aware way)
    # We reuse your fixed set of bags to get the timeline
    stats = get_branch_financials(inventory, target_bag_ids, config)
    total_gb = total_bytes / (1024**3)

    # 3. User Confirmation with your preferred wording
    print("-" * 65)
    print(f"  ACTION:  Reset and re-upload {len(target_bag_ids)} bag(s).")
    print(f"  VOLUME:  {total_gb:.2f} GB ({len(leaves_to_reset)} leaves)")
    print(f"  ENCRYPT: {encrypt_count} yes / {plain_count} no")
    
    if stats["max_days_remaining"] > 0:
        print(f"  NOTICE:  There are {stats['max_days_remaining']}-days remaining in the early deletion period.")
        print(f"           If deleted now, the total cost will be ${stats['total_penalty']:.2f}")
    else:
        print(f"  NOTICE:  180-day minimum storage commitment met. No early deletion fees.")
    print("-" * 65)
    
    if not is_live:
        print("!!! DRY RUN: No actual S3 deletions or inventory changes will occur !!!")
    
    try:
        confirm = input(f"Proceed with rebagging? [y/N]: ")
    except (KeyboardInterrupt, EOFError):
        print("\n\n[ABORTED] User interrupted. No changes made.")
        return False
    if confirm.lower() != 'y':
        print("Aborted.")
        return False

    # 4. Execution: Reset state and delete from S3 (if live)
    deleted_s3_keys = set()
    for item in leaves_to_reset:
        leaves = item['leaves_dict']
        lk = item['leaf_key']
        s3_key = item['s3_key']

        if is_live and s3_key and s3_key not in deleted_s3_keys:
            try:
                print(f"  [S3 DELETE]: Removing s3://{config['settings']['s3_bucket']}/{s3_key}")
                response = s3_client.delete_object(Bucket=config['settings']['s3_bucket'], Key=s3_key)
                log_aws_transaction("DELETE_BAG", s3_key, 0, "N/A", response.get('ResponseMetadata', {}), "DEL-BAG")
                deleted_s3_keys.add(s3_key)
            except Exception as e:
                print(f"  [WARN] Failed to delete {s3_key}: {e}")

        # --- THE REQUEUE LOGIC ---
        if requeue:
            # RESET MODE: Keep the leaf, but set it for re-upload
            leaves[lk]["needs_upload"] = True
            leaves[lk]["tar_id"] = None
            leaves[lk]["archive_key"] = None
            if "encrypted" in leaves[lk]: del leaves[lk]["encrypted"]
        else:
            # DELETE MODE: Remove the leaf from the inventory entirely
            if lk in leaves:
                del leaves[lk]

    print("\n[OK] Leaves reset in memory.")
    
    # Return the branch key to main() for isolation
    return list(affected_branches)[0] if affected_branches else True

def get_branch_financials(inventory, unique_bags, config):
    """
    Fixed financial calculator: Aggregates data from LEAVES since 
    there is no top-level 'bags' section in the inventory.
    """
    # Pull rates from config with safe fallbacks
    monthly_rate = float(config.get('pricing', 'price_gb_month', fallback=0.00099))
    min_days = int(config.get('pricing', 'min_retention_days', fallback=180))
    
    res = {
        "total_bytes": 0,
        "total_gb": 0.0,
        "total_penalty": 0.0,
        "max_days_remaining": 0,
        "min_days": min_days
    }

    now = datetime.now()
    found_any_data = False

    # We must scan all branches to find leaves belonging to the unique_bags set
    for branch_name, branch_data in inventory.get("branches", {}).items():
        for leaf_path, details in branch_data.get("leaves", {}).items():
            bag_id = details.get("tar_id")
            
            if bag_id in unique_bags:
                found_any_data = True
                size = details.get("size_bytes", 0)
                res["total_bytes"] += size
                
                # Check for the last_upload timestamp in the leaf
                upload_date_str = details.get("last_upload")
                if upload_date_str:
                    try:
                        # Handle ISO format (2026-01-20T12:00:00) 
                        upload_dt = datetime.fromisoformat(upload_date_str)
                        # Ensure we are comparing naive to naive or aware to aware
                        # If your ISO strings aren't timezone aware, we use now()
                        days_held = (now - upload_dt.replace(tzinfo=None)).days
                        days_remaining = max(0, min_days - days_held)
                        
                        if days_remaining > 0:
                            leaf_gb = size / (1024**3)
                            months_rem = days_remaining / 30.0
                            res["total_penalty"] += (leaf_gb * monthly_rate * months_rem)
                            
                            if days_remaining > res["max_days_remaining"]:
                                res["max_days_remaining"] = days_remaining
                    except (ValueError, TypeError):
                        continue

    res["total_gb"] = res["total_bytes"] / (1024**3)
    return res

def perform_branch_reset(inventory, search_term, config, is_live, tree_lines):

    print(f"\n--- Branch Reset: Searching for '{search_term}' ---")
    
    # 1. Search the Inventory AND the Tree File
    matches = []
    # Check inventory first (for existing data impact report)
    for k in inventory["branches"].keys():
        path_only = k.split(" ::")[0].strip()
        if search_term == path_only or search_term == k:
            matches.append(k)
    
    # If not in inventory, check tree_lines (to allow NEW branches)
    if not matches:
        for line in tree_lines:
            path_only = line.split(" ::")[0].strip()
            if search_term == path_only or search_term == line:
                print(f"  [NEW] Branch found in tree.txt but not in inventory.")
                return line # Return the full line so main() can process it

    if not matches:
        print(f"  [WARN] No branch found matching '{search_term}'")
        return False 
    
    target_branch = matches[0]
    print(f"  [FOUND] {target_branch}")

    # 2. Gather Impact Metrics
    branch_data = inventory["branches"][target_branch]["leaves"]
    unique_bags = set()
    total_bytes = 0
    leaves_affected = 0
    s3_keys_to_delete = set()
    
    for leaf_key, details in branch_data.items():
        leaves_affected += 1
        if details.get("tar_id"):
            unique_bags.add(details["tar_id"])
        if details.get("archive_key"):
            s3_keys_to_delete.add(details["archive_key"])
        total_bytes += details.get("size_bytes", 0)

    # 3. Format Financial Penalty 
    
    # We pass the unique_bags set/list we gathered in section #2
    stats = get_branch_financials(inventory, unique_bags, config)
    
    # Map results to variables for your confirmation UI
    total_gb = stats["total_gb"]
    total_penalty = stats["total_penalty"]
    
    if stats["max_days_remaining"] > 0:
        safe_dt = (datetime.now() + timedelta(days=stats["max_days_remaining"])).strftime('%Y-%m-%d')
        penalty_detail = f"(Total remaining {stats['max_days_remaining']}-days of {stats['min_days']}-day commitment)"
        safe_to_delete = f" [Safe to delete: {safe_dt}]"
    else:
        penalty_detail = f"({stats['min_days']}-day commitment met; no early deletion fee)"
        safe_to_delete = ""

    # 4. Confirmation
    print("-" * 60)
    print(f"  This will DELETE AWS S3 copies of this branch.")
    print(f"  LEAVES:  {leaves_affected}")
    print(f"  BAGS:    {len(unique_bags)} (S3 Objects: {len(s3_keys_to_delete)})")
    print(f"  SIZE:    {total_gb:.2f} GB")
    if stats["max_days_remaining"] > 0:
        print(f"  NOTICE:  There are {stats['max_days_remaining']}-days remaining in the early deletion period.")
        print(f"           If deleted now, the total cost will be ${total_penalty:.2f}")
    else:
        print(f"  NOTICE:  180-day minimum storage commitment met. No early deletion fees.")
    print("-" * 60)
    
    if not is_live:
        print("!!! DRY RUN: No actual S3 deletions or inventory changes will be permanent !!!")

    try:
        confirm = input(f"Proceed with purging branch '{search_term}'? [y/N]: ")
    except (KeyboardInterrupt, EOFError):
        print("\n\n[ABORTED] User interrupted. No changes made.")
        return False
    if confirm.lower() != 'y':
        print("Aborted.")
        return False
    
    # 5. Execution
    if is_live:
        for s3_key in s3_keys_to_delete:
            try:
                print(f"  [S3 DELETE] {s3_key}")
                response = s3_client.delete_object(Bucket=config['settings']['s3_bucket'], Key=s3_key)
                log_aws_transaction("DELETE_BRANCH_PURGE", s3_key, 0, "N/A", response.get('ResponseMetadata', {}), "DEL-BCH")
            except Exception as e:
                print(f"  [WARN] Failed to delete {s3_key}: {e}")

        # PURGE: Remove the branch entirely from the inventory memory.
        # This prevents the 'ghost' entry.
        if target_branch in inventory["branches"]:
          del inventory["branches"][target_branch]

        # Clean up any abandoned staging files for this branch
        for f in glob.glob(os.path.join(STAGING_DIR, "stage_*")):
            shutil.rmtree(f, ignore_errors=True)

        print(f"\n  [OK] Branch deleted from inventory. New settings will apply on next mirroring.")
    else:
        print(f"\n[DRY RUN] Would delete S3 objects and remove '{target_branch}' from inventory.")

    
    return target_branch

def find_leaf_owner(file_path, inventory):
    """
    Reverse lookup: Find which Leaf and Branch owns a specific file path.
    Returns: (leaf_path, branch_name)
    """
    best_leaf = None
    best_branch = None
    
    # Iterate through every branch and leaf in the database
    for branch, data in inventory.get("branches", {}).items():
        for leaf_path in data.get("leaves", {}):
            # Check if the file belongs to this leaf (is inside the directory)
            # We look for the 'longest' match to handle nested paths correctly
            if file_path.startswith(leaf_path):
                if best_leaf is None or len(leaf_path) > len(best_leaf):
                    best_leaf = leaf_path
                    best_branch = branch
                    
    return best_leaf, best_branch

def stage_remote_leaf(remote_conn, remote_base_path, local_leaf_path, local_branch_root, stage_dir, expected_size=0, hb=None):
    """
    Staging with clean progress display while monitoring rsync.
    """
    # 1. Calculate the subpath
    prefix = local_branch_root.rstrip('/') + '/'
    subpath = local_leaf_path.replace(prefix, "").lstrip('/')
    leaf_rel_path = subpath.rstrip('/')
    
    # 2. Reconstruct the actual remote source
    remote_base = remote_base_path.rstrip('/')
    remote_path_safe = shlex.quote(f"{remote_base}/{subpath}".rstrip('/'))
    true_remote_src = f"{remote_conn}:{remote_path_safe}"

    # Create the staging directory if it doesn't exist
    if not os.path.exists(stage_dir):
        os.makedirs(stage_dir)

    # --- Context-Aware Exclude Logic ---
    base_dir = os.path.dirname(os.path.abspath(__file__))
    exclude_src = os.path.join(base_dir, "exclude.txt")
    temp_exclude_path = None
    exclude_args = []

    if os.path.exists(exclude_src):
        leaf_id = hashlib.md5(local_leaf_path.encode()).hexdigest()[:8]
        temp_exclude_path = os.path.join(STAGING_DIR, f"exclude_{leaf_id}.txt")
        if not os.path.exists(STAGING_DIR): os.makedirs(STAGING_DIR)

        has_rules = False
        with open(exclude_src, 'r') as f_in, open(temp_exclude_path, 'w') as f_out:
            for line in f_in:
                line = line.strip()
                if not line or line.startswith('#'): continue
                
                # Rewriting logic (Same as before)
                if not leaf_rel_path:
                    f_out.write(line + "\n")
                    has_rules = True
                    continue
                pat_prefix = leaf_rel_path + "/"
                if line.startswith(pat_prefix):
                    new_pattern = "/" + line[len(pat_prefix):]
                    f_out.write(new_pattern + "\n")
                    has_rules = True
                elif "/" not in line:
                    f_out.write(line + "\n")
                    has_rules = True

        if has_rules:
            exclude_args = ['--exclude-from', temp_exclude_path]
    # -----------------------------------

    # 3. Build the rsync command
    # Using --info=progress2 for a more machine-readable progress format
    cmd = ['rsync', '-a', '--delete', '--inplace', '--info=progress2', '-e', 'ssh']
    if exclude_args: cmd.extend(exclude_args)
    cmd.extend([f"{true_remote_src}/", f"{stage_dir}/"])

    # 4. Start the process with pipe for stdout
    start_time = time.time()
    last_update = time.time()
    
    try:
        # Run the process with pipes for stdout/stderr
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1  # Line buffered
        )
        
        # Variables to track progress
        current_bytes = 0
        total_bytes = 0  # Unknown initially
        transfer_speed = 0
        
        while process.poll() is None:
            # Read output without blocking
            line = process.stdout.readline().strip()
            if line:
                # Try to parse rsync's info=progress2 format
                # Format: "    14,648,320  16%   23.50MB/s    0:00:48"
                parts = line.split()
                if len(parts) >= 3:
                    try:
                        # The first part is bytes transferred (with commas)
                        bytes_str = parts[0].replace(',', '')
                        current_bytes = int(bytes_str)
                        
                        # Speed is typically the third part (with MB/s or KB/s)
                        speed_str = parts[2]
                        if speed_str.endswith('MB/s'):
                            transfer_speed = float(speed_str[:-4]) * 1024 * 1024
                        elif speed_str.endswith('KB/s'):
                            transfer_speed = float(speed_str[:-4]) * 1024
                        elif speed_str.endswith('B/s'):
                            transfer_speed = float(speed_str[:-3])
                            
                        # If there's a percentage, extract total bytes
                        if "%" in line:
                            percent_str = parts[1].replace('%', '')
                            percent = float(percent_str)
                            if percent > 0:
                                total_bytes = int(current_bytes * 100 / percent)
                        
                        # Only update display every 0.5 seconds to avoid flicker
                        if time.time() - last_update > 0.5:
                            # Format display
                            current_str = format_bytes(current_bytes)
                            speed_str = format_bytes(transfer_speed) + "/s"

                            # --- COLUMN 15 ALIGNMENT ---
                            # 7 spaces + [RSYNC] = 14 chars. :<15 puts the colon at Col 15.
                            header = f"{' ' * 7}[RSYNC]"
                            full_header = f"{header:<15}:"
                            
                            # If we have total bytes, show progress and ETA
                            if total_bytes > 0:
                                total_str = format_bytes(total_bytes)
                                percent = min(100.0, (current_bytes / total_bytes) * 100)
                                
                                # Calculate ETA
                                eta_str = "--:--"
                                if transfer_speed > 0 and current_bytes < total_bytes:
                                    seconds_left = (total_bytes - current_bytes) / transfer_speed
                                    if seconds_left > 0:
                                        m, s = divmod(int(seconds_left), 60)
                                        h, m = divmod(m, 60)
                                        eta_str = f"{h:02d}:{m:02d}:{s:02d}" if h > 0 else f"{m:02d}:{s:02d}"
                                
                                progress_line = f"{full_header} {percent:5.1f}% transferring [{current_str}/{total_str}] @ {speed_str} | ETA: {eta_str}"
                            else:
                                progress_line = f"{full_header} {current_str} transferring @ {speed_str}"
                            
                            sys.stdout.write(f"\r{progress_line:<110}")
                            sys.stdout.flush()
                            last_update = time.time()
                    except (ValueError, IndexError):
                        # Not a progress line, ignore
                        pass
            else:
                # No new output, sleep briefly
                time.sleep(0.1)
        
        # Process finished, get exit code
        exit_code = process.returncode
        
        # Calculate total time
        elapsed_seconds = time.time() - start_time
        if elapsed_seconds >= 3600:
            elapsed_str = f"{int(elapsed_seconds // 3600):02d}:{int((elapsed_seconds % 3600) // 60):02d}:{int(elapsed_seconds % 60):02d}"
        else:
            elapsed_str = f"{int(elapsed_seconds // 60):02d}:{int(elapsed_seconds % 60):02d}"
        
        final_size = get_tree_size(stage_dir)
        size_str = format_bytes(final_size)

        # COLUMN 15 ALIGNMENT
        header = f"{' ' * 7}[RSYNC]"
        full_header = f"{header:<15}:"

        sys.stdout.write(f"\r{full_header} [DONE | {elapsed_str}] - {size_str} transferred{'':80}\n")
        sys.stdout.flush()
        
        # Check exit code
        if exit_code != 0:
            stderr_output = process.stderr.read()
            if exit_code == 23:
                print(f"\n[WARN] Partial transfer (Code 23) for {local_leaf_path}.")
                print(f"       Last error: {stderr_output.strip().splitlines()[-1] if stderr_output else 'Unknown'}")
                return False
            else:
                print(f"\n[ERROR] Sync failed for {local_leaf_path}: {stderr_output}")
                return False
        
        return True
        
    except Exception as e:
        print(f"\n[ERROR] Failed to run rsync: {e}")
        return False
        
    finally:
        # Clean up temporary exclude file
        if temp_exclude_path and os.path.exists(temp_exclude_path):
            os.remove(temp_exclude_path)

def perform_prune(inventory, do_delete, check_age):
    """Identifies and removes S3 bags not present in the current botanical inventory."""
    print(f"\n--- S3 Pruning Mission [{'LIVE' if do_delete else 'DRY RUN'}] ---")
    
    # 1. THE WHITELIST: Every bag mentioned in the botanical inventory stays.
    live_bags = set()
    for branch in inventory.get('branches', {}).values():
        for leaf in branch.get('leaves', {}).values():
            if leaf.get('archive_key'):
                live_bags.add(leaf['archive_key'])

    # 2. THE GLOBAL SCAN: Look for orphaned .tar bags
    print(f"Scanning S3 bucket '{S3_BUCKET}' for orphaned leaf bags...")
    paginator = s3_client.get_paginator('list_objects_v2')
    orphans = []
    
    try:
        for page in paginator.paginate(Bucket=S3_BUCKET):
            for obj in page.get('Contents', []):
                key = obj['Key']
                # PROTECT SYSTEM ARTIFACTS: Only target .tar outside system/manifests
                if key.endswith('.tar') and "/system/" not in key and "/manifests/" not in key:
                    if key not in live_bags:
                        orphans.append(key)
    except Exception as e:
        print(f"Error accessing S3: {e}")
        return

    if not orphans:
        print("  [OK] S3 is in sync with your Botanical Inventory. No orphans found.")
        return

    print(f"Found {len(orphans)} orphaned bags on S3.")

    # 3. AGE VERIFICATION & STAGING
    keys_to_delete = []
    for key in orphans:
        age_days = get_s3_file_age(key)
        
        # Guard against AWS Early Deletion Fees (180-day rule)
        if check_age and age_days is not None and age_days < 180:
            print(f"  [GUARDED] {key} is {age_days}d old (<180). Skipping to avoid fees.")
            continue
        
        if do_delete:
            keys_to_delete.append({'Key': key})
            print(f"  [STAGING] {key} ({age_days}d old)")
        else:
            print(f"  [DRY RUN] Would delete {key} ({age_days}d old)")

    # 4. EXECUTE BULK DELETE
    if do_delete and keys_to_delete:
        print(f"\nExecuting Bulk Delete ({len(keys_to_delete)} files)...")
        for i in range(0, len(keys_to_delete), 1000):
            batch = keys_to_delete[i:i + 1000]
            try:
                response = s3_client.delete_objects(Bucket=S3_BUCKET, Delete={'Objects': batch})
                for item in batch:
                    log_aws_transaction("PRUNE_CLEANUP", item['Key'], 0, "N/A", response.get('ResponseMetadata', {}), "DEL-PRN")
                print(f"    Deleted batch of {len(batch)} items.")
            except Exception as e:
                print(f"[ERROR] Batch deletion failed: {e}")
        print("  [OK] Pruning complete.")

def show_bag(inventory, target_bag):
    target_bag = target_bag.strip()
    
    print("\n" + "="*105)
    print(f"{'MANIFEST FOR: ' + target_bag:^105}")
    print("="*105)
    # Adjusted widths: Size(12), Origin(28), File Path(remaining)
    print(f"{'SIZE':>12} | {'ORIGIN (BRANCH)':<28} | {'FILE PATH'}")
    print("-" * 105)

    found_count = 0
    total_bytes = 0

    for branch_name, data in inventory.get('branches', {}).items():
        # Better extraction: Get 'user@host:dir' and strip tags
        # Example: 'greenc@acre:/home/greenc/repos'
        branch_clean = branch_name.split(' ::')[0]
        
        # If it's a long remote path, keep the 'user@host' and the last dir
        if ':' in branch_clean:
            host_part, path_part = branch_clean.split(':')
            folder = path_part.rstrip('/').split('/')[-1]
            origin = f"{host_part}:{folder}"
        else:
            # Local path handling
            origin = branch_clean.rstrip('/').split('/')[-1]

        leaves = data.get('leaves', {})
        for path, meta in leaves.items():
            if meta.get('tar_id') == target_bag:
                size_str = meta.get('size_human', '0 B')
                total_bytes += meta.get('size_bytes', 0)
                # Clip origin to 28 chars to keep columns aligned
                print(f"{size_str:>12} | {origin[:28]:<28} | {path}")
                found_count += 1

    print("-" * 105)
    if found_count > 0:
        print(f"TOTAL: {found_count} files | Aggregate Size: {format_bytes(total_bytes)}")
    else:
        print(f"No records found for {target_bag}.")
    print("="*105 + "\n")

def show_leaf(inventory, target_path):
    target_path = target_path.strip().rstrip('/')
    found_in_inventory = False
    bag_id = None

    # 1. Find the Bag ID for this leaf in the inventory
    for branch_name, data in inventory.get('branches', {}).items():
        leaves = data.get('leaves', {})
        if target_path in leaves:
            bag_id = leaves[target_path].get('tar_id')
            found_in_inventory = True
            break

    if not found_in_inventory or not bag_id:
        print(f"\n[ERROR] Leaf '{target_path}' not found in inventory or has no Bag ID.")
        return

    # 2. Find the most recent liverun manifest for this bag
    # Pattern looks for the Bag ID and "liverun" in the filename
    manifest_pattern = os.path.join("manifests", f"*_{bag_id}_liverun.txt")
    manifest_files = glob.glob(manifest_pattern)

    if not manifest_files:
        print(f"\n[ERROR] No liverun manifest found for {bag_id} in manifests/")
        return

    # Sort by filename (since they start with YYYYMMDD) or mtime to get the newest
    latest_manifest = sorted(manifest_files)[-1]

    print("\n" + "="*100)
    print(f"FILE LIST FOR LEAF: {target_path}")
    print(f"Source Manifest: {latest_manifest}")
    print("="*100)

    # 3. Read the manifest and filter for the leaf's contents
    found_files = 0
    try:
        with open(latest_manifest, 'r') as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if line.startswith("#") or not line:
                    continue
                
                # Only show files that are inside the requested leaf directory
                if line.startswith(target_path):
                    print(f"  {line}")
                    found_files += 1
    except OSError as e:
        print(f"Error reading manifest: {e}")

    print("-" * 100)
    print(f"TOTAL: {found_files} files found inside leaf.")
    print("="*100 + "\n")

def show_branch(inventory, user_input):
    """
    Displays status for a branch with actionable Mirror Status messages.
    """
    user_input = user_input.strip()
    branches = inventory.get('branches', {})
    
    # Fuzzy matching for branch identification
    target_branch = None
    if user_input in branches:
        target_branch = user_input
    else:
        matches = [b for b in branches.keys() if b.startswith(user_input) or b.split(' ::')[0] == user_input]
        if len(matches) == 1:
            target_branch = matches[0]
        elif len(matches) > 1:
            print(f"\n[AMBIGUOUS] Multiple branches match '{user_input}':")
            for m in matches: print(f"  - {m}")
            return

    if not target_branch:
        print(f"\n[ERROR] Branch '{user_input}' not found in inventory.")
        return

    data = branches[target_branch]
    leaves = data.get('leaves', {})
    
    # Aggregate data
    unique_bags = set()
    total_bytes = 0
    needs_upload_count = 0
    
    for meta in leaves.values():
        total_bytes += meta.get('size_bytes', 0)
        bid = meta.get('tar_id')
        if bid: unique_bags.add(bid)
        if meta.get('needs_upload'):
            needs_upload_count += 1

    # Actionable Sync Status Logic
    if needs_upload_count > 0:
        status_msg = f"NOT IN SYNC ({needs_upload_count} leaves flagged - run --audit and/or --prune)"
    else:
        status_msg = "SYNCED (files on AWS are in sync with files in inventory.json)"

    print("\n" + "="*95)
    print(f"{'BRANCH STATUS: ' + target_branch:^95}")
    print("="*95)
    print(f"{'Total Size:':<18} {format_bytes(total_bytes)}")
    print(f"{'Total Leaves:':<18} {len(leaves)}")
    print(f"{'Distributed in:':<18} {len(unique_bags)} Bag(s) {sorted(list(unique_bags))}")
    print(f"{'Sync Status:':<18} {status_msg}")
    
    print("-" * 95)
    print(f"{'LEAF PATH':<65} {'BAG ID':>12} {'SIZE':>12}")
    print("-" * 95)

    for path in sorted(leaves.keys()):
        meta = leaves[path]
        size_str = meta.get('size_human', '0 B')
        bag_id = meta.get('tar_id', 'PENDING')
        
        # Consistent display path handling
        display_path = (path[:30] + "..." + path[-30:]) if len(path) > 62 else path
        print(f"{display_path:<65} {bag_id:>12} {size_str:>12}")

    print("="*95 + "\n")

def show_tree(inventory):
    branches = inventory.get('branches', {})
    if not branches:
        print("\n[EMPTY] No branches found in inventory.")
        return

    print("\n" + "="*115)
    print(f"{'GLACIER ARCHIVE: FULL INVENTORY TREE':^115}")
    print("="*115)
    print(f"{'TAGS':<6} {'BRANCH':<60} {'BAGS':>6} {'SIZE':>12} {'LAST UPLOAD':>18}")
    print("-" * 115)

    for branch_full_name in sorted(branches.keys()):
        data = branches[branch_full_name]
        
        # 1. Parse Tags and Base Path
        parts = branch_full_name.split(' ::')
        base_path = parts[0]
        tags_raw = parts[1:] if len(parts) > 1 else []
        
        # Create Shorthand: M (Mutable), I (Immutable), C (Compress), E (Encrypt)
        tag_short = ""
        if "MUTABLE" in tags_raw: tag_short += "M"
        if "IMMUTABLE" in tags_raw: tag_short += "I"
        if "COMPRESS" in tags_raw: tag_short += "C"
        if "ENCRYPT" in tags_raw: tag_short += "E"
        if "LOCKED" in tags_raw: tag_short += "L"
        if not tag_short: tag_short = "-"

        # 2. Aggregate Data
        leaves = data.get('leaves', {})
        total_bytes = sum(meta.get('size_bytes', 0) for meta in leaves.values())
        unique_bags = {meta.get('tar_id') for meta in leaves.values() if meta.get('tar_id')}
        
        last_date = "NEVER"
        for meta in leaves.values():
            lu = meta.get('last_upload')
            if lu and (last_date == "NEVER" or lu > last_date):
                last_date = lu

        display_date = last_date.split('T')[0] if last_date != "NEVER" else "NEVER"

        # 3. Print with consistent alignment
        if len(base_path) > 58:
            # Long path: Print tags and path on line 1, stats on line 2
            print(f"{tag_short:<6} {base_path}")
            print(f"{'':<6} {'':<60} {len(unique_bags):>6} {format_bytes(total_bytes):>12} {display_date:>18}")
        else:
            # Standard path: Everything on one line
            print(f"{tag_short:<6} {base_path:<60} {len(unique_bags):>6} {format_bytes(total_bytes):>12} {display_date:>18}")

    print("-" * 115)
    print("TAG KEY: (M)utable, (I)mmutable, (C)ompress, (E)ncrypt, (L)ocked")
    print("="*115 + "\n")

def log_aws_transaction(action, archive_key, size_bytes, etag, response_metadata, aukive):
    """
    Appends a permanent, auditable NDJSON record to the transaction ledger.
    Ensures directory existence and immediate disk flushing for audit integrity.
    """
    # Define log path relative to your config
    log_dir = os.path.join(os.path.dirname(config_path), 'logs')
    log_file = os.path.join(log_dir, 'aws.log')
    
    # Ensure logs directory exists (mkdir -p logic)
    os.makedirs(log_dir, exist_ok=True)

    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "aukive": aukive,
        "action": action.upper(),
        "archive_key": archive_key,
        "system": SYSTEM_NAME,
        "version": VERSION,
        "size_bytes": size_bytes,
        "local_host": socket.gethostname(),

        # Amazon tracking
        "etag": etag,                                          # Content hash gen by Amazon used to verify file integrity
        "aws_account_id": AWS_ACCOUNT_ID,                      # Your unique 12-digit AWS identifier
        "aws_region": AWS_REGION,                              # The specific AWS data center (e.g., us-east-1)
        "request_id": response_metadata.get('RequestId', 'N/A'),    # AWS transaction ID for this event
        "aws_host_id": response_metadata.get('HostId', 'N/A'),      # The specific S3 node that processed bits
        "storage_class": response_metadata.get('StorageClass', 'DEEP_ARCHIVE'), # Verification of the "cold" tier logic
        "server_side_encryption": response_metadata.get('ServerSideEncryption', 'AES256'), # Method used to "seal" the bits
        "amazon_size": response_metadata.get('ContentLength', size_bytes) # The exact byte count Amazon committed to disk
    }

    try:
        # Open in append mode; NDJSON (newline-delimited) format
        with open(log_file, "a") as f:
            f.write(json.dumps(entry) + "\n")
            f.flush()  # Force the OS to commit the write to disk immediately
    except Exception as e:
        # Log failure must not crash the 2.24 TB production transfer
        print(f"\n    [!] LOGGING ERROR: Could not write to {log_file} ({e})")

def is_branch_ripe(branch_line, inventory, config):
    """
    Gatekeeper: Checks if a branch is old enough to be processed based on 'last_scan'.
    Returns (Boolean, Reason_String).
    """
    # 1. Get Interval (Default 190 days)
    try:
        interval_days = int(config.get('settings', 'scan_interval_days', fallback=190))
    except ValueError:
        interval_days = 190

    # 2. Check History
    last_scan_str = None
    if branch_line in inventory.get("branches", {}):
        last_scan_str = inventory["branches"][branch_line].get("last_scan")

    # 3. Decision Logic
    if not last_scan_str:
        return True, "[NEW/NEVER SCANNED]"
    
    try:
        last_scan = datetime.fromisoformat(last_scan_str)
        age = (datetime.now() - last_scan).days
        
        if age >= interval_days:
            return True, f"[MATURE] Last scan: {age} days ago (Interval: {interval_days})"
        else:
            return False, f"[FRESH] Last scan: {age} days ago (Interval: {interval_days})"
            
    except ValueError:
        return True, "[ERROR] Invalid date format in inventory"

def is_action_permitted(branch_line, action):
    """
    The 'Guard Gate': Evaluates if an operation is permitted based on tree.txt tags.
    
    Actions:
    - MIRROR:  Standard synchronization of local files to S3 (Soft Mirror).
    - FORCE:    Destructive reset of S3/Inventory state before a fresh mirror (Hard Mirror).
    - DELETE:  Permanent removal of data from S3 and the local inventory database.
    - REPACK:  Optimization of existing bags to reduce storage waste.
    """
    # Extract tags from the branch line (e.g., ::LOCKED ::COMPRESS)
    tags = [t.strip().upper() for t in branch_line.split(" ::")[1:]]
    is_locked = "LOCKED" in tags
                
    # List of operations that are forbidden when a branch is in a 'LOCKED' state
    restricted_actions = ["MIRROR", "FORCE", "DELETE", "REPACK"]

    if is_locked and action in restricted_actions:
        return False, "Branch is LOCKED. Remove ::LOCKED from tree.txt to modify or mirror."
    
    return True, None

def perform_tree_delete(inventory, config, is_live, tree_lines):
    """
    Orchestrates a global purge of all UNLOCKED branches.
    """
    print("\n" + "!"*65)
    print("!!! GLOBAL DELETE INITIATED: PROCESSING ALL UNLOCKED BRANCHES !!!")
    print("!"*65)

    unlocked_branches = []
    locked_count = 0

    # 1. Sort branches into Unlocked (target) and Locked (safe)
    for b_key in list(inventory.get("branches", {}).keys()):
        # Check tree.txt for the ::LOCKED tag
        target_line = next((l for l in tree_lines if b_key.split(" ::")[0] in l), b_key)
        allowed, _ = is_action_permitted(target_line, "FORCE")        

        if allowed:
            unlocked_branches.append(b_key)
        else:
            locked_count += 1

    if not unlocked_branches:
        print(f"\n[OK] No unlocked branches found to delete. ({locked_count} branches are safely LOCKED).")
        return False

    # 2. Global Financial Impact Report
    all_unlocked_bags = set()
    for b in unlocked_branches:
        for l in inventory["branches"][b]["leaves"].values():
            if l.get("tar_id"):
                all_unlocked_bags.add(l["tar_id"])
    
    stats = get_branch_financials(inventory, all_unlocked_bags, config)
    
    print(f"\nFOUND: {len(unlocked_branches)} Unlocked Branches")
    print(f"SAFE:  {locked_count} Locked Branches (Will NOT be touched)")
    print("-" * 65)
    print(f"TOTAL VOLUME TO PURGE: {stats['total_gb']:.2f} GB")
    if stats['max_days_remaining'] > 0:
        print(f"NOTICE: There are up to {stats['max_days_remaining']}-days remaining in early deletion periods.")
        print(f"        Total early deletion fees: ${stats['total_penalty']:.2f}")
    else:
        print(f"NOTICE: 180-day commitment met for all unlocked data. No penalty.")
    print("-" * 65)

    if not is_live:
        print("!!! DRY RUN: No actual S3 deletions will occur !!!")

    try:
        confirm = input(f"ARE YOU SURE you want to wipe all {len(unlocked_branches)} unlocked branches? [y/N]: ")
    except (KeyboardInterrupt, EOFError):
        print("\n\n[ABORTED] User interrupted. No changes made.")
        return False
    if confirm.lower() != 'y':
        print("Aborted.")
        return False

    # 3. Execution: Batch process using the existing branch reset logic
    for b_key in unlocked_branches:
        perform_branch_reset(inventory, b_key, config, is_live, tree_lines)

    return True

def get_tree_size(path):
    """Fast directory summation using scandir for large-scale leaf staging."""
    total = 0
    try:
        if os.path.exists(path):
            if os.path.isdir(path):
                # Using scandir is significantly faster for 250k files than os.walk
                for entry in os.scandir(path):
                    if entry.is_file(follow_symlinks=False):
                        total += entry.stat().st_size
                    elif entry.is_dir(follow_symlinks=False):
                        total += get_tree_size(entry.path)
            else:
                total = os.path.getsize(path)
    except (OSError, PermissionError):
        pass
    return total

def run_smart_cron(inventory, tree_lines, config, args, encryption_config=None):
    """
    Rolling Scheduler: Checks 'last_scan' of each branch.
    Only processes branches that have been dormant for > INTERVAL days.
    
    Returns:
        bool: True if any work was performed, False otherwise
    """
    # Get scan interval setting
    interval_days = get_scan_interval(config)
    
    # Identify branches that need processing
    eligible_branches = find_eligible_branches(inventory, tree_lines, interval_days)
    
    # Process eligible branches
    return process_eligible_branches(eligible_branches, inventory, tree_lines, args, encryption_config)


def get_scan_interval(config):
    """
    Get the scan interval from config.
    
    Returns:
        int: Scan interval in days (default 190)
    """
    try:
        return int(config.get('settings', 'scan_interval_days', fallback=190))
    except ValueError:
        return 190


def find_eligible_branches(inventory, tree_lines, interval_days):
    """
    Identify branches that are eligible for scanning based on their age.
    
    Args:
        inventory: The inventory dictionary
        tree_lines: List of branch definitions from tree.txt
        interval_days: Minimum age in days to be eligible for scanning
    
    Returns:
        list: List of eligible branch definitions
    """
    eligible_branches = []
    now = datetime.now()
    
    for line in tree_lines:
        line = line.strip()
        # Skip comments or empty lines
        if not line or line.startswith("#"):
            continue
            
        # Get branch status and decision
        should_scan, reason = is_branch_due_for_scan(inventory, line, interval_days, now)
        
        # Add to eligible list if needed
        if should_scan:
            eligible_branches.append({
                'line': line,
                'reason': reason
            })
    
    return eligible_branches


def is_branch_due_for_scan(inventory, branch_line, interval_days, current_time=None):
    """
    Determine if a branch needs scanning based on its last scan timestamp.
    
    Args:
        inventory: The inventory dictionary
        branch_line: Branch definition from tree.txt
        interval_days: Minimum age in days to be eligible for scanning
        current_time: Current datetime (for testing, defaults to now)
    
    Returns:
        tuple: (should_scan (bool), reason (str))
    """
    if current_time is None:
        current_time = datetime.now()
    
    # Get last scan timestamp
    last_scan_str = None
    if branch_line in inventory.get("branches", {}):
        last_scan_str = inventory["branches"][branch_line].get("last_scan")

    # Never scanned before
    if not last_scan_str:
        return True, "[NEW/NEVER SCANNED]"
    
    # Check age
    try:
        last_scan = datetime.fromisoformat(last_scan_str)
        age_days = (current_time - last_scan).days
        
        if age_days >= interval_days:
            return True, f"[MATURE] Last scan: {age_days} days ago (Interval: {interval_days})"
        else:
            return False, f"[FRESH] Last scan: {age_days} days ago (Interval: {interval_days})"
    except ValueError:
        return True, "[ERROR] Invalid date format in inventory"


def process_eligible_branches(eligible_branches, inventory, tree_lines, args, encryption_config):
    """
    Process branches that are eligible for scanning.
    
    Args:
        eligible_branches: List of eligible branch definitions
        inventory: The inventory dictionary
        tree_lines: List of branch definitions from tree.txt
        args: Command-line arguments
    
    Returns:
        bool: True if any branches were processed, False otherwise
    """
    if not eligible_branches:
        return False
    
    run_count = 0
    
    for branch in eligible_branches:
        line = branch['line']
        reason = branch['reason']
        
        print(f"\n>>> TRIGGERING: {line}")
        print(f"    Reason: {reason}")
        
        # Check if branch is locked
        allowed, msg = is_action_permitted(line, "MIRROR")
        if not allowed:
            print(f"    [SKIPPING] {msg}")
            continue
        
        # Initialize stats tracking for this branch
        branch_stats = {}
        
        # Process the branch
        process_branch(
            line, 
            inventory, 
            branch_stats, 
            args.run, 
            args.limit, 
            False,  # is_repack
            PASSPHRASE_FILE,
            encryption_config
        )
        
        run_count += 1
    
    if run_count > 0:
        print(f"\n[DONE] Smart Cron processed {run_count} branches.")
        
    return run_count > 0

def get_restore_targets(args, inventory):
    """
    Function New #1: The Resolver.
    Translates user intent (--restore-file/bag/branch) into a standardized list of Restore Jobs.
    
    Returns:
        dict: { 
            'bag_id': {
                'archive_key': str,       # S3 Key
                'tier': str,              # Standard/Bulk
                'files_to_extract': list, # Specific files (or None for "All")
                'dest_root': str          # Where to put them
            } 
        }
    """
    # 1. Validation
    if not args.to:
        print("\n[ERROR] Restore operations require a destination.")
        print("        Please add: --to \"/path/to/restore/folder\"")
        sys.exit(1)

    dest_dir = os.path.abspath(args.to)
    if not os.path.exists(dest_dir):
        try:
            os.makedirs(dest_dir)
        except OSError as e:
            print(f"[ERROR] Could not create destination directory: {e}")
            sys.exit(1)

    targets = {}
    tier = args.tier
    
    # --- MODE A: SINGLE FILE LOOKUP ---
    if args.restore_file:
        search_term = args.restore_file
        print(f"\n--- Smart Restore: Searching for '{search_term}' ---")
        
        found_bag_id = None
        exact_file_path = None

        # Scan Manifests (Reusing logic from find_file but capturing data)
        if not os.path.exists(MANIFEST_DIR):
            print(f"[ERROR] Manifest directory missing: {MANIFEST_DIR}")
            sys.exit(1)

        manifests = [f for f in os.listdir(MANIFEST_DIR) if f.endswith(".txt")]
        
        for manifest in tqdm(manifests, desc="  Scanning Manifests", unit="file", leave=False):
            try:
                path = os.path.join(MANIFEST_DIR, manifest)
                with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        if search_term.lower() in line.lower():
                            # Extract Bag ID from filename (e.g. 20260125_hostname_bag_00045_liverun.txt)
                            # We assume standard naming: ..._bag_XXXXX...
                            if "_bag_" in manifest:
                                parts = manifest.split("_bag_")
                                if len(parts) > 1:
                                    # Extract "00045"
                                    raw_id = parts[1].split("_")[0]
                                    found_bag_id = f"bag_{int(raw_id):05d}"
                                    exact_file_path = line.strip()
                                    break
            except: continue
            if found_bag_id: break
        
        if found_bag_id:
            print(f"  [FOUND] File located in {found_bag_id}")
            # Now find the S3 Key for this bag in the inventory
            s3_key = None
            for branch_data in inventory.get("branches", {}).values():
                for leaf in branch_data.get("leaves", {}).values():
                    if leaf.get("tar_id") == found_bag_id:
                        s3_key = leaf.get("archive_key")
                        break
                if s3_key: break
            
            if s3_key:
                targets[found_bag_id] = {
                    "archive_key": s3_key,
                    "tier": tier,
                    "files_to_extract": [exact_file_path], # Surgical extraction
                    "dest_root": dest_dir
                }
            else:
                print(f"  [ERROR] Bag {found_bag_id} found in manifests but MISSING from inventory.json.")
        else:
            print(f"  [404] File not found in any local manifest.")

    # --- MODE B: SPECIFIC BAGS ---
    elif args.restore_bag:
        requested_bags = []
        for b in args.restore_bag:
            # Normalize inputs (45 -> bag_00045, bag_45 -> bag_00045)
            try:
                norm_id = f"bag_{int(b.lower().replace('bag_', '')):05d}"
                requested_bags.append(norm_id)
            except ValueError:
                print(f"  [WARN] Invalid bag ID format: {b}")

        print(f"\n--- Smart Restore: Locating {len(requested_bags)} Bag(s) ---")
        
        # Scan inventory to resolve S3 Keys
        for branch_data in inventory.get("branches", {}).values():
            for leaf in branch_data.get("leaves", {}).values():
                tid = leaf.get("tar_id")
                if tid in requested_bags and tid not in targets:
                    targets[tid] = {
                        "archive_key": leaf.get("archive_key"),
                        "tier": tier,
                        "files_to_extract": None, # Extract everything
                        "dest_root": dest_dir
                    }

    # --- MODE C: FULL BRANCH ---
    elif args.restore_branch:
        target_branch = args.restore_branch
        print(f"\n--- Smart Restore: Resolving Branch '{target_branch}' ---")
        
        # Fuzzy match the branch name
        matched_key = None
        for k in inventory.get("branches", {}).keys():
            if target_branch in k: 
                matched_key = k
                break
        
        if matched_key:
            print(f"  [MATCH] Found: {matched_key}")
            leaves = inventory["branches"][matched_key].get("leaves", {})
            
            for leaf in leaves.values():
                tid = leaf.get("tar_id")
                akey = leaf.get("archive_key")
                
                if tid and akey and tid not in targets:
                    targets[tid] = {
                        "archive_key": akey,
                        "tier": tier,
                        "files_to_extract": None, # Extract everything
                        "dest_root": dest_dir
                    }
        else:
            print(f"  [ERROR] Branch '{target_branch}' not found in inventory.")

    # MODE D: ENTIRE TREE
    elif args.restore_tree:
        print(f"\n--- Smart Restore: Resolving ENTIRE INVENTORY ---")
        
        # Iterate over EVERY branch in the inventory
        for branch_name, branch_data in inventory.get("branches", {}).items():
            leaves = branch_data.get("leaves", {})
            for leaf in leaves.values():
                tid = leaf.get("tar_id")
                akey = leaf.get("archive_key")
                
                # Add to targets (Dictionary handles deduplication automatically)
                if tid and akey and tid not in targets:
                    targets[tid] = {
                        "archive_key": akey,
                        "tier": tier,
                        "files_to_extract": None, # Extract everything
                        "dest_root": dest_dir
                    }
        print(f"  [PLAN] Found {len(targets)} total bags across all branches.")

    return targets

def check_s3_restore_status(bucket, key):
    """
    Function New #2: The S3 Negotiator.
    Returns: 'READY', 'PENDING', 'FROZEN', or 'STANDARD'
    """
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        
        # 1. Check if it's Glacier/Deep Archive
        storage_class = response.get('StorageClass', 'STANDARD')
        if storage_class not in ['GLACIER', 'DEEP_ARCHIVE']:
            return 'STANDARD' # Always ready

        # 2. Check Restore Status
        # restore_string example: 'ongoing-request="true"' OR 'ongoing-request="false", expiry-date="..."'
        restore_string = response.get('Restore')
        
        if restore_string is None:
            return 'FROZEN'
            
        if 'ongoing-request="true"' in restore_string:
            return 'PENDING'
            
        if 'ongoing-request="false"' in restore_string:
            return 'READY'
            
        return 'FROZEN' # Fallback

    except Exception as e:
        print(f"  [ERROR] Failed to check status of {key}: {e}")
        return 'ERROR'

def initiate_thaw(bucket, key, tier):
    """Sends the restore request to AWS."""
    try:
        s3_client.restore_object(
            Bucket=bucket, 
            Key=key,
            RestoreRequest={
                'Days': 7, # Keep thawed for 1 week
                'GlacierJobParameters': {'Tier': tier}
            }
        )
        return True
    except Exception as e:
        print(f"  [ERROR] Thaw request failed: {e}")
        return False

def process_local_restore(bag_path, dest_root, passphrase_file, specific_files=None):
    """
    Function New #3: The Pipeline Manager.
    1. Unpacks the outer Bag.
    2. Identifies leaves (Encrypted? Compressed?).
    3. Processes them into the destination.
    """
    # 1. Create a temp zone for the Bag contents
    bag_id = os.path.basename(bag_path).replace(".tar", "")
    temp_dir = os.path.join(STAGING_DIR, f"restore_{bag_id}")
    if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)

    print(f"  [PROCESS] Unpacking bag to staging area...")
    try:
        subprocess.run(["tar", "-xf", bag_path, "-C", temp_dir], check=True)
    except subprocess.CalledProcessError:
        print("  [FATAL] Failed to unpack bag. Corrupt download?")
        return

    # 2. Walk the leaves found inside
    for root, dirs, files in os.walk(temp_dir):
        for file in files:
            leaf_path = os.path.join(root, file)
            
            # Determine relative path for destination
            # If leaf is at temp/home/user/doc.tar.gz -> dest/home/user/
            rel_dir = os.path.relpath(root, temp_dir)
            final_dest = os.path.join(dest_root, rel_dir)
            if not os.path.exists(final_dest): os.makedirs(final_dest)

            # PIPELINE CONSTRUCTION
            # We build a command string based on extensions
            cmd_pipeline = []
            
            # INPUT: The file on disk
            current_input = shlex.quote(leaf_path)
            
            # STAGE 1: Decryption
            if file.endswith(".gpg"):
                if not passphrase_file:
                    print(f"  [SKIP] Encrypted leaf found but no key.txt: {file}")
                    continue
                # gpg -d --passphrase-file key.txt --batch [INPUT]
                cmd_pipeline.append(f"gpg -d --quiet --batch --passphrase-file {shlex.quote(passphrase_file)} {current_input}")
                current_input = "-" # Subsequent commands read from stdin
            
            # STAGE 2: Decompression
            # Check file name stripped of .gpg (e.g., leaf.tar.gz.gpg -> leaf.tar.gz)
            base_name = file.replace(".gpg", "")
            if base_name.endswith(".gz") or base_name.endswith(".tgz"):
                if cmd_pipeline:
                    cmd_pipeline.append("gunzip")
                else:
                    cmd_pipeline.append(f"gunzip -c {current_input}")
                    current_input = "-"

            # STAGE 3: Extraction (Untar)
            # Standard tar extraction reading from Stdin or File
            # 1. Cleanly quote the destination directory
            safe_dest = shlex.quote(final_dest)
            extract_cmd = f"tar -x -C {safe_dest}"

            # 2. Build file arguments safely
            if specific_files:
                # Use shlex.quote on every individual filename to handle spaces/apostrophes
                quoted_files = [shlex.quote(os.path.basename(f)) for f in specific_files]
                file_args = " ".join(quoted_files)
                extract_cmd += f" {file_args}"

            if cmd_pipeline:
                # Chain: GPG | GUNZIP | TAR
                cmd_pipeline.append(extract_cmd)
                full_cmd = " | ".join(cmd_pipeline)
            else:
                # Direct Tar: tar -xf file.tar -C dest [files...]
                # Note: current_input should have been quoted when it was defined!
                full_cmd = f"tar -xf {current_input} -C {safe_dest}"
                if specific_files:
                    full_cmd += f" {file_args}"

            # EXECUTE
            print(f"  [EXTRACT] Processing leaf: {file}")
            try:
                # Now full_cmd is a perfectly escaped shell string
                subprocess.run(full_cmd, shell=True, check=True, stderr=subprocess.PIPE)
            except subprocess.CalledProcessError as e:
                print(f"  [ERROR] Failed to extract {file}: {e.stderr.decode().strip()}")


    # Cleanup staging
    shutil.rmtree(temp_dir)

def perform_restore_orchestration(restore_jobs, config, passphrase_file):
    """
    Function New #4: The Conductor.
    Iterates through jobs, checks status, initiates thaws, or downloads & extracts.
    """
    s3_bucket = config['settings']['s3_bucket']
    
    print(f"\n--- EXECUTION PHASE ---")
    
    pending_count = 0
    ready_count = 0
    frozen_count = 0

    for bag_id, job in restore_jobs.items():
        key = job['archive_key']
        tier = job['tier']
        dest = job['dest_root']
        
        print(f"\n[JOB] Bag: {bag_id} | Dest: {dest}")
        
        # 1. Check Status
        status = check_s3_restore_status(s3_bucket, key)
        print(f"  [STATUS] S3 Object is: {status}")
        
        if status == 'FROZEN':
            print(f"  [ACTION] Requesting Thaw ({tier})...")
            if initiate_thaw(s3_bucket, key, tier):
                print(f"  [OK] Request sent. Check back in 12-48 hours.")
                frozen_count += 1
            else:
                print(f"  [FAIL] Could not request thaw.")
        
        elif status == 'PENDING':
            print(f"  [WAIT] Thaw in progress. AWS is retrieving data from tape.")
            pending_count += 1
            
        elif status in ['READY', 'STANDARD']:
            print(f"  [READY] Downloading...")
            ready_count += 1
            
            # Download to STAGING_DIR/bag_ID.tar
            local_tar = os.path.join(STAGING_DIR, f"{bag_id}.tar")
            
            try:
                # Visual download bar
                file_size = s3_client.head_object(Bucket=s3_bucket, Key=key)['ContentLength']
                with tqdm(total=file_size, unit='B', unit_scale=True, desc="  Downloading") as pbar:
                    s3_client.download_file(
                        s3_bucket, 
                        key, 
                        local_tar,
                        Callback=lambda bytes_transferred: pbar.update(bytes_transferred)
                    )
                
                # Process the bag
                process_local_restore(local_tar, dest, passphrase_file, job['files_to_extract'])
                
                # Cleanup download
                os.remove(local_tar)
                print(f"  [SUCCESS] Data restored to {dest}")
                
            except Exception as e:
                print(f"  [ERROR] Download/Process failed: {e}")

    # Summary
    print("\n" + "="*60)
    print(f"RESTORE BATCH SUMMARY")
    print(f"  Requested (Frozen): {frozen_count}")
    print(f"  Pending (Wait):     {pending_count}")
    print(f"  Restored (Done):    {ready_count}")
    print("="*60)

def cleanup_staging_dir(staging_path):
    """
    Removes orphaned temp files and directories from previous runs.
    Only prints output if items are actually found and removed.
    """
    if not os.path.exists(staging_path):
        return
        
    # 1. Identify all matching items first
    patterns = ["comp_*", "stage_*", "enc_*", "bundle_*", "*.tar"]
    items_to_delete = []
    
    for pattern in patterns:
        full_pattern = os.path.join(staging_path, pattern)
        items_to_delete.extend(glob.glob(full_pattern))

    # 2. Only proceed with printing if there's work to do
    if not items_to_delete:
        return # Silent exit for a clean terminal

    print(f"  [INIT] Clearing staging directory: {staging_path}")
    deleted_count = 0
    
    try:
        for item in items_to_delete:
            if os.path.isdir(item):
                shutil.rmtree(item)
            else:
                os.remove(item)
            deleted_count += 1
        
        print(f"  [OK] Cleaned {deleted_count} orphaned items.")
            
    except Exception as e:
        print(f"  [WARN] Startup cleanup encountered an issue: {e}")

def load_encryption_config(config_file):
    """Load encryption configuration from the config file"""
    config = configparser.ConfigParser()
    config.read(config_file)
    
    encryption = {}
    if 'encryption' in config:
        encryption['method'] = config['encryption'].get('method', 'password')
        encryption['password_file'] = config['encryption'].get('password_file', 'key.txt')
        encryption['gpg_key_id'] = config['encryption'].get('gpg_key_id', 'backup@glaciermirror.local')
    else:
        # Default to password method if section doesn't exist
        encryption['method'] = 'password'
        encryption['password_file'] = 'key.txt'
        
    return encryption

def encrypt_file(input_file, output_file, config):
    """Encrypt a file using GPG based on configuration"""
    if config['method'] == 'key':
        # Use key-based encryption
        cmd = [
            "gpg", "--batch", "--yes",
            "--recipient", config['gpg_key_id'],
            "--output", output_file,
            "--encrypt", input_file
        ]
        subprocess.run(cmd, check=True)
    else:
        # Use password-based encryption
        password_file = config['password_file']
        if not os.path.exists(password_file):
            raise FileNotFoundError(f"Password file not found: {password_file}")
        
        with open(password_file, 'r') as f:
            password = f.read().strip()
        
        # Create a temporary batch file with the passphrase
        with tempfile.NamedTemporaryFile('w', delete=False) as batch_file:
            batch_file.write(f"%echo Encrypting\n")
            batch_file.write(f"passphrase {password}\n")
            batch_file.write(f"%commit\n")
            batch_path = batch_file.name
        
        try:
            cmd = [
                "gpg", "--batch", "--yes",
                "--passphrase-file", batch_path,
                "--symmetric",
                "--cipher-algo", "AES256",
                "--output", output_file,
                "--encrypt", input_file
            ]
            subprocess.run(cmd, check=True)
        finally:
            # Securely delete the temporary batch file
            os.unlink(batch_path)

def decrypt_file(input_file, output_file, config):
    """Decrypt a file using GPG based on configuration"""
    if config['method'] == 'key':
        # Key-based decryption
        cmd = [
            "gpg", "--batch", "--yes",
            "--output", output_file,
            "--decrypt", input_file
        ]
        subprocess.run(cmd, check=True)
    else:
        # Password-based decryption
        password_file = config['password_file']
        if not os.path.exists(password_file):
            raise FileNotFoundError(f"Password file not found: {password_file}")
            
        with open(password_file, 'r') as f:
            password = f.read().strip()
        
        # Create a temporary batch file with the passphrase
        with tempfile.NamedTemporaryFile('w', delete=False) as batch_file:
            batch_file.write(f"%echo Decrypting\n")
            batch_file.write(f"passphrase {password}\n")
            batch_file.write(f"%commit\n")
            batch_path = batch_file.name
            
        try:
            cmd = [
                "gpg", "--batch", "--yes",
                "--passphrase-file", batch_path,
                "--output", output_file,
                "--decrypt", input_file
            ]
            subprocess.run(cmd, check=True)
        finally:
            # Securely delete the temporary batch file
            os.unlink(batch_path)

def generate_gpg_key():
    """Generate a new GPG key pair for backup encryption"""
    print("Generating new GPG key pair...")
    print("You'll be asked for details about the key.")
    
    subprocess.run(["gpg", "--full-generate-key"], check=True)
    
    print("\nKey generated. Now run with --show-key-id to see your key ID.")
    print("Then update your glacier.cfg file with this key ID.")

def show_gpg_key_id(key_id=None):
    """Show information about available GPG keys"""
    if key_id:
        print(f"Looking up information for key: {key_id}")
        subprocess.run(["gpg", "--list-keys", key_id])
    else:
        print("Available GPG keys:")
        subprocess.run(["gpg", "--list-keys"])

def export_gpg_key(output_file, key_id, private=False):
    """
    Export a GPG key to a file
    
    Args:
        output_file: Path to save the exported key
        key_id: ID of the key to export
        private: If True, exports private key; if False, exports public key
    """
    if key_id is None:
        print("Error: No GPG key ID specified in config.")
        print("Please set gpg_key_id in the [encryption] section of your config file,")
        print("or specify a key ID with --key-id.")
        return False
    
    if private:
        print("CAUTION: You are exporting a private key.")
        print("This key provides full access to decrypt your backups.")
        print("Keep this file secure and delete it after use!")
        confirm = input("Type 'I UNDERSTAND' to continue: ")
        if confirm != "I UNDERSTAND":
            print("Export cancelled.")
            return False
        
        cmd = ["gpg", "--armor", "--export-secret-keys", key_id, "--output", output_file]
    else:
        cmd = ["gpg", "--armor", "--export", key_id, "--output", output_file]
    
    try:
        subprocess.run(cmd, check=True)
        
        # Set appropriate permissions
        if private:
            os.chmod(output_file, 0o600)  # Restrictive permissions for private key
            print(f"Private key exported to {output_file}")
            print("IMPORTANT: This file provides full access to decrypt your backups.")
            print("Secure this file and delete it when no longer needed.")
        else:
            os.chmod(output_file, 0o644)  # More permissive for public key
            print(f"Public key exported to {output_file}")
            print("This file can be safely shared with systems that need to encrypt data.")
        
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error exporting key: {e}")
        return False

def main():

    parser = argparse.ArgumentParser(
        description=f"{SYSTEM_NAME} v{VERSION}\n{SYSTEM_DESCRIPTION}", 
        formatter_class=argparse.RawTextHelpFormatter
    )

    # --- 1. MIRRORING SUITE ---
    mirror_group = parser.add_argument_group('Mirroring')
    mirror_group.add_argument("--mirror-tree", action="store_true", help="Mirror all unlocked branches (default action)")
    mirror_group.add_argument("--mirror-branch", type=str, metavar='NAME', help="Mirror a specific branch to S3")
    mirror_group.add_argument("--mirror-bag", nargs='+', metavar='ID', help="Mirror specific bag IDs to S3")
    mirror_group.add_argument("--force-reset", action="store_true", help="With --mirror: Wipe S3 + Inventory then mirror.")
    mirror_group.add_argument("--cron", action="store_true", help="Only runs if older than scan_interval_days (glacier.cfg)")
    mirror_group.add_argument('--interval', type=int, help='Override scan_interval_days for use with --cron')

    # --- 2. RESTORE SUITE ---
    restore_group = parser.add_argument_group('Restoring')
    restore_group.add_argument("--restore-file", type=str, metavar="FILENAME", help="Find and recover a specific file")
    restore_group.add_argument("--restore-bag", nargs='+', metavar="BAG_ID", help="Recover specific bag(s)")
    restore_group.add_argument("--restore-branch", type=str, metavar="PATH", help="Recover a branch")
    restore_group.add_argument("--restore-tree", action="store_true", help="Recover all branches")
    restore_group.add_argument("--to", type=str, metavar="PATH", help="Destination directory (Required for restore)")
    restore_group.add_argument("--tier", type=str, choices=["Standard", "Bulk"], default="Standard", help="AWS Retrieval Tier (Default: Standard)")

    # --- 3. DELETE SUITE ---
    delete_group = parser.add_argument_group('Deleting')
    delete_group.add_argument("--delete-bag", nargs='+', metavar="ID", help="Delete bag(s) from S3 and inventory")
    delete_group.add_argument("--delete-branch", type=str, metavar="NAME", help="Delete a branch from S3 and inventory")
    delete_group.add_argument("--delete-tree", action="store_true", help="Delete all unlocked branches from S3 and inventory")

    # --- 4. VIEW SUITE ---
    view_group = parser.add_argument_group('Showing')
    view_group.add_argument('--show-tree', action='store_true', help='Show archive tree')
    view_group.add_argument('--show-branch', type=str, metavar="NAME", help='Show branch contents')
    view_group.add_argument('--show-leaf', type=str, metavar="PATH", help='Show leaf contents')
    view_group.add_argument('--show-bag', type=str, metavar="ID", help='Show leaves in a bag')

    # --- 5. GPG MANAGEMENT
    key_group = parser.add_argument_group('GPG Management')
    key_group.add_argument('--generate-gpg-key', action='store_true', help='Generate a new GPG key pair for backup encryption')
    key_group.add_argument('--show-key-id', action='store_true', help='Show the ID of the currently configured GPG key')
    key_group.add_argument('--export-key', metavar='FILE', help='Export a GPG key to a file')
    key_group.add_argument('--key-type', choices=['public', 'private'], default='public', help='Type of key to export (default: public)')

    # --- 6. MAINTENANCE ---
    mgmt_group = parser.add_argument_group('Maintenance')
    mgmt_group.add_argument("--run", action="store_true", help="Trigger live-run (otherwise dry-run)")
    mgmt_group.add_argument("--limit", type=int, default=0, help="Upload limit in MB/s")
    mgmt_group.add_argument("--repack", action="store_true", help="Efficiently repack leaf assignments")
    mgmt_group.add_argument("--report", action="store_true", help="S3 usage and cost report")
    mgmt_group.add_argument("--find", type=str, help="Search manifests for a filename")
    mgmt_group.add_argument("--audit", action="store_true", help="Audit S3 files match inventory.json")
    mgmt_group.add_argument("--prune", action="store_true", help="Remove orphaned S3 bags")
    mgmt_group.add_argument("--tree-file", default=DEFAULT_TREE_FILE, help=f"Path to tree definition file (default: {DEFAULT_TREE_FILE})")

    # If no arguments are provided print full help
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    # --- SELECTIVE SILENCE BUFFER ---
    # Capture all stdout. Only release it if work is performed or an error occurs.
    buffer = io.StringIO()
    original_stdout = sys.stdout
    sys.stdout = buffer
    is_unmuzzled = False

    def ensure_unmuzzled():
        nonlocal is_unmuzzled
        if not is_unmuzzled:
            sys.stdout = original_stdout
            print(buffer.getvalue(), end='') # Flush captured buffer
            is_unmuzzled = True

    try:
        # If we are NOT in cron mode, unmuzzle immediately for a snappy CLI feel.
        # This makes the buffer purely a "Cron Safety" mechanism.
        if not args.cron:
            ensure_unmuzzled()

        if args.repack and (args.reset_branch or args.reset_bag):
            ensure_unmuzzled()
            print("\n[FATAL ERROR] Ambiguous Command.")
            print("-" * 60)
            print("You cannot combine the GLOBAL '--repack' flag with LOCAL reset commands.")
            print("")
            print("1. To repack a SINGLE branch line from tree.txt:")
            print("    Use: glacier --reset-branch <name> --run")
            print("    (This automatically repacks that branch during re-upload)")
            print("")
            print("2. To repack YOUR ENTIRE INVENTORY:")
            print("    Use: glacier --repack --run")
            print("-" * 60)
            sys.exit(1)

        if not os.path.exists(args.tree_file):
            ensure_unmuzzled()
            print(f"Fatal: Tree file {args.tree_file} not found.")
            sys.exit(1)

        # This will exit the script if inventory.json is corrupted
        inventory = load_inventory(INVENTORY_FILE)

        # Load encryption configuration
        encryption_config = load_encryption_config(config_path)

        # This will exit the script if encryption is required but key.txt is missing
        encrypt_list_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "encrypt.txt")    
        PASSPHRASE_FILE = validate_encryption_config(args.tree_file)

        with open(args.tree_file, 'r') as f:
            tree_lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]

        # CLI OVERRIDE: If user provides --interval, it overrides the config file for this run
        if args.cron and args.interval is not None:
            config['scan_interval_days'] = args.interval

        # Empty the staging diectory
        if args.run:
          cleanup_staging_dir(STAGING_DIR)

        # --- INTERACTIVE COMMANDS ---
        # These always unmuzzle immediately (handled by the `if not args.cron` check above)
        if args.find:
            find_file(args.find)
            sys.exit(0)

        if args.audit:
            audit_s3(inventory)
            sys.exit(0)

        if args.report:
            generate_full_report(inventory, config)
            sys.exit(0)

        if args.show_tree:
            show_tree(inventory)
            sys.exit(0)

        if args.show_branch:
            show_branch(inventory, args.show_branch)
            sys.exit(0)

        if args.show_bag:
            show_bag(inventory, args.show_bag)
            sys.exit(0)

        if args.show_leaf:
            show_leaf(inventory, args.show_leaf)
            sys.exit(0)

        # For GPG key management commands
        if args.generate_gpg_key:
            generate_gpg_key()
            sys.exit(0)
    
        if args.show_key_id:
            show_gpg_key_id(encryption_config['gpg_key_id'])
            sys.exit(0)

        if args.export_key:
            if args.key_type == 'private':
                export_gpg_key(args.export_key, encryption_config['gpg_key_id'], private=True)
            else:
                export_gpg_key(args.export_key, encryption_config['gpg_key_id'], private=False)
            sys.exit(0)

        if args.restore_file or args.restore_bag or args.restore_branch or args.restore_tree:
            ensure_unmuzzled()
            
            # 1. Resolve Targets (Function New #1)
            restore_jobs = get_restore_targets(args, inventory)
            
            if not restore_jobs:
                print("\n[STOP] No valid restore targets found.")
                sys.exit(1)

            print(f"\n[PLAN] Identified {len(restore_jobs)} target bag(s) for restoration.")
            
            # 2. Orchestration (Placeholder for the next step)
            perform_restore_orchestration(restore_jobs, config, PASSPHRASE_FILE)

            sys.exit(0)
            
        target_only = None 

        # 1. BAG LOGIC (Mirror or Delete)
        if args.mirror_bag or args.delete_bag:
            ensure_unmuzzled() # Always loud
            target_ids = args.mirror_bag if args.mirror_bag else args.delete_bag
            is_delete_only = True if args.delete_bag else False
            
            # Trace the bag back to its branch to see if the archive is sealed
            try:
                first_bag = f"bag_{int(target_ids[0].replace('bag_', '')):05d}"
            except (ValueError, IndexError):
                print(f"[ERROR] Invalid Bag ID: {target_ids[0]}")
                sys.exit(1)

            parent_branch_key = None
            for b_name, b_data in inventory.get("branches", {}).items():
                for l_meta in b_data.get("leaves", {}).values():
                    if l_meta.get("tar_id") == first_bag:
                        parent_branch_key = b_name
                        break
                if parent_branch_key: break
            
            if parent_branch_key:
                target_line = next((l for l in tree_lines if parent_branch_key.split(" ::")[0] in l), None)
                if target_line:
                    # If we are mirroring WITH force-reset, or just deleting, we need PURGE permission
                    action_needed = "FORCE" if (args.force_reset or is_delete_only) else "MIRROR"
                    allowed, reason = is_action_permitted(target_line, action_needed)
                    if not allowed:
                        print(f"\n[!] FORBIDDEN: {reason}")
                        sys.exit(1)

            # Execute: requeue=True for mirror/reset, False for pure delete
            if perform_rebag(inventory, target_ids, config, args.run, requeue=(not is_delete_only)):
                if args.run:
                    with open(INVENTORY_FILE, 'w') as f:
                        json.dump(inventory, f, indent=4)
                
                if is_delete_only: 
                    print(f"  [OK]: Bag(s) {target_ids} deleted from S3 and Inventory.")
                    sys.exit(0)
                
                print(f"  [MIRROR]: Bag(s) reset in inventory. Proceeding to sync...")

        target_only = None

        # 2. BRANCH LOGIC (Mirror or Delete)
        if args.mirror_branch or args.delete_branch:
            target_name = args.mirror_branch if args.mirror_branch else args.delete_branch
            is_delete_only = True if args.delete_branch else False

            # --- IDENTITY TRACE ---
            target_line = next((l for l in tree_lines if target_name in l), None)
            if not target_line:
                target_line = next((k for k in inventory["branches"].keys() if target_name in k), None)

            # --- LOCKED GUARD ---
            action_type = "FORCE" if (args.force_reset or is_delete_only) else "MIRROR"
            if target_line:
                # CRON GATEKEEPER for Single Branch
                if args.cron and not is_delete_only:
                     ripe, reason = is_branch_ripe(target_line, inventory, config)
                     if not ripe:
                         # Silent Exit: Requested branch is fresh
                         sys.exit(0)
                     else:
                         ensure_unmuzzled()
                         print(f"\n>>> CRON TRIGGER: {target_name}")
                         print(f"    {reason}")

                allowed, reason = is_action_permitted(target_line, action_type)
                if not allowed:
                    ensure_unmuzzled()
                    print(f"\n[!] FORBIDDEN: {reason}")
                    sys.exit(1)

            # --- DESTRUCTIVE PHASE ---
            if args.force_reset or is_delete_only:
                ensure_unmuzzled()
                reset_result = perform_branch_reset(inventory, target_name, config, args.run, tree_lines)
                if reset_result and args.run:
                    with open(INVENTORY_FILE, 'w') as f:
                        json.dump(inventory, f, indent=4)
                    status_msg = "purged" if is_delete_only else "reset for fresh mirror"
                    print(f"  [STATUS]: Inventory {status_msg}.")
            
            # --- EXIT or CONTINUE ---
            if is_delete_only: 
                sys.exit(0)
                
            target_only = target_name

        if args.delete_branch:
            ensure_unmuzzled()
            # 1. Guard Gate: Find the line in tree.txt to check for ::LOCKED
            target_line = next((l for l in tree_lines if args.delete_branch in l), None)
            
            # If not in tree.txt, check the inventory keys to be sure
            if not target_line:
                target_line = next((k for k in inventory["branches"].keys() if args.delete_branch in k), None)

            if target_line:
                allowed, reason = is_action_permitted(target_line, "FORCE")
                if not allowed:
                    print(f"\n[!] FORBIDDEN: {reason}")
                    sys.exit(1)

            # 2. Execution: perform_branch_reset handles the S3 deletes and JSON removal
            deleted_branch_key = perform_branch_reset(inventory, args.delete_branch, config, args.run, tree_lines)
            
            if deleted_branch_key:
                if args.run:
                    with open(INVENTORY_FILE, 'w') as f:
                        json.dump(inventory, f, indent=4)
                    print(f"  [STATUS] Branch '{args.delete_branch}' deleted from inventory.")
                else:
                    print(f"  [DRY RUN] Delete analysis complete.")
            sys.exit(0)

        if args.delete_tree:
            ensure_unmuzzled()
            if perform_tree_delete(inventory, config, args.run, tree_lines):
                if args.run:
                    with open(INVENTORY_FILE, 'w') as f:
                        json.dump(inventory, f, indent=4)
                    print(f"\n[OK] Global tree purge complete. Inventory updated.")
                else:
                    print(f"\n[DRY RUN] Global analysis complete.")
            sys.exit(0)

        if args.prune:
            ensure_unmuzzled()
            perform_prune(inventory, args.run, False)
            sys.exit(0)

        run_stats = {}
        work_was_performed = False

        if not args.run: 
            # If not --run, unmuzzle to show warning
            ensure_unmuzzled()
            print("!!! DRY-RUN MODE (Pass --run to execute) !!!")

        for line in tree_lines:
            current_path = line.split(' ::')[0].strip()        

            allowed, reason = is_action_permitted(line, "MIRROR")
            
            if not allowed:
                # In cron mode, locked branches are skipped silently.
                if not args.cron:
                    ensure_unmuzzled()
                    print(f"  [LOCKED]: Skipping {current_path}")
                continue

            if target_only:
                target_path = target_only.split(' ::')[0].strip()
                if current_path != target_path:
                    continue
            
        # --- CRON EXECUTION ---
        if args.cron:
            work_was_performed = run_smart_cron(inventory, tree_lines, config, args, encryption_config)
        else:
            # Standard Loop (non-cron)
            for line in tree_lines:
                current_path = line.split(' ::')[0].strip()        
                
                # Check for locks
                allowed, reason = is_action_permitted(line, "MIRROR")
                if not allowed:
                    ensure_unmuzzled()
                    print(f"  [LOCKED]: Skipping {current_path}")
                    continue

                if target_only:
                    target_path = target_only.split(' ::')[0].strip()
                    if current_path != target_path:
                        continue
                
                work_was_performed = True
                process_branch(line, inventory, run_stats, args.run, args.limit, args.repack, PASSPHRASE_FILE, encryption_config)

        # Summary and final artifacts happen after all lines are processed
        if work_was_performed:
            # Ensure we are visible before printing summary
            ensure_unmuzzled()
            generate_summary(inventory, run_stats, args.run)

            if args.run:
                upload_system_artifacts()

            print("\n[COMPLETE] All backup jobs finished.")
        else:
            # If in Cron mode and no work was done, we exit silently.
            # (Buffer is discarded, nothing printed to real stdout)
            pass

    except Exception as e:
        # EMERGENCY UNMUZZLE: Print whatever was in the buffer + the error
        sys.stdout = original_stdout
        print(buffer.getvalue(), end='') 
        print(f"\n[CRITICAL ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
