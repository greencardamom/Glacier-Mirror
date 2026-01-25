#!/usr/bin/env python3
import os
import sys

try:
    import boto3
    from tqdm import tqdm
except ImportError:
    print("Error: Missing dependencies.")
    print("Please install required packages: pip install -r requirements.txt")
    print("Or ensure your virtual environment is activated.")
    sys.exit(1)

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
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from boto3.s3.transfer import TransferConfig # Added for throttling

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

# System Metadata
VERSION = "1.0"
SYSTEM_NAME = "Aukive Glacier Backup (Amazon S3 Glacier Deep Archive Tape Backup)"
DEFAULT_TREE_FILE = "tree.cfg"  # <--- Chang to rename the file globally

# --- CONFIGURATION LOADER ---
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'glacier.cfg')
config = configparser.ConfigParser()

if not os.path.exists(config_path):
    print(f"Error: Configuration file not found at {config_path}")
    sys.exit(1)

config.read(config_path)

AWS_ACCOUNT_ID, AWS_REGION = ensure_aws_metadata(config)

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
# ----------------------------

# Globals
CURRENT_YEAR = datetime.now().strftime('%Y')
S3_PREFIX = f"{CURRENT_YEAR}-backup/"
BYTES_PER_GB = 1024 * 1024 * 1024
TARGET_SIZE_BYTES = TARGET_BAG_GB * BYTES_PER_GB
s3_client = boto3.client('s3')

# UI Formatting Constants
# Length of "[NET: RSYNC]" is 12. We add 1 for spacing = 13.
STATUS_WIDTH = 13

# --- HELPER FUNCTIONS ---

class Heartbeat(threading.Thread):
    def __init__(self, filepath, target_size, status="Processing", is_dir=False):
        super().__init__()
        self.daemon = True
        self.filepath = filepath
        self.target_size = target_size
        self.status = status
        self.is_dir = is_dir
        self.stop_signal = threading.Event()
        self.is_compressed_phase = False
        
        # Pre-calculate UI elements
        self.tag = f"[{self.status}]"
        self._derive_verbs()
        self.last_line = ""

    def _derive_verbs(self):
        """Sets the active (ing) and past (ed) verbs based on the current mode."""
        if "RSYNC" in self.status:
            self.verb_active = "transferring"
            self.verb_past = "transferred"
        elif "GPG" in self.status:
            self.verb_active = "encrypting"
            self.verb_past = "encrypted"
        elif "TAR" in self.status:
            if self.is_compressed_phase:
                self.verb_active = "compressing"
                self.verb_past = "compressed"
            else:
                self.verb_active = "packaging"
                self.verb_past = "packaged"
        else:
            self.verb_active = "writing"
            self.verb_past = "written"

    def run(self):
        while not self.stop_signal.is_set():
            if os.path.exists(self.filepath):
                try:
                    current = get_tree_size(self.filepath) if self.is_dir else os.path.getsize(self.filepath)
                    ratio = 0.6 if self.is_compressed_phase else 1.0
                    divisor = (self.target_size * ratio) if self.target_size > 0 else 1
                    pct = (current / divisor) * 100
                    if pct > 100.0: pct = 100.0

                    # Use the ACTIVE verb (e.g. "transferring")
                    self.last_line = f"  {self.tag:<{STATUS_WIDTH}}: {pct:.1f}% {self.verb_active} ({format_bytes(current)})    "
                    print(f"{self.last_line:<100}", end="\r", flush=True)
                except (OSError, ZeroDivisionError):
                    pass
            else:
                self.last_line = f"  {self.tag:<{STATUS_WIDTH}}: Preparing stream..."
                print(f"{self.last_line:<100}", end="\r", flush=True)
            
            time.sleep(0.1)

    def snap_done(self):
        # Use the PAST tense verb (e.g. "transferred")
        final_line = f"  {self.tag:<{STATUS_WIDTH}}: 100.0% {self.verb_past} [DONE]"
        print(f"\r{final_line:<100}")

    def update_target(self, new_filepath, new_status=None, is_compressed=True, is_dir=False):
        self.filepath = new_filepath
        self.is_dir = is_dir
        self.is_compressed_phase = is_compressed
        if new_status: 
            self.status = new_status
            self.tag = f"[{self.status}]"
            self._derive_verbs()  # Re-calculate verbs for the new status

    def stop(self):
        self.stop_signal.set()
        self.snap_done()

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
    # This ensures "0README.md" (or any future removal) gets deleted from the cloud.
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

def upload_system_artifacts():
    """Backs up the code, config, and brain to S3 for disaster recovery."""
    print("\n--- System Artifact Backup ---")
    # tree.txt and exclude.txt are treated as optional via the os.path.exists check below
    sys_files = ["glacier.py", "prune.py", "glacier.cfg", "tree.txt", "inventory.json", "0README.md", "README.md", "requirements.txt", "exclude.txt"]
    base_dir = os.path.dirname(os.path.abspath(__file__))
    s3_folder = os.path.join(S3_PREFIX, "system")
    
    for fname in sys_files:
        local_path = os.path.join(base_dir, fname)
        if os.path.exists(local_path):  # <--- This keeps it optional
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

def construct_tar_command(tar_path, leaf_list, branch_root, designator, passphrase_file, branch_leaves, remote_conn, remote_base_path):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    encrypt_list_path = os.path.join(base_dir, "encrypt.txt")
    
    exclude_path = os.path.join(base_dir, "exclude.txt")
    exclude_flag = f"--exclude-from=\"{exclude_path}\"" if os.path.exists(exclude_path) else ""
    
    temp_files_to_clean = []
    tar_sequence = [] 
    
    # State tracking for display styles
    mutable_header_printed = False
    pad_width = STATUS_WIDTH + 2

    total_leaves = len(leaf_list)
    for idx, leaf in enumerate(leaf_list, 1):
        leaf_path = leaf['path']
        rel_path = os.path.relpath(leaf_path, branch_root)
        
        needs_encrypt = check_encryption_needed(leaf_path, designator, encrypt_list_path)
        needs_compress = check_compression_needed(designator)
        
        # --- DISPLAY LOGIC ---
        if needs_encrypt or needs_compress:                    
            # STYLE: Active Ledger (The "Work" Style)
            # We reset the mutable header flag so if we switch back, it prints again (rare mixed case)
            mutable_header_printed = False 
            
            label = f"  > Leaf {idx}/{total_leaves}"
            print(f"\n{label:<{pad_width}}: {leaf_path}")
            
        else:
            # STYLE: Mutable List (The "Fast" Style)
            if not mutable_header_printed:
                label = "  > Leaves"
                print(f"\n{label:<{pad_width}}:")
                mutable_header_printed = True
            
            # Print the indented list item
            # [01/44] format keeps it tidy
            print(f"        [{idx:02d}/{total_leaves}] {leaf_path}")
        # ---------------------

        if needs_encrypt:
            suffix = ".gz.gpg" if needs_compress else ".gpg"
            inner_name = "__BRANCH_ROOT__" + suffix if leaf['is_branch_root'] else f"{rel_path}{suffix}"
            
            cluster_files = leaf['files'] if leaf['is_branch_root'] else None

            temp_file = encrypt_leaf(leaf_path, 
                                     files=cluster_files, 
                                     passphrase_file=passphrase_file, 
                                     compress=needs_compress, 
                                     remote_conn=remote_conn, 
                                     remote_base_path=remote_base_path,
                                     local_branch_root=branch_root,
                                     expected_size=leaf['size']
                        )
            
            if temp_file:
                temp_files_to_clean.append(temp_file)
                base_tmp = os.path.basename(temp_file)
                arg = f"--transform='s#{base_tmp}#{inner_name}#' \"{base_tmp}\""
                tar_sequence.append((STAGING_DIR, arg))
                branch_leaves[leaf['key']]['encrypted'] = True
                branch_leaves[leaf['key']]['compressed'] = needs_compress

        elif needs_compress:
            inner_name = "__BRANCH_ROOT__.tar.gz" if leaf['is_branch_root'] else f"{rel_path}.tar.gz"
            cluster_files = leaf['files'] if leaf['is_branch_root'] else None
            temp_file = compress_leaf(leaf_path, 
                                      files=cluster_files, 
                                      remote_conn=remote_conn, 
                                      remote_base_path=remote_base_path, 
                                      local_branch_root=branch_root, 
                                      expected_size=leaf['size']
                        )            
            
            if temp_file:
                temp_files_to_clean.append(temp_file)
                base_tmp = os.path.basename(temp_file)
                arg = f"--transform='s#{base_tmp}#{inner_name}#' \"{base_tmp}\""
                tar_sequence.append((STAGING_DIR, arg))
                branch_leaves[leaf['key']]['compressed'] = True
                branch_leaves[leaf['key']]['encrypted'] = False

        else:
            # Mutable Passthrough
            branch_leaves[leaf['key']]['encrypted'] = False
            branch_leaves[leaf['key']]['compressed'] = False
            if leaf['is_branch_root']:
                for f in leaf['files']: tar_sequence.append((branch_root, f"\"{f}\""))
            else:
                tar_sequence.append((branch_root, f"\"{rel_path}\""))

    optimized_args = []
    current_context = None
    for context, arg in tar_sequence:
        if context != current_context:
            optimized_args.append(f"-C \"{context}\"")
            current_context = context
        optimized_args.append(arg)

    is_gnu = "GNU" in subprocess.getoutput("tar --version")
    sparse_flag = "-S" if is_gnu else ""

    cmd = f"tar {sparse_flag} {exclude_flag} -cf \"{tar_path}\" {' '.join(optimized_args)}"
    
    return cmd, temp_files_to_clean

def process_bag(bag_num, leaf_list, branch_root, short_name, bag_size_bytes, is_live, branch_leaves, hostname, branch_stats, upload_limit_mb, designator, passphrase_file, remote_conn, remote_base_path):
    safe_prefix = short_name.replace(" ", "_")
    tar_name = f"{hostname}_{safe_prefix}_bag_{bag_num:05d}.tar"
    tar_path = os.path.join(STAGING_DIR, tar_name)
    s3_key = os.path.join(S3_PREFIX, tar_name)
    
    # Standardized Label Formatting
    bag_label_str = "  > Bag"
    pad_width = STATUS_WIDTH + 2
    
    print(f"\n--- Leaf Bag {bag_num:05d} [{format_bytes(bag_size_bytes)}] ---")

    # 1. Manifest Generation
    manifest_leaves = []
    for leaf in leaf_list:
        if leaf['is_branch_root']:
            manifest_leaves.append({'path': branch_root, 'is_branch_root': True, 'files': leaf['files']})
        else:
            manifest_leaves.append({'path': leaf['path'], 'is_branch_root': False})
    generate_real_manifest(tar_name, manifest_leaves, is_live)

    # 2. Inventory Check
    needs_upload = False
    for leaf in leaf_list:
        key = leaf['key']
        if branch_leaves.get(key, {}).get('needs_upload', True):
            needs_upload = True
            break
    
    if not needs_upload:
        print(f"\n{bag_label_str:<{pad_width}}: {tar_name}")
        print(f"  [SKIP] Inventory Match.")
        branch_stats['skip_count'] += 1
        branch_stats['skip_bytes'] += bag_size_bytes
        return

    branch_stats['up_count'] += 1
    branch_stats['up_bytes'] += bag_size_bytes
    
    # Update inventory with new location
    for leaf in leaf_list:
        if leaf['key'] in branch_leaves:
            branch_leaves[leaf['key']]['archive_key'] = s3_key

    if not is_live:
        print(f"\n{bag_label_str:<{pad_width}}: {tar_name}")
        print(f"  [DRY RUN] Would upload: s3://{S3_BUCKET}/{s3_key}")
        return

    # 3. BUILD ARCHIVE
    cmd, temp_files_to_clean = construct_tar_command(tar_path, leaf_list, branch_root, designator, passphrase_file, branch_leaves, remote_conn, remote_base_path)

    try:
        if not os.path.exists(STAGING_DIR): os.makedirs(STAGING_DIR)
        
        # --- Heartbeat for Bag Assembly ---
        # This is the SINGLE Header for this bag
        print(f"\n{bag_label_str:<{pad_width}}: {tar_name}")
        
        hb = Heartbeat(tar_path, bag_size_bytes, status="DISK: TAR")
        hb.start()
        
        try:
            # Silence stdout so Heartbeat owns the screen
            subprocess.run(cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            hb.stop()
            hb.join()
            print(f"\n[FATAL] Tar failed: {e.stderr.decode() if e.stderr else str(e)}")
            raise e
        finally:
            hb.stop()
            hb.join()
        # ----------------------------------
        
        # 4. UPLOAD
        if upload_limit_mb > 0:
            t_config = TransferConfig(max_bandwidth=upload_limit_mb * 1024 * 1024, max_concurrency=10, use_threads=True)
        else:
            t_config = TransferConfig(use_threads=True)

        desc = "  " + "[NET: AWS]".ljust(STATUS_WIDTH)

        file_size = os.path.getsize(tar_path)
        with tqdm(
            total=file_size,
            unit='B',
            unit_scale=True,
            leave=True,
            ncols=100,
            # Ensure bar_format handles the colon cleanly
            bar_format="{desc} {percentage:5.1f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
        ) as pbar:
            pbar.set_description(desc)
            s3_client.upload_file(
                tar_path, 
                S3_BUCKET, 
                s3_key, 
                ExtraArgs={'StorageClass': 'DEEP_ARCHIVE'},
                Config=t_config,
                Callback=lambda bytes_transferred: pbar.update(bytes_transferred)
            )

        # 5. COMMIT SUCCESS
        try:
            print("") # Spacer line

            response = s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
            etag = response.get('ETag', '').replace('"', '') 
            
            metadata = response.get('ResponseMetadata', {})
            
            log_aws_transaction(
                action="UPLOAD", 
                archive_key=s3_key, 
                size_bytes=file_size, 
                etag=etag, 
                response_metadata=metadata,
                aukive="UPL-STD"
            )
            
        except Exception as e:
            etag = "VERIFY_FAILED"
            log_aws_transaction("VERIFY_FAILURE", s3_key, file_size, "N/A", {"Error": str(e)}, "ERR-VFY")

        # Update local manifest/inventory
        os.remove(tar_path)
        for leaf in leaf_list:
            key = leaf['key']
            if key in branch_leaves:
                branch_leaves[key]['needs_upload'] = False
                branch_leaves[key]['last_upload'] = datetime.now().isoformat()
                branch_leaves[key]['etag'] = etag 

        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"  [OK] {timestamp} | Verified ETag: {etag}")

    except Exception as e:
        print(f"[FATAL] {e}")
        sys.exit(1)
    
    finally:
        # 6. CLEANUP
        for f in temp_files_to_clean:
            if os.path.exists(f):
                os.remove(f)

def process_branch(branch_line, inventory, run_stats, is_live, upload_limit_mb, is_repack, passphrase_file):
    """Handles parsing types (MUTABLE, IMMUTABLE), mounting, and processing."""
    
    # --- UPDATED PARSING LOGIC (Supports multiple tags) ---
    full_tags = "MUTABLE"   # Default metadata string (for encryption checks)
    logic_type = "MUTABLE"  # Default scanning logic (for directory handling)

    if " ::" in branch_line:
        parts = branch_line.split(" ::")
        branch_path = parts[0].strip()
        raw_tags = [p.strip().upper() for p in parts[1:]]
        full_tags = " ".join(raw_tags) 
        logic_type = "IMMUTABLE" if "IMMUTABLE" in raw_tags else "MUTABLE"
    else:
        branch_path = branch_line.strip()
        full_tags = "MUTABLE"  # Ensure this isn't empty so logic doesn't break
        logic_type = "MUTABLE"

    # ------------------------------------------------------

    run_stats[branch_line] = {'up_count': 0, 'up_bytes': 0, 'skip_count': 0, 'skip_bytes': 0}
    branch_stats = run_stats[branch_line]

    mount_point_to_cleanup = None
    try:
        scan_path, mount_point_to_cleanup = mount_remote_branch(branch_path)
        
        if not os.path.exists(scan_path):
            print(f"[ERROR] Path not found: {scan_path}")
            return

        short_name = os.path.basename(os.path.normpath(scan_path))
        if ":" in branch_path:
            remote_conn, remote_base_path = branch_path.split(":", 1)
            hostname = remote_conn.split("@")[-1]
        else:
            remote_conn = None
            remote_base_path = None
            hostname = socket.gethostname()

        print(f"------------------------------------------------")
        print(f"Processing [{full_tags}]:")
        print(f"  {branch_path}") 
        print(f"------------------------------------------------")

        if branch_line not in inventory["branches"]:
            inventory["branches"][branch_line] = {"leaves": {}}
        branch_leaves = inventory["branches"][branch_line]["leaves"]

        # 2. IDENTIFY LEAVES BASED ON LOGIC TYPE
        found_leaves = [] 

        if logic_type == "IMMUTABLE":
            # Sovereign Storage: The entire path is a single Leaf.
            found_leaves.append({
                "key": scan_path, "path": scan_path, "is_branch_root": False, "files": []
            })
            
        elif logic_type == "MUTABLE":
            # Shared Storage: Every sub-directory is a unique Leaf.
            try:
                subdirs = [d for d in sorted(os.listdir(scan_path)) if os.path.isdir(os.path.join(scan_path, d))]
                for d in subdirs:
                    full_p = os.path.join(scan_path, d)
                    found_leaves.append({
                        "key": full_p, "path": full_p, "is_branch_root": False, "files": []
                    })
                
                # Catch-all: All loose files in the root become one "Branch Root Leaf".
                loose_files = [f for f in sorted(os.listdir(scan_path)) if os.path.isfile(os.path.join(scan_path, f))]
                if loose_files:
                    cluster_key = os.path.join(scan_path, "__BRANCH_ROOT__")
                    found_leaves.append({
                        "key": cluster_key, "path": scan_path, "is_branch_root": True, "files": loose_files
                    })
            except OSError as e:
                print(f"[ERROR] Could not scan directory {scan_path}: {e}")
                return
        else:
            print(f"[ERROR] Unknown logic type derived from branch: {branch_line}")
            return

        # 3. SCAN METADATA & UPDATE INVENTORY
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

        # 4. ASSIGN BAGS
        if is_repack:
            print("  [REPACK] Ignoring existing leaf bag IDs. Consolidating all leaves...")
            # If repacking, we treat every leaf as if it has no seat assignment
            for leaf in leaves_to_bag:
                leaf["tar_id"] = None
            bag_counter = 1
        else:
            # GLOBAL COUNTER MODE: Find the highest bag number in the ENTIRE inventory
            existing_bag_nums = []
            for branch_data in inventory["branches"].values():
                for unit in branch_data.get("leaves", {}).values():
                    tid = unit.get('tar_id')
                    if tid and tid.startswith('bag_'):
                        try:
                            existing_bag_nums.append(int(tid.split('_')[-1]))
                        except ValueError: pass
            
            bag_counter = max(existing_bag_nums) if existing_bag_nums else 0
            
            # If this specific branch already has bags, we might be continuing its last bag
            # or starting a brand new one. Let's find THIS branch's highest bag.
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

        # 5. GROUP BY BAG ID
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

        # --- WASTE & COST REPORT CALCULATION ---
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

        # 6. EXECUTE BAGS
        sorted_bag_ids = sorted(bags.keys(), key=lambda x: bags[x]["bag_num_int"])

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
                remote_base_path
            )

        if is_repack and is_live:
            print(f"  [CLEANUP] Checking for orphaned tail bags on S3...")
            
            # Construct the specific prefix for THIS branch's bags
            # Pattern: 2026-backup/hostname_shortname_bag_
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


    finally:
        if mount_point_to_cleanup:
            unmount_remote_branch(mount_point_to_cleanup)

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
    """Compares local inventory against actual S3 bucket contents with conditional ETag verification."""
    print(f"\n--- S3 Integrity Audit ---")
    
    # 1. Map expected bags from inventory
    # We use a dict to aggregate leaf sizes and grab the ETag if it exists
    expected_bags = {} 
    for branch, data in inventory.get("branches", {}).items():
        for leaf, details in data.get("leaves", {}).items():
            key = details.get('archive_key')
            if not key:
                continue
                
            inv_etag = details.get('etag') # Returns None if missing (universal)
            size = details.get('size_bytes', 0)
            
            if key not in expected_bags:
                expected_bags[key] = {'size': 0, 'etag': inv_etag}
            expected_bags[key]['size'] += size

    # 2. Get actual list from S3
    print(f"  [FETCHING] Remote file list from s3://{S3_BUCKET}/{S3_PREFIX}...")
    actual_s3_data = {}
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get('Contents', []):
            # S3 returns ETags wrapped in quotes; we strip them for a clean match
            actual_s3_data[obj['Key']] = {
                'size': obj['Size'], 
                'etag': obj['ETag'].replace('"', '')
            }

    # 3. Perform the Audit
    missing = []
    corruption = []
    total_audited = len(expected_bags)
    verified_with_etag = 0
    
    for bag_key, info in expected_bags.items():
        # A) Check Existence
        if bag_key not in actual_s3_data:
            missing.append(bag_key)
            continue
            
        # B) Check Integrity (Only if ETag was recorded in inventory)
        if info['etag']:
            verified_with_etag += 1
            if info['etag'] != actual_s3_data[bag_key]['etag']:
                corruption.append(bag_key)

    # 4. Reporting
    if not missing and not corruption:
        print(f"  [OK] All {total_audited} expected bags are present on S3.")
        if verified_with_etag > 0:
            print(f"  [OK] Cryptographic integrity verified for {verified_with_etag} bags.")
    else:
        if missing:
            print(f"[ALERT] {len(missing)} bags are MISSING from S3!")
            for m in missing: print(f"          MISSING: {m}")
        if corruption:
            print(f"[CRITICAL] {len(corruption)} bags FAILED integrity check!")
            for c in corruption: 
                print(f"          MISMATCH: {c}")
                print(f"          (Inventory: {expected_bags[c]['etag']} vs S3: {actual_s3_data[c]['etag']})")

    # 5. Orphan Check
    orphans = [k for k in actual_s3_data if k not in expected_bags and "system/" not in k and "manifests/" not in k]
    if orphans:
        print(f"  [NOTE] Found {len(orphans)} orphan bags on S3. Run prune.py to clean up.")

def check_compression_needed(line_metadata):
    """Returns True if the COMPRESS tag is present in the branch line."""
    return "COMPRESS" in line_metadata

def compress_leaf(leaf_path, files=None, remote_conn=None, remote_base_path=None, local_branch_root=None, expected_size=0):
    """Creates a temporary .tar.gz of a leaf, using rsync if remote_conn is provided."""
    leaf_id = hashlib.md5(leaf_path.encode()).hexdigest()[:8]
    compressed_path = os.path.join(STAGING_DIR, f"comp_{leaf_id}.tar.gz")
    
    local_raw = None
    target_path = leaf_path
    parent = os.path.dirname(leaf_path)
    base = os.path.basename(leaf_path)

    # Hybrid logic: Sync locally if remote
    if remote_conn and not files: 
        try:
            # Create a dedicated staging subdirectory for this specific leaf
            leaf_stage_dir = os.path.join(STAGING_DIR, f"stage_{leaf_id}")
            if not os.path.exists(leaf_stage_dir): 
                os.makedirs(leaf_stage_dir)
            
            # Use MNT_BASE from your config
            if stage_remote_leaf(remote_conn, remote_base_path, leaf_path, local_branch_root, leaf_stage_dir, expected_size=expected_size):
                # SUCCESS: local_raw is now the directory we need to clean up later
                local_raw = leaf_stage_dir
                target_path = local_raw
                parent = os.path.dirname(local_raw)
                base = os.path.basename(local_raw)
            else:
                return None
        except Exception as e:
            print(f"[ERROR] Local staging failed for {leaf_path}: {e}")
            return None

    # Processing (Now at local SSD speeds if staged)
    if files:
        files_list = " ".join([f"\"{f}\"" for f in files])
        cmd = f"tar -C \"{leaf_path}\" -czf \"{compressed_path}\" {files_list}"
    else:
        cmd = f"tar -C \"{parent}\" -czf \"{compressed_path}\" \"{base}\""

    # 2. Start the Heartbeat
    hb = Heartbeat(compressed_path, expected_size, status="DISK: TAR")
    hb.start()    
        
    try:
        subprocess.run(cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        return compressed_path
    except subprocess.CalledProcessError:
        print(f"\n    [ERROR] Compression failed: {e}")
        return None
    finally:
        hb.stop()
        hb.join()
        if local_raw and os.path.exists(local_raw):
            shutil.rmtree(local_raw)

def check_encryption_needed(leaf_path, line_metadata, encrypt_list_path="encrypt.txt"):
    """Returns True if this leaf matches tree.txt metadata or encrypt.txt patterns."""
    if "ENCRYPT" in line_metadata:
        return True
    
    if os.path.exists(encrypt_list_path):
        # Normalize the leaf path (remove trailing slash)
        normalized_leaf = leaf_path.rstrip('/')
        
        with open(encrypt_list_path, 'r') as f:
            for line in f:
                pattern = line.strip()
                if not pattern or pattern.startswith('#'):
                    continue
                
                # Normalize the pattern path
                if normalized_leaf == pattern.rstrip('/'):
                    return True
                    
    return False

def encrypt_leaf(leaf_path, files=None, passphrase_file=None, compress=False, remote_conn=None, remote_base_path=None, local_branch_root=None, expected_size=0):
    if not passphrase_file: return None
    leaf_id = hashlib.md5(leaf_path.encode()).hexdigest()[:8]
    
    local_raw = None
    target_path = leaf_path
    parent = os.path.dirname(leaf_path)
    base = os.path.basename(leaf_path)

    if remote_conn and not files:
        try:
            leaf_stage_dir = os.path.join(STAGING_DIR, f"stage_{leaf_id}")
            if not os.path.exists(leaf_stage_dir): 
                os.makedirs(leaf_stage_dir)

            if stage_remote_leaf(remote_conn, remote_base_path, leaf_path, local_branch_root, leaf_stage_dir, expected_size=expected_size):
                local_raw = leaf_stage_dir
                target_path = local_raw
                parent = os.path.dirname(local_raw)
                base = os.path.basename(local_raw)
            else:
                return None
        except Exception as e:
            print(f"[ERROR] Local staging failed for {leaf_path}: {e}")
            return None

    # 1. Prepare intermediate file
    tar_flags = "-czf" if compress else "-cf"
    ext = ".tar.gz" if compress else ".tar"
    intermediate_file = os.path.join(STAGING_DIR, f"bundle_{leaf_id}{ext}")
    
    if files:
        files_list = " ".join([f"\"{f}\"" for f in files])
        tar_cmd = f"tar -C \"{leaf_path}\" {tar_flags} \"{intermediate_file}\" {files_list}"
    else:
        tar_cmd = f"tar -C \"{parent}\" {tar_flags} \"{intermediate_file}\" \"{base}\""
    
    # 2. Encrypt
    encrypted_path = os.path.join(STAGING_DIR, f"enc_{leaf_id}.gpg")
    gpg_cmd = ["gpg", "--batch", "--yes", "--passphrase-file", passphrase_file,
               "--symmetric", "--cipher-algo", "AES256", "--output", encrypted_path, intermediate_file]

    # 1. Start the Heartbeat watching the TAR file (intermediate_file)
    # This ensures progress shows up IMMEDIATELY during phase 1.
    hb = Heartbeat(intermediate_file, expected_size, status="CPU: GPG")
    hb.start()

    try:
        # PHASE 1: Create the Tarball
        # Change capture_output to DEVNULL/PIPE to avoid terminal buffering
        subprocess.run(tar_cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    
        # PHASE 2: Switch the Heartbeat to watch the GPG file
        # Also tell it we are now in the compressed phase for accurate % calculation
        hb.update_target(encrypted_path, new_status="CPU: GPG", is_compressed=True)

        subprocess.run(gpg_cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        return encrypted_path
        
    except subprocess.CalledProcessError:
        return None

    finally:
        hb.stop()
        hb.join()
        if os.path.exists(intermediate_file): 
            os.remove(intermediate_file)
        if local_raw and os.path.exists(local_raw): 
            shutil.rmtree(local_raw)

def validate_encryption_config(tree_file, encrypt_list_path):
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

    # 2. Check encrypt.txt for active patterns
    if not needs_crypto and os.path.exists(encrypt_list_path):
        with open(encrypt_list_path, 'r') as f:
            for line in f:
                pattern = line.strip()
                if pattern and not pattern.startswith('#'):
                    needs_crypto = True
                    break

    # 3. Validation Logic Gate
    if needs_crypto:
        if not os.path.exists(passphrase_file) or os.stat(passphrase_file).st_size == 0:
            print(f"\n[FATAL ERROR] Encryption is active, but key file is missing/empty.")
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

def perform_rebag(inventory, target_bag_ids, config, is_live, encrypt_list_path, requeue=True):
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
                    
                    will_encrypt = check_encryption_needed(leaf_key, meta_str, encrypt_list_path)
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

from datetime import datetime, timedelta

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

def stage_remote_leaf(remote_conn, remote_base_path, local_leaf_path, local_branch_root, stage_dir, expected_size=0):
    """
    Staging with Heartbeat-monitored rsync.
    Includes DYNAMIC EXCLUDE REWRITING to handle relative path mismatches.
    Uses subprocess.run to correctly capture rsync errors.
    """
    # 1. Calculate the subpath
    prefix = local_branch_root.rstrip('/') + '/'
    subpath = local_leaf_path.replace(prefix, "").lstrip('/')
    leaf_rel_path = subpath.rstrip('/')
    
    # 2. Reconstruct the actual remote source
    remote_base = remote_base_path.rstrip('/')
    true_remote_src = f"{remote_conn}:{remote_base}/{subpath}".rstrip('/')

    # 3. Initialize Heartbeat
    hb = Heartbeat(stage_dir, expected_size, status="NET: RSYNC", is_dir=True)
    hb.start()

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

    # 4. Build the rsync command 
    cmd = ['rsync', '-a', '--delete', '--inplace', '-e', 'ssh']
    if exclude_args: cmd.extend(exclude_args)
    cmd.extend([f"{true_remote_src}/", f"{stage_dir}/"])

    try:
        # UPDATED: Use subprocess.run with capture_output=True
        # This prevents the deadlock risk of check_call+PIPE and actually captures stderr
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return True
        
    except subprocess.CalledProcessError as e:
        # Now e.stderr will actually contain the text!
        err_msg = e.stderr if e.stderr else str(e)
        
        # Filter out harmless Code 24 (Vanished files) if you want, 
        # but for Code 23 we want to see it.
        if e.returncode == 23:
            print(f"\n[WARN] Partial transfer (Code 23) for {local_leaf_path}.")
            # Print the last few lines of error to help debug
            print(f"       Last error: {err_msg.strip().splitlines()[-1] if err_msg else 'Unknown'}")
            # If you want to FAIL on partials, return False. 
            # If you want to ALLOW partials (ignore permission errors), return True.
            return False 
            
        print(f"\n[ERROR] Sync failed for {local_leaf_path}: {err_msg}")
        return False
        
    finally:
        hb.stop()
        hb.join()
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

import boto3

def ensure_aws_metadata(config):
    """
    Retrieves the current AWS Account ID and Region dynamically.
    Falls back to 'REDACTED' if metadata cannot be fetched.
    """
    try:
        # Create an STS client to get caller identity (Account ID)
        sts = boto3.client('sts')
        account_id = sts.get_caller_identity().get('Account', 'REDACTED')
        
        # Create a session to get the configured region
        session = boto3.Session()
        region = session.region_name if session.region_name else 'REDACTED'
        
        return account_id, region
    except Exception as e:
        # If offline or no credentials, use redacted placeholders
        return "REDACTED", "REDACTED"

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

def is_action_permitted(branch_line, action):
    """
    The 'Guard Gate': Evaluates if an operation is permitted based on tree.txt tags.
    
    Actions:
    - MIRROR:  Standard synchronization of local files to S3 (Soft Mirror).
    - FORCE:   Destructive reset of S3/Inventory state before a fresh mirror (Hard Mirror).
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

def main():

    parser = argparse.ArgumentParser()

    # --- Mirroring Suite ---
    parser.add_argument("--mirror-tree", action="store_true", help="Sync all unlocked branches (default action)")
    parser.add_argument("--mirror-branch", type=str, metavar='NAME', help="Sync a specific branch to S3")
    parser.add_argument("--mirror-bag", nargs='+', metavar='ID', help="Sync specific bag IDs to S3")
    parser.add_argument("--force-reset", action="store_true", help="With --mirror: Wipe S3 + Inventory then mirror.")

    # --- Delete Suite ---
    parser.add_argument("--delete-bag", nargs='+', help="Surgically delete bag(s) from S3 and inventory")
    parser.add_argument("--delete-branch", type=str, help="Wipe a branch from S3 and permanently remove from inventory")
    parser.add_argument("--delete-tree", action="store_true", help="Wipe all unlocked branches from S3 and the inventory")

    # --- View Suite ---
    parser.add_argument('--show-tree', action='store_true', help='Show archive tree')
    parser.add_argument('--show-branch', type=str, help='Show branch contents')
    parser.add_argument('--show-leaf', type=str, help='Show leaf contents')
    parser.add_argument('--show-bag', type=str, help='Show leaves in a bag')

    # --- Management & Maintenance ---
    parser.add_argument("--tree-file", default=DEFAULT_TREE_FILE, help=f"Path to tree definition file (default: {DEFAULT_TREE_FILE})")
    parser.add_argument("--run", action="store_true", help="Trigger live-run (otherwise dry-run)")
    parser.add_argument("--limit", type=int, default=0, help="Upload limit in MB/s")
    parser.add_argument("--repack", action="store_true", help="Efficiently repack leaf assignments")
    parser.add_argument("--report", action="store_true", help="S3 usage and cost report")
    parser.add_argument("--find", type=str, help="Search manifests for a filename")
    parser.add_argument("--audit", action="store_true", help="Audit S3 vs local inventory")
    parser.add_argument("--prune", action="store_true", help="Remove orphaned S3 bags")
    parser.add_argument("--force-prune", action="store_true", help="Prune without 180-day check")

    # If no arguments are provided print full help
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    if args.repack and (args.reset_branch or args.reset_bag):
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
        print(f"Fatal: Tree file {args.tree_file} not found.")
        sys.exit(1)

    # This will exit the script if inventory.json is corrupted
    inventory = load_inventory(INVENTORY_FILE)

    # This will exit the script if encryption is required but key.txt is missing
    encrypt_list_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "encrypt.txt")    
    PASSPHRASE_FILE = validate_encryption_config(args.tree_file, encrypt_list_path)

    with open(args.tree_file, 'r') as f:
        tree_lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]

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

    target_only = None # New flag

    # 1. BAG LOGIC (Mirror or Delete)
    if args.mirror_bag or args.delete_bag:
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
        if perform_rebag(inventory, target_ids, config, args.run, encrypt_list_path, requeue=(not is_delete_only)):
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
            allowed, reason = is_action_permitted(target_line, action_type)
            if not allowed:
                print(f"\n[!] FORBIDDEN: {reason}")
                sys.exit(1)

        # --- DESTRUCTIVE PHASE ---
        if args.force_reset or is_delete_only:
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
        if perform_tree_delete(inventory, config, args.run, tree_lines):
            if args.run:
                with open(INVENTORY_FILE, 'w') as f:
                    json.dump(inventory, f, indent=4)
                print(f"\n[OK] Global tree purge complete. Inventory updated.")
            else:
                print(f"\n[DRY RUN] Global analysis complete.")
        sys.exit(0)

    if args.prune or args.force_prune:
        perform_prune(inventory, args.run, not args.force_prune)
        sys.exit(0)

    run_stats = {}

    if not args.run: print("!!! DRY-RUN MODE (Pass --run to execute) !!!")

    for line in tree_lines:
        current_path = line.split(' ::')[0].strip()        

        allowed, reason = is_action_permitted(line, "MIRROR")
        
        if not allowed:
            print(f"  [LOCKED]: Skipping {current_path}")
            continue

        if target_only:
            target_path = target_only.split(' ::')[0].strip()
            if current_path != target_path:
                continue
        
        # Process the branch
        process_branch(line, inventory, run_stats, args.run, args.limit, args.repack, PASSPHRASE_FILE)

        # --- NEW: Save state immediately after each branch is finished ---
        if args.run:
            try:
                with open(INVENTORY_FILE, 'w') as f:
                    json.dump(inventory, f, indent=4)
                print(f"  [SAVED] Inventory updated for: {current_path}")
            except Exception as e:
                print(f"[CRITICAL ERROR] Failed to save inventory: {e}")
                # We stop here to prevent further uploads that wouldn't be recorded
                sys.exit(1)

    # Summary and final artifacts happen after all lines are processed
    generate_summary(inventory, run_stats, args.run)

    if args.run:
        upload_system_artifacts()

    print("\n[COMPLETE] All backup jobs finished.")

if __name__ == "__main__":
    main()


