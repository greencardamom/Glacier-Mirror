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

import json
import hashlib
import tarfile
import subprocess
import shutil
import socket
import argparse
import configparser
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from boto3.s3.transfer import TransferConfig # Added for throttling

# --- CONFIGURATION LOADER ---
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'glacier.cfg')
config = configparser.ConfigParser()

if not os.path.exists(config_path):
    print(f"Error: Configuration file not found at {config_path}")
    sys.exit(1)

config.read(config_path)

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

# --- HELPER FUNCTIONS ---

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
    
    pbar = tqdm(desc="    Scanning metadata", unit=" files", leave=False)

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

def generate_real_manifest(bag_name, atom_definitions, is_live):
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
        
        pbar = tqdm(desc=f"    Building Manifest", unit=" files", leave=False)
        
        with open(manifest_path, "w") as f:
            f.write(f"# Manifest for {bag_name}\n")
            for atom in atom_definitions:
                path = atom['path']
                if atom.get('is_cluster_root', False):
                    for filename in atom.get('files', []):
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
    except Exception as e:
        print(f"\n[WARN] Manifest failed: {e}")

def upload_system_artifacts():
    """Backs up the code, config, and brain to S3 for disaster recovery."""
    print("\n--- System Artifact Backup ---")
    # list.txt and exclude.txt are treated as optional via the os.path.exists check below
    sys_files = ["glacier.py", "prune.py", "glacier.cfg", "list.txt", "inventory.json", "0README.md", "README.md", "requirements.txt", "exclude.txt"]
    base_dir = os.path.dirname(os.path.abspath(__file__))
    s3_folder = os.path.join(S3_PREFIX, "system")
    
    for fname in sys_files:
        local_path = os.path.join(base_dir, fname)
        if os.path.exists(local_path):  # <--- This keeps it optional
            s3_key = os.path.join(s3_folder, fname)
            print(f"    [UPLOADING] {fname}...")
            try:
                s3_client.upload_file(local_path, S3_BUCKET, s3_key)
            except Exception as e:
                print(f"    [WARN] Failed to upload {fname}: {e}")

def mount_remote_source(source_string):
    if ":" not in source_string:
        return os.path.abspath(source_string), None
        
    remote_conn, remote_path = source_string.split(":", 1)
    host_slug = remote_conn.split("@")[-1]
    base_slug = os.path.basename(os.path.normpath(remote_path)).replace(" ", "_")
    mount_point = os.path.join(MNT_BASE, f"{host_slug}_{base_slug}")

    if not os.path.exists(mount_point): os.makedirs(mount_point)
    
    res = subprocess.run(["mountpoint", "-q", mount_point])
    if res.returncode != 0:
        print(f"--> Bridge: Connecting {source_string}...")
        
        # Strategy 1: Attempt High-Performance Mount
        cmd_hp = ["sshfs", "-o", "reconnect,cache=yes,kernel_cache", source_string, mount_point]
        
        # Strategy 2: Fallback to Universal Mount (Works everywhere)
        cmd_univ = ["sshfs", "-o", "reconnect", source_string, mount_point]
        
        try:
            # Try HP first
            subprocess.run(cmd_hp, check=True, capture_output=True)
        except subprocess.CalledProcessError:
            try:
                # Fallback to Universal
                print("    [NOTE] High-performance flags not supported. Falling back to standard mount.")
                subprocess.run(cmd_univ, check=True)
            except subprocess.CalledProcessError:
                print(f"[FATAL] Could not mount {source_string}. Check SSH keys and paths."); sys.exit(1)
            
    return mount_point, mount_point

def unmount_remote_source(mount_point):
    if not mount_point: return
    res = subprocess.run(["mountpoint", "-q", mount_point])
    if res.returncode == 0:
        print(f"--> Bridge: Disconnecting {mount_point}...")
        subprocess.run(["fusermount", "-u", mount_point], check=True)

def generate_summary(inventory, run_stats, is_live):
    print("\n" + "="*105)
    print(f"{'INVENTORY STATE':<45} {'BAGS':<8} {'SIZE':<10} {'WASTE %':<10} {'REPACK RISK':<12}")
    print("-" * 105)
    
    total_atoms = 0
    total_bags_global = 0
    total_size = 0
    total_risk = 0
    
    price_gb = float(config['pricing'].get('price_per_gb_month', 0.00099))
    min_days = int(config['pricing'].get('min_retention_days', 180))

    for source, data in inventory.get("molecular_sources", {}).items():
        atoms = data.get("atomic_units", {})
        source_size = sum(a.get("size_bytes", 0) for a in atoms.values())
        unique_bags = set(a.get("tar_id") for a in atoms.values() if a.get("tar_id"))
        num_bags = len(unique_bags)
        
        total_cap = num_bags * TARGET_SIZE_BYTES
        waste_p = ((total_cap - source_size) / total_cap * 100) if total_cap > 0 else 0
        
        # Risk estimate: Full storage cost for the retention period
        repack_risk = (source_size / BYTES_PER_GB) * price_gb * (min_days / 30)
        
        display_name = source.split(' ::')[0]
        if len(display_name) > 43: display_name = "..." + display_name[-40:]
        print(f"{display_name:<45} {num_bags:<8} {format_bytes(source_size):<10} {waste_p:>7.1f}% {f'${repack_risk:.2f}':>12}")
        
        total_bags_global += num_bags
        total_size += source_size
        total_risk += repack_risk

    print("-" * 105)
    print(f"{'TOTALS':<45} {total_bags_global:<8} {format_bytes(total_size):<10} {'---':<10} {f'${total_risk:.2f}':>12}")
    print("="*105)

    # --- TIME ESTIMATION ---
    # total size of atoms that actually NEED upload
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

def process_bag(bag_num, atom_list, source_root, short_name, bag_size_bytes, is_live, molecule_atoms, hostname, source_stats, upload_limit_mb):
    safe_prefix = short_name.replace(" ", "_")
    tar_name = f"{hostname}_{safe_prefix}_bag_{bag_num:05d}.tar"
    tar_path = os.path.join(STAGING_DIR, tar_name)
    s3_key = os.path.join(S3_PREFIX, tar_name)
    
    print(f"\n--- Bag {bag_num:03d} [{format_bytes(bag_size_bytes)}] ---")

    # Prepare atom definitions for manifest
    manifest_atoms = []
    for atom in atom_list:
        if atom['is_cluster_root']:
            manifest_atoms.append({'path': source_root, 'is_cluster_root': True, 'files': atom['files']})
        else:
            manifest_atoms.append({'path': atom['path'], 'is_cluster_root': False})

    generate_real_manifest(tar_name, manifest_atoms, is_live)

    needs_upload = False
    for atom in atom_list:
        key = atom['key']
        if molecule_atoms.get(key, {}).get('needs_upload', True):
            needs_upload = True
            break
    
    if not needs_upload:
        print(f"    [SKIP] Inventory Match. No changes detected.")
        source_stats['skip_count'] += 1
        source_stats['skip_bytes'] += bag_size_bytes
        return

    source_stats['up_count'] += 1
    source_stats['up_bytes'] += bag_size_bytes

    # Record Location
    for atom in atom_list:
        key = atom['key']
        if key in molecule_atoms:
            molecule_atoms[key]['archive_key'] = s3_key

    if not is_live:
        print(f"    [DRY RUN] Would upload: s3://{S3_BUCKET}/{s3_key}")
        return

    # Tar Construction
    exclude_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'exclude.txt')
    is_gnu = "GNU" in subprocess.getoutput("tar --version")
    sparse_flag = "-S" if is_gnu else ""
    
    tar_cmd_parts = ["tar", sparse_flag, "-cf", f"\"{tar_path}\""]
    
    # Only add the flag if the file exists to avoid empty argument errors
    if os.path.exists(exclude_file):
        tar_cmd_parts.append(f"--exclude-from='{exclude_file}'")
        
    tar_cmd_parts.extend(["-C", f"\"{source_root}\""])

    for atom in atom_list:
        if atom['is_cluster_root']:
            for f in atom['files']:
                tar_cmd_parts.append(f"\"{f}\"")
        else:
            rel_path = os.path.relpath(atom['path'], source_root)
            tar_cmd_parts.append(f"\"{rel_path}\"")
            
    cmd = " ".join(tar_cmd_parts)

    try:
        if not os.path.exists(STAGING_DIR): os.makedirs(STAGING_DIR)
        
        print(f"    [PACKAGING] Creating archive locally...", flush=True)
        subprocess.run(cmd, shell=True, check=True)
        
        # Bandwidth Throttling Logic
        if upload_limit_mb > 0:
            print(f"    [UPLOADING] Sending to S3 Deep Archive (Capped at {upload_limit_mb}MB/s)...", flush=True)
            t_config = TransferConfig(max_bandwidth=upload_limit_mb * 1024 * 1024, max_concurrency=10, use_threads=True)
        else:
            print(f"    [UPLOADING] Sending to S3 Deep Archive...", flush=True)
            t_config = TransferConfig(use_threads=True)

        file_size = os.path.getsize(tar_path)
        with tqdm(total=file_size, unit='B', unit_scale=True, desc="    Progress", leave=True) as pbar:
            s3_client.upload_file(
                tar_path, 
                S3_BUCKET, 
                s3_key, 
                ExtraArgs={'StorageClass': 'DEEP_ARCHIVE'},
                Config=t_config,
                Callback=lambda bytes_transferred: pbar.update(bytes_transferred)
            )

        os.remove(tar_path)
        
        for atom in atom_list:
            key = atom['key']
            if key in molecule_atoms:
                molecule_atoms[key]['needs_upload'] = False
                molecule_atoms[key]['last_upload'] = datetime.now().isoformat()
        print(f"    [OK] Success.")
    except Exception as e:
        print(f"    [FATAL] {e}"); sys.exit(1)

def process_source(line_raw, inventory, run_stats, is_live, upload_limit_mb, is_repack=False):

    """Handles parsing types (MUTABLE, IMMUTABLE) and processing."""
    if " ::" in line_raw:
        path_part, type_part = line_raw.split(" ::", 1)
        source_path = path_part.strip()
        designator = type_part.strip().upper()
    else:
        source_path = line_raw.strip()
        designator = "MUTABLE" # Default

    run_stats[line_raw] = {'up_count': 0, 'up_bytes': 0, 'skip_count': 0, 'skip_bytes': 0}
    source_stats = run_stats[line_raw]

    mount_point_to_cleanup = None
    try:
        scan_path, mount_point_to_cleanup = mount_remote_source(source_path)
        
        if not os.path.exists(scan_path):
            print(f"[ERROR] Path not found: {scan_path}")
            return

        short_name = os.path.basename(os.path.normpath(scan_path))
        if ":" in source_path:
            hostname = source_path.split(":")[0].split("@")[-1]
        else:
            hostname = socket.gethostname()

        print(f"------------------------------------------------")
        print(f"Processing [{designator}]: {short_name}")
        print(f"------------------------------------------------")

        if line_raw not in inventory["molecular_sources"]:
            inventory["molecular_sources"][line_raw] = {"atomic_units": {}}
        molecule_atoms = inventory["molecular_sources"][line_raw]["atomic_units"]

# 2. IDENTIFY ATOMS BASED ON DESIGNATOR
        found_items = [] 

        if designator == "IMMUTABLE":
            # Sovereign Storage: The entire path is a single Atom.
            found_items.append({
                "key": scan_path, "path": scan_path, "is_cluster_root": False, "files": []
            })
            
        elif designator == "MUTABLE":
            # Shared Storage: Every sub-directory is a unique Atom.
            subdirs = [d for d in sorted(os.listdir(scan_path)) if os.path.isdir(os.path.join(scan_path, d))]
            for d in subdirs:
                full_p = os.path.join(scan_path, d)
                found_items.append({
                    "key": full_p, "path": full_p, "is_cluster_root": False, "files": []
                })
            
            # Catch-all: All loose files in the root become one "Root Atom".
            loose_files = [f for f in sorted(os.listdir(scan_path)) if os.path.isfile(os.path.join(scan_path, f))]
            if loose_files:
                cluster_key = os.path.join(scan_path, "__ROOT__")
                found_items.append({
                    "key": cluster_key, "path": scan_path, "is_cluster_root": True, "files": loose_files
                })
        else:
            print(f"[ERROR] Unknown tag '::{designator}' on line: {line_raw}")
            return

        # 3. SCAN METADATA & UPDATE INVENTORY
        items_to_bag = [] 

        for item in found_items:
            if item['is_cluster_root']:
                current_hash, size = get_metadata_hash(item['path'], recursive=False, file_list=item['files'])
            else:
                current_hash, size = get_metadata_hash(item['path'], recursive=True)

            entry = molecule_atoms.get(item['key'], {})
            
            # Logic: If it exists in the inventory, it is "Pinned" by default.
            existing_tid = entry.get("tar_id", None)
            existing_key = entry.get("archive_key", None)
            
            # Determine if we need to upload
            is_changed = entry.get("last_metadata_hash") != current_hash
            
            # An atom needs upload if:
            # 1. It's new (no entry)
            # 2. The data changed (hash mismatch)
            # 3. It was previously marked as needing upload but never finished
            needs_upload = is_changed or entry.get("needs_upload", True)

            # If the data changed, the old archive_key is invalid
            archive_key = existing_key if not is_changed else None

            molecule_atoms[item['key']] = {
                "last_metadata_hash": current_hash,
                "needs_upload": needs_upload,
                "size_bytes": size,
                "size_human": format_bytes(size),
                "tar_id": existing_tid, # Keep the existing ID (Implicit Pin)
                "archive_key": archive_key,
                "last_upload": entry.get("last_upload", None)
            }
            
            item_data = item.copy()
            # We pass the existing_tid along to the bagger
            item_data.update({"size": size, "tar_id": existing_tid})
            items_to_bag.append(item_data)

        # 4. ASSIGN BAGS
        if is_repack:
            print("    [REPACK] Ignoring existing bag IDs. Consolidating all atoms...")
            # If repacking, we treat every atom as if it has no seat assignment
            for item in items_to_bag:
                item["tar_id"] = None
            bag_counter = 1
        else:
            # GLOBAL COUNTER MODE: Find the highest bag number in the ENTIRE inventory
            existing_bag_nums = []
            for source_data in inventory["molecular_sources"].values():
                for unit in source_data.get("atomic_units", {}).values():
                    tid = unit.get('tar_id')
                    if tid and tid.startswith('bag_'):
                        try:
                            existing_bag_nums.append(int(tid.split('_')[-1]))
                        except ValueError: pass
            
            bag_counter = max(existing_bag_nums) if existing_bag_nums else 0
            
            # If this specific item already has bags, we might be continuing its last bag
            # or starting a brand new one. Let's find THIS item's highest bag.
            item_bag_nums = []
            for unit in molecule_atoms.values():
                tid = unit.get('tar_id')
                if tid and tid.startswith('bag_'):
                    try:
                        item_bag_nums.append(int(tid.split('_')[-1]))
                    except ValueError: pass
            
            if not item_bag_nums:
                # This item has never been backed up, so it gets the NEXT global number
                bag_counter += 1
            else:
                # This item exists; we continue from its own highest bag
                bag_counter = max(item_bag_nums)

        current_bag_size = 0
        
        # In standard mode, calculate how much is already in the last bag
        if not is_repack:
            last_bag_id = f"bag_{bag_counter:05d}"
            for unit in molecule_atoms.values():
                if unit.get('tar_id') == last_bag_id:
                    current_bag_size += unit.get('size_bytes', 0)

        for item in items_to_bag:
            # If not repacking, respect the "Reserved Seat"
            if not is_repack and item["tar_id"]:
                continue 
            
            # Logic for assigning to a bag (new atoms or ALL atoms if repacking)
            if (current_bag_size + item["size"] > TARGET_SIZE_BYTES) and current_bag_size > 0:
                bag_counter += 1
                current_bag_size = 0
            
            new_tid = f"bag_{bag_counter:05d}"
            item["tar_id"] = new_tid
            current_bag_size += item["size"]
            
            # Update the inventory brain
            molecule_atoms[item["key"]]["tar_id"] = new_tid

        # 5. GROUP BY BAG ID
        bags = {}
        for item in items_to_bag:
            tid = item["tar_id"]
            if tid not in bags:
                try:
                    b_num = int(tid.split('_')[-1])
                except (ValueError, AttributeError):
                    b_num = 0
                
                bags[tid] = {"atoms": [], "size": 0, "bag_num_int": b_num}
            
            bags[tid]["atoms"].append(item)
            bags[tid]["size"] += item["size"]

        # --- WASTE & COST REPORT CALCULATION ---
        # Load pricing from config but fallback to 2026 defaults
        price_gb = float(config['pricing'].get('price_per_gb_month', 0.00099))
        min_days = int(config['pricing'].get('min_retention_days', 180))
        
        total_data_in_source = sum(item["size"] for item in items_to_bag)
        num_bags_in_source = len(bags)
        total_capacity = num_bags_in_source * TARGET_SIZE_BYTES
        
        waste_bytes = total_capacity - total_data_in_source
        waste_percent = (waste_bytes / total_capacity * 100) if total_capacity > 0 else 0

        # Calculate "At-Risk" Deletion Fees
        # If we repack, we assume the worst case: all current bags are < 180 days old.
        # Penalty = Size(GB) * Price * (Remaining Days / 30)
        # We'll estimate the 'Average' penalty as 6 months of storage for the full source size.
        at_risk_fee = (total_data_in_source / BYTES_PER_GB) * price_gb * (min_days / 30)

        source_stats['waste_bytes'] = waste_bytes
        source_stats['waste_percent'] = waste_percent
        source_stats['total_bags'] = num_bags_in_source
        source_stats['at_risk_fee'] = at_risk_fee

        if is_repack:
            print(f"    [REPACK REPORT] Potential Waste Saved: {format_bytes(waste_bytes)}")
            if is_live:
                print(f"    [FINANCIAL WARNING] This repack could trigger ~${at_risk_fee:.2f} in early deletion fees.")

        # 6. EXECUTE BAGS
        sorted_bag_ids = sorted(bags.keys(), key=lambda x: bags[x]["bag_num_int"])

        for tid in sorted_bag_ids:
            bag_data = bags[tid]
            process_bag(
                bag_data["bag_num_int"], 
                bag_data["atoms"], 
                scan_path, 
                short_name, 
                bag_data["size"], 
                is_live, 
                molecule_atoms,
                hostname,
                source_stats,
                upload_limit_mb
            )

            # Save progress immediately after each bag succeeds
            if is_live:
                # OPTIONAL DAILY BACKUP: Only runs if inventory_bak_dir is defined
                if INVENTORY_BAK_DIR:
                    if not os.path.exists(INVENTORY_BAK_DIR):
                        os.makedirs(INVENTORY_BAK_DIR)
                    
                    # One backup per day: inventory.json.20260120.bak
                    bak_filename = f"{os.path.basename(INVENTORY_FILE)}.{datetime.now().strftime('%Y%m%d')}.bak"
                    bak_path = os.path.join(INVENTORY_BAK_DIR, bak_filename)
                    
                    if not os.path.exists(bak_path):
                        shutil.copy2(INVENTORY_FILE, bak_path)

                temp_inventory = INVENTORY_FILE + ".tmp"
                with open(temp_inventory, 'w') as f:
                    json.dump(inventory, f, indent=4)
                os.replace(temp_inventory, INVENTORY_FILE)

            else:
                dry_path = os.path.join(os.path.dirname(INVENTORY_FILE), "inventory_dryrun.json")
                with open(dry_path, 'w') as f:
                    json.dump(inventory, f, indent=4)
        
    finally:
        if mount_point_to_cleanup:
            unmount_remote_source(mount_point_to_cleanup)

def generate_full_report(inventory, config):
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
    for source, data in inventory.get("molecular_sources", {}).items():
        for atom, details in data.get("atomic_units", {}).items():
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

    # --- THE REPORT ---
    print("\n" + "="*60)
    print(f"       GLACIER ARCHIVE COST & HEALTH REPORT - {datetime.now().strftime('%Y-%m-%d')}")
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
    
    if total_gb > 0:
        savings = (total_gb * 0.023) - monthly_cost
        print(f"  SOVEREIGN SAVINGS: ${savings:.2f}/mo (vs. S3 Standard)")
    print("="*60 + "\n")

def find_file(search_term):
    """Searches through local manifests to find which bag contains a file."""
    print(f"\n--- Searching for: '{search_term}' ---")
    found_count = 0
    
    if not os.path.exists(MANIFEST_DIR):
        print(f"[ERROR] Manifest directory not found: {MANIFEST_DIR}")
        return

    for manifest in sorted(os.listdir(MANIFEST_DIR)):
        if not manifest.endswith(".txt"): continue
        
        path = os.path.join(MANIFEST_DIR, manifest)
        with open(path, 'r') as f:
            for line in f:
                if search_term.lower() in line.lower():
                    # Extract the bag name from the manifest filename
                    # Format: 20260120_hostname_shortname_bag_001_liverun.txt
                    parts = manifest.split('_')
                    bag_info = "_".join(parts[1:-1]) # Extract the bag identity
                    print(f"  [FOUND] In {bag_info}")
                    print(f"          Path: {line.strip()}")
                    found_count += 1
    
    if found_count == 0:
        print("  No matches found in local manifests.")
    else:
        print(f"--- Found {found_count} matches ---")

def audit_s3(inventory):
    """Compares local inventory against actual S3 bucket contents."""
    print(f"\n--- S3 Integrity Audit ---")
    
    # 1. Get every bag mentioned in the inventory
    expected_bags = {}
    for source, data in inventory.get("molecular_sources", {}).items():
        for atom, details in data.get("atomic_units", {}).items():
            key = details.get('archive_key')
            size = details.get('size_bytes', 0)
            if key:
                expected_bags[key] = expected_bags.get(key, 0) + size

    # 2. Get actual list from S3
    print(f"  [FETCHING] Remote file list from s3://{S3_BUCKET}/{S3_PREFIX}...")
    actual_s3_files = {}
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get('Contents', []):
            actual_s3_files[obj['Key']] = obj['Size']

    # 3. Compare
    missing = []
    mismatch = []
    
    for bag_key in expected_bags:
        if bag_key not in actual_s3_files:
            missing.append(bag_key)
        # Note: Size check is approximate because TAR overhead exists on S3
        # but the S3 file should NEVER be smaller than the sum of its atoms.

    if not missing:
        print(f"  [OK] All {len(expected_bags)} expected bags are present on S3.")
    else:
        print(f"  [ALERT] {len(missing)} bags are MISSING from S3!")
        for m in missing:
            print(f"          MISSING: {m}")

    # 4. Check for Orphans (Files on S3 not in Inventory)
    orphans = [k for k in actual_s3_files if k not in expected_bags and "system/" not in k and "manifests/" not in k]
    if orphans:
        print(f"  [NOTE] Found {len(orphans)} orphan bags on S3 (not in inventory).")
        print("         Run prune.py to clean these up.")

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("input_list", help="Path to list.txt containing backup sources")
    parser.add_argument("--run", action="store_true", help="By default all commands are dry-run. This will trigger a live-run.")
    parser.add_argument("--limit", type=int, default=0, help="Upload speed limit in MB/s (0 for unlimited)")
    parser.add_argument("--repack", action="store_true", help="Ignore existing bag assignments and pack everything efficiently")
    parser.add_argument("--report", action="store_true", help="Generate a comprehensive S3 usage and cost report")
    parser.add_argument("--find", type=str, help="Search manifests for a filename")
    parser.add_argument("--audit", action="store_true", help="Audit S3 bucket against local inventory")

    # If no arguments are provided print full help
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    if not os.path.exists(args.input_list):
        print(f"Fatal: List file {args.input_list} not found.")
        sys.exit(1)

    if os.path.exists(INVENTORY_FILE):
        try:
            with open(INVENTORY_FILE, 'r') as f:
                inventory = json.load(f)
        except json.JSONDecodeError as e:
            print(f"[FATAL] {INVENTORY_FILE} is malformed! Error: {e}")
            # If a backup exists, you might want to manually rename it here
            sys.exit(1)
    else:
        # File doesn't exist yet
        inventory = {"molecular_sources": {}}

    if args.find:
        find_file(args.find)
        sys.exit(0)

    if args.audit:
        audit_s3(inventory)
        sys.exit(0)

    if args.report:
        generate_full_report(inventory, config)
        sys.exit(0)

    run_stats = {}

    if not args.run: print("!!! DRY-RUN MODE (Pass --run to execute) !!!")

    with open(args.input_list, 'r') as f:
        lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    for line in lines:
        process_source(line, inventory, run_stats, args.run, args.limit, args.repack)

    generate_summary(inventory, run_stats, args.run)

    if args.run:
        upload_system_artifacts()

    print("\n[COMPLETE] All backup jobs finished.")

if __name__ == "__main__":
    main()
