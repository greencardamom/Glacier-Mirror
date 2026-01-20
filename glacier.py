#!/usr/bin/env python3
import os
import sys
import json
import hashlib
import tarfile
import subprocess
import shutil
import boto3
import socket
import argparse
import configparser
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

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
    """
    Generates the 'Seal' for an atomic unit.
    recursive=True: Standard behavior (walks everything).
    recursive=False: Used for CLUSTER roots (hashes only files in top dir).
    file_list: Optional list of specific filenames to hash (overrides directory walk).
    """
    hasher = hashlib.md5()
    total_size = 0
    
    # Mode A: Specific File List (Cluster Root)
    if file_list:
        for name in sorted(file_list):
            path = os.path.join(directory, name)
            try:
                stat = os.stat(path)
                # rel_path is just the filename for cluster roots
                meta_str = f"{name}|{stat.st_size}|{stat.st_mtime}"
                hasher.update(meta_str.encode('utf-8'))
                total_size += stat.st_size
            except OSError: continue
            
    # Mode B: Directory Walk (Atom/Molecule)
    else:
        for root, dirs, files in os.walk(directory):
            if not recursive:
                # If non-recursive, clear dirs so os.walk doesn't go deeper
                dirs[:] = []
            
            dirs.sort()
            for name in sorted(files):
                path = os.path.join(root, name)
                try:
                    stat = os.stat(path)
                    rel_path = os.path.relpath(path, directory)
                    meta_str = f"{rel_path}|{stat.st_size}|{stat.st_mtime}"
                    hasher.update(meta_str.encode('utf-8'))
                    total_size += stat.st_size
                except OSError: continue
                
    return hasher.hexdigest(), total_size

def generate_real_manifest(bag_name, atom_definitions, is_live):
    """
    Generates a recursive file listing for the bag.
    atom_definitions: List of dicts {'path': str, 'is_cluster_root': bool, 'files': []}
    """
    timestamp = datetime.now().strftime('%Y%m%d')
    base_name = bag_name.replace(".tar", "")
    suffix = "_liverun.txt" if is_live else "_dryrun.txt"
    txt_name = f"{timestamp}_{base_name}{suffix}"
    
    manifest_path = os.path.join(MANIFEST_DIR, txt_name)
    s3_key = os.path.join(S3_PREFIX, "manifests", txt_name)
    
    try:
        if not os.path.exists(MANIFEST_DIR): os.makedirs(MANIFEST_DIR)
        
        with open(manifest_path, "w") as f:
            f.write(f"# Manifest for {bag_name} ({'LIVE RUN' if is_live else 'DRY RUN'})\n")
            f.write(f"# Generated: {datetime.now().isoformat()}\n")
            f.write("-" * 60 + "\n")
            
            for atom in atom_definitions:
                path = atom['path']
                if atom.get('is_cluster_root', False):
                    # For cluster roots, we only list the specific files
                    for filename in atom.get('files', []):
                        f.write(f"{os.path.join(path, filename)}\n")
                else:
                    # Standard behavior: find all files inside
                    subprocess.run(["find", path, "-type", "f"], stdout=f, check=False)
                
        if is_live:
             s3_client.upload_file(manifest_path, S3_BUCKET, s3_key)
             
    except Exception as e:
        print(f"[WARN] Manifest generation/upload failed for {bag_name}: {e}")

def upload_system_artifacts():
    """Backs up the code, config, and brain to S3 for disaster recovery."""
    print("\n--- System Artifact Backup ---")
    sys_files = ["glacier.py", "prune.py", "glacier.cfg", "list.txt", "inventory.json", "0README.md", "README.md", "requirements.txt"]
    base_dir = os.path.dirname(os.path.abspath(__file__))
    s3_folder = os.path.join(S3_PREFIX, "system")
    
    for fname in sys_files:
        local_path = os.path.join(base_dir, fname)
        if os.path.exists(local_path):
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
        print(f"--> Bridge: Connecting {source_string} -> {mount_point}")
        cmd = ["sshfs", "-o", "reconnect", source_string, mount_point]
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError:
            print(f"[FATAL] Could not mount {source_string}. Check SSH keys."); sys.exit(1)
    else:
        print(f"--> Bridge: Already active at {mount_point}")

    return mount_point, mount_point

def unmount_remote_source(mount_point):
    if not mount_point: return
    res = subprocess.run(["mountpoint", "-q", mount_point])
    if res.returncode == 0:
        print(f"--> Bridge: Disconnecting {mount_point}...")
        subprocess.run(["fusermount", "-u", mount_point], check=True)

def generate_summary(inventory, run_stats, is_live):
    print("\n" + "="*80)
    print(f"{'INVENTORY STATE':<45} {'ATOMS':<8} {'BAGS':<8} {'SIZE':<10}")
    print("-" * 80)
    
    total_atoms = 0
    total_bags_global = 0
    total_size = 0
    
    for source, data in inventory.get("molecular_sources", {}).items():
        atoms = data.get("atomic_units", {})
        num_atoms = len(atoms)
        source_size = sum(a.get("size_bytes", 0) for a in atoms.values())
        unique_bags = set(a.get("tar_id") for a in atoms.values() if a.get("tar_id"))
        num_bags = len(unique_bags)
        
        display_name = source.split(' ::')[0]
        if len(display_name) > 43: display_name = "..." + display_name[-40:]
        print(f"{display_name:<45} {num_atoms:<8} {num_bags:<8} {format_bytes(source_size):<10}")
        
        total_atoms += num_atoms
        total_bags_global += num_bags
        total_size += source_size

    print("-" * 80)
    print(f"{'TOTALS':<45} {total_atoms:<8} {total_bags_global:<8} {format_bytes(total_size):<10}")
    print("="*80)

    mode = "Real Run" if is_live else "Dry Run"
    print(f"\n{'EXECUTION REPORT (' + mode + ')':<80}")
    print("-" * 80)
    print(f"{'MOLECULAR SOURCE':<45} {'UPLOADS':<15} {'SKIPS':<15}")
    print("-" * 80)

    tot_up_cnt = 0
    tot_up_sz = 0
    tot_sk_cnt = 0
    tot_sk_sz = 0

    for source in run_stats:
        s = run_stats[source]
        up_str = f"{s['up_count']} ({format_bytes(s['up_bytes'])})"
        sk_str = f"{s['skip_count']} ({format_bytes(s['skip_bytes'])})"
        
        display_name = source.split(' ::')[0]
        if len(display_name) > 43: display_name = "..." + display_name[-40:]
        print(f"{display_name:<45} {up_str:<15} {sk_str:<15}")

        tot_up_cnt += s['up_count']
        tot_up_sz += s['up_bytes']
        tot_sk_cnt += s['skip_count']
        tot_sk_sz += s['skip_bytes']

    print("-" * 80)
    final_up = f"{tot_up_cnt} ({format_bytes(tot_up_sz)})"
    final_sk = f"{tot_sk_cnt} ({format_bytes(tot_sk_sz)})"
    print(f"{'TOTALS':<45} {final_up:<15} {final_sk:<15}")
    print("="*80 + "\n")

# --- PROCESSING ---

def process_bag(bag_num, atom_list, source_root, short_name, bag_size_bytes, is_live, molecule_atoms, hostname, source_stats):
    safe_prefix = short_name.replace(" ", "_")
    tar_name = f"{hostname}_{safe_prefix}_bag_{bag_num:03d}.tar"
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
    # We must build the command carefully. 
    # For directories: -C "parent" "folder_name"
    # For cluster roots: -C "source_root" "file1" "file2"...
    
    tar_cmd_parts = [f"tar -Scf {tar_path} -C \"{source_root}\""]
    
    for atom in atom_list:
        if atom['is_cluster_root']:
            # Append all loose files
            for f in atom['files']:
                tar_cmd_parts.append(f"\"{f}\"")
        else:
            # Append the directory name relative to source_root
            rel_path = os.path.relpath(atom['path'], source_root)
            tar_cmd_parts.append(f"\"{rel_path}\"")
            
    cmd = " ".join(tar_cmd_parts)

    try:
        if not os.path.exists(STAGING_DIR): os.makedirs(STAGING_DIR)
        
        print(f"    [PACKAGING] Creating archive locally...")
        subprocess.run(cmd, shell=True, check=True)
        
        print(f"    [UPLOADING] Sending to S3 Deep Archive...")
        file_size = os.path.getsize(tar_path)
        with tqdm(total=file_size, unit='B', unit_scale=True, desc="    Progress", leave=True) as pbar:
            s3_client.upload_file(
                tar_path, 
                S3_BUCKET, 
                s3_key, 
                ExtraArgs={'StorageClass': 'DEEP_ARCHIVE'},
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

def process_source(line_raw, inventory, run_stats, is_live):
    """Handles parsing types (ATOM, MOLECULE, CLUSTER) and processing."""
    
    # 1. Parse Designator
    if " ::" in line_raw:
        path_part, type_part = line_raw.split(" ::", 1)
        source_path = path_part.strip()
        designator = type_part.strip().upper()
    else:
        source_path = line_raw.strip()
        designator = "MOLECULE" # Default

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

        if designator == "ATOM":
            found_items.append({
                "key": scan_path, "path": scan_path, "is_cluster_root": False, "files": []
            })
            
        elif designator == "MOLECULE":
            subdirs = [d for d in sorted(os.listdir(scan_path)) if os.path.isdir(os.path.join(scan_path, d))]
            for d in subdirs:
                full_p = os.path.join(scan_path, d)
                found_items.append({
                    "key": full_p, "path": full_p, "is_cluster_root": False, "files": []
                })

        elif designator == "CLUSTER":
            subdirs = [d for d in sorted(os.listdir(scan_path)) if os.path.isdir(os.path.join(scan_path, d))]
            for d in subdirs:
                full_p = os.path.join(scan_path, d)
                found_items.append({
                    "key": full_p, "path": full_p, "is_cluster_root": False, "files": []
                })
            
            loose_files = [f for f in sorted(os.listdir(scan_path)) if os.path.isfile(os.path.join(scan_path, f))]
            if loose_files:
                cluster_key = os.path.join(scan_path, "__CLUSTER_ROOT__")
                found_items.append({
                    "key": cluster_key, "path": scan_path, "is_cluster_root": True, "files": loose_files
                })

        # 3. SCAN METADATA & UPDATE INVENTORY
        items_to_bag = [] 

        for item in found_items:
            if item['is_cluster_root']:
                current_hash, size = get_metadata_hash(item['path'], recursive=False, file_list=item['files'])
            else:
                current_hash, size = get_metadata_hash(item['path'], recursive=True)

            entry = molecule_atoms.get(item['key'], {})
            is_changed = entry.get("last_metadata_hash") != current_hash
            is_pinned = entry.get("pinned", False)
            existing_tid = entry.get("tar_id", None)
            existing_key = entry.get("archive_key", None)
            
            tar_id = existing_tid if is_pinned else None
            archive_key = existing_key if not is_changed else None

            molecule_atoms[item['key']] = {
                "last_metadata_hash": current_hash,
                "needs_upload": is_changed or entry.get("needs_upload", True),
                "size_bytes": size,
                "size_human": format_bytes(size),
                "tar_id": tar_id,
                "archive_key": archive_key,
                "pinned": is_pinned,
                "last_upload": entry.get("last_upload", None)
            }
            
            item_data = item.copy()
            item_data.update({"size": size, "pinned": is_pinned, "tar_id": tar_id})
            items_to_bag.append(item_data)

        # 4. ASSIGN BAGS (FIXED: INCREMENTAL COUNTING)
        # Find the highest bag number already used in this inventory
        existing_bag_nums = []
        for unit in molecule_atoms.values():
            tid = unit.get('tar_id')
            if tid and tid.startswith('bag_'):
                try:
                    existing_bag_nums.append(int(tid.split('_')[-1]))
                except ValueError: pass
        
        # Start counting from Max + 1
        bag_counter = (max(existing_bag_nums) if existing_bag_nums else 0) + 1
        current_bag_size = 0
        
        for item in items_to_bag:
            if item["pinned"] and item["tar_id"]:
                continue 
            
            if (current_bag_size + item["size"] > TARGET_SIZE_BYTES) and current_bag_size > 0:
                bag_counter += 1
                current_bag_size = 0
            
            new_tid = f"bag_{bag_counter:03d}"
            item["tar_id"] = new_tid
            current_bag_size += item["size"]
            
            molecule_atoms[item["key"]]["tar_id"] = new_tid

        # 5. GROUP BY BAG ID
        bags = {}
        for item in items_to_bag:
            tid = item["tar_id"]
            if tid not in bags:
                bags[tid] = {"atoms": [], "size": 0, "bag_num_int": int(tid.split('_')[-1]) if '_' in tid else 999}
            bags[tid]["atoms"].append(item)
            bags[tid]["size"] += item["size"]

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
                source_stats
            )

        if is_live:
            with open(INVENTORY_FILE, 'w') as f: json.dump(inventory, f, indent=4)
        else:
            dry_file = f"inventory_dryrun.json"
            dry_path = os.path.join(os.path.dirname(INVENTORY_FILE), dry_file)
            with open(dry_path, 'w') as f: json.dump(inventory, f, indent=4)
        
    finally:
        if mount_point_to_cleanup:
            unmount_remote_source(mount_point_to_cleanup)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_list", help="Path to list.txt containing backup sources")
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()

    if not os.path.exists(args.input_list):
        print(f"Fatal: List file {args.input_list} not found.")
        sys.exit(1)

    if os.path.exists(INVENTORY_FILE):
        with open(INVENTORY_FILE, 'r') as f: inventory = json.load(f)
    else:
        inventory = {"molecular_sources": {}}

    run_stats = {}

    if not args.run: print("!!! DRY-RUN MODE (Pass --run to execute) !!!")

    with open(args.input_list, 'r') as f:
        lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    for line in lines:
        process_source(line, inventory, run_stats, args.run)

    generate_summary(inventory, run_stats, args.run)

    if args.run:
        upload_system_artifacts()

    print("\n[COMPLETE] All backup jobs finished.")

if __name__ == "__main__":
    main()
