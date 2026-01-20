# Glacier Backup System

**Created:** January 2026
**Author:** greenc

---

## 1. Introduction: The "Living" Insurance Policy

### The Problem
Let's say you have multiple TBs of data stored locally on drives. Maybe even multiple hard drives. This is still not sufficient in case of a catastrophic loss like fire, lightning, theft, filesystem corruption. An LTO tape drive is very expensive, a hassle to manage and often overkill for home users. You need an insurance policy where the data can be stored on tape in a remote datacenter that is cheap, secure and has an indefinite time period. And you need a way to make incremental backups as your local files change.

### The Solution: Amazon S3 Deep Archive
Amazon S3 Glacier Deep Archive is extremely cheap for storage (approx. **$1.00 per TB/month**). However, **restoring** is expensive, and uploading millions of small files incurs significant API fees. This system bridges the gap by treating S3 as a "Write-Mostly" tape archive.

### The Strategy: "Bags"
Amazon has a **180-day minimum retention policy**. If you delete a file 30 days after uploading it, you are still charged for the full 180 days. Additionally, Amazon charges a Request Fee for every single file upload.

To solve this, this system collects your files into **"Bags"** (.tar files) of roughly uniform size (e.g., 40GB).
* **Request Fee Savings:** Grouping files into large bags drastically reduces the number of API requests, saving significant money on upload fees.
* **The 180-Day Solution:** This system is designed to be run **Once Per Year**. By the time you run the 2027 backup, the 2026 bags have aged >365 days, bypassing the early deletion penalty.
* **Atomic Updates:** If a single file within a bag changes, the entire bag is re-uploaded and the old one deleted.

### Cost Analysis Example (10 TB Dataset)
Imagine you have **10 TB** of data consisting of **10 million small files** (photos, docs, code).

| Metric | Raw Upload (Bad Strategy) | Glacier System (Bag Strategy) |
| :--- | :--- | :--- |
| **Object Count** | 10,000,000 files | ~250 Bags (40GB each) |
| **Storage Cost** | $10 / month | $10 / month |
| **Upload (PUT) Fees** | **~$500.00** (one time) | **~$0.01** (one time) |
| **Management** | Nightmare | Simple |

By bagging the data, you save hundreds of dollars in request fees alone.

*All prices as of January 2026*

---

## 2. Architecture Terminology

* **Molecule (The Source):** A single line in `list.txt`.
    * *Example:* `/media/greenc/Atlas12T/Notre`
* **Atom (The Unit):** A top-level subfolder inside a Molecule. This is the **atomic packaging unit**. Everything in this folder is backed up.
    * The system calculates one hash for this folder (including all sub-content) and never splits it across two bags.
    * *Example:* `/media/greenc/Atlas12T/Notre/Books`
* **Bag (The Archive):** The `.tar` file uploaded to S3. Contains one or more Atoms.
    * *Target Size:* 40 GB (Configurable).

---

## 3. Prerequisites & Dependencies

Before running the system, ensure the following are installed:

### System Tools
* **Python 3.x**
* **tar:** Standard GNU tar.
* **sshfs (Optional):** Only required if you are backing up remote servers.
    * `sudo apt install sshfs`

### Python Libraries
* **boto3:** The AWS SDK for Python.
    * `pip install boto3`
* **tqdm:** For progress bar display.
    * `pip install tqdm`

---

## 4. AWS Configuration Guide

Follow these steps to set up your cloud environment from scratch.

### Step 0: Account & CLI Installation
1.  **Sign Up:** Create an account at [aws.amazon.com](https://aws.amazon.com).
2.  **Install CLI:** Install the official AWS Command Line Interface.
    * **Linux:** https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

### Step 1: Create the Bucket
1.  Log in to the **AWS Console** and search for **S3**.
2.  Click **Create bucket**.
3.  **Bucket Name:** Choose a unique name (e.g., `greenc-bucket`). *Update glacier.cfg.*
4.  **Region:** Choose the region closest to you (e.g., `US East (N. Virginia) us-east-1`).
5.  **Block Public Access:** Ensure "Block all public access" is CHECKED (Default).
6.  Click **Create bucket**.

### Step 2: Create an IAM User
1.  Search for **IAM** (Identity and Access Management) in the console.
2.  Click **Users** -> **Create user**.
3.  Name: `glacier-backup-bot` (or similar). 
4.  **Permissions:** Select "Attach policies directly" and choose **AmazonS3FullAccess**.
    * *Note: For tighter security, you can create a custom policy limited to just one bucket.*
5.  Finish creating the user.

### Step 3: Generate Access Keys
1.  Click on the newly created user.
2.  Go to the **Security credentials** tab.
3.  Scroll to **Access keys** and click **Create access key**.
4.  Select **Command Line Interface (CLI)**.
5.  **Copy** the `Access Key ID` and `Secret Access Key`. (Save these! You cannot see the secret again).

### Step 4: Add rule to delete incomplete uploads
This will save you money - if an upload is aborted it can leave temporary fragments in the bucket
1. Go to AWS Console -> S3 -> Your Bucket.
2. Click Management tab.
3. Click Create lifecycle rule.
4. Name: "Clean Incomplete Uploads".
5. Scope: Apply to all objects.
6. Under "Lifecycle rules actions", check Delete expired object delete markers or incomplete multipart uploads.
7. Check Delete incomplete multipart uploads and set it to 7 days.

### Step 5: Configure the System
Run this command on your Linux machine and paste the keys when prompted:
```bash
aws configure
```
* **Region:** Use the same region code from Step 1 (e.g., `us-east-1`).
* **Output format:** `json`.

---

## 5. The Yearly Lifecycle: How Incremental Works

This system works best on a **yearly cadence**. Here is the lifecycle of your data:

### Year 1: The Initial Upload
You run the script. It hashes all your local data, packs them into bags, and uploads everything to the folder `2026-backup/`. The `inventory.json` records the hash of every atom.

### The Waiting Period (Jan - Dec)
Your data sits in Deep Archive. You pay the monthly storage fee. The 180-day early deletion penalty clock ticks down. By July, your files are "mature" and free to delete without penalty.

### Year 2: The Incremental Run
It is now January 2027. You run the script again. It re-scans your local drive and compares it to the `inventory.json`.

**Scenario A: Data Unchanged (95% of your files)**
* The script sees the hash matches.
* **Action:** Skips upload entirely.
* **Cost:** $0.
* **Result:** The inventory points to the existing file in `2026-backup/`.

**Scenario B: Data Changed (5% of your files)**
* The script sees the hash has changed (or new files added).
* **Action:** Re-packs just those specific atoms into a NEW bag and uploads it to `2027-backup/`.
* **Cost:** Small upload fee for just the changes.
* **Result:** The inventory updates to point to the new file in `2027-backup/`.

### The Cleanup (Pruning)
After the Year 2 run, you have a mix of 2026 and 2027 files in your inventory. However, the *old versions* of the changed files are still sitting in `2026-backup/`, costing you money.
* You run `prune.py`.
* It detects that some of the old 2026 bags are no longer referenced by the inventory.
* It deletes them from S3.
* **No Penalty:** Because they sat there for >180 days, deleting them is free.

---

## 6. Configuration Files

### `glacier.cfg`

**Example:**
```text
[settings]
s3_bucket = greenc-bucket
target_bag_gb = 40
staging_dir = /home/greenc/glacier/stage
manifest_dir = /home/greenc/glacier/manifests
inventory_file = /home/greenc/glacier/inventory.json
mnt_base = /home/greenc/mnt
```

* **`s3_bucket`:** The name of your S3 bucket (see AWS Configuration Guide above).
* **`target_bag_gb`:** The max size of each bag. Recommend 10GB to 100GB depending on your data. Note that large data files like VMs will get their own bag as large as needed.
* **`staging_dir`:** A temporary directory used for creating a bag (.tar) file. It should have enough free space for your largest bag.
* **`manifest_dir`:** Where to put the manifest files.
* **`inventory_file`:** Location of the inventory file.
* **`mnt_base`:** Root mounting point for a remote server for SSHFS purposes.

### `list.txt` 
This file defines your **Molecules** (the top-level storage locations). The system scans these paths, and every **sub-directory** underneath is treated as an **Atom** (an indivisible unit for bagging purposes).

**Format:** Lines in `list.txt` can be either a local directory name or a remote directory name. Just list the directory path; do not include "Local" or "Remote" labels.

**Example:**
```text
rabbit:[/home/greenc/] cat list.txt
greenc@fox:/home/greenc/Backup
/home/greenc/Backup
```
In this example, the system will backup all files on the remote server (`fox`) in the `/home/greenc/Backup` directory, and on the local server (`rabbit`) in the `/home/greenc/Backup` directory.

### `inventory.json`
This file tracks the state of every atom. Deletion of the file will wipe the system memory and start like a fresh archive. **Keep backups of this file.** It is the database connecting the state of local and remote files.

* **Recommendation:** If you lose this file by accident, delete all prior files on S3 and start over with a fresh upload to prevent massive duplication.
* **`last_metadata_hash`:** Used to detect changes.
* **`tar_id`:** The bag assignment (e.g., `bag_001`).
* **`archive_key`:** The exact S3 path where this file lives (e.g., `2026-backup/...`).

### `manifests/*.txt` (File-Level Indexes)
These files provide a searchable, recursive list of every single file inside every bag. While list.txt tracks "Molecules" (top-level folders to archive), `inventory.json` tracks "Bags" and "Atoms" (sub-folders 1-level deep within Molecules), the manifests track the specific files *inside* those atomic folders.

* **Purpose:** Allows you to find a specific file (e.g., "Where is `tax_return_2024.pdf`?") without having to pay to restore and download 40GB archives just to look inside them.
* **Format:** Plain text files generated via the `find` command.
* **Location (Local):** Stored in your configured `manifest_dir`.
* **Location (Remote):** Uploaded to `s3://[bucket]/[year]-backup/manifests/`.
* **Storage Class:** **S3 Standard (Hot)**.
    * Unlike the heavy `.tar` bags which are frozen in Glacier Deep Archive, the manifests are kept in Standard storage.
    * **Benefit:** You can instantly download the entire folder of text files and `grep` them to locate data in seconds, effectively for free.

**Disaster Recovery:**
If you lose your local computer, you can bootstrap the entire system from the "system" folder in S3.

1. **Rebuild the Environment:**
   ```bash
   mkdir -p ~/glacier/manifests
   cd ~/glacier
   
   # Download the Brain, Code, Config, and Lists
   # (Replace '2026' with the latest year available in your bucket)
   aws s3 cp s3://greenc-bucket/2026-backup/system/ . --recursive
   
   # Download the Search Index
   aws s3 cp s3://greenc-bucket/2026-backup/manifests/ ./manifests/ --recursive
   ```

---

## 7. Quick Start

### Step 1: Update glacier.cfg

### Step 2: Create the list of "molecules"
Create a file named `list.txt` that lists the folders you want to back up.

### Step 3: The "Dry Run" (Simulation)
Use this to check what *would* happen without uploading anything.
```bash
./glacier.py list.txt
```
* **Output:** Generates `inventory_dryrun.json` for inspection.

### Step 4: The "Real Run" (Action)
Use this to actually upload files and update the master inventory.
```bash
./glacier.py list.txt --run
```
* **Output:** Updates `inventory.json` and uploads `.tar` files to S3.
* **Note:** The system will auto-mount remote SSH sources as needed.

---

## 8. Verification & Health Checks

**View Contents:**
```bash
aws s3 ls s3://greenc-bucket/2026-backup/ --human-readable --summarize
```

**Verify Storage Class (Must be DEEP_ARCHIVE):**
If this is not `DEEP_ARCHIVE`, you are paying significantly more than necessary.
```bash
aws s3api list-objects-v2 --bucket greenc-bucket --prefix 2026-backup/ --query 'Contents[].{Key: Key, StorageClass: StorageClass}' --output table
```

---

## 9. Advanced Feature: "Pinning"

### What is it?
Pinning allows you to pin a specific atom to a specific bag ID forever. Normally, the script recalculates bag numbers dynamically (Bin Packing).

### Why use it?
If you have a massive 2TB folder (e.g., `bag_050`), you do not want it to be renamed to `bag_049` just because you deleted a small file earlier in the list. Pinning prevents this "Cascade Effect."

### How to Pin
1.  Run a Dry Run to generate the initial inventory.
2.  Open `inventory.json`.
3.  Find the large atom you want to protect.
4.  Change `"pinned": false` to `"pinned": true`.
---

## 10. Maintenance: Garbage Collection

Because the system only uploads *changes*, your backup in S3 will eventually be a mix of years (e.g., some 2026 files, some 2027 files).

Use `prune.py` to identify and delete only the files that are obsolete (duplicated).
```bash
./prune.py          # Dry Run
./prune.py --delete # Execute Delete
```

---

## 11. Restoration Procedure (WARNING: Costs $$$)

**Cost Warning:** Downloading 8TB of data can cost $700-$900. Only restore what you absolutely need.

### Step 1: Locate the Bag
Open `inventory.json` and find the `archive_key` for the folder you need.

### Step 2: Thaw (Move from Tape to HDD)
**Option A: Single File (Standard Tier - 12-48 hours)**
```bash
aws s3api restore-object --bucket greenc-bucket --key 2026-backup/rabbit_host-agros_bag_003.tar --restore-request '{"Days":7,"GlacierJobParameters":{"Tier":"Standard"}}'
```

**Option B: Thaw Everything (Bulk Tier - 48 hours)**
Use this `tcsh` loop to request a restore for every file in the folder.
```tcsh
foreach file (`aws s3 ls s3://greenc-bucket/2026-backup/ --recursive | awk '{print $4}'`)
    aws s3api restore-object --bucket greenc-bucket --key "$file" --restore-request '{"Days":14,"GlacierJobParameters":{"Tier":"Bulk"}}'
end
```

### Step 3: Check Status
Use `head-object` to check if the file is ready. Look for `"ongoing-request": "false"` in the output.
```bash
aws s3api head-object --bucket greenc-bucket --key 2026-backup/rabbit_host-agros_bag_003.tar
```

### Step 4: Download
```bash
# Single Bag
aws s3 cp s3://greenc-bucket/2026-backup/rabbit_host-agros_bag_003.tar /media/greenc/NewDrive/Restore/

# All Bags
aws s3 cp s3://greenc-bucket/2026-backup/ /media/greenc/NewDrive/Restore/ --recursive
```

### Step 5: Extract (Sparse Mode)
**IMPORTANT:** Use the `-S` flag to handle sparse files efficiently. This is critical for VM images.
```bash
tar -Sxvf rabbit_host-agros_bag_003.tar -C /home/greenc/Backup/VMs/
```

