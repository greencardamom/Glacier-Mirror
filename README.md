    =========================================
          G L A C I E R   B A C K U P
    =========================================

# Glacier Backup System
![Version](https://img.shields.io/badge/Version-1.0-blue)
![Storage](https://img.shields.io/badge/Storage-S3_Deep_Archive-informational)

**Created:** January 2026
**Author:** Greenc

Python tool for managing large-scale backups to Amazon S3 Glacier Deep Archive (tape backup). Designed to maximum cost savings and run automatically and incrementaly. 

---

## 1. Introduction: The Tape Backup Insurance Policy

### The Problem
Let's say you have multiple TBs of data stored locally on drives. Perhaps mirrored on multiple drives. It is still not sufficient for catastrophic losses like fire, lightning, theft, filesystem corruption. The LTO tape drive path is very expensive, a hassle to manage and usually overkill for most users, unless you have 100s of TBs. You want an insurance policy where the data can be stored on tape in a datacenter that is cheap, secure and that will have peace of mind "forever". And you want to make occasional incremental backups.

### The Solution: AWS S3 Glacier Deep Archive
Enter Glacier Deep Archive, the AWS S3 tape backup service. It is cheap (approx. **$1.00 per TB/month**). However, **restoring** is expensive, and uploading millions of small files incurs API fees. This system manages AWS to obtain the greatest benefit for the cheapest price. 

The main assumption is you want an insurance policy against catastrophic loss. You will probably never make a full restorations due to the cost. Your have other backups elsewhere, such as on local hard drives. A complete restoration from tape would be a once in a lifetime event. But this system is also designed to modularize the data so restoring smaller pieces is very affordable. Lost a file or directory? It's easy and cheap to restore. A 40GB chunk of data is about $4 to restore. 

### The Strategy: "Bags"

The basic idea is to containerize the data into uninform size 'shipping containers' (.tar files) called "Bags" of about 40GB each (defineable). Track what they contain in a local manifest. This method reduces how many files are uploaded (API costs), and reduces how many files to download (API costs), when you need to restore some files.

Note: Amazon has a **180-day minimum retention policy**. If you delete a file 30 days after uploading you are still charged for the full 180 days. Thus incremental backups only make financial sense every 6 months or more.

* **Request Fee Savings:** Grouping files into bags reduces the number of API requests.
* **The 180-Day Solution:** This system is designed to make incremental backups **Once or Twice Per Year** due to AWS retention policy.
* **Atomic Updates:** If a single file within a bag changes, the entire bag is re-uploaded and the old one deleted. In this system, uploading and deleting files is so cheap as to be nearly free. They only charge much on the download/restore side.

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

## 2. Prerequisites & Dependencies

Before running the system, ensure the following are installed:

### System
* **Linux**

### Tools
* **Python 3.x**
* **tar:** Standard GNU tar.
* **sshfs (Optional):** Only required if you are also backing up files from remote servers.
    * `sudo apt install sshfs`

### Python Libraries
* **boto3:** The AWS SDK for Python.
    * `pip install boto3`
* **tqdm:** For progress bar display.
    * `pip install tqdm`

---

## 3. AWS Configuration Guide

Setup Glacier Deep Archive on AWS

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
* **Region:** Use the same region code from Step 1 (e.g., `us-alaska-1`).
* **Output format:** `json`.

---

## 4. Logistical Concepts

The architecture of this tool relies on two primary concepts: Atoms and Bags.

* **Atom (stuff being shipped):** An Atom is a directory and its subdirectories the system tracks, sort of like a manifest of goods being sent by a company overseas. If any file within an Atom changes—triggering a change in the Atom's metadata hash—the entire Atom is considered "dirty" and will be repacked into a new bag(s) and reuploaded.

* **Bag (the shipping container):** A Bag is a .tar file of a uniform size (size defined in the configuration). Bags are the physical units uploaded to S3. Bags can contain 1 atom, multiple atoms, or partial atoms. In the same way a shipping container can be filled with loose goods from 1 company, multiple companies, or split across multiple shipping containers.

---

## 4. The Yearly Lifecycle: How Incremental Works

This system works best on a **yearly or twice-yearly cadence**. Here is the lifecycle of your data:

### Year 1: The Initial Upload
You run the script. It hashes all your local data, packs them into bags, and uploads everything to the folder `2026-backup/`. It creates `inventory.json` a record of the hash of every atom. 

### The Waiting Period (Jan - Dec)
Your data sits in Deep Archive. You pay the monthly storage fee. 

### Year 2: The Incremental Run
It is now January 2027. You run the script again. It re-scans your local drive and compares it to the previous `inventory.json` and updates it.

**Scenario A: Data Unchanged (95% of your files)**
* The script sees the hash matches ie. the file is unchanged.
* **Action:** Skips upload entirely.
* **Cost:** $0.
* **Result:** The inventory.json points to the existing file in `2026-backup/`.

**Scenario B: Data Changed (5% of your files)**
* The script sees the hash has changed (or new files added)
* **Action:** Re-packs just those specific atoms into a NEW bag and uploads it to `2027-backup/`.
* **Cost:** Small upload fee for just the changes.
* **Result:** The inventory.json is updated to point to the new file in `2027-backup/`.

### The Cleanup (Pruning)
After the Year 2 run, you have a mix of 2026 and 2027 files in your inventory.json - However, the *old versions* of the changed files are still sitting in `2026-backup/`, costing you money.
* You run `prune.py`.
* It detects that some of the old 2026 bags are no longer referenced by inventory.json
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
inventory_bak_dir = /home/greenc/glacier/invbak
mnt_base = /home/greenc/mnt

# Prices as of January 2026
[pricing]
min_retention_days = 180

# Storage and Upload
price_gb_month = 0.00099
price_put_1k = 0.05

# Recovery Components (N. Virginia)
price_egress_gb = 0.09
price_thaw_standard_gb = 0.02
price_thaw_bulk_gb = 0.0025

# Request Fees
price_req_standard_1k = 0.10
price_req_bulk_1k = 0.025
```

* **`s3_bucket`:** The name of your S3 bucket created during the AWS Configuration Guide above.
* **`target_bag_gb`:** The max size of each bag. Recommend 10GB to 100GB depending on your data. Note that large data files like VMs will get their own bag that is as large as needed to maintain a single bag.
* **`staging_dir`:** A temporary directory used for creating bag (.tar) files. It should have enough free space for the largest bag.
* **`manifest_dir`:** Where to store the manifest files.
* **`inventory_file`:** Location of the inventory.json file.
* **`inventory_bak_dir`:** If set, an optional location to store automated backups of inventory.json - no more than 1 per file created day or per run. Recommended.
* **`mnt_base`:** Root mounting point for a remote server for SSHFS purposes. 
* **`[pricing]`:** Prices need to be filled in manually. They are not required, but useful for generating reports. Prices haved remained generally stable. They might change by locale.

### `list.txt` 
This file defines what locations ("items") are backed up. 

**Format:** Items in `list.txt` can be either a local directory name or a remote directory name. 

**Example list.txt:**
```text
/home/greenc/cache/Backup ::IMMUTABLE
greenc@fox:/home/greenc/Books ::MUTABLE
/home/greenc/Desktop ::MUTABLE
```

Each line has two elements: location <space> ::TAG  .. where tag can be:

* **`::IMMUTABLE (Private Container)`:** Use this for large, standalone directories. The script grants this item total isolation. It will never share a bag with another Atom. If the item is smaller than the bag size (40GB), the bag is shipped partially empty to ensure sovereignty.
* **`::MUTABLE (Shared Container)`:** Use this for parent directories containing many smaller folders. The script breaks these down into individual Atoms and packs them efficiently into shared 40GB bags. 

Examples:

* **`::IMMUTABLE`:** You have a specific software project MyApp_v1/ that contains src/, bin/, lib/, and docs/. Items in this directly are useless apart, you likely would always restore the entire directory.
* **`::MUTABLE`:** You have a Books/ folder with 500 author subdirectories. If you accidentally delete only your Dickens collection, you only have to restore the "Charles Dickens" bag (2GB). You do not have to pay to retrieve an entire 1TB /Books bag just to get one author back.
* **`::MUTABLE`:** You have a Repos/ folder with 50 code repository subdirectories that have a lot of data files (1TB total). If you accidentally delete only 1 repo, you only have to restore the bag that contains that repo. You do not have to pay to retrieve the entire 1TB /Repo bag had it been set IMMUTABLE. 
* **`::MUTABLE`:** Your Desktop/ or My Documents/ folder. You have folders for Taxes, Receipts, Letters, but you also have 50 random PDF files sitting directly in the folder. This is the messy folder, you are not sure what you will need to restore in the future.

The location can be a local directory or a remote directory (see example above). If remote it will connect via sshfs. It assumes you have passwordless ssh configured.

### `exclude.txt`
This is an exclude file used by tar. If you want to exclude directories or files within an Atom directory, use this. The exclude.txt file should be kept in the same location as glacier.py

**Example exclude.txt:**
```text
number/dump
today/bin/data
```

ie. the first line would exclude any files in the path /home/my/number/dump 

### `inventory.json`
This is automatically created and tracks the state of every atom. Deletion of the file will wipe the system and start like a fresh archive. **Keep backups of this file.** It is the database connecting the state of local and remote files.

* **Recommendation:** If you lose this file by accident, delete all prior files on S3 and start over with a fresh upload to prevent massive duplication.
* **`last_metadata_hash`:** Used to detect changes.
* **`tar_id`:** The bag name (e.g., `bag_001`).
* **`archive_key`:** The exact S3 path where this file lives (e.g., `2026-backup/...`).

### `manifests/*.txt` (File-Level Indexes)
These files provide a searchable, recursive list of every single file inside every bag. While list.txt tracks top-level folders to archive, and `inventory.json` tracks "Atoms", the manifests track the specific files *inside* those atomic folders.

* **Purpose:** Allows you to find a specific file (e.g., "Where is `tax_return_2024.pdf`?") without having to pay to restore and download 40GB archives just to look inside them.
* **Format:** Plain text files generated via the `find` command.
* **Location (Local):** Stored in your configured `manifest_dir`.
* **Location (Remote):** Uploaded to `s3://[bucket]/[year]-backup/manifests/`.
* **Storage Class:** **S3 Standard (Hot)**.
    * Unlike the heavy `.tar` bags which are frozen in Glacier Deep Archive, the manifests are kept in Standard storage.
    * **Benefit:** You can instantly download the entire folder of text files and `grep` them to locate data in seconds, effectively for free.

---

## 7. Quick Start

### Step 1: Update glacier.cfg (see options above)

### Step 2: Create list.txt of "items" to backup (see instructions above)

### Step 3: The "Dry Run" (Simulation)
Use this to check what *would* happen without uploading anything.
```bash
./glacier.py list.txt
```
* **Output:** Generates `inventory_dryrun.json` and files in the manifest directory for inspection.

### Step 4: The "Real Run" 
Use this to actually upload files and update the master inventory.json
```bash
./glacier.py list.txt --run
```
* **Output:** Updates `inventory.json` and uploads `.tar` files to S3.
* **Note:** The system will auto-mount remote SSH sources if/as needed. You should have passwordless ssh setup.

---

## 8. Verification & Health Checks

**View Contents:**
```bash
aws s3 ls s3://greenc-bucket/2026-backup/ --human-readable --summarize
```

**Verify Storage Class (Must be DEEP_ARCHIVE):**
If the .tar files are not `DEEP_ARCHIVE`, they are not stored on tape, and you are paying significantly more than necessary. Ignore the /manifests and /systems files, they are intentionally not on tape.
```bash
aws s3api list-objects-v2 --bucket greenc-bucket --prefix 2026-backup/ --query 'Contents[].{Key: Key, StorageClass: StorageClass}' --output table
```

---
## 9. Operations

Operation methods. **Order of `Steps` are significant!**

### Editing list.txt

* **Add a new line to `list.txt`**: 
  * **Step 1**: Add the new line to `list.txt`
  * **Step 2**: `glacier --run`

* **Delete a line from `list.txt`**: 
  * **Step 1**: Remove the line from `list.txt`
  * **Step 2**: Run glacier: `glacier --reset-source PATHNAME --run` where `PATHNAME` will be deleted from `list.txt`
    * *Note: `PATHNAME` must match the deleted line. Do not include any ::tags.*

* **Rename a line in `list.txt`**:
  * **Step 1**: Rename the line in list.txt
  * **Step 2**: Run glacier: `glacier --reset-source OLD_PATHNAME --run` where `OLD_PATHNAME` is the original name from `list.txt`
    * *Note: `OLD_PATHNAME` must match the original line without any ::tags.*

* **Edit ``::ENCRYPT`` tags**:
  * **Step 1**: Add/remove the tag in `list.txt`
  * **Step 2**: Run glacier: `glacier --reset-source PATHNAME --run` where the `PATHNAME` is the name from `list.txt`
    * *Note: `PATHNAME` must match the modified line without any `::tags`.*

### Managing Bags

* **Refresh or Repair a specific bag**:
  * *Use this if a bag is missing from S3 or you suspect corruption.*
  * **Step 1**: Run glacier: `glacier --reset-bag BAG_ID --run`
    * *Example*: `glacier --reset-bag bag_0001 --run`
    * *What happens*: *The script deletes bag_0001 from S3, then immediately re-packs that data into a **new** bag ID (e.g. bag_0055) and uploads it.*

* **Consolidate multiple small bags**:
  * *Use this to merge several small bags into one efficient bag to save on "Request" fees.*
  * *Note: Bags are soveriegn to each line in `list.txt` thus you can not merge bags across `list.txt` lines.*
  * **Step 1**: Run glacier: `glacier --reset-bag BAG_ID_1 BAG_ID_2 ... --run`
    * *Example*: `glacier --reset-bag bag_0001 bag_0002 bag_0003 --run`
    * *What happens*: *The script deletes the three old bags. During the re-upload phase, it will group all that data together and pack it into as few new bags as possible.*
    * See `--repack` to do this for the entire inventory of bags, however it will delete everything from Amazon S3 as if starting over new.

* **Note on Bag IDs**:
  * *When you reset a bag, the old ID (e.g. bag_0001) is permanently retired. The data will reappear in the next available highest bag number. The only way to recover unused IDs is `--repack`*

---

## 9. Advanced Features:

### Encryption
Glacier supports GPG encryption. It works either at the Atomic level or can be applied to a single line in `list.txt`.

**Prerequisites**:
 * Encryption requires a file named `key.txt` to exist in the same directory as the script. To create this file safely:
   * **Method A (Secure)**: Run this command to type your password without it appearing in your history: `stty -echo; printf 'Passphrase: '; read pw; stty echo; echo; echo "$pw" > key.txt && chmod 600 key.txt`
   * **Method B (Quick)**: Run this command (Warning: your password will appear in shell history): `echo 'your_passphrase_here' > key.txt && chmod 600 key.txt`
   * **Record Passphrase**: Record or remember your passphrase or risk never retrieving your encrypted files. Use a secure passphrase! 
 * *Note: If you are setting up glacier for the first time, run glacier (dry run) once to generate inventory_dryrun.json. You can use this file to find the correct Atom pathnames.*

**Setting up Encryption**

  **Option 1: Encrypt an entire source (line in list.txt)** Append the tag ::ENCRYPT to the line.

  ```text
  /home/greenc/cache/Backup ::IMMUTABLE ::ENCRYPT
  ```

  **Option 2: Encrypt specific Atoms only (via `encrypt.txt`)** Create or edit `encrypt.txt` in the same directory as glacier.py. Add the full local mount path of the Atom.
    * **Wrong**: `greenc@mycomputer:/home/greenc/tools/artifact`

    * **Correct**: `/home/greenc/mnt/mycompyter_tools/artifact`

**Modifying Encryption on Uploaded Data**

  * **Option 1: Specific Atoms (via encrypt.txt)** *Use this to efficiently re-upload only the specific bag containing the atom.*

    * **Step 1**: Find the **BagID** containing the atom:
      * Method A: `glacier --find FILENAME` (Look for the bag name in the output).
      * Method B: Open `inventory.json`, find the atom path, and note the `"tar_id"` (e.g., `bag_00042`).
    * **Step 2**: Add or remove the atom path in `encrypt.txt`.
    * **Step 3**: Run glacier: `glacier --reset-bag BAG_ID --run`
      * *Example*: `glacier --reset-bag bag_00042 --run`

  * **Option 2: Entire Source (via list.txt)** *Use this to re-upload the entire source with new settings.*
    * **Step 1**: Add or remove the `::ENCRYPT` tag in `list.txt`.
    * **Step 2**: Run glacier: `glacier --reset-source PATHNAME --run`
      *Note: `PATHNAME` must match the modified line. Do not include any ::tags.*

### Upload speed
Uploading TB of data takes a long time, days; it can cause problems on your network. It is recommend to run Glacier at about 50% of your network capacity to keep you and your ISP happy. To determine capacity, Google "broadband speed test", run it and note the upload speed. If it is reported in bits (not bytes) divide by 8. Then divide in half. For example a 350Mb (bits) connection would be comfortable at about 20MB/s (bytes). Thus:

* `--limit 20`

Will limit Glacier to 20MB/s upload speed. 

### Repacking

The first time Glacier runs, it efficiently creates bags of uniform size. An atom larger than the set bag size can be spread across 
multiple bags. If over time files are deleted from the atom, it might not need multiple bags anymore. But the system still maintains 
multiple bags, only smaller now. This is not efficient. Repacking the bags is like defragging a hard drive, or compacting a tape: it 
reshuffles files around to reduce the number of bags (files) needed to store the same amount of data.

To repack, first run in dryrun mode to see what it would do:

* `./glacier.py list.txt --repack`

Then run the repack

* `./glacier.py list.txt --repack --run`

### Report

Generate a report of assets and costs

* `--report`

### Audit

Confirm local inventory.json is in sync with files on Amazon

* `--audit`

### Find

Search all Bags to find a file. For restoration purposes

* `--find`

---

## 10. Maintenance: Garbage Collection

Because the system only uploads changes, your backup in S3 will eventually be a mix of years (e.g., some 2026 files, some 2027 files).

Use `prune.py` to identify and delete only the files that are duplicated.

```bash
./prune.py                      # Dry Run
./prune.py --delete             # Execute Delete
./prune.py --delete --check-age # Refuse delete any files younger than 180 days
```

## 11. Full automation

The below options run glacier immediately followed by prune which is recommended. If glacier does not exit gracefully prune will not run.

### Option A: Once a Year (Safest/Simplest)

```bash
# Run at 2:00 AM on January 15th every year
0 2 15 1 * cd /home/greenc/glacier && ./glacier.py list.txt --run && ./prune.py --delete >> /home/greenc/glacier/backup.log 2>&1
```

### Option B: Every ~182 Days (Twice a Year) 

This strategy maximizes protection while strictly avoiding Amazon's early deletion fees.

```bash
# Run at 2:00 AM on January 1st
0 2 1 1 * cd /home/greenc/glacier && ./glacier.py list.txt --run && ./prune.py --delete >> backup.log 2>&1
# Run at 2:00 AM on July 2nd
0 2 2 7 * cd /home/greenc/glacier && ./glacier.py list.txt --run && ./prune.py --delete >> backup.log 2>&1
```

### Option C: Exact 182-Day Interval (Day of Year Logic)

Same as Option B but a precise day-count. In a leap year, Option B would technically shift by one calendar day, but in C it stays exactly 182 days apart from the start of the year. Your crontab may or may not support this formatting, be sure to test it first.

```bash
# Executes only on Day 182 and Day 364 of each year
0 2 * * * [ $(($(date +\%j) \% 182)) -eq 0 ] && cd /home/greenc/glacier && ./glacier.py list.txt --run && ./prune.py --delete >> /home/greenc/glacier/backup.log 2>&1
```

Why 182 days and not 180? Amazon S3 Glacier counts the "Early Deletion" period in seconds from the moment the upload completes. There are also timezone offsets and other things. This gives you some leeway. You may even want 190 days to be safe.

This system, as currently designed, is for backup strategies every six months or yearly. You *can* upload more frequently than 182 days, but things get complicated quickly both technically and financially. For example, you may incure extra fees for duplicate copies (tape drives are additive not overwrite) if you forget to prune. You may incure extra fees if you make a modification change at less than 180 days. The scenarios are variable. As a suggestion: load this repo into AI and ask it specifically what you want to achieve.

---

## 11. Restoration Procedure (WARNING: Costs $$$)

## Disaster Recovery (system):
If you lose your local computer, you can bootstrap the entire Glacier system which is automatically backed up in the "system" folder on S3. This will cost almost nothing because it's just a few text files and not on tape drive.

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

## Disaster Recovery (files):

Be aware of the costs to recover data. To thaw 10TB might cost $1,000. This is why this system was designed to be modular: you can download only the specific Bags that contain the files you need. A 40GB bag might cost around $4 to recover.

### Step 1: Locate the Bag
* Option 1: Use `glacier.py --find FILENAME` to determine the Bag identifier
* Option 2: Open `inventory.json` and find the `archive_key` for the folder you need.

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

* **Note**: Tier "Bulk" is about 20% cheaper than Tier "Standard"

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
**IMPORTANT:** Use the `-S` flag to handle sparse files efficiently. This is particularly critical for VM images.
```bash
tar -Sxvf rabbit_host-agros_bag_003.tar -C /home/greenc/Backup/VMs/
```

### Purge your S3 account
To delete everything on your S3 Glacier tape backup
```bash
aws s3 rm s3://your-bucket-name --recursive
```
This is irreversible. 
