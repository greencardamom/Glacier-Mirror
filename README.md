<!--
Copyright (c) 2026 Greencardamom
https://github.com/greencardamom/Glacier

This documentation is licensed under a Creative Commons Attribution-ShareAlike 4.0 International License.
See LICENSE.md for more details.
-->
    =========================================
          G L A C I E R   M I R R O R
    =========================================

# Glacier Mirror
![Version](https://img.shields.io/badge/Version-1.0-blue)
![Storage](https://img.shields.io/badge/Storage-S3_Deep_Archive-informational)

**Created:** January 2026  
**Author:** Greenc

A Python tool for mirroring your files on Amazon's S3 "Glacier" Deep Archive ([a tape-based service](why-tape-tech.md)). Maximize cost savings. Cheaper than buying a tape drive youself and definitely easier.

Check out [Tape Backup Cost Analysis](tape-cost-analysis.md) to determine when buying your own tape storage becomes cost-effective. Until then, cloud tape backup is an affordable option. 

## Table of Contents
1. [Introduction](#1-introduction-the-tape-backup-insurance-policy)
2. [Features](#2-features)
3. [Prerequisites & Dependencies](#3-prerequisites--dependencies)
4. [AWS Configuration Guide](#4-aws-configuration-guide)
5. [Configuration Files](#5-configuration-files)
6. [Quick Start](#6-quick-start)
7. [Operations](#7-operations)
8. [Automation](#8-automation)
9. [Advanced Features](#9-advanced-features)
10. [Recovery](#10-recovery)
11. [Appendix: Manual File Extraction](#11-appendix-manual-file-extraction)

---

## 1. Introduction: The Tape Backup Insurance Policy

### The Problem
You have multiple TBs of data stored locally on drives. Perhaps mirrored across multiple drives. But it is still not secure against catastrophic loss: fire, lightning, theft, magnetic degradation, filesystem corruption, etc. Buying an LTO tape drive is very expensive, a hassle to manage, and overkill for most cases. You want a solution that will continuously mirror the data to tape, at a tier-1 datacenter that is cheap, secure and that will provide peace of mind "forever".

### The Solution: AWS S3 Deep Archive
Enter "Glacier" Deep Archive, the AWS S3 tape backup service. It is cheap to upload and store (approx. **$1.00 per TB/month**). However, **restoring** is expensive for big things, and cheap for small things. This system aims to obtain the greatest benefit for the cheapest price.

### The Strategy: "Leaf Bags"

In theory one could make a single giant tar file. But if you wanted to restore a single directory you would need to download the entire tar ball which is very expensive. Or you could upload every file individually, but due to per-file API costs this gets expensive. There is a sweet spot where a few thousand files costs literally pennies for upload. The strategy then is to containerize the data into uniform size 'shipping containers' (.tar files) called "Bags" of about 40GB each (you choose the size). Then track what the bags contain in a local database (JSON file). This method reduces how many files are uploaded (API costs), and reduces how many files are needed to download (API costs) when you need to restore some files.

In this system you define what is uploaded according to "branches" and "leaves" - this creates a list of items which are put into bags, the physical tar file.

A simple example:

```text
/home/books/A
/home/books/B
...
/home/books/Z
```

The branch is `/home/books` which has 26 leaves e.g. `/home/books/A` is a leaf. Each leaf gets added to a bag until the bag reaches 40GB then a new bag is created and so on. Thus bag1 might contain A..C, bag2 contains D..F, etc.. the bags might have a name like `books_bag1.tar`, `books_bag2.tar`, etc..

The bags are uploaded to Glacier. Every 180 days via cron the local files are checked for changes, if so new bags created and uploaded and the old bags deleted.

---

## 2. Features
* **Tape Mirroring**: Automatically synchronizes local directory trees to Amazon S3's tape service for long-term cold storage.
* **Tape Storage**: Files can be unattached from mirroring turning it into traditional tape storage (in the "cloud").
* **Optimized Packing**: Aggregates small files into uniform size "bags" to bypass AWS minimum object size penalties and reduce API request fees.
* **Security**: Supports client-side GPG (AES-256) encryption so data is fully opaque before it leaves your machine.
* **Financial Intelligence**: Generates reports on storage efficiency, monthly run rates, and estimated recovery costs.
* **State-Aware Recovery**: Automates the "Thaw, Download, Decrypt, and Extract" pipeline with a single command.
* **Logging**: Maintains a ledger of every AWS transaction and byte transferred for full accountability.
* **Flexible Scheduling**: Supports flexible mirroring intervals (e.g., "Every 180 Days") on a per-directory basis.
* **Vendor Neutral**: Program is a single Python script. Stores data in standard GNU Tar and GPG formats, ensuring files can always be recovered without this software.
* **Total Recovery**: Keeps a copy of all programs and metadata on S3 so entire system can be recreated from scratch on a new machine.
* **Intelligent Design**: Designed and written in collaboration with AI and a professional programmer.
* **Dry Run mode**: Program prints exactly what it would do but performs zero network actions or inventory writes. Default.

---

## 3. Prerequisites & Dependencies

Before running the system, ensure the following are installed:

### System
* **Linux**

### Tools
* **Python 3.8+**
* **tar:** Standard GNU tar.
* **gpg (Optional):** If you want to encrypt files. Standard installed.
* **sshfs (Optional):** If you are backing up files from remote servers.
    * `sudo apt install sshfs`
* **ssh passwordless login:** If you are backing up files from remote servers.
  **Example**: `ssh user@machine.net` (logs in without prompting for a password)

### Python Libraries
* **boto3:** The AWS SDK for Python.
    * `pip install boto3`
* **tqdm:** For progress bar display.
    * `pip install tqdm`

---

## 4. AWS Configuration Guide

Setup Glacier Deep Archive on AWS

### Step 0: Account & CLI Installation
1.  **Sign Up:** Create an account at [aws.amazon.com](https://aws.amazon.com).
2.  **Install CLI:** Install the official AWS Command Line Interface.
    * **Linux:** https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

### Step 1: Create the Bucket
1.  Log in to the **AWS Console** and search for **S3**.
2.  Click **Create bucket**.
3.  **Bucket Name:** Choose a unique name (e.g., `my-bucket`). *Update glacier.cfg.*
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

## 5. Configuration Files

### `glacier.cfg`

**Example:**
```ini
[settings]
s3_bucket = my-bucket
target_bag_gb = 40
scan_interval_days = 190 # recommend at least > 180 + 2 due to various technical factors
staging_dir = /path/to/glacier/stage
manifest_dir = /path/to/glacier/manifests
inventory_file = /path/to/glacier/inventory.json
inventory_bak_dir = /path/to/glacier/invbak
mnt_base = /path/to/mnt

[encryption]
method = password
password_file = /path/to/glacier.key
gpg_key_id = youremail@example.com

[AWS]
aws_account_id = 1234567890
aws_region = us-alaska

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
* **`target_bag_gb`:** The max size of each leaf bag. Recommend 10GB to 100GB depending on your data. Note that large data files like VMs will get their own leaf bag that is as large as needed to maintain a single leaf bag.
* **`scan_interval_days`:** How many days to wait before attempting mirror. Recommend a number >= 182 due to slack in AWS accounting.
* **`staging_dir`:** A temporary directory used for creating leaf bag (.tar) files. It should have enough free space for the largest leaf bag.
* **`manifest_dir`:** Where to store the manifest files.
* **`inventory_file`:** Location of the inventory.json file.
* **`inventory_bak_dir`:** If set, an optional location to store automated backups of inventory.json - no more than 1 per file created day or per run. Recommended.
* **`mnt_base`:** Root mounting point for a remote server for SSHFS purposes. 
* **`[encryption]`:** See documention for setting up encryption.
* **`[AWS]`:** This is created automatically the first time Glacier Mirror runs. It is used for logging. If you wish to keep this private change the values to `REDACTED` and they will show in the logs as redacted. e.g. `aws_account_id = REDACTED`
* **`[pricing]`:** Prices need to be filled in manually. They are not required, but useful for generating reports. Prices haved remained generally stable over time. They can change by locale.

### `tree.cfg` 

This file defines what locations ("branches") are mirrored. Branches are 1-level up from a leaf. Branches are nothing more than a directory name. One branch can have multiple leaves associated underneath it.

**Format:** Branches in `tree.cfg` can be either a local directory name or a remote directory name. 

**Example tree.txt:**
```text
/path/to/data ::IMMUTABLE 
/path/to/desktop ::MUTABLE ::ENCRYPT
user@machine.net:/path/to/data ::MUTABLE
```

Each line has two elements: *location* and *::TAG*

**Logic Tags**

* **`::MUTABLE`**
  * **Behavior**: "Shared Storage" mode. It treats every top-level subdirectory as a separate leaf.
  * **Use Case**: Best for active folders where you update new sub-projects constantly (e.g., /home/user/projects).
  * **Example**: `/path/to/data ::MUTABLE`
  * **Example**: You have a Books/ folder with 500 author subdirectories. If you accidentally delete only your Dickens collection, you only have to restore the "Charles Dickens" bag (2GB). You do not have to pay to retrieve an entire 1TB /Books bag just to get one author back.

* **`::IMMUTABLE`**
  * **Behavior**: "Sovereign Storage" mode. It treats the entire path as a single leaf.
  * **Use Case**: Best for single massive entities or drives that should never be split up (e.g., a specific hard drive or a VM image folder).
  * **Example**: `/path/to/data ::IMMUTABLE`
  * **Example**: You have a specific software project MyApp_v1/ that contains src/, bin/, lib/, and docs/. Items in this directly are useless apart, you likely would always restore the entire directory.

**Action Tags**

* **`::COMPRESS`**
  * **Behavior**: Runs the leaves through gzip, stored inside the bag as .tar.gz
  * **Effect**: Saves space but takes more CPU/time.
  * **Example**: `/path/to/data ::MUTABLE ::COMPRESS`

* **`::ENCRYPT`**
  * **Behavior**: Encrypts the leaves using GPG, stored inside the the bag as .tar.gpg or .tar.gz.gpg
  * **Effect**: Security at all layers from network transfer to storage at S3. Takes more CPU/time.
  * **Example**: `/path/to/data ::MUTABLE ::ENCRYPT`
    * *Note: for text files ::COMPRESS + ::ENCRYPT is ok. For already compressed files (zip, mp3, m4b) recommend only ::ENCRYPT although it won't hurt anything to use both.*

* **`::LOCKED`**
  * **Behavior**: Glacier will refuse to perform any action on this branch (Mirror, Delete, Repack) unless you manually remove this tag.
  * **Effect**: Glacier is no longer a mirror. S3 is a static backup of your bag files. Useful for storing data that will never change locally.
  * **Example**: `/path/to/data ::MUTABLE ::ENCRYPT ::LOCKED`

* **::EXCLUDE <Pattern>**
  * **Behavior**: Skips specific folders within that branch. Allows for fine-tuning settings within a master branch.
    * *Note: You can have multiple exclude tags on one line.*
  * **Example**: `/path/to/data ::MUTABLE ::EXCLUDE temp_folder ::EXCLUDE log`
  * **Example**: `/path/to/data/temp_folder ::IMMUTABLE ::COMPRESS`
  * **Example**: `/path/to/data/log ::IMMUTABLE ::ENCRYPT`
    * *Note: In the above example every subdirectory in the branch /path/to/data is a leaf stored as-is, except for temp_folder and log which are stored as compressed and encrypted.*

The /path/to/data can be a local directory or a remote directory eg. user@machine.net:/path/to/data -- If remote it will connect via sshfs and rsync. It assumes you have passwordless ssh configured.

### `exclude.txt`
This file contains a list of patterns to exclude from every backup job. It is used by both tar and rsync to filter out unwanted files (like temporary caches, trash, or build artifacts).

**Rules for exclude patterns**:
* **Do NOT use full paths**: Do not write `/home/user/data/junk`
* **Recursive by default**: If you write a folder name, it will be excluded everywhere it is found in every branch in the entire tree.
* **Wildcards**: You can use * (matches anything) and ? (matches one character).

To exclude a specific directory, you must write the path relative to the root of the branch being backed up.
* **Example 1 (The Global)**: `__pycache__` (Excludes` __pycache__` folders anywhere they appear in any branch.)
* **Example 2 (The Specific)**: If you are mirroring the branch `/home/user/projects` and want to exclude just the build folder inside "Project-X": `Project-X/build` (This excludes `/home/user/projects/Project-X/build`).

Warning: Since this file is global, if you have another branch (e.g., `/home/user/archive`) that also happens to have a folder named `Project-X/build`, it will be excluded there too.

* **Tip**: For truly isolated exclusions that apply only to one specific branch, use the ::EXCLUDE tag in your tree.cfg instead: `/home/user/projects ::MUTABLE ::EXCLUDE Project-X/build`

**Example `exclude.txt`:**
```text
number/dump
__pycache__
```

### `inventory.json`
This is automatically updated and tracks the state of every leaf. Deletion of the file will wipe the system and start over like a fresh archive. **Keep backups of this file.** It is the database connecting the state of local and remote branches.

* **Recommendation:** If you lose this file by accident, delete all prior files on S3 and start over with a fresh upload to prevent massive duplication.
* **`last_metadata_hash`:** Used to detect changes.
* **`tar_id`:** The bag name (e.g., `bag_001`).
* **`archive_key`:** The exact S3 path where this file lives (e.g., `2026-backup/...`).

### `manifests/*.txt` (File-Level Indexes)
These files provide a searchable list of every single file inside every bag. While `tree.cfg` tracks top-level branches to archive, and `inventory.json` tracks leaves, the manifests track the specific files *inside* those leaf folders.

* **Purpose:** Allows you to find a specific file (e.g., "Where is `tax_return_2024.pdf`?") without having to pay to restore and download 40GB archives just to look inside them.
* **Format:** Plain text files generated via the `find` command.
* **Location (Local):** Stored in your configured `manifest_dir`.
* **Location (Remote):** Uploaded to `s3://[bucket]/[year]-backup/manifests/`.
* **Storage Class:** **S3 Standard (Hot)**.
    * Unlike the `.tar` bags which are frozen in Glacier Deep Archive, the manifests are kept in Standard storage.
    * **Benefit:** You can instantly download the entire folder of text files and `grep` them to locate data in seconds, effectively for free.

---

## 6. Quick Start

How to test the system.

### Step 1: Update glacier.cfg (see options above)
* Update the directory paths and `s3_bucket`. Set `target_bag_gb` to 1 GB for this test.

### Step 2: Make and populate test directories
* **Bash**: `mkdir -p glacier-test/{A,B,C} && for d in A B C; do fallocate -l 400M glacier-test/$d/test_data.bin; done`
  *Note: this will create directories off your CWD called glacier-test/A B and C each with a 400MB test_data.bin*

### Step 3: Create `tree.cfg` with a branch
* Add this line to `tree.cfg`: `/absolute/path/to/glacier-test ::MUTABLE`

### Step 4: The "Dry Run" (Simulation)
Use this to check what *would* happen without uploading anything.
* **Dry-run**: `./glacier.py --mirror-tree`
* **Output:** The script will print the plan to the screen, showing how it packs A, B, and C into specific bags. It does not modify any files.

### Step 5: The "Real Run" 
Use this to actually upload files and update the master inventory.json
* **Live Run**: `./glacier.py --mirror-tree --run`
* **Output:** Updates `inventory.json` and uploads `.tar` files to S3.
  *Note: There is an S3 cost for this test. Due to the 180-day minimum retention policy, you will be billed for 6 months of storage upon deletion, but for 1.2GB this totals less than $0.01.*

### Step 6: Cleanup
To cleanup after the test. **Important** to avoid any latent charges from Amazon.

* **Purge from S3 & Inventory**: This command will delete the S3 objects associated with this branch and remove the entry from `inventory.json`.
  * `./glacier.py --delete-branch '/absolute/path/to/glacier-test' --run`
  * *Note: Ensure the path matches exactly what you put in `tree.cfg` not including any ::TAGS*
* **Remove Local Files**: Delete the dummy data.
  * `rm -rf glacier-test`
* **Update Config**: Open `tree.cfg` and remove the line you added in Step 3.

---

## 7. Operations

Operation features and methods

### Managing Branches (changes to `tree.cfg`)

**Order of `Steps` are significant!**

* **Add a new branch to `tree.cfg`**: 
  * **Step 1**: Edit `tree.cfg` and add the line.
  * **Step 2**: Run `glacier --mirror-branch /path/to/branch --run`
    * *(The script will detect the new line and upload it.)*

* **Delete a branch from `tree.cfg`**: 
  * **Step 1**: Run `glacier --delete-branch /path/to/branch --run`
    * *(Removes the data from S3 and the local inventory database.)*
  * **Step 2**: Edit `tree.cfg` and remove the line.

* **Rename a branch in `tree.cfg`**:
  * *Note: In object storage, a rename is simply a "Delete Old" followed by an "Add New".*
  * **Step 1**: Run `glacier --delete-branch /path/to/branch --run`
  * **Step 2**: Edit `tree.cfg` and rename the line.
  * **Step 3**: Run `glacier --mirror-tree --run`

* **Edit ::ENCRYPT, ::MUTABLE or ::IMMUTABLE tags**:
  * *Note: Changing these settings changes the fundamental structure of the archive. You must wipe the old version and re-upload.*
  * **Step 1**: Edit the tag in `tree.cfg`
  * **Step 2**: Run `glacier --mirror-branch /path/to/branch --force-reset --run`
    * *(The `--force-reset` flag tells the script to delete the existing S3 data for this branch and immediately re-upload it with the new settings.)*

* **Note on Leaf Bag IDs**:
  * Bag IDs (bag_0001) are immutable, once used they are not used again. Thus if the system needs to delete an old bag and upload a new it will have a new ID. It is possible to globally redo leaf bag IDs to compact and reuse retired numbers, see `--repack`

### Reporting & Inspection

Tools to view your archive without modifying data.

* **`--show-tree`**: Displays the full hierarchy of branches, bags, and leaves currently tracked in your local inventory.
  * Run `./glacier.py --show-tree`

* **`--show-branch`**: Shows details about a specific branch, including which bags it owns and the last time it was scanned.
  * Run `./glacier.py --show-branch /path/to/branch`

* **`--show-branch`**: Lists all leaves contained inside a specific `tar_id` or bag ID (e.g., bag_00042).
  * Run `./glacier.py --show-bag bag_00042`

* **`--find`**: Searches all local manifests for a specific filename. Useful for checking in which bag a file exists.
  * Run `./glacier.py --find "my_important_doc.pdf"`

  * The `--find` flag performs a reverse lookup, helping you locate which specific bag contains a file you need to restore.

  * **How it works**:
    * **Manifest Scan**: The script `greps` the local text manifest files (stored in your configured manifest_dir) for the search string.
    * **Output**: It prints the specific bag ID (e.g., bag_00451) and the branch path.
    * **Why use it**: If you need to recover a single file, this tells you exactly which .tar object to download from the S3 console, saving you from downloading the whole archive.

* **`--report`**: Generates a detailed breakdown of storage usage, packing efficiency, and estimated monthly costs per branch.
  * Run `./glacier.py --report`

  * **What is included in the report**
    * **Inventory State**: A breakdown of every branch, including the number of Bags and total Size.
    * **Packing Efficiency (Waste %)**: A metric showing how much "empty air" is inside your bags.
    * **Repack Risk**: The estimated cost to `--repack` a branch.
    * *Note: This calculation includes Early Deletion Fees and Request costs to reorganize data.*
    * **Monthly Storage Cost**: Estimated bill based on current AWS Glacier Deep Archive rates.
    * **Disaster Recovery Estimates**:
    * **Thaw Cost**: The standard retrieval fee to pull your entire archive off the tape archive.
    * **Egress Cost**: The bandwidth cost to download the data to your local machine.

* **`--audit`**: Checks if the files listed in your local `inventory.json` actually exist in the S3 bucket.
  * Run `./glacier.py --audit`

  * **What the Audit checks**:
    * **Completeness**: Ensures every leaf bag ID listed in your inventory actually exists on S3.
    * **Orphans**: Identifies files existing on S3 that are not in your local inventory. Delete these to save monthly costs.

  * **Understanding Audit Results**
    * **[OK]**: Your local state and S3 are perfectly synchronized.
    * **[ALERT]**: One or more leaf bags are missing from S3. This indicates a failed upload or accidental deletion on the AWS console. See **Managing Leaf Bags** -> **Refresh or Repair a specific leaf bag** 
    * **[NOTE]**: Orphan leaf bags were found. These are safe to delete.
      * **Method A**: `glacier.py --prune --run` 
        * *Delete all orphans on S3*
        * *The age check logic will stop leaf bags from being deleted to avoid early deletion fees on leaf bags younger than 180 day*
      * **Method B**: `aws s3 rm s3://my-bucket/2026-backup/my_host-book_bag_003.tar` 
        * *Target a specific leaf bag by name*
        * *(This method is not suggested it bypasses Glacier's logging of transactions)*

### Maintenance & Tuning

Options for optimizing storage, managing bandwidth, and system cleanup.

**Optimization & Cleanup**

* `--repack`
  * **Description**: A global defragmentation tool. It runs the "Bin Packing" algorithm across the entire inventory tree.
  * **Why use it**: 
    * **Optimize Content**: Reshuffles file-to-bag assignments to ensure all bags are as close to the target size (e.g., 40GB) as possible.
    * **Recover Bag IDs**: Optimizes the numbering sequence, potentially recovering and reusing "retired" bag numbers from deleted archives to close gaps in the sequence.
  * **Usage**: `./glacier.py --mirror-branch /path/to/branch --repack --run`
    * Note: This may trigger re-uploads if files are moved to new, more efficient bags. Use sparingly, fragmented bags are often not a problem vs. re-uploading entire branches.*
  * It is a good idea to run an audit after a repack to make sure there are no orphan leaf bags taking up space
    * Audit: Run `glacier --audit`
      *(If orphans are found: `--prune --run`)*

* `--prune`
  * **Description**: It scans the S3 bucket for any bag files that exist but are not present in your `local inventory.json`. It then deletes the orphans.
  * **Why use it**: Prune would typically be run right after `--mirror-tree --cron` to ensure your S3 bucket only has the needed files. It is also run if `--audit` finds orphans.
  * **Usage**: Run `./glacier.py --prune --run`

**System Configuration**

* `--limit LIMIT`
  * **Description**: Sets a hard bandwidth limit for uploads in MB/s (Megabytes per second).
  * **Why use it**: To prevent the backup script from consuming your entire internet connection bandwidth during large initial syncs.
  * **Usage**: Run `./glacier.py --mirror-tree --limit 10 --run`
    * *(Limits upload speed to 10 MB/s)*

  * Uploading TB of data takes a long time, even days. It can cause problems for your network. It is recommend to run Glacier Mirror at about 50% of your network capacity to keep you and your ISP happy. To determine capacity, check your upload speed at a broadband speed test service. If it is reported in bits (not bytes) divide by 8. Then divide in half. Examples:

  | Advertised Upload (Mbps) | ~Max Capacity (MB/s) | Comfortable `--limit` (50%) |
  | :--- | :--- | :--- |
  | 10 Mbps | 1.25 MB/s | `--limit 1` |
  | 40 Mbps | 5 MB/s | `--limit 2` |
  | 100 Mbps | 12.5 MB/s | `--limit 6` |
  | 300 Mbps | 37.5 MB/s | `--limit 18` |
  | 500 Mbps | 62.5 MB/s | `--limit 30` |
  | 1000 Mbps (Gigabit) | 125 MB/s | `--limit 60` |

* `--tree-file TREE_FILE`
  * **Description**: Tells the script to use a different configuration file instead of the default `tree.cfg`.
  * **Why use it**: Useful for testing.
  * **Usage**: `./glacier.py --mirror-tree --tree-file my_test_tree.cfg --run`
    * **CAUTION**: It is not possible to run multiple tree.cfg files e.g. `redwood.cfg` and `birch.cfg` - Glacier.py has only 1 inventory.json and a single S3 bucket. If you want multiple trees create a new S3 Bucket for each tree and install multiple installations of Glacier Mirror for each tree.

* `--run`
  * **Description**: The Global Safety Switch.
  * **Behavior**:
    * **Without `--run`**: The system runs in DRY RUN mode. It calculates hashes, checks S3, and prints exactly what it would do (e.g., "Uploading X", "Deleting Y"), but performs zero network actions or inventory writes.
    * **With `--run`**: The system executes the plan, uploads files, deletes objects, and updates inventory.json.

## 8. Automation

* `--cron`
  * **Description**: Suitable for automated scheduling with `crontab`.
  * **Behavior**:
    * Instead of immediately scanning files, it first checks the `last_scan` timestamp in your inventory.json.
    * If the time elapsed is less than the `scan_interval_days` (defined in `glacier.cfg`), the script exits silently without doing any work.
    * If the interval has passed, it wakes up, performs the mirror, and updates the timestamp.
  * **Why use it**: Allows you to schedule the script to run daily (e.g., via crontab), while ensuring it only actually spins up your hard drives and checks for changes once every 6 months (or whatever interval you set).
  * **Usage**: 
    * **Tree**: `./glacier.py --mirror-tree --cron --run`
    * **Branch**: `./glacier.py --mirror-branch /path/to/branch --cron --run`
      *(This allows you to schedule branches on different cadences depending on urgency.)*

### Full automation

**Option A**: The Whole Tree

This runs the entire tree. It is the simplest method.

  * **Schedule**: Runs daily at 2:00 AM.
  * **Logic**: The script checks `inventory.json`. If 180 days (or your configured interval) haven't passed for any given branch, it exits silently.
  * **Prune**: We append the prune command to clean up orphaned bags after the mirror is done.
```bash
# Run entire tree. Script runs every 6 months per default setting in glacier.cfg
0 2 * * * cd /path/to/glacier && ./glacier.py --mirror-tree --cron --run && ./glacier.py --prune --run >> /path/to/glacier/logs/backup.log 2>&1
```

**Option B**: Staggered Branches

Use `--interval` if you want to process different branches at different cadences e.g. important papers every 6 months and deep archives every year or longer.

```bash
# Branch 1: "Work" - Runs every 6 months
0 2 * 1,7 * cd /path/to/glacier && ./glacier.py --mirror-branch '/path/to/work' --cron --interval 180 --run >> /path/to/glacier/logs/work_backup.log 2>&1

# Branch 2: "Mediafiles" - Runs every 2 years
0 5 * 6 * cd /path/to/glacier && ./glacier.py --mirror-branch '/path/to/mediafiles' --cron --interval 730 --run >> /path/to/glacier/media_backup.log 2>&1
```

---

## 9. Advanced Features

### Encryption
Glacier supports GPG encryption using the GPG program.

There are two options for storing your password. One is easier and less secure; the other more complex but more secure.

* **Simple Password Method**
  * *Good for personal use and simple setups*
  * Create a file with just your password (no new lines or padded spaces)
  * In `glacier.cfg`: Set `method = password` and `password_file = /path/to/glacier-key.txt` 
  * Set strict permissions: `chmod 600 glacier-key.txt`
    * *(It's good practice to put the file in a different directory such as ~/secrets or ~/.glacier with the directory `chmod 700` permissons)*
  * To decrypt 1 file: `gpg --output decrypted_file.tar --decrypt encrypted_backup.gpg`
    * *(gpg will prompt for a password)*
  * To decrypt multiple files in a script: `gpg --batch --passphrase-file /path/to/glacier-key.txt --output decrypted_file.tar --decrypt encrypted_backup.gpg`    

* **Key-Based Method**
  * *More secure for automated/server environments*
  * Run `glacier.py --generate-gpg-key` to create a key
    * *During this process, you'll be asked to provide your name, email, etc.*
  * Run `glacier.py --show-key-id` to see your newly created key
  * In `glacier.cfg`: Set `method = key` and `gpg_key_id` to match your key's email or ID
  * To decrypt on any system with your private key: `gpg --output decrypted_file.tar --decrypt encrypted_backup.gpg`
  
* **Managing Keys:**
  * Export your public key (for encryption only): `glacier.py --export-key glacier_public.asc --key-type public`
  * Export your private key (for decryption, handle with care): `glacier.py --export-key glacier_private.asc --key-type private`
  * Import a key on another system:
      ```
      gpg --import glacier_public.asc   # For encryption only
      gpg --import glacier_private.asc  # For decryption (keep secure!)
      ```  
* **Warning**: Methods are not interchangeable i.e. bags encrypted with the Simple method will not decrypt with the Key method even if they share the same password.
  * How to check the method used: `gpg --list-packets encrypted_backup.gpg`
    * **For password encryption**: Look for "sym alg" (symmetric algorithm)
    * **For key encryption**: Look for "pubkey alg" (public key algorithm)

* **Versions**: Different GPG versions might have slight differences in command syntax. These instructions are for GPG 2.x. If using GPG 1.x, some options may differ.

---

## 10. Recovery

The system includes restore functions.

### Basic Workflow

The restore commands are state-aware. You run the same command to initiate a thaw request and to download the files once they are ready.

* **Day 1 (Request)**: Run the command. The script detects the files are Frozen (Deep Archive) and issues a Thaw Request to AWS.
* **Day 2/3 (Recover)**: Run the same command. The script sees the files are Ready, downloads them, decrypts them (if needed), and extracts them to your destination.

**Restore Commands**
* **Restore a single file**: Locates which bag contains the file, requests the thaw, and extracts only that file.
  * Run `./glacier.py --restore-file "tax_return_2024.pdf" --to /home/user/restore_zone --run`

* **Restore a bag**: If you know the ID (e.g., from an audit or log), recover just that bag.
  * Run `./glacier.py --restore-bag bag_00542 --to /home/user/restore_zone --run`

* **Restore a branch**: Recovers the full branch.
  * Run `./glacier.py --restore-branch "/home/user/photos" --to /mnt/external_drive --tier Bulk --run`

* **Restore tree**: Recovers the full tree.
  * Run `./glacier.py --restore-tree --to /mnt/external_drive --tier Bulk --run`

**Restore Options**
 * **`--to PATH`**: Required. The destination directory for recovered files.
 * **`--tier [Standard|Bulk]`**:
    * **Standard (Default)**: Faster (12 hours). 
    * **Bulk**: Cheaper by about 8 times than Standard, but slower (48 hours).

### Bootstrap
Restoration of Glacier Mirror and all data file on a new machine

* **Step 1:** Restore the software
```bash
sudo apt install python3-pip awscli tar gpg
pip install boto3 tqdm
```

* **Step 2**: Configure AWS (see steps elsewhere)

* **Step 3**: Download the config, inventory etc..

```bash
mkdir -p ~/glacier/manifests
cd ~/glacier

# 1. Download System Core (Script, Config, Inventory, Excludes)
aws s3 cp s3://my-bucket/2026-backup/system/ . --recursive

# 2. Download Search Index (Manifests)
aws s3 cp s3://my-bucket/2026-backup/manifests/ ./manifests/ --recursive

# 3. Create your key.txt (GPG password)
```

* **Step 4**: Verify it works
```bash
chmod +x glacier.py
./glacier.py --audit
```

* **Step 5**: Data Restoration
  * See **Recovery** section

### Snowball

Amazon has a service called "Snowball" where they send you a computer with your data on a hard drive. You copy it and send the computer back. The break-even point for this service is around 6TB (as of 2026). If you need to download a large amount and don't want to saturate your network Snowball can be cheaper and faster.


## 11. Appendix: Manual File Extraction
**IMPORTANT:** Use the `-S` flag to handle sparse files efficiently. This is particularly critical for VM images.
```bash
tar -Sxvf rabbit_host-agros_bag_003.tar -C /home/user/Backup
```

#### Workflow 1: Encryption Only
*Used when the `ENCRYPT` tag is present but `COMPRESS` is absent.*

| Step | Source File | Program | Output File / Result |
| :--- | :--- | :--- | :--- |
| 1. Unbag | `bag_XXXXX.tar` | `tar -xf` | `leaf.gpg` |
| 2. Decrypt | `leaf.gpg` | `gpg -d` | `leaf.tar` |
| 3. Extract | `leaf.tar` | `tar -xf` | **Original Directory & Files** |

#### Workflow 2: Encryption + Compression
*Used when both `ENCRYPT` and `COMPRESS` tags are present.*

| Step | Source File | Program | Output File / Result |
| :--- | :--- | :--- | :--- |
| 1. Unbag | `bag_XXXXX.tar` | `tar -xf` | `leaf.gz.gpg` |
| 2. Decrypt | `leaf.gz.gpg` | `gpg -d` | `leaf.tar.gz` |
| 3. Decompress | `leaf.tar.gz` | `gunzip` | `leaf.tar` |
| 4. Extract | `leaf.tar` | `tar -xf` | **Original Directory & Files** |

#### Workflow 3: Compression Only
*Used when only the `COMPRESS` tag is present.*

| Step | Source File | Program | Output File / Result |
| :--- | :--- | :--- | :--- |
| 1. Unbag | `bag_XXXXX.tar` | `tar -xf` | `leaf.tar.gz` |
| 2. Decompress | `leaf.tar.gz` | `gunzip` | `leaf.tar` |
| 3. Extract | `leaf.tar` | `tar -xf` | **Original Directory & Files** |


### Purge your S3 account
To delete everything on your S3 Glacier tape backup
```bash
aws s3 rm s3://your-bucket-name --recursive
```
This is irreversible. 

