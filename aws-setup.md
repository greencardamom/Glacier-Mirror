# AWS Configuration Guide

Setup Guide for Glacier Deep Archive on AWS

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
Run this command on your Linux machine and paste in the keys from `Step 3` when prompted:
```bash
aws configure
```
* **Region:** Use the same region code from Step 1 (e.g., `us-east-1`).
* **Output format:** `json`.
