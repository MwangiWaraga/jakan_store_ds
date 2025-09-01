# Oraimo Scraper - Scheduling Guide

## � Before You Start

**Replace these placeholders with your actual paths:**
- `[YOUR_PROJECT_PATH]` → Your full project directory path  
  Example: `c:\Users\YourName\Desktop\Projects\oraimo_scrap`
- `[YOUR_CREDS_PATH]` → Your full credentials file path  
  Example: `c:\Users\YourName\Desktop\Projects\oraimo_scrap\creds\gsheets-user-creds.json`

## �🚀 First Time Setup

### 1. Create Your Batch Script
Copy the template to create your personalized script:
```batch
copy run_scraper.bat.template run_scraper.bat
```

Then edit `run_scraper.bat` and customize these paths:
- `YOUR_PROJECT_PATH_HERE` → Your actual project path
- `YOUR_FULL_PATH_TO_CREDS_JSON` → Your full path to credentials file

**Example:**
```batch
cd /d "[YOUR_PROJECT_PATH]"
set GOOGLE_APPLICATION_CREDENTIALS=[YOUR_CREDS_PATH]
```

### 2. Test Your Script
Run your customized script to make sure it works:
```batch
.\run_scraper.bat
```

## ✅ Security Note
Your `run_scraper.bat` file is automatically ignored by Git (it's in `.gitignore`) to protect your personal paths and credential locations.

## 🕒 Scheduling Options

### Option 1: Windows Task Scheduler (Recommended)

#### Step-by-Step Setup:

1. **Open Task Scheduler:**
   - Press `Win + R`, type `taskschd.msc`, press Enter
   - Or search "Task Scheduler" in Start menu

2. **Create Basic Task:**
   - Click "Create Basic Task..." in right panel
   - Name: `Oraimo Price Scraper`
   - Description: `Daily scraping of Oraimo product prices`

3. **Set Trigger (When to run):**
   - **Daily:** Choose your preferred time (e.g., 9:00 AM)
   - **Weekly:** Choose day(s) and time
   - **Monthly:** Choose specific day(s)

4. **Set Action:**
   - Choose "Start a program"
   - Program/script: `[YOUR_PROJECT_PATH]\run_scraper.bat`
   - Start in: `[YOUR_PROJECT_PATH]`

5. **Advanced Settings:**
   - Right-click your task → Properties
   - **Security options:** "Run whether user is logged on or not"
   - **Configure for:** Windows 10/11
   - **Settings tab:** Check "Run task as soon as possible after a scheduled start is missed"

#### Recommended Schedules:
- **Daily:** 9:00 AM (business hours for fresh data)
- **Twice Daily:** 9:00 AM and 6:00 PM
- **Weekly:** Monday 8:00 AM (start of business week)

### Option 2: PowerShell Scheduled Job

Create a PowerShell scheduled job:

```powershell
# Run this in PowerShell as Administrator
$trigger = New-JobTrigger -Daily -At "9:00 AM"
$scriptBlock = { & "[YOUR_PROJECT_PATH]\run_scraper.bat" }
Register-ScheduledJob -Name "OraimoScraper" -ScriptBlock $scriptBlock -Trigger $trigger
```

### Option 3: Manual Quick Run

Double-click `run_scraper.bat` anytime to run manually.

## 📊 Monitoring & Logs

### Success Indicators:
- ✅ Batch script shows "Scraper completed successfully"
- ✅ Google Sheets gets updated with new data
- ✅ Process completes in ~30-45 seconds

### Log Files:
- The scraper creates log entries with timestamps
- Check console output for any errors
- Google Sheets will show latest data with timestamps

### Troubleshooting:
- If task fails, check that virtual environment exists
- Ensure Google credentials file is in place
- Verify internet connection
- Check Google Sheets API quotas

## 🔧 Maintenance

### Monthly Tasks:
- [ ] Check Google Sheets for data consistency
- [ ] Review log outputs for any warnings
- [ ] Verify virtual environment is healthy
- [ ] Update dependencies if needed: `pip install --upgrade -r requirements.txt`

### Backup:
Your current setup is backed up in Git. Key files:
- `run_scraper.bat` - Execution script
- `oraimo_scraper.py` - Main scraper
- `creds/` - Credentials (not in Git for security)
- `requirements.txt` - Dependencies

## 📈 Expected Results

**Performance:**
- Execution time: ~30-45 seconds
- Products scraped: ~220-250 items
- Pages visited: ~12 pages (efficient!)
- Data uploaded to Google Sheets automatically

**Data Quality:**
- Product names, prices, ratings, availability
- Categorized by product type
- Timestamped for historical tracking
- Deduplicated within categories

## 🚀 Ready to Schedule!

Your scraper is optimized and ready for automation. Choose your preferred schedule and set it up with Task Scheduler for reliable daily runs!
