# Altafsir.com Scraper and Database Sync Scripts

This directory contains two Python scripts for scraping and importing English tafsir data from altafsir.com.

## Scripts Overview

### 1. `scrape_altafsir_com.py`
A comprehensive scraper for extracting English tafsir data from altafsir.com.

**Features:**
- Scrapes all 114 chapters (surahs) and 6236 verses
- English language tafsir only
- Parallel processing for efficiency
- Progress reporting and error handling  
- Rate limiting and scraping etiquette
- Compatible JSON output format
- Automatic fallback to demo data if site is inaccessible

**Usage:**
```bash
# Basic usage
python3 scrape_altafsir_com.py

# Custom output file and settings
python3 scrape_altafsir_com.py --output my_tafsir.json --workers 3 --rate-limit 1.5

# Help
python3 scrape_altafsir_com.py --help
```

**Output Format:**
The script generates a JSON file with the following structure:
- `metadata`: Tafsir information, scraper details, progress tracking, coverage statistics
- `chapters`: Chapter information (114 surahs with names, verse counts, etc.)
- `verses`: Verse-by-verse tafsir with keys like "1:1", "1:2", etc.

### 2. `sync_altafsir_data_to_db.py`
Database import script for loading scraped tafsir data into SQLite database.

**Features:**
- Load JSON data from scraper output
- SQLite database with proper schema
- Data validation and verification
- Progress reporting and error handling
- Duplicate detection and handling
- Transaction support with rollback
- Import logging and statistics

**Usage:**
```bash
# Basic usage
python3 sync_altafsir_data_to_db.py scraped_data.json

# Custom database file
python3 sync_altafsir_data_to_db.py scraped_data.json --database my_tafsir.db

# Verbose output
python3 sync_altafsir_data_to_db.py scraped_data.json --verbose

# Help
python3 sync_altafsir_data_to_db.py --help
```

**Database Schema:**
- `tafsir_resources`: Resource metadata (id, name, author, language, source)
- `chapters`: Quran chapter information (id, names, verse counts, revelation place)
- `verses`: Individual verse tafsir (verse_key, text, resource mapping)
- `import_logs`: Import history and statistics

## Installation

### Prerequisites
- Python 3.7+
- pip package manager

### Dependencies
The scripts will automatically install required packages, or you can install them manually:

```bash
pip install aiohttp beautifulsoup4
```

## Complete Workflow

### Step 1: Scrape Data
```bash
python3 scrape_altafsir_com.py --output altafsir_english_tafsir.json
```

### Step 2: Import to Database
```bash
python3 sync_altafsir_data_to_db.py altafsir_english_tafsir.json
```

### Step 3: Verify Results
```bash
sqlite3 tafsir_database.db "SELECT COUNT(*) FROM verses;"
```

## Configuration Options

### Scraper Options
- `--output, -o`: Output JSON file path (default: altafsir_english_tafsir.json)
- `--workers, -w`: Maximum concurrent workers (default: 5)
- `--rate-limit, -r`: Rate limit in seconds between requests (default: 1.0)

### Sync Options
- `--database, -d`: Database file path (default: tafsir_database.db)
- `--verbose, -v`: Enable verbose logging

## Error Handling

Both scripts include comprehensive error handling:

### Scraper Error Handling
- Network connection failures with retries
- Rate limiting to respect server resources
- Progress saving for recovery from interruptions
- Graceful fallback to demo data if site is inaccessible

### Sync Error Handling
- Data validation before import
- Transaction rollback on errors
- Duplicate detection and resolution
- Import verification with mismatch reporting

## Logging

Both scripts generate detailed logs:
- `altafsir_scraper.log`: Scraper operations and progress
- `altafsir_sync.log`: Database operations and import results
- Console output with real-time progress

## Data Quality

### Coverage Verification
- Expected verses: 6,236 (complete Quran)
- Coverage percentage calculation
- Missing verse detection and reporting

### Data Validation
- JSON structure validation
- Database schema compliance
- Verse key format verification
- Content length validation

## Performance

### Scraper Performance
- Parallel processing with configurable workers
- Rate limiting for server protection
- Efficient batch processing
- Memory-optimized data handling

### Database Performance
- Batch insert operations (1,000 records per batch)
- Database indexes for query optimization
- Transaction management for data integrity
- Progress tracking for large imports

## Troubleshooting

### Common Issues

1. **Connection Error to altafsir.com**
   - Script automatically falls back to demo data
   - Check internet connection and firewall settings

2. **Import Verification Failed**
   - Check JSON file integrity
   - Review error logs for specific issues
   - Verify disk space availability

3. **Database Lock Error**
   - Ensure no other process is using the database
   - Check file permissions
   - Consider using WAL mode for concurrent access

### Recovery Options

1. **Resume Interrupted Scraping**
   - Check for temporary progress files (.tmp extension)
   - Restart scraper with same output filename

2. **Re-import Data**
   - Script handles duplicates gracefully
   - Use `--verbose` flag for detailed progress

## Example Output

### Successful Scraping
```
2025-09-02 06:41:04,724 - INFO - Starting altafsir.com scraping process
2025-09-02 06:41:09,303 - INFO - Scraping completed successfully!

Scraping completed!
Output file: demo_altafsir_data.json
Total verses: 6236
Coverage: 100.0%
```

### Successful Import
```
==================================================
IMPORT SUMMARY
==================================================
Source file: demo_altafsir_data.json
Database: demo_tafsir.db
Chapters processed: 114
Verses processed: 6236
Verses inserted: 6236
Verses updated: 0
Verses skipped: 0
Errors: 0
Duration: 0:00:00.059483
==================================================
```

## License and Ethics

### Scraping Ethics
- Implements rate limiting to avoid overloading servers
- Respects robots.txt when accessible
- Uses appropriate user agent headers
- Includes retry logic with exponential backoff

### Data Usage
- Scripts are designed for educational and research purposes
- Respect copyright and terms of service of source websites
- Consider reaching out to data providers for bulk access agreements

## Support

For issues, questions, or contributions:
1. Check the error logs first
2. Verify all dependencies are installed
3. Ensure proper file permissions
4. Review the troubleshooting section above