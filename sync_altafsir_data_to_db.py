#!/usr/bin/env python3
"""
Altafsir Data Database Synchronization Script

This script imports scraped altafsir.com JSON data into the database.
It performs validation, verification, and progress reporting during the import process.

Features:
- Load JSON data from scraper output
- Database insertion with transaction support
- Data validation and verification
- Progress reporting and error handling
- Duplicate detection and handling
- Rollback support for failed imports
"""

import json
import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('altafsir_sync.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class AltafsirDataSyncer:
    """Synchronizes altafsir.com scraped data with database."""
    
    def __init__(self, json_file: str, db_file: str = "tafsir_database.db"):
        """
        Initialize the syncer.
        
        Args:
            json_file: Path to scraped JSON data file
            db_file: Path to SQLite database file
        """
        self.json_file = json_file
        self.db_file = db_file
        self.data = None
        self.connection = None
        
        # Import statistics
        self.stats = {
            "chapters_processed": 0,
            "verses_processed": 0,
            "verses_inserted": 0,
            "verses_updated": 0,
            "verses_skipped": 0,
            "errors": 0,
            "start_time": None,
            "end_time": None
        }
    
    def load_json_data(self) -> bool:
        """
        Load and validate JSON data from the scraped file.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Loading data from {self.json_file}")
            
            with open(self.json_file, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
            
            # Validate required structure
            required_keys = ['metadata', 'chapters', 'verses']
            for key in required_keys:
                if key not in self.data:
                    logger.error(f"Missing required key: {key}")
                    return False
            
            logger.info(f"Data loaded successfully")
            logger.info(f"Source: {self.data['metadata'].get('source', 'unknown')}")
            logger.info(f"Chapters: {len(self.data['chapters'])}")
            logger.info(f"Verses: {len(self.data['verses'])}")
            logger.info(f"Language: {self.data['metadata'].get('language_name', 'unknown')}")
            
            return True
            
        except FileNotFoundError:
            logger.error(f"JSON file not found: {self.json_file}")
            return False
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format: {e}")
            return False
        except Exception as e:
            logger.error(f"Error loading JSON data: {e}")
            return False
    
    def connect_database(self) -> bool:
        """
        Connect to the database and create tables if needed.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Connecting to database: {self.db_file}")
            self.connection = sqlite3.connect(self.db_file)
            self.connection.row_factory = sqlite3.Row  # Enable dict-like access
            
            # Create tables if they don't exist
            self._create_tables()
            
            logger.info("Database connection established")
            return True
            
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            return False
    
    def _create_tables(self) -> None:
        """Create database tables if they don't exist."""
        cursor = self.connection.cursor()
        
        # Tafsir resources table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tafsir_resources (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                author_name TEXT,
                language_name TEXT,
                source TEXT,
                slug TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Chapters table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chapters (
                id INTEGER PRIMARY KEY,
                name_simple TEXT NOT NULL,
                name_complex TEXT,
                name_arabic TEXT,
                verses_count INTEGER NOT NULL,
                revelation_place TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Verses table 
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS verses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                verse_key TEXT NOT NULL,
                chapter_id INTEGER NOT NULL,
                verse_number INTEGER NOT NULL,
                text TEXT NOT NULL,
                resource_id INTEGER NOT NULL,
                resource_name TEXT,
                language_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (chapter_id) REFERENCES chapters (id),
                FOREIGN KEY (resource_id) REFERENCES tafsir_resources (id),
                UNIQUE (verse_key, resource_id)
            )
        ''')
        
        # Import logs table for tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS import_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_file TEXT NOT NULL,
                resource_id INTEGER,
                total_verses INTEGER,
                verses_inserted INTEGER,
                verses_updated INTEGER,
                verses_skipped INTEGER,
                status TEXT,
                notes TEXT
            )
        ''')
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_verses_verse_key ON verses (verse_key)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_verses_chapter_id ON verses (chapter_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_verses_resource_id ON verses (resource_id)')
        
        self.connection.commit()
        logger.info("Database tables created/verified")
    
    def _insert_or_update_resource(self) -> int:
        """
        Insert or update tafsir resource information.
        
        Returns:
            Resource ID
        """
        cursor = self.connection.cursor()
        metadata = self.data['metadata']
        
        # Check if resource exists
        cursor.execute(
            "SELECT id FROM tafsir_resources WHERE id = ?",
            (metadata.get('tafsir_id'),)
        )
        existing = cursor.fetchone()
        
        if existing:
            # Update existing resource
            cursor.execute('''
                UPDATE tafsir_resources 
                SET name = ?, author_name = ?, language_name = ?, 
                    source = ?, slug = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (
                metadata.get('tafsir_name'),
                metadata.get('author_name'),
                metadata.get('language_name'),
                metadata.get('source'),
                metadata.get('slug'),
                metadata.get('tafsir_id')
            ))
            resource_id = metadata.get('tafsir_id')
            logger.info(f"Updated existing resource: {resource_id}")
        else:
            # Insert new resource
            cursor.execute('''
                INSERT INTO tafsir_resources (id, name, author_name, language_name, source, slug)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                metadata.get('tafsir_id'),
                metadata.get('tafsir_name'),
                metadata.get('author_name'),
                metadata.get('language_name'),
                metadata.get('source'),
                metadata.get('slug')
            ))
            resource_id = metadata.get('tafsir_id')
            logger.info(f"Inserted new resource: {resource_id}")
        
        self.connection.commit()
        return resource_id
    
    def _insert_or_update_chapters(self) -> bool:
        """
        Insert or update chapter information.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            cursor = self.connection.cursor()
            
            for chapter_id, chapter_data in self.data['chapters'].items():
                # Check if chapter exists
                cursor.execute("SELECT id FROM chapters WHERE id = ?", (chapter_data['id'],))
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing chapter
                    cursor.execute('''
                        UPDATE chapters 
                        SET name_simple = ?, name_complex = ?, name_arabic = ?,
                            verses_count = ?, revelation_place = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (
                        chapter_data['name_simple'],
                        chapter_data['name_complex'],
                        chapter_data['name_arabic'],
                        chapter_data['verses_count'],
                        chapter_data['revelation_place'],
                        chapter_data['id']
                    ))
                else:
                    # Insert new chapter
                    cursor.execute('''
                        INSERT INTO chapters (id, name_simple, name_complex, name_arabic, 
                                            verses_count, revelation_place)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        chapter_data['id'],
                        chapter_data['name_simple'],
                        chapter_data['name_complex'],
                        chapter_data['name_arabic'],
                        chapter_data['verses_count'],
                        chapter_data['revelation_place']
                    ))
                
                self.stats['chapters_processed'] += 1
            
            self.connection.commit()
            logger.info(f"Processed {self.stats['chapters_processed']} chapters")
            return True
            
        except Exception as e:
            logger.error(f"Error processing chapters: {e}")
            self.connection.rollback()
            return False
    
    def _insert_or_update_verses(self, resource_id: int) -> bool:
        """
        Insert or update verse data.
        
        Args:
            resource_id: ID of the tafsir resource
            
        Returns:
            True if successful, False otherwise
        """
        try:
            cursor = self.connection.cursor()
            batch_size = 1000
            processed = 0
            
            # Process verses in batches
            verse_items = list(self.data['verses'].items())
            total_verses = len(verse_items)
            
            logger.info(f"Processing {total_verses} verses in batches of {batch_size}")
            
            for i in range(0, total_verses, batch_size):
                batch = verse_items[i:i + batch_size]
                
                # Begin transaction for batch
                cursor.execute("BEGIN")
                
                try:
                    for verse_key, verse_data in batch:
                        self._process_single_verse(cursor, verse_data, resource_id)
                        processed += 1
                        
                        if processed % 500 == 0:
                            logger.info(f"Progress: {processed}/{total_verses} verses")
                    
                    # Commit batch
                    cursor.execute("COMMIT")
                    
                except Exception as e:
                    logger.error(f"Error in batch starting at {i}: {e}")
                    cursor.execute("ROLLBACK")
                    self.stats['errors'] += 1
                    continue
            
            logger.info(f"Verses processed: {self.stats['verses_processed']}")
            logger.info(f"Verses inserted: {self.stats['verses_inserted']}")
            logger.info(f"Verses updated: {self.stats['verses_updated']}")
            logger.info(f"Verses skipped: {self.stats['verses_skipped']}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing verses: {e}")
            return False
    
    def _process_single_verse(self, cursor, verse_data: Dict, resource_id: int) -> None:
        """
        Process a single verse for insertion or update.
        
        Args:
            cursor: Database cursor
            verse_data: Verse data dictionary
            resource_id: Resource ID
        """
        try:
            verse_key = verse_data['verse_key']
            
            # Check if verse exists for this resource
            cursor.execute(
                "SELECT id FROM verses WHERE verse_key = ? AND resource_id = ?",
                (verse_key, resource_id)
            )
            existing = cursor.fetchone()
            
            if existing:
                # Update existing verse
                cursor.execute('''
                    UPDATE verses 
                    SET text = ?, resource_name = ?, language_name = ?, 
                        updated_at = CURRENT_TIMESTAMP
                    WHERE verse_key = ? AND resource_id = ?
                ''', (
                    verse_data['text'],
                    verse_data.get('resource_name'),
                    verse_data.get('language_name'),
                    verse_key,
                    resource_id
                ))
                self.stats['verses_updated'] += 1
                
            else:
                # Insert new verse
                cursor.execute('''
                    INSERT INTO verses (verse_key, chapter_id, verse_number, text, 
                                      resource_id, resource_name, language_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    verse_key,
                    verse_data['chapter_id'],
                    verse_data['verse_number'],
                    verse_data['text'],
                    resource_id,
                    verse_data.get('resource_name'),
                    verse_data.get('language_name')
                ))
                self.stats['verses_inserted'] += 1
            
            self.stats['verses_processed'] += 1
            
        except Exception as e:
            logger.error(f"Error processing verse {verse_data.get('verse_key', 'unknown')}: {e}")
            self.stats['verses_skipped'] += 1
            self.stats['errors'] += 1
    
    def _log_import_summary(self, resource_id: int, status: str) -> None:
        """
        Log import summary to database.
        
        Args:
            resource_id: Resource ID that was imported
            status: Import status (success/failed)
        """
        try:
            cursor = self.connection.cursor()
            
            cursor.execute('''
                INSERT INTO import_logs 
                (source_file, resource_id, total_verses, verses_inserted, 
                 verses_updated, verses_skipped, status, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                self.json_file,
                resource_id,
                self.stats['verses_processed'],
                self.stats['verses_inserted'],
                self.stats['verses_updated'],
                self.stats['verses_skipped'],
                status,
                f"Errors: {self.stats['errors']}"
            ))
            
            self.connection.commit()
            logger.info("Import summary logged to database")
            
        except Exception as e:
            logger.error(f"Error logging import summary: {e}")
    
    def verify_import(self, resource_id: int) -> bool:
        """
        Verify the imported data integrity.
        
        Args:
            resource_id: Resource ID to verify
            
        Returns:
            True if verification passes, False otherwise
        """
        try:
            cursor = self.connection.cursor()
            
            # Count total verses in database for this resource
            cursor.execute(
                "SELECT COUNT(*) as count FROM verses WHERE resource_id = ?",
                (resource_id,)
            )
            db_count = cursor.fetchone()['count']
            
            # Count verses in JSON data
            json_count = len(self.data['verses'])
            
            logger.info(f"Verification: JSON verses = {json_count}, DB verses = {db_count}")
            
            if db_count == json_count:
                logger.info("✓ Verification passed: All verses imported successfully")
                return True
            else:
                logger.warning(f"⚠ Verification warning: Count mismatch")
                
                # Check for missing verses
                cursor.execute(
                    "SELECT verse_key FROM verses WHERE resource_id = ?",
                    (resource_id,)
                )
                db_verses = {row['verse_key'] for row in cursor.fetchall()}
                json_verses = set(self.data['verses'].keys())
                
                missing_in_db = json_verses - db_verses
                extra_in_db = db_verses - json_verses
                
                if missing_in_db:
                    logger.warning(f"Missing in DB: {len(missing_in_db)} verses")
                    for verse_key in sorted(missing_in_db)[:10]:  # Show first 10
                        logger.warning(f"  Missing: {verse_key}")
                
                if extra_in_db:
                    logger.warning(f"Extra in DB: {len(extra_in_db)} verses")
                
                return False
                
        except Exception as e:
            logger.error(f"Error during verification: {e}")
            return False
    
    def sync_data(self) -> bool:
        """
        Main synchronization process.
        
        Returns:
            True if successful, False otherwise
        """
        logger.info("Starting data synchronization")
        self.stats['start_time'] = datetime.now()
        
        try:
            # Insert/update resource
            resource_id = self._insert_or_update_resource()
            
            # Insert/update chapters
            if not self._insert_or_update_chapters():
                return False
            
            # Insert/update verses
            if not self._insert_or_update_verses(resource_id):
                return False
            
            # Verify import
            verification_passed = self.verify_import(resource_id)
            
            # Log summary
            status = "success" if verification_passed else "completed_with_warnings"
            self._log_import_summary(resource_id, status)
            
            self.stats['end_time'] = datetime.now()
            duration = self.stats['end_time'] - self.stats['start_time']
            
            logger.info("Data synchronization completed")
            logger.info(f"Duration: {duration}")
            logger.info(f"Final status: {status}")
            
            return True
            
        except Exception as e:
            logger.error(f"Synchronization failed: {e}")
            if self.connection:
                self.connection.rollback()
            
            self._log_import_summary(0, "failed")
            return False
    
    def close_connection(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def print_summary(self) -> None:
        """Print import summary."""
        print("\n" + "="*50)
        print("IMPORT SUMMARY")
        print("="*50)
        print(f"Source file: {self.json_file}")
        print(f"Database: {self.db_file}")
        print(f"Chapters processed: {self.stats['chapters_processed']}")
        print(f"Verses processed: {self.stats['verses_processed']}")
        print(f"Verses inserted: {self.stats['verses_inserted']}")
        print(f"Verses updated: {self.stats['verses_updated']}")
        print(f"Verses skipped: {self.stats['verses_skipped']}")
        print(f"Errors: {self.stats['errors']}")
        
        if self.stats['start_time'] and self.stats['end_time']:
            duration = self.stats['end_time'] - self.stats['start_time']
            print(f"Duration: {duration}")
        
        print("="*50)

def main():
    """Main function to run the synchronization."""
    parser = argparse.ArgumentParser(
        description="Sync altafsir.com scraped data to database"
    )
    parser.add_argument("json_file", help="Path to scraped JSON data file")
    parser.add_argument("--database", "-d", default="tafsir_database.db",
                        help="Database file path")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate input file
    if not Path(args.json_file).exists():
        logger.error(f"Input file not found: {args.json_file}")
        return 1
    
    # Create syncer
    syncer = AltafsirDataSyncer(args.json_file, args.database)
    
    try:
        # Load JSON data
        if not syncer.load_json_data():
            logger.error("Failed to load JSON data")
            return 1
        
        # Connect to database
        if not syncer.connect_database():
            logger.error("Failed to connect to database")
            return 1
        
        # Sync data
        success = syncer.sync_data()
        
        # Print summary
        syncer.print_summary()
        
        if success:
            logger.info("Synchronization completed successfully!")
            return 0
        else:
            logger.error("Synchronization completed with errors")
            return 1
            
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1
    finally:
        syncer.close_connection()

if __name__ == "__main__":
    sys.exit(main())