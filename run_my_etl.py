import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    CSV_FILE = "denormalized_pages.csv" 
    DB_URL = "postgresql://postgres:postgres@localhost:5432/postgres"
    
    from KRA_repos.complete_etl import run_simple_etl
    
    logger.info(f"Starting ETL process...")
    logger.info(f"CSV: {CSV_FILE}")
    logger.info(f"Database: {DB_URL}")
    
    stats = run_simple_etl(CSV_FILE, DB_URL)
    
    logger.info("\nETL completed successfully!")
    logger.info(f"Created {stats['pages_created']} new pages")
    logger.info(f"Skipped {stats['pages_skipped']} pages")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())