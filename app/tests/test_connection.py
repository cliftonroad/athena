# test_connection.py
import logging
from app.config.config import Config
from app.core.db_handler import DatabaseHandler

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def test_database_connection():
    try:
        # Initialize config
        logger.debug("Initializing configuration...")
        config = Config()
        
        # Get database configuration
        db_config = config.db_config
        logger.debug(f"Database configuration loaded: {db_config['user']}@{db_config['host']}")
        
        # Initialize database handler
        logger.debug("Initializing database handler...")
        db = DatabaseHandler(config)
        
        # Test connection
        logger.debug("Testing connection...")
        connection_info = db.test_connection()
        logger.info(f"Connection test results: {connection_info}")
        
    except Exception as e:
        logger.error(f"Error during database connection test: {str(e)}", exc_info=True)

if __name__ == "__main__":
    test_database_connection()