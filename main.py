# main.py
from app.config.config import Config
from app.core.file_handler import LocalFileHandler, S3FileHandler
from app.core.db_handler import DatabaseHandler
from app.core.data_validator import DataValidator
from app.core.processor import FileProcessor
from app.models.control import Base
from app.utils.logger import Logger

def main():
    # Initialize configuration
    config = Config()
    
    # Initialize services
    logger = Logger(config)
    db_handler = DatabaseHandler(config)
    
    # Create database tables
    Base.metadata.create_all(db_handler.engine)
    
    # Initialize file handler
    file_handler = (
        S3FileHandler(config) if config.storage_config['type'] == 's3'
        else LocalFileHandler(config)
    )
    
    # Initialize validator
    data_validator = DataValidator(db_handler, logger)
    
    # Initialize processor
    processor = FileProcessor(
        config,
        db_handler,
        file_handler,
        data_validator,
        logger
    )
    
    # Process each file pattern
    for pattern_name, pattern_config in config.file_patterns.items():
        logger.logger.info(f"Processing pattern: {pattern_name}")
        
        # List matching files
        files = file_handler.list_files(pattern_config['pattern'])
        
        # Process each file
        for file_path in files:
            processor.process_file(file_path, pattern_config)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger = Logger(Config())
        logger.logger.error(f"Application error: {str(e)}")
        raise