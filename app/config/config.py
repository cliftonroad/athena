import os
import yaml
from dotenv import load_dotenv
import logging
from pathlib import Path

class Config:
    def __init__(self):
        # Set up logging
        logging.basicConfig(level=logging.DEBUG)
        
        # Load environment variables
        # Get the project root directory (assuming config.py is in app/config/)
        project_root = Path(__file__).parent.parent.parent

        # Load .env from project root
        env_path = project_root / '.env'
        load_dotenv(dotenv_path=env_path)        
        #load_dotenv()
        logging.debug("Environment variables loaded")
        
        # Load file patterns
        self._load_file_patterns()
    
    def _load_file_patterns(self):
        patterns_path = os.getenv('FILE_PATTERNS_PATH')
        logging.debug(f"Loading patterns from: {patterns_path}")
        
        try:
            with open(patterns_path, 'r') if patterns_path else None as f:
                self.file_patterns = yaml.safe_load(f)['patterns'] if f else {}
        except Exception as e:
            logging.warning(f"Could not load file patterns: {e}")
            self.file_patterns = {}
    
    @property
    def db_config(self):
        config = {
            'type': os.getenv('DB_TYPE'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        
        # Log configuration (without password)
        debug_config = config.copy()
        debug_config['password'] = '*****' if debug_config['password'] else None
        logging.debug(f"Database configuration: {debug_config}")
        
        # Validate configuration
        missing = [k for k, v in config.items() if not v]
        if missing:
            logging.error(f"Missing database configuration values: {missing}")
            raise ValueError(f"Missing required database configuration: {missing}")
            
        return config

    @property
    def storage_config(self):
        return {
            'type': os.getenv('FILE_STORAGE_TYPE'),
            'input_folder': os.getenv('INPUT_FOLDER'),
            'archive_folder': os.getenv('ARCHIVE_FOLDER'),
            'error_folder': os.getenv('ERROR_FOLDER'),
            'aws_access_key': os.getenv('AWS_ACCESS_KEY_ID'),
            'aws_secret_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'aws_region': os.getenv('AWS_REGION'),
            's3_bucket': os.getenv('S3_BUCKET')
        }
    
    @property
    def log_config(self):
        return {
            'level': os.getenv('LOG_LEVEL', 'INFO'),
            'file_path': os.getenv('LOG_FILE_PATH')
        }