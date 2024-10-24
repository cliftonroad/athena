import logging
import os
from datetime import datetime

class Logger:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger('athena')
        
        # Set log level from config
        log_level = getattr(logging, self.config.log_config['level'].upper())
        self.logger.setLevel(log_level)
        
        # Create log directory if it doesn't exist
        log_path = self.config.log_config['file_path']
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        
        # File handler
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(log_level)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)