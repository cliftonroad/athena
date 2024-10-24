# app/core/file_handler.py
import os
import shutil
import re
import pandas as pd
from abc import ABC, abstractmethod
import boto3
from datetime import datetime

class FileHandler(ABC):
    @abstractmethod
    def list_files(self, pattern):
        pass
    
    @abstractmethod
    def read_file(self, file_path, file_type, options=None):
        pass
    
    @abstractmethod
    def move_file(self, source, destination):
        pass

class LocalFileHandler(FileHandler):
    def __init__(self, config):
        self.config = config        
    
    def list_files(self, pattern):
        storage_config = self.config.storage_config
        
        files = []
        for file in os.listdir(storage_config['input_folder']):
            if re.match(pattern, file):
                files.append(os.path.join(storage_config['input_folder'], file))
        return files
    
    def read_file(self, file_path, file_type, options=None):
        if file_type == 'csv':
            return pd.read_csv(file_path, **options)
        elif file_type == 'json':
            return pd.read_json(file_path)
        elif file_type == 'xlsx':
            return pd.read_excel(file_path)
        raise ValueError(f"Unsupported file type: {file_type}")
    
    def move_file(self, source, destination):
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        shutil.move(source, destination)

class S3FileHandler(FileHandler):
    def __init__(self, config):
        self.config = config
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=config.aws_config['access_key'],
            aws_secret_access_key=config.aws_config['secret_key'],
            region_name=config.aws_config['region']
        )
        self.bucket = config.aws_config['bucket']
    
    def list_files(self, pattern):
        files = []
        response = self.s3.list_objects_v2(Bucket=self.bucket)
        for obj in response.get('Contents', []):
            if re.match(pattern, obj['Key']):
                files.append(obj['Key'])
        return files
    
    def read_file(self, file_path, file_type, options=None):
        obj = self.s3.get_object(Bucket=self.bucket, Key=file_path)
        if file_type == 'csv':
            return pd.read_csv(obj['Body'], **options)
        elif file_type == 'json':
            return pd.read_json(obj['Body'])
        elif file_type == 'xlsx':
            return pd.read_excel(obj['Body'])
        raise ValueError(f"Unsupported file type: {file_type}")
    
    def move_file(self, source, destination):
        self.s3.copy_object(
            Bucket=self.bucket,
            CopySource={'Bucket': self.bucket, 'Key': source},
            Key=destination
        )
        self.s3.delete_object(Bucket=self.bucket, Key=source)