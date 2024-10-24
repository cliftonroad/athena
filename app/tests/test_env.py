# test_env.py
from dotenv import load_dotenv
import os
import pathlib

print("Current working directory:", pathlib.Path.cwd())
print("\nChecking if .env file exists:")
env_path = pathlib.Path.cwd() / '.env'
print(f"Looking for .env at: {env_path}")
print(f"File exists: {env_path.exists()}")

print("\nTrying to load .env file:")
load_dotenv(verbose=True)  # verbose=True will print debug info

print("\nDatabase Environment Variables:")
db_vars = {
    'DB_TYPE': os.getenv('DB_TYPE'),
    'DB_HOST': os.getenv('DB_HOST'),
    'DB_PORT': os.getenv('DB_PORT'),
    'DB_NAME': os.getenv('DB_NAME'),
    'DB_USER': os.getenv('DB_USER'),
    'DB_PASSWORD': '***' if os.getenv('DB_PASSWORD') else None
}

for key, value in db_vars.items():
    print(f"{key}: {value}")

print("\nContent of .env file (if exists):")
try:
    with open('.env', 'r') as f:
        print(f.read())
except FileNotFoundError:
    print(".env file not found")
except Exception as e:
    print(f"Error reading .env file: {e}")