import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    TRINO_HOST = os.getenv('TRINO_HOST', 'localhost')
    TRINO_PORT = int(os.getenv('TRINO_PORT', 8080))
    TRINO_USER = os.getenv('TRINO_USER', 'tracer_api_user')
    TRINO_CATALOG = os.getenv('TRINO_CATALOG', 'hive')
    TRINO_SCHEMA = os.getenv('TRINO_SCHEMA', 'default')
    TRACES_TABLE = os.getenv('TRACES_TABLE', 'trazas')