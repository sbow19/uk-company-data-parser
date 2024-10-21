import sqlalchemy as db
from config import config

engine = db.create_engine(f"mysql://{config.MYSQL_USER}:{config.MYSQL_PASSWORD}@localhost/freelance?charset=utf8mb4",
                          pool_size=10,
                          max_overflow=20,
                          pool_timeout=500000,
                          pool_recycle=3600)

def get_connection():
    return engine

