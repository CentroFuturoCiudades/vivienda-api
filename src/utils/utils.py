import os
from functools import lru_cache

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

APP_ENV = os.getenv('APP_ENV', 'local')
load_dotenv(f'.env.{APP_ENV}')


@lru_cache()
def get_engine():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")

    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(connection_string)

def get_all(query):
    engine = get_engine()
    df = pd.read_sql(query, engine)
    return df
