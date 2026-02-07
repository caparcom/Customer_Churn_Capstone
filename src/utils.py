from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# load the .env file variables
load_dotenv()

def db_connect():

    engine = None
    try:
        engine = create_engine(os.getenv('DATABASE_URL'))
        # Test the connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Connection successful")
    except SQLAlchemyError as e:
        print("Failed to connect to the database")
        print("Error:", e)
    
    return engine
