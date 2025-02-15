"""
data_ingestion.py

This module is responsible for ingesting data from various sources into a SQLite database.
It includes functionality to log the data ingestion process and execute SQL queries to join
different data tables.

Modules:
    - sqlalchemy: For creating and managing the database engine.
    - logging: For logging the data ingestion process.
    - pandas: For data manipulation and analysis.

Functions:
    - create_engine: Creates a database engine.
    - text: Executes SQL text queries.
"""

from sqlalchemy import create_engine, text
import logging
import pandas as pd
# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def create_db_engine(db_path):
    """
    Creates a database engine using SQLAlchemy.

    This function creates a database engine with the provided database path.
    It tests the connection to ensure the engine is created successfully. If the engine
    is created, it logs a success message and returns the engine object. If there is an
    ImportError, it logs an error message indicating that SQLAlchemy is required. For any
    other exceptions, it logs the error message and raises the exception.

    Args:
        db_path (str): The database path in the format required by SQLAlchemy.

    Returns:
        engine: The SQLAlchemy engine object if created successfully.

    Raises:
        ImportError: If SQLAlchemy is not installed.
        Exception: If there is any other error in creating the database engine.
    """
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine # Return the engine object if it all works well
    except ImportError: #If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:# If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e
    
    
def query_data(engine, sql_query):
    """
    Executes a SQL query and returns the result as a pandas DataFrame.

    This function connects to the database using the provided engine and executes the given
    SQL query. It returns the result as a pandas DataFrame. If the query returns an empty
    DataFrame, it logs an error message and raises a ValueError. For any other exceptions,
    it logs the error message and raises the exception.

    Args:
        engine: The SQLAlchemy engine object used to connect to the database.
        sql_query (str): The SQL query to be executed.

    Returns:
        DataFrame: A pandas DataFrame containing the query results.

    Raises:
        ValueError: If the query returns an empty DataFrame.
        Exception: If there is any other error in querying the database.
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e: 
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e
    
    
def read_from_web_CSV(URL):
    """
    Reads a CSV file from a given URL and returns it as a pandas DataFrame.

    This function attempts to read a CSV file from the specified URL using pandas. If the
    CSV file is read successfully, it logs a success message and returns the DataFrame. If
    the URL does not point to a valid CSV file, it logs an error message and raises an
    EmptyDataError. For any other exceptions, it logs the error message and raises the
    exception.

    Args:
        URL (str): The URL pointing to the CSV file.

    Returns:
        DataFrame: A pandas DataFrame containing the CSV data.

    Raises:
        pd.errors.EmptyDataError: If the URL does not point to a valid CSV file.
        Exception: If there is any other error in reading the CSV file from the web.
    """
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e