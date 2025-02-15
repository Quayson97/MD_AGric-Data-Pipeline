import pandas as pd
from data_ingestion import create_db_engine, query_data, read_from_web_CSV
import logging

class FieldDataProcessor:

    def __init__(self, config_params, logging_level="INFO"): # When we instantiate this class, we can optionally specify what logs we want to see

        # Initialising class with attributes we need. Refer to the code above to understand how each attribute relates to the code
        self.db_path = config_params['db_path']
        self.sql_query = config_params["sql_query"]
        self.columns_to_rename = config_params["columns_to_rename"]
        self.values_to_rename = config_params["values_to_rename"]
        self.weather_map_data = config_params["weather_mapping_csv"]
        
        self.initialize_logging(logging_level)

        # We create empty objects to store the DataFrame and engine in
        self.df = None
        self.engine = None
        
    # This method enables logging in the class.
    def initialize_logging(self, logging_level):
        """
        Sets up logging for this instance of FieldDataProcessor.
        """
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False  # Prevents log messages from being propagated to the root logger

        # Set logging level
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":  # Option to disable logging
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO  # Default to INFO

        self.logger.setLevel(log_level)

        # Only add handler if not already added to avoid duplicate messages
        if not self.logger.handlers:
            ch = logging.StreamHandler()  # Create console handler
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

        # Use self.logger.info(), self.logger.debug(), etc. 

        
    def ingest_sql_data(self):
        """
        Ingests data from an SQL database and stores it in a DataFrame.

        This method creates a database engine using the provided database path, executes an SQL
        query to retrieve data, and stores the result in a DataFrame attribute. It logs a success
        message upon successful data loading.

        Returns:
            DataFrame: The DataFrame containing the ingested data.
        """
        self.engine = create_db_engine(self.db_path)
        self.df = query_data(self.engine, self.sql_query)
        self.logger.info("Sucessfully loaded data.")
        return self.df  
     
    
    def rename_columns(self):
        """
        Renames two specified columns in the DataFrame by swapping their names.

        This method extracts the column names to be swapped from the configuration, generates
        a temporary name to avoid naming conflicts, and performs the renaming operation. It
        logs the swap operation for tracking purposes.

        Raises:
            KeyError: If the specified columns are not found in the DataFrame.
        """
        # Extract the columns to rename from the configuration
        column1, column2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0]

        # Temporarily rename one of the columns to avoid a naming conflict
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"

        # Perform the swap
        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})
        
        # Log the swap
        self.logger.info(f"Swapped columns: {column1} with {column2}")

    
    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """
        Applies corrections to specified columns in the DataFrame.

        This method performs two main corrections:
        1. Converts all values in the specified column to their absolute values.
        2. Renames values in the specified column based on a predefined mapping.

        Args:
            column_name (str): The name of the column to apply value renaming. Default is 'Crop_type'.
            abs_column (str): The name of the column to convert values to absolute values. Default is 'Elevation'.

        Returns:
            None
        """
        self.df[abs_column] = self.df[abs_column].abs()
        self.df[column_name] = self.df[column_name].apply(lambda crop: self.values_to_rename.get(crop, crop))

    def weather_station_mapping(self):
        """
        Merges the weather station data with the main DataFrame and returns the weather data.
        """
        if self.df is not None:
            # Read the weather station mapping data from the URL into a DataFrame
            weather_map_df= read_from_web_CSV(self.weather_map_data)
            
            # Perform the merge
            self.df = pd.merge(self.df, weather_map_df, on='Field_ID', how='left')
            
            # Return the weather data
            return weather_map_df
       

    def process(self):
        """
        Executes the complete data processing pipeline.
        """
        self.ingest_sql_data()
        self.rename_columns()
        self.apply_corrections()
        self.weather_station_mapping()
        self.df = self.df.drop(columns="Unnamed: 0") # Drop "Unnamed: 0" from the merged self.df