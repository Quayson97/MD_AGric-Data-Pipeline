import os
import sys
import pandas as pd
import pytest
import importlib.util

# Helper function to load modules
def load_module_from_path(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# Load the modules from file paths
data_ingestion = load_module_from_path('data_ingestion', './data_ingestion.py')
field_data_processor = load_module_from_path('field_data_proccessor', './field_data_proccessor.py')
weather_data_processor = load_module_from_path('weather_data_processor', './weather_data_processor.py')

# Define paths for test CSV files
weather_csv_path = 'C:/Users/aser/Documents/ALX_DS/Python/CC/sampled_weather_df.csv'
field_csv_path = 'C:/Users/aser/Documents/ALX_DS/Python/CC/sampled_field_df.csv'

def validate_data_pipeline():
    """
    Validates each stage of the data pipeline and writes outputs to CSV files.
    """
    try:
        # Run data ingestion
        data = data_ingestion.ingest_data()
        
        # Process field data and save to CSV
        field_df = field_data_processor.process_field_data(data)
        field_df.to_csv(field_csv_path, index=False)
        print(f"[PASS] Field data processed and saved to {field_csv_path}")

        # Process weather data and save to CSV
        weather_df = weather_data_processor.process_weather_data(data)
        weather_df.to_csv(weather_csv_path, index=False)
        print(f"[PASS] Weather data processed and saved to {weather_csv_path}")

    except Exception as e:
        print(f"[FAIL] Pipeline validation failed: {e}")

def cleanup_files():
    """
    Deletes the sample CSV files if they exist.
    """
    for file_path in [weather_csv_path, field_csv_path]:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Deleted {file_path}")
        else:
            print(f"{file_path} does not exist.")

# Define pytest fixture to set up and tear down
@pytest.fixture(scope="module")
def setup_and_teardown():
    validate_data_pipeline()
    yield
    cleanup_files()

# Test cases
def test_read_weather_DataFrame_shape(setup_and_teardown):
    """Check if the weather DataFrame has at least one row and expected columns."""
    weather_df = pd.read_csv(weather_csv_path)
    assert weather_df.shape[0] > 0, "Weather DataFrame has no rows"

def test_read_field_DataFrame_shape(setup_and_teardown):
    """Check if the field DataFrame has at least one row and expected columns."""
    field_df = pd.read_csv(field_csv_path)
    assert field_df.shape[0] > 0, "Field DataFrame has no rows"

def test_weather_DataFrame_columns(setup_and_teardown):
    """Validate the presence of required columns in the weather DataFrame."""
    weather_df = pd.read_csv(weather_csv_path)
    expected_columns = ['Weather_station_ID', 'Message', 'Measurement', 'Value']
    for column in expected_columns:
        assert column in weather_df.columns, f"Weather DataFrame missing column: {column}"

def test_field_DataFrame_columns(setup_and_teardown):
    """Validate the presence of required columns in the field DataFrame."""
    field_df = pd.read_csv(field_csv_path)
    expected_columns = ['Field_ID', 'Elevation', 'Latitude', 'Longitude', 'Location', 'Slope',
       'Rainfall', 'Min_temperature_C', 'Max_temperature_C', 'Ave_temps',
       'Soil_fertility', 'Soil_type', 'pH', 'Pollution_level', 'Plot_size',
       'Annual_yield', 'Crop_type', 'Standard_yield', 'Weather_station']
    for column in expected_columns:
        assert column in field_df.columns, f"Field DataFrame missing column: {column}"

def test_field_DataFrame_non_negative_elevation(setup_and_teardown):
    """Ensure elevation values are non-negative in the field DataFrame."""
    field_df = pd.read_csv(field_csv_path)
    assert (field_df["Elevation"] >= 0).all(), "Elevation values contain negatives"

def test_crop_types_are_valid(setup_and_teardown):
    """Check if crop types in the field DataFrame are valid."""
    field_df = pd.read_csv(field_csv_path)
    valid_crop_types = {'cassava', 'tea', 'wheat', 'potato', 'banana', 'coffee', 'rice',
       'maize', 'wheat ', 'tea ', 'cassava '} 
    assert set(field_df["Crop_type"]).issubset(valid_crop_types), "Field DataFrame has invalid crop types"

def test_positive_rainfall_values(setup_and_teardown):
    """Ensure rainfall values are positive in the weather DataFrame."""
    field_df = pd.read_csv(field_csv_path)
    assert (field_df["Rainfall"] > 0).all(), "Rainfall contains non-positive values"

# Run validation and cleanup manually if running as a standalone script
if __name__ == "__main__":
    validate_data_pipeline()
    cleanup_files()

