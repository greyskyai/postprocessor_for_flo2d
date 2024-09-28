import sys
import pandas as pd
import numpy as np
from modules.utilities import time_function

@time_function
def extract_area_reduction_factors(file_path):
    '''
    Extracts grid IDs and Area Reduction Factors from a file.

    Parameters:
        file_path (str): The path to the file to be processed.

    Returns:
        pd.DataFrame: A DataFrame containing 'grid_id' and 'arf' columns.
    '''
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.split()
            if parts:
                if parts[0] == 'T':
                    grid_id = int(parts[1]) - 1  # Adjust to 0-based index
                    arf = 1.0
                else:
                    try:
                        grid_id = int(parts[0]) - 1  # Adjust to 0-based index
                        arf = float(parts[1])
                    except ValueError:
                        continue
                data.append((grid_id, arf))

    df = pd.DataFrame(data, columns=['grid_id', 'arf'])
    return df

@time_function
def merge_arf_with_model_data(model_data, arf_df):
    '''
    Merges ARF data with the model data.

    Parameters:
        model_data (pd.DataFrame): The main model data.
        arf_df (pd.DataFrame): The ARF data.

    Returns:
        pd.DataFrame: The merged DataFrame.
    '''
    merged_df = pd.merge(model_data, arf_df, on='grid_id', how='left')
    merged_df['arf'] = merged_df['arf'].fillna(1.0)
    return merged_df

if __name__ == "__main__":
    
    if len(sys.argv) < 2:
        print("Usage: python arf_extraction.py <path_to_arf_file>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    arf_df = extract_area_reduction_factors(file_path)