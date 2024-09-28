import os
import pandas as pd
from modules.utilities import time_function

@time_function
def extract_swmm_rating_tables(file_path):
    """
    Extracts rating tables from a SWMMFLORT.DAT file.

    Args:
    file_path (str): Path to the directory containing SWMMFLORT.DAT file.

    Returns:
    list: A list of dictionaries, each containing table name and data.
    """
    file_path = os.path.join(file_path, "SWMMFLORT.DAT")
    rating_tables = []
    current_table = None

    try:
        with open(file_path, 'r') as file:
            for line in file:
                parts = line.strip().split()
                
                if line.startswith('D') and len(parts) >= 3:
                    if current_table:
                        rating_tables.append(current_table)
                    current_table = {"Table": parts[2], "Data": []}
                
                elif line.startswith('N') and len(parts) == 3 and current_table:
                    try:
                        stage = float(parts[1])
                        discharge = float(parts[2])
                        current_table["Data"].append({"Stage": stage, "Flow": discharge})
                    except ValueError:
                        print(f"Warning: Could not convert values to float: {parts}")

        if current_table:
            rating_tables.append(current_table)

        # Convert data to pandas DataFrame
        for table in rating_tables:
            table["Data"] = pd.DataFrame(table["Data"])

    except Exception as e:
        print(f"An error occurred while processing the file: {str(e)}")
        return []

    return rating_tables

# If you want to test the function when the script is run directly
if __name__ == "__main__":
# Example usage
    folder_path = r"R:\_anichols\Projects\_flo2d_postprocessor_tests\Detroit_Basin_Prop100y24h"
    rating_data = extract_swmm_rating_tables(folder_path)

    # Print the extracted data
    for table in rating_data:
        print(f"Table: {table['Table']}")
        print(table['Data'])
        print()