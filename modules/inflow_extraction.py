import re
import pandas as pd
import os

def extract_inflow_hydrographs(folder_path):
    file_path = os.path.join(folder_path, 'INFLOW.DAT')

    # Initialize variables
    hydrograph_data = {}
    current_grid_element = None
    all_time_steps = set()  # To collect all unique time steps

    # Regex patterns to identify grid elements and hydrograph data
    grid_element_pattern = re.compile(r'F\s+\d+\s+(\d+)')
    hydrograph_pattern = re.compile(r'H\s+([\d\.]+)\s+([\d\.]+)')

    # Open the file and process it line by line
    with open(file_path, 'r') as file:
        for line in file:
            # Check for grid element ID lines
            grid_match = grid_element_pattern.match(line)
            if grid_match:
                current_grid_element = int(grid_match.group(1))
                # Initialize an empty dictionary for this grid element if it's new
                if current_grid_element not in hydrograph_data:
                    hydrograph_data[current_grid_element] = {}

            # Check for hydrograph data lines
            hydrograph_match = hydrograph_pattern.match(line)
            if hydrograph_match and current_grid_element is not None:
                time_step = float(hydrograph_match.group(1))
                flow_value = float(hydrograph_match.group(2))

                # Add the time step to the set of all time steps
                all_time_steps.add(time_step)

                # Add the flow value for the current time step to the current grid element's data
                hydrograph_data[current_grid_element][time_step] = flow_value

    # Convert the time steps set to a sorted list for consistent indexing
    sorted_time_steps = sorted(all_time_steps)

    # Create a dictionary to hold the series data for each grid element
    series_dict = {}

    # Use pd.concat for more efficient data addition
    for grid_element, hydrograph in hydrograph_data.items():
        series_data = pd.Series(hydrograph, index=sorted_time_steps)
        series_dict[grid_element] = series_data

    # Create a DataFrame from the series dictionary
    df_hydrographs = pd.concat(series_dict, axis=1)

    # Fill missing data with 0 as default
    df_hydrographs = df_hydrographs.fillna(0)
    df_hydrographs.index.name = 'Time (hours)'

    return df_hydrographs