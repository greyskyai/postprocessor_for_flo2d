import pandas as pd

def extract_time_out_data(file_path):
    """
    Extracts data from the TIME.OUT file.

    Args:
        file_path (str): Path to the TIME.OUT file.

    Returns:
        pandas.DataFrame: DataFrame containing the extracted data.
    """
    grid_ids = []
    num_time_decrements = []

    with open(file_path, 'r') as file:
        lines = file.readlines()
        
    extract_data = False
    for line in lines:
        if line.strip().startswith("FLOODPLAIN NODES    NUMBER OF TIMES EXCEEDED"):
            extract_data = True
            continue

        if line.strip().startswith("THE LAST"):
            extract_data = False
            continue
        
        if extract_data and line.strip():
            parts = line.split()
            if len(parts) == 2:
                grid_ids.append(int(parts[0]))
                num_time_decrements.append(int(parts[1]))

    return pd.DataFrame({'grid_id': grid_ids, 'num_time_decrements': num_time_decrements})
