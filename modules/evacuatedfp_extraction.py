import pandas as pd

def extract_evacuatedfp_data(file_path):
    """
    Extracts data from the EVACUATEDFP.OUT file.

    Args:
        file_path (str): Path to the EVACUATEDFP.OUT file.

    Returns:
        pandas.DataFrame: DataFrame containing the extracted data.
    """
    grid_ids = []
    num_evacuations = []

    with open(file_path, 'r') as file:
        lines = file.readlines()
        
    extract_data = False
    for line in lines:
        if line.strip().startswith("ELEMENT    NUMBER OF EVACUATIONS"):
            extract_data = True
            continue
        
        if extract_data and line.strip():
            parts = line.split()
            if len(parts) == 2:
                grid_ids.append(int(parts[0]))
                num_evacuations.append(int(parts[1]))

    return pd.DataFrame({'grid_id': grid_ids, 'num_evacuations': num_evacuations})
