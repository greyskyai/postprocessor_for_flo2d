import dask.dataframe as dd
import pandas as pd
import numpy as np
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import psutil
import traceback

def log_time(message, start_time):
    elapsed_time = time.time() - start_time
    print(f"{message} took {elapsed_time:.2f} seconds")

def read_with_dask_optimized(file_path, column_names=None, **kwargs):
    file_size = os.path.getsize(file_path)
    chunk_size = max(min(file_size // 100, 256 * 1024 * 1024), 32 * 1024 * 1024)  # Between 32MB and 256MB
    
    data = dd.read_csv(file_path, delim_whitespace=True, header=None, names=column_names, 
                       blocksize=chunk_size, **kwargs)
    
    total_mem = psutil.virtual_memory().total
    partition_size = max(chunk_size, total_mem // 100)  # Aim for 1% of total memory per partition
    npartitions = max(data.npartitions, file_size // partition_size)
    
    data = data.repartition(npartitions=npartitions)
    return data

def read_file_with_line_number(file_path, column_names, skiprows=0):
    df = pd.read_csv(file_path, delim_whitespace=True, header=None, names=column_names, skiprows=skiprows)
    df.insert(0, 'grid_id', range(len(df)))
    return df

def extract_rain_data(file_path):
    rain_file = os.path.join(file_path, 'RAIN.DAT')
    with open(rain_file, 'r') as file:
        lines = file.readlines()

    multiplier_value = float(lines[1].split()[0])
    last_r_index = max(i for i, line in enumerate(lines) if line.startswith('R'))
    data_lines = lines[last_r_index + 1:]
    
    data = [line.split() for line in data_lines if line.strip()]
    df = pd.DataFrame(data, columns=['grid_id', 'rain_depth'])
    
    df['grid_id'] = pd.to_numeric(df['grid_id'], errors='coerce').astype('Int64')
    df['rain_depth'] = pd.to_numeric(df['rain_depth'], errors='coerce') * multiplier_value
    
    return df

def read_infil_dat(file_path):
    with open(file_path, 'r') as file:
        for _ in range(3):
            next(file)
        
        data = []
        for line in file:
            parts = line.strip().split()
            if parts[0] == 'F':
                data.append(parts[1:])

    columns = ['grid_id', 'xksat', 'psif', 'dtheta', 'abstrinf', 'rtimpf', 'soil_depth']
    df = pd.DataFrame(data, columns=columns)
    return df.apply(pd.to_numeric, errors='coerce')

def read_fpxsec_data_as_df(file_path):
    fpxsec_file = os.path.join(file_path, 'FPXSEC.DAT')
    if not os.path.exists(fpxsec_file):
        return pd.DataFrame()

    rows = []
    with open(fpxsec_file, 'r') as file:
        line_number = 0
        for line in file:
            parts = line.split()
            if parts and parts[0] == 'X':
                line_number += 1
                for grid_id in parts[3:]:
                    if grid_id.isdigit():
                        rows.append({'grid_id': int(grid_id) - 1, 'fpxsec': line_number})
    
    return pd.DataFrame(rows)

def ensure_unique_columns(df):
    cols = df.columns.to_series()
    for dup in cols[cols.duplicated()].unique():
        mask = cols.eq(dup)
        cols[mask] = [dup + '_' + str(i) if i != 0 else dup for i in range(mask.sum())]
    df.columns = cols.tolist()
    return df

def process_file(name, params, file_path):
    file_path_full = os.path.join(file_path, name)
    if not os.path.exists(file_path_full):
        return name, None, f"File {name} not found."

    try:
        if 'custom_extraction_function' in params:
            df = params['custom_extraction_function'](file_path)
        elif name in ['TOPO.DAT', 'INFIL_DEPTH.OUT']:
            column_names = params.pop('column_names', None)
            skiprows = params.pop('skiprows', 0)  # Extract skiprows if available
            df = read_file_with_line_number(file_path_full, column_names, skiprows=skiprows)
        else:
            column_names = params.pop('column_names', None)
            df = read_with_dask_optimized(file_path_full, column_names=column_names, **params)
            df = df.compute()  # Compute only when necessary

        if 'grid_id' in df.columns and name not in ['TOPO.DAT', 'INFIL_DEPTH.OUT']:
            df['grid_id'] = df['grid_id'] - 1  # Adjust to 0-based index

        return name, df, None
    except Exception as e:
        return name, None, str(e)

def verify_grid_ids(data_frames):
    for name, df in data_frames.items():
        if 'grid_id' in df.columns:
            unique_count = df['grid_id'].nunique()
            total_count = len(df)
            print(f"{name}: {unique_count} unique grid_ids out of {total_count} total rows")
            if unique_count != total_count:
                print(f"Warning: {name} has duplicate grid_ids")

def controlled_merge(main_df, data_frames):
    print("Starting controlled merge...")
    result = main_df.copy()
    total_rows = len(result)
    
    for name, df in data_frames.items():
        if name != 'DEPTH.OUT' and not df.empty:
            if 'grid_id' in df.columns:
                print(f"Merging {name}...")
                result = pd.merge(result, df, on='grid_id', how='left', suffixes=('', f'_{name}'))
                if len(result) != total_rows:
                    print(f"Warning: Row count changed after merging {name}. Expected {total_rows}, got {len(result)}")
                    total_rows = len(result)
            elif name == 'FPXSEC.DAT':
                print(f"Merging {name}...")
                result = pd.merge(result, df, left_on='grid_id', right_on='grid_id', how='left')
                result['fpxsec'] = result['fpxsec'].fillna(0)
        print(f"Current dataframe shape after merging {name}: {result.shape}")
    
    print("Merge complete.")
    return result

def extract_super_data(file_path):
    super_file = os.path.join(file_path, 'SUPER.OUT')
    with open(super_file, 'r') as file:
        lines = file.readlines()

    # Skip header lines
    data_lines = lines[7:]

    data = []
    for line in data_lines:
        parts = line.split()
        if len(parts) == 5:
            data.append({
                'grid_id': int(parts[0]) - 1,  # Adjust to 0-based index
                'max_froude_no': float(parts[1]),
                'depth_super': float(parts[2]),  # Changed from 'depth' to 'depth_super'
                'time_super': float(parts[3]),   # Changed from 'time' to 'time_super'
                'num_supercritical_timesteps': int(parts[4])
            })

    return pd.DataFrame(data)

def extractModelDataToDF(file_path):
    print("Started extracting model data")
    start_time = time.time()

    files = {
        'DEPTH.OUT': dict(column_names=['grid_id', 'x', 'y', 'depth_max']),
        'MANNINGS_N.DAT': dict(column_names=['grid_id', 'mannings_n']),
        'TOPO.DAT': dict(column_names=['x', 'y', 'topo']),
        'VELFP.OUT': dict(column_names=['grid_id', 'x', 'y', 'velocity']),
        'MAXQHYD.OUT': dict(column_names=None, skiprows=4),
        'MAXWSELEV.OUT': dict(column_names=['grid_id', 'x', 'y', 'wse_max']),
        'INFIL_DEPTH.OUT': dict(column_names=['x', 'y', 'infil_depth', 'infil_stop'], skiprows=1),
        'TIMEONEFT.OUT': dict(column_names=['grid_id', 'x', 'y', 'time_of_oneft']),
        'TIMETWOFT.OUT': dict(column_names=['grid_id', 'x', 'y', 'time_of_twoft']),
        'TIMETOPEAK.OUT': dict(column_names=['grid_id', 'x', 'y', 'time_to_peak']),
        'FINALVEL.OUT': dict(column_names=['grid_id', 'x', 'y', 'final_velocity']),
        'FINALDEP.OUT': dict(column_names=['grid_id', 'x', 'y', 'final_depth']),
        'RAIN.DAT': dict(custom_extraction_function=extract_rain_data),
        'SUPER.OUT': dict(custom_extraction_function=extract_super_data)  # Add this line
    }

    data_frames = {}

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_file, name, params, file_path): name for name, params in files.items()}
        for future in as_completed(futures):
            name, df, error = future.result()
            if error:
                print(f"Error processing {name}: {error}")
            else:
                if df is not None and not df.empty:
                    data_frames[name] = df
                    print(f"Processed {name}: {len(df)} rows")
                else:
                    print(f"Warning: {name} is empty or None")

    # Process MAXQHYD.OUT separately
    if 'MAXQHYD.OUT' in data_frames:
        maxqhyd_df = data_frames['MAXQHYD.OUT']
        maxqhyd_df = maxqhyd_df[maxqhyd_df.iloc[:, 0] >= 1]
        if not maxqhyd_df.empty:
            maxqhyd_df = maxqhyd_df.iloc[:, [0, 7, 8]].rename(columns={0: 'grid_id', 7: 'q_max', 8: 'flow_direction'})
            maxqhyd_df['grid_id'] = maxqhyd_df['grid_id'] - 1
            data_frames['MAXQHYD.OUT'] = maxqhyd_df
        else:
            print("Warning: MAXQHYD.OUT contains no valid data after filtering")

    # Include INFIL.DAT and FPXSEC.DAT
    infil_file = os.path.join(file_path, 'INFIL.DAT')
    if os.path.exists(infil_file):
        data_frames['INFIL.DAT'] = read_infil_dat(infil_file)
    
    fpxsec_df = read_fpxsec_data_as_df(file_path)
    if len(fpxsec_df.columns) > 0:
        data_frames['FPXSEC.DAT'] = fpxsec_df

    # Verify grid_ids
    verify_grid_ids(data_frames)

    # Controlled merge
    main_df = data_frames['DEPTH.OUT']
    print(f"Main dataframe (DEPTH.OUT) shape: {main_df.shape}")
    
    main_df = controlled_merge(main_df, data_frames)

    # Ensure SUPER.OUT data is included in the merge
    if 'SUPER.OUT' in data_frames:
        print("Merging SUPER.OUT data...")
        main_df = pd.merge(main_df, data_frames['SUPER.OUT'], on='grid_id', how='left')
        print(f"Dataframe shape after merging SUPER.OUT: {main_df.shape}")

    main_df = ensure_unique_columns(main_df)
    
    log_time("Extracting model data", start_time)
    return main_df

if __name__ == "__main__":
    folder_path = r'K:/23008051 - Entrata (Mohave County)/Project Documents/Reports/Drainage/Models/FLO2D/Model_Runs/Subdomain_10/Domain_10_LID=1.5'
    try:
        df = extractModelDataToDF(folder_path)
        print(df.head())
        print(f"Final dataframe shape: {df.shape}")
        print(f"Total memory usage: {df.memory_usage(deep=True).sum() / 1e6:.2f} MB")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        traceback.print_exc()