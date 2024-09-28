import pandas as pd
import numpy as np
import os
from modules.utilities import time_function

@time_function
def extract_xsec_data(file_path):
    data = []
    with open(file_path, 'r') as file:
        cross_section = None
        for line in file:
            if line.startswith('X'):
                cross_section = int(line.split()[1])
            elif line.strip():
                station, elevation = map(float, line.split())
                data.append((cross_section, station, elevation))
    return pd.DataFrame(data, columns=['Cross Section Number', 'Station', 'Elevation'])

@time_function
def extract_chanmax_data(file_path):
    data = []
    with open(file_path, 'r', encoding='ISO-8859-1') as file:
        for line in file:
            if line.strip() and not line.startswith('CHANNEL SEGMENT NO'):
                try:
                    node, max_discharge, time_max_discharge, max_stage, time_max_stage = line.split()
                    data.append((int(node), float(max_discharge), float(time_max_discharge),
                                 float(max_stage), float(time_max_stage)))
                except ValueError:
                    continue
    return pd.DataFrame(data, columns=['NODE', 'Max Discharge (CFS)', 'Time of Max Discharge (Hrs)',
                                       'Max Stage', 'Time of Max Stage (Hrs)'])

@time_function
def extract_chan_data(file_path):
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            if line.strip() and line[0].isalpha():
                parts = line.split()
                data.append((parts[0], int(parts[1]), float(parts[2]), float(parts[3]), int(parts[4])))
    return pd.DataFrame(data, columns=['Cross Section Type', 'FLO-2D Grid ID', 'N-Value',
                                       'Length to Next Cross Section', 'Cross Section Number'])

@time_function
def extract_veloc_depch_data(file_path, relevant_grid_ids):
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.split()
            grid_id = int(parts[0])
            if grid_id in relevant_grid_ids:
                data.append((grid_id, float(parts[3])))
    return pd.DataFrame(data, columns=['FLO-2D Grid ID', os.path.basename(file_path).split('.')[0].upper()])

@time_function
def combine_channel_data(xsec_df, chanmax_df, chan_df, depch_df, veloc_df):
    combined_df = pd.merge(xsec_df, chan_df, on='Cross Section Number')
    combined_df = pd.merge(combined_df, chanmax_df.rename(columns={'NODE': 'FLO-2D Grid ID'}),
                           on='FLO-2D Grid ID', how='left')
    combined_df = pd.merge(combined_df, veloc_df, on='FLO-2D Grid ID', how='left')
    combined_df = pd.merge(combined_df, depch_df, on='FLO-2D Grid ID', how='left')
    return combined_df

@time_function
def extract_channel_data(file_path):
    xsec_df = extract_xsec_data(os.path.join(file_path, 'XSEC.DAT'))
    chanmax_df = extract_chanmax_data(os.path.join(file_path, 'CHANMAX.OUT'))
    chan_df = extract_chan_data(os.path.join(file_path, 'CHAN.DAT'))
    relevant_grid_ids = set(chan_df['FLO-2D Grid ID'])
    depch_df = extract_veloc_depch_data(os.path.join(file_path, 'DEPCH.OUT'), relevant_grid_ids)
    veloc_df = extract_veloc_depch_data(os.path.join(file_path, 'VELOC.OUT'), relevant_grid_ids)
    
    combined_df = combine_channel_data(xsec_df, chanmax_df, chan_df, depch_df, veloc_df)
    return combined_df
