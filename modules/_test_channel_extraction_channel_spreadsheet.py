import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import os
import time
import numpy as np

def time_function(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"{func.__name__} took {end_time - start_time:.2f} seconds")
        return result
    return wrapper

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

@time_function
def create_channel_excel(file_path, combined_df):
    output_excel_path = os.path.join(file_path, 'flo2d_plots', 'channel_results.xlsx')
    with pd.ExcelWriter(output_excel_path, engine='xlsxwriter') as writer:
        combined_df.to_excel(writer, sheet_name='Full Summary', index=False)
        unique_df = combined_df.drop_duplicates(subset=['Cross Section Number'])
        unique_df.to_excel(writer, sheet_name='Unique Cross Section Summary', index=False)
    print(f"Excel file created: {output_excel_path}")

@time_function
def create_channel_plots(combined_df, output_pdf_path):
    unique_cross_sections = combined_df['Cross Section Number'].unique()
    num_plots = len(unique_cross_sections)
    
    cs_data_dict = {cs_num: combined_df[combined_df['Cross Section Number'] == cs_num] for cs_num in unique_cross_sections}
    
    with PdfPages(output_pdf_path) as pdf:
        fig, axs = plt.subplots(2, 2, figsize=(8.5, 11))
        fig.subplots_adjust(hspace=0.4, wspace=0.3)
        axs = axs.flatten()
        
        for i in range(0, num_plots, 4):
            for j in range(4):
                if i + j < num_plots:
                    cross_section_number = unique_cross_sections[i + j]
                    cs_data = cs_data_dict[cross_section_number]
                    
                    ax = axs[j]
                    ax.plot(cs_data['Station'], cs_data['Elevation'], 'k-', linewidth=1.25)
                    max_stage = cs_data['Max Stage'].max()
                    ax.axhline(y=max_stage, color='b', linestyle='--', label='Max Water Surface')
                    
                    ax.set_title(f'Cross-Section {cross_section_number}')
                    ax.set_xlabel('Station')
                    ax.set_ylabel('Elevation')
                    
                    max_discharge = cs_data['Max Discharge (CFS)'].max()
                    time_to_peak = cs_data['Time of Max Discharge (Hrs)'].max()
                    max_velocity = cs_data['VELOC'].max()
                    max_depth = cs_data['DEPCH'].max()
                    
                    ax.text(0.05, 0.95, (f'Max Q: {max_discharge:.2f} cfs\n'
                                         f'Max Stage: {max_stage:.2f} ft\n'
                                         f'Time to Peak: {time_to_peak:.2f} hrs\n'
                                         f'Max Velocity: {max_velocity:.2f} ft/s\n'
                                         f'Max Depth: {max_depth:.2f} ft'),
                            transform=ax.transAxes, verticalalignment='top', fontsize=7,
                            bbox=dict(facecolor='white', alpha=0.7))
                    
                    ax.legend(fontsize=7, loc='upper right')
                else:
                    axs[j].clear()  # Clear the axis if no data for it
            
            fig.tight_layout()
            pdf.savefig(fig)
            plt.clf()  # Clear the figure to reuse the axes
    
    print(f"PDF file created: {output_pdf_path}")

@time_function
def channel_spreadsheet_and_plots(file_path):
    out_folder_path = os.path.join(file_path, 'flo2d_plots')
    os.makedirs(out_folder_path, exist_ok=True)
    
    channel_data = extract_channel_data(file_path)
    create_channel_excel(file_path, channel_data)
    create_channel_plots(channel_data, os.path.join(out_folder_path, 'channel_plots.pdf'))

if __name__ == "__main__":
    file_path = r"S:\21002795 - Lake Havasu\Project Documents\Engineering-Planning-Power and Energy\Reports\CLOMR\Models\FLO2D\PROP\20240716"
    channel_spreadsheet_and_plots(file_path)
