import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import os
import xlsxwriter
from modules.utilities import time_function

@time_function
def create_channel_excel(file_path, combined_df):
    output_excel_path = os.path.join(file_path, 'flo2d_plots', 'channel_results.xlsx')
    with pd.ExcelWriter(output_excel_path, engine='xlsxwriter') as writer:
        # Full summary sheet
        combined_df.to_excel(writer, sheet_name='Full Summary', index=False)
        
        # Unique cross section summary sheet
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
def channel_spreadsheet_and_plots(file_path, channel_data):
    out_folder_path = os.path.join(file_path, 'flo2d_plots')
    os.makedirs(out_folder_path, exist_ok=True)
    
    create_channel_excel(file_path, channel_data)
    create_channel_plots(channel_data, os.path.join(out_folder_path, 'channel_plots.pdf'))
