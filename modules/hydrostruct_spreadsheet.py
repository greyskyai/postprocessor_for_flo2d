import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import re
import os

def parse_hydrograph_data(folder_path):
    """
    Parse hydrograph data from the provided file.
    Args:
    file_path (str): Path to the file containing the hydrograph data.
    Returns:
    dict: Dictionary containing structure names as keys and their corresponding
          hydrograph data as pandas DataFrames.
    """
    file_path = os.path.join(folder_path, 'HYDROSTRUCT.OUT')
    hydrograph_data = {}
    current_structure = None
    current_data = []
    structure_header_re = re.compile(r'THE MAXIMUM DISCHARGE FOR:\s+(\S+)\s+')
    data_row_re = re.compile(r'^\s*(\d+\.\d+)\s+(-?\d+\.\d+)\s+(-?\d+\.\d+)')
    with open(file_path, 'r') as file:
        for line in file:
            header_match = structure_header_re.search(line)
            if header_match:
                if current_structure and current_data:
                    df = pd.DataFrame(current_data, columns=['Time (Hrs)', 'Inflow (CFS)', 'Outflow (CFS)'])
                    hydrograph_data[current_structure] = df
                    current_data = []
                current_structure = header_match.group(1)
            else:
                data_match = data_row_re.search(line)
                if data_match:
                    time, inflow, outflow = data_match.groups()
                    current_data.append([float(time), float(inflow), float(outflow)])
        if current_structure and current_data:
            df = pd.DataFrame(current_data, columns=['Time (Hrs)', 'Inflow (CFS)', 'Outflow (CFS)'])
            hydrograph_data[current_structure] = df
    return hydrograph_data

def hydrostruct_hydrographs_to_excel(hydrograph_data, output_folder):
    """
    Export hydrograph data to an Excel file with each structure's data and plot on the same sheet.
    The plot is placed starting from cell E1, and the title of the plot includes the peak discharge
    and time of peak discharge.
    Args:
    hydrograph_data (dict): Dictionary containing structure names as keys and their corresponding
                            hydrograph data as pandas DataFrames.
    output_folder (str): Folder where the output Excel file will be saved.
    """
    output_file = f'{output_folder}/hydrostruct_hydrographs.xlsx'
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        for structure, data in hydrograph_data.items():
            data.to_excel(writer, sheet_name=structure, index=False, startrow=3)
            workbook = writer.book
            worksheet = writer.sheets[structure]
            
            peak_inflow_time = data['Time (Hrs)'][data['Inflow (CFS)'].idxmax()]
            peak_inflow_value = data['Inflow (CFS)'].max()

            chart = workbook.add_chart({'type': 'line'})
            chart.add_series({
                'name': 'Outflow',
                'categories': f'={structure}!$A$5:$A${len(data)+4}',
                'values': f'={structure}!$C$5:$C${len(data)+4}',
                'line': {'color': 'blue'}
            })
            chart.add_series({
                'name': 'Inflow',
                'categories': f'={structure}!$A$5:$A${len(data)+4}',
                'values': f'={structure}!$B$5:$B${len(data)+4}',
                'line': {'color': 'red'}
            })
            chart.set_title({'name': f'{structure}'})
            chart.set_x_axis({'name': 'Time (hrs)', 'label_position': 'low'})
            chart.set_y_axis({'name': 'Discharge (cfs)'})
            chart.set_legend({'position': 'bottom'})
            chart.set_size({'width': 960, 'height': 576})
            worksheet.insert_chart('E1', chart)
            
            worksheet.write('A1', 'Peak Discharge (cfs)')
            worksheet.write('B1', peak_inflow_value)
            worksheet.write('A2', 'Time to Peak (hrs)')
            worksheet.write('B2', peak_inflow_time)

def hydrostruct_pdf_plots(hydrograph_data, output_pdf_path):
    """
    Creates a PDF with 4 plots per page for the given hydrograph data, with labels for peak discharge and time to peak,
    adjusted to be below the legend, and axis titles in lowercase.
    """
    with PdfPages(output_pdf_path) as pdf:
        structures = list(hydrograph_data.keys())
        num_pages = (len(structures) + 3) // 4

        for page in range(num_pages):
            fig, axs = plt.subplots(2, 2, figsize=(8.5, 11))
            fig.subplots_adjust(hspace=0.4, wspace=0.3)
            axs = axs.flatten()

            for i in range(4):
                idx = page * 4 + i
                if idx >= len(structures):
                    break
                structure = structures[idx]
                data = hydrograph_data[structure]
                peak_inflow_time = data['Time (Hrs)'][data['Inflow (CFS)'].idxmax()]
                peak_inflow_value = data['Inflow (CFS)'].max()

                axs[i].plot(data['Time (Hrs)'], data['Inflow (CFS)'], label='Inflow', color='blue')
                axs[i].plot(data['Time (Hrs)'], data['Outflow (CFS)'], label='Outflow', color='red')
                axs[i].set_title(f'{structure}')
                axs[i].set_xlabel('Time (hrs)')
                axs[i].set_ylabel('Discharge (cfs)')
                axs[i].grid(True)
                axs[i].legend()
                label = f'Peak Discharge: {peak_inflow_value:.2f} cfs\nTime of Peak: {peak_inflow_time:.2f} hrs'
                axs[i].text(0.05, 0.80, label, ha='left', va='top', transform=axs[i].transAxes, fontsize=8,
                            bbox=dict(facecolor='white', alpha=0.6))

            # Remove unused subplots
            for j in range(i + 1, 4):
                fig.delaxes(axs[j])

            pdf.savefig(fig)
            plt.close(fig)

# Main function to process the data and generate outputs
def hydrostruct_spreadsheet_and_plots(folder_path, hydrograph_data):
    out_folder_path = os.path.join(folder_path, 'flo2d_plots')
    hydrostruct_hydrographs_to_excel(hydrograph_data, out_folder_path)
    hydrostruct_pdf_plots(hydrograph_data, os.path.join(out_folder_path, 'hydrostruct_plots.pdf'))

