import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import os
import re
import xlsxwriter

def extract_hydrograph_data(file_path):
    """
    Extracts hydrograph data (time and discharge) from the specified file, integrating the maximum discharge
    at its correct time position and extracting the maximum water surface elevation.
    """
    hydrograph_data = {}
    max_discharge_info = {}  # Store max discharge info for each section
    max_wse_info = {}  # Store max water surface elevation info for each section
    current_section = None  # Initialize the current_section variable
    current_wse = -float('inf')  # Initialize the current_wse variable

    with open(file_path, 'r') as file:
        for line in file:
            # Check for the line with maximum discharge information
            if 'THE MAXIMUM DISCHARGE FROM CROSS SECTION' in line:
                match = re.search(r'THE MAXIMUM DISCHARGE FROM CROSS SECTION\s*(\d+) IS:\s*([\d.]+) CFS AT TIME:\s*([\d.]+)', line)
                if match:
                    section = int(match.group(1))
                    max_discharge = float(match.group(2))
                    max_time = float(match.group(3))
                    max_discharge_info[section] = (max_time, max_discharge)
                continue

            # Check for the line with maximum water surface elevation information
            if 'MAXIMUM WATER SURFACE ELEVATION AT CROSS SECTION' in line:
                match = re.search(r'MAXIMUM WATER SURFACE ELEVATION AT CROSS SECTION\s*(\d+)\s*IS:\s*([\d.]+)', line)
                if match:
                    section = int(match.group(1))
                    max_wse = float(match.group(2))
                    max_wse_info[section] = max_wse
                continue

            # Check for the start of a hydrograph section
            if 'HYDROGRAPH AND FLOODPLAIN HYDRAULICS' in line:
                match = re.search(r'FOR CROSS SECTION NO:\s*(\d+)', line)
                if match:
                    current_section = int(match.group(1))
                    hydrograph_data[current_section] = []
                    current_wse = -float('inf')  # Reset for new section
                continue

            # Detect the start of the data (after the column headers)
            if 'TIME' in line and 'DISCHARGE' in line:
                continue  # Skip the header line

            # Extract data if in a data section
            if current_section is not None:
                try:
                    parts = line.split()
                    time = float(parts[0])
                    wse = float(parts[3])  # Extract WS ELEV
                    discharge = float(parts[5])
                    hydrograph_data[current_section].append((time, discharge))
                    if wse > current_wse:
                        current_wse = wse
                    max_wse_info[current_section] = current_wse
                except (IndexError, ValueError):
                    # Handle lines that do not contain valid data
                    continue

    # Convert lists to pandas DataFrames and integrate max discharge
    for section in hydrograph_data:
        df = pd.DataFrame(hydrograph_data[section], columns=['Time', 'Discharge'])
        if section in max_discharge_info:
            df = integrate_max_discharge_in_df(df, max_discharge_info[section])
        hydrograph_data[section] = df

    return hydrograph_data, max_wse_info

def integrate_max_discharge_in_df(hydrograph_data, max_discharge_info):
    """
    Integrates the maximum discharge information into the DataFrame at its correct time position.
    """
    max_time, max_discharge = max_discharge_info

    # Check if the max time already exists in the DataFrame
    if max_time in hydrograph_data['Time'].values:
        hydrograph_data.loc[hydrograph_data['Time'] == max_time, 'Discharge'] = max_discharge
    else:
        # Insert a new row for the maximum discharge
        new_row = pd.DataFrame({'Time': [max_time], 'Discharge': [max_discharge]})
        hydrograph_data = pd.concat([hydrograph_data, new_row], ignore_index=True)
        hydrograph_data = hydrograph_data.sort_values(by='Time').reset_index(drop=True)

    return hydrograph_data

def export_hydrographs_to_excel_with_plots(hydrograph_data, max_wse_info, file_path):
    """
    Exports hydrograph data to an Excel file with each cross section's data and an integrated plot on the same sheet.
    """
    with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        for section, data in hydrograph_data.items():
            # Export data
            sheet_name = f"Section {section}"
            data.to_excel(writer, sheet_name=sheet_name, index=False)

            # Access the workbook and the sheet
            workbook = writer.book
            worksheet = writer.sheets[sheet_name]

            # Create a chart object
            line_chart = workbook.add_chart({'type': 'line'})
            line_chart.set_title({'name': f"Hydrograph for Section {section}"})
            line_chart.set_y_axis({'name': 'Discharge (cfs)'})
            line_chart.set_x_axis({'name': 'Time (hours)'})

            # Data for the chart
            line_chart.add_series({
                'values': [sheet_name, 1, 1, len(data), 1],
                'categories': [sheet_name, 1, 0, len(data), 0],
                'name': 'Discharge'
            })

            # Find max discharge and its time
            max_discharge = data['Discharge'].max()
            max_time = data[data['Discharge'] == max_discharge]['Time'].iloc[0]
            max_wse = max_wse_info.get(section, 'N/A')

            # Add labels to the chart
            peak_discharge_label = f"Qp = {max_discharge:.2f} cfs"
            time_to_peak_label = f"Tp = {max_time:.2f} hrs"
            max_wse_label = f"Max WSE = {max_wse:.2f} ft"
            line_chart.set_size({'width': 600, 'height': 400})
            line_chart.set_legend({'position': 'none'})
            line_chart.set_title({'name': f"Hydrograph for Section {section}\n{peak_discharge_label}, {time_to_peak_label}, {max_wse_label}"})

            # Place the chart on the sheet
            worksheet.insert_chart('E2', line_chart)

def create_pdf_plots(hydrograph_data, max_wse_info, output_pdf_path):
    """
    Creates a PDF with 4 plots per page for the given hydrograph data, using corrected max WSE information.
    """
    with PdfPages(output_pdf_path) as pdf:
        sections = list(hydrograph_data.keys())
        num_pages = (len(sections) + 3) // 4

        for page in range(num_pages):
            fig, axs = plt.subplots(2, 2, figsize=(8.5, 11))
            fig.subplots_adjust(hspace=0.4, wspace=0.3)
            axs = axs.flatten()

            for i in range(4):
                idx = page * 4 + i
                if idx >= len(sections):
                    break
                section = sections[idx]
                data = hydrograph_data[section]
                max_discharge = data['Discharge'].max()
                max_time = data[data['Discharge'] == max_discharge]['Time'].iloc[0]
                max_wse = max_wse_info.get(section, 'N/A')
                
                axs[i].plot(data['Time'], data['Discharge'], label='Discharge', color='blue')
                axs[i].set_title(f'Cross Section {section}')
                axs[i].set_xlabel('Time (hours)')
                axs[i].set_ylabel('Discharge (cfs)')
                axs[i].grid(True)
                if isinstance(max_wse, float):
                    label = f'Peak Discharge: {max_discharge:.2f} cfs\nMax WSE: {max_wse:.2f} ft\nTime of Peak: {max_time:.2f} hrs'
                else:
                    label = f'Peak Discharge: {max_discharge:.2f} cfs\nMax WSE: {max_wse}\nTime of Peak: {max_time:.2f} hrs'
                axs[i].text(0.05, 0.95, label, ha='left', va='top', transform=axs[i].transAxes, fontsize=8,
                            bbox=dict(facecolor='white', alpha=0.6))

            # Remove unused subplots
            for j in range(i + 1, 4):
                fig.delaxes(axs[j])

            pdf.savefig(fig)
            plt.close(fig)

# Main function to process the data and generate outputs
def hycross_spreadsheet_and_plots(folder_path):
    file_path = os.path.join(folder_path, 'HYCROSS.OUT')
    out_folder_path = os.path.join(folder_path, 'flo2d_plots')
    output_excel_path = os.path.join(out_folder_path, 'fpxsec_hydrographs.xlsx')
    output_pdf_path = os.path.join(out_folder_path, 'fpxsec_plots.pdf')

    # Extracting hydrograph data
    hydrograph_data, max_wse_info = extract_hydrograph_data(file_path)

    # Exporting to Excel
    export_hydrographs_to_excel_with_plots(hydrograph_data, max_wse_info, output_excel_path)

    # Creating and saving plots to PDF
    create_pdf_plots(hydrograph_data, max_wse_info, output_pdf_path)

