import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import os

# Function to create PDF plots
def create_pdf_plots(hydrograph_data, output_pdf_path):
    """
    Creates a PDF with 4 plots per page for the given hydrograph data.
    Labels the max discharge and time of max discharge.
    """
    grid_ids = hydrograph_data.columns  # Use the grid element IDs as column names
    num_pages = (len(grid_ids) + 3) // 4  # 4 plots per page

    with PdfPages(output_pdf_path) as pdf:
        for page in range(num_pages):
            fig, axs = plt.subplots(2, 2, figsize=(8.5, 11))
            fig.subplots_adjust(hspace=0.4, wspace=0.3)
            axs = axs.flatten()

            for i in range(4):
                idx = page * 4 + i
                if idx >= len(grid_ids):
                    break

                grid_id = grid_ids[idx]
                data = hydrograph_data[grid_id]
                time = hydrograph_data.index

                # Find max discharge and its corresponding time
                max_discharge = data.max()
                max_time = time[data == max_discharge][0]
                
                # Plotting
                axs[i].plot(time, data, label='Discharge', color='blue')
                axs[i].set_title(f'Grid ID {grid_id}')
                axs[i].set_xlabel('Time (hours)')
                axs[i].set_ylabel('Flow (cfs)')
                axs[i].grid(True)

                # Annotate with peak flow info
                label = f'Peak Discharge: {max_discharge:.2f} cfs\nTime of Peak: {max_time:.2f} hrs'
                axs[i].text(0.05, 0.95, label, ha='left', va='top', transform=axs[i].transAxes, fontsize=8,
                            bbox=dict(facecolor='white', alpha=0.6))

            # Remove any unused subplots
            for j in range(i + 1, 4):
                fig.delaxes(axs[j])

            pdf.savefig(fig)
            plt.close(fig)

# Function to export hydrograph data to Excel with charts
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import os
import logging

def create_pdf_plots(hydrograph_data, output_pdf_path, batch_size=100):
    """
    Creates a PDF with multiple hydrograph plots. Plots are batched to handle large datasets efficiently.

    Args:
        hydrograph_data (pd.DataFrame): DataFrame containing hydrograph data with time as index and grid IDs as columns.
        output_pdf_path (str): Path where the output PDF will be saved.
        batch_size (int): Number of plots per PDF batch.
    """
    logger = logging.getLogger(__name__)
    grid_ids = hydrograph_data.columns
    total_plots = len(grid_ids)
    num_batches = (total_plots + batch_size - 1) // batch_size  # Ceiling division

    # Switch to non-interactive backend to prevent GUI issues
    plt.switch_backend('Agg')

    logger.info(f"Starting PDF plot creation: {output_pdf_path}")
    logger.info(f"Total Grid IDs to plot: {total_plots}. Batch size: {batch_size}. Total batches: {num_batches}.")

    with PdfPages(output_pdf_path) as pdf:
        for batch_num in range(num_batches):
            fig, axs = plt.subplots(10, 10, figsize=(11, 8.5))  # Adjust subplot grid as needed
            fig.subplots_adjust(hspace=0.5, wspace=0.3)
            axs = axs.flatten()

            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, total_plots)
            current_batch = grid_ids[start_idx:end_idx]

            for i, grid_id in enumerate(current_batch):
                data = hydrograph_data[grid_id]
                time = hydrograph_data.index

                if data.empty:
                    axs[i].text(0.5, 0.5, 'No Data Available', ha='center', va='center', fontsize=8)
                    axs[i].set_title(f'Grid ID {grid_id}', fontsize=8)
                    axs[i].axis('off')
                    logger.warning(f"No data available for Grid ID {grid_id}. Plot skipped.")
                    continue

                # Find max discharge and its corresponding time
                max_discharge = data.max()
                max_time = data.idxmax()

                # Plotting
                axs[i].plot(time, data, label='Discharge', color='blue', linewidth=0.5)
                axs[i].set_title(f'Grid ID {grid_id}', fontsize=8)
                axs[i].set_xlabel('Time (hrs)', fontsize=6)
                axs[i].set_ylabel('Flow (cfs)', fontsize=6)
                axs[i].grid(True)

                # Annotate with peak flow info
                label = f'Peak: {max_discharge:.2f} cfs\nTime: {max_time:.2f} hrs'
                axs[i].text(
                    0.05, 0.95, label, ha='left', va='top',
                    transform=axs[i].transAxes, fontsize=6,
                    bbox=dict(facecolor='white', alpha=0.6)
                )

                logger.debug(f"Plotted Grid ID {grid_id}: Max Discharge = {max_discharge}, Time of Peak = {max_time}")

            # Remove any unused subplots
            for j in range(len(current_batch), len(axs)):
                axs[j].axis('off')

            pdf.savefig(fig)
            plt.close(fig)
            logger.info(f"Batch {batch_num + 1}/{num_batches} saved to PDF.")

    logger.info(f"PDF plot creation completed: {output_pdf_path}")

def export_hydrograph_to_excel(hydrograph_data, output_excel_path, time_scale=10):
    """
    Exports hydrograph data to an Excel file with all grid IDs in a single sheet and a summary sheet.
    Adjusts time values by the specified scaling factor.
    
    Args:
        hydrograph_data (pd.DataFrame): DataFrame with time as index and grid IDs as columns.
        output_excel_path (str): Path to save the Excel file.
        time_scale (float): Factor to scale time values (default is 10 to correct 3.2 hrs to 32 hrs).
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Starting Excel export: {output_excel_path}")
    grid_ids = hydrograph_data.columns

    # Adjust time by the scaling factor
    adjusted_time = hydrograph_data.index * time_scale

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    with pd.ExcelWriter(output_excel_path, engine='xlsxwriter') as writer:
        # Consolidate all hydrographs into one sheet
        all_data = pd.DataFrame({'Time': adjusted_time})
        for grid_id in grid_ids:
            all_data[f'Flow_{grid_id}'] = hydrograph_data[grid_id]

        # Write main data to the first sheet
        all_data.to_excel(writer, sheet_name='Hydrographs', index=False)

        # Calculate max discharge and time of max discharge for each grid ID
        max_discharge = hydrograph_data.max()
        max_time = hydrograph_data.idxmax() * time_scale  # Scale time accordingly

        summary_data = pd.DataFrame({
            'Grid_ID': grid_ids,
            'Max_Discharge_cfs': max_discharge,
            'Time_of_Max_Discharge_hrs': max_time
        })

        # Write summary data to the second sheet
        summary_data.to_excel(writer, sheet_name='Summary', index=False)

    logger.info(f"Excel export completed: {output_excel_path}")

