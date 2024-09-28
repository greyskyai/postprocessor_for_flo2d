# main.py

import os
import pandas as pd
import time
import argparse
import logging
import shutil
from modules.data_extraction import extractModelDataToDF, extract_super_data  # Import the new function
from modules.hycross_extraction import extract_fpxsec_results
from modules.geospatial import convertToGeoDataFrame, calculate_cell_size
from modules.rasterization import create_raster_from_gdf
from modules.vectorization import convert_gdf_to_shapefile
from modules.fpxsec_vectorization import create_fpxsec_shapefile
from modules.utilities import create_required_folders
from modules.hystruc_vectorization import create_hystruc_shapefile
from modules.hystruc_extraction import extract_hystruc_results
from modules.hystruc_spreadsheet import hystruc_spreadsheet_and_plots, create_rating_curve_spreadsheet, plot_rating_curves_to_pdf
from modules.fpxsec_spreadsheet import hycross_spreadsheet_and_plots
from modules.hydrostruct_spreadsheet import hydrostruct_spreadsheet_and_plots, parse_hydrograph_data
from modules.rain_spreadsheet import rain_spreadsheet_and_plot
from modules.swmm_extraction import extract_swmm_data, create_swmm_shapefiles
from modules.arf_extraction import extract_area_reduction_factors, merge_arf_with_model_data
from modules.swmm_inlets_spreadsheets import swmm_inlet_spreadsheets_and_pdf
from modules.inflow_extraction import extract_inflow_hydrographs
from modules.inflow_spreadsheets import create_pdf_plots, export_hydrograph_to_excel
from modules.swmm_rating_tables_extraction import extract_swmm_rating_tables
from modules.swmm_rating_tables_spreadsheet import swmm_rating_tables_and_plots
from modules.evacuatedfp_extraction import extract_evacuatedfp_data  # Add this import
from modules.time_out_extraction import extract_time_out_data  # Add this import
import geopandas as gpd  # Ensure geopandas is imported

class TimingLogger:
    """
    A helper class to log the timing of each processing step.
    """
    def __init__(self, logger):
        self.logger = logger
        self.start_time = time.time()
        self.last_log_time = self.start_time

    def log(self, message):
        current_time = time.time()
        elapsed = current_time - self.last_log_time
        total_elapsed = current_time - self.start_time
        self.logger.info(f"{message} (Step Duration: {elapsed:.2f} seconds, Total Elapsed: {total_elapsed:.2f} seconds)")
        self.last_log_time = current_time

def setup_logger(level=logging.INFO, log_file=None):
    """
    Sets up the logger with the specified level and log file.

    Args:
        level (int): Logging level.
        log_file (str): Path to the log file.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger('FLO2D_Postprocessor')
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Clear existing handlers to prevent duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File Handler
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            logger.debug(f"Logging initialized. Logs will be saved to: {log_file}")
        except IOError as e:
            logger.warning(f"Unable to create log file at {log_file}. Logging will continue on console only. Error: {e}")

    return logger

def process_flo2d(file_path, coord_system, create_flo2d_points, verbose=False, log_file=None, style_folder=None, output_format="Shapefile"):
    """
    Processes a single FLO-2D project directory.

    Args:
        file_path (str): Path to the FLO-2D project directory.
        coord_system (int): EPSG code for the coordinate system.
        create_flo2d_points (bool): Flag to create FLO-2D points shapefile.
        verbose (bool): Flag to enable verbose logging.
        log_file (str): Path to the log file.
        style_folder (str): Path to the folder containing style files.
        output_format (str): Desired output format ("Shapefile" or "GeoPackage").

    Returns:
        str: Status message upon completion.
    """
    # Determine log file path
    if log_file is None:
        log_file = os.path.join(file_path, "flo2d_postprocessor.log")

    # Initialize logging
    logger = setup_logger(level=logging.DEBUG if verbose else logging.INFO, log_file=log_file)
    timing_logger = TimingLogger(logger)

    # Define output directories
    raster_outpath = os.path.join(file_path, 'flo2d_rasters')
    shp_outpath = os.path.join(file_path, 'flo2d_shp')
    plots_outpath = os.path.join(file_path, 'flo2d_plots')
    output_folders = [raster_outpath, shp_outpath, plots_outpath]

    logger.info("=== FLO-2D Postprocessor Started ===")
    logger.info(f"Project Directory: {file_path}")
    logger.info(f"Coordinate System: EPSG:{coord_system}")
    if style_folder:
        logger.info(f"Style Files Directory: {style_folder}")
    else:
        logger.info("No Style Files Directory provided.")

    # Step 1: Create required output directories
    timing_logger.log("Creating necessary output directories")
    create_required_folders(output_folders)
    timing_logger.log("Output directories successfully created")

    # Step 2: Extract model data
    timing_logger.log("Extracting model data from FLO-2D files")
    model_data = extractModelDataToDF(file_path)
    fpxsec_grids = model_data[pd.notna(model_data['fpxsec'])]
    timing_logger.log("Model data extraction completed")

    # Step 3: Extract Area Reduction Factors (ARF)
    arf_file = os.path.join(file_path, 'ARF.DAT')
    if os.path.exists(arf_file):
        timing_logger.log("Extracting Area Reduction Factors (ARF)")
        arf_df = extract_area_reduction_factors(arf_file)
        model_data = merge_arf_with_model_data(model_data, arf_df)
        timing_logger.log("ARF data successfully merged with model data")
    else:
        logger.warning(f"ARF file not found at {arf_file}. Skipping ARF extraction.")

    # Step 4: Convert DataFrame to GeoDataFrame
    timing_logger.log("Converting model data to GeoDataFrame for spatial processing")
    geo_df = convertToGeoDataFrame(model_data)
    timing_logger.log("Conversion to GeoDataFrame completed")

    # Step 5: Create FLO-2D Points Output (Shapefile or GeoPackage)
    if create_flo2d_points:
        timing_logger.log("Initiating creation of FLO-2D Points Output")

        if 'flow_direction' not in geo_df.columns:
            logger.error("'flow_direction' column is missing in the GeoDataFrame. Output creation aborted.")
        else:
            geo_df_subset = geo_df[['grid_id', 'flow_direction', 'geometry']]

            gpkg_file = os.path.join(shp_outpath, 'flow_direction.gpkg')
            try:
                geo_df_subset.to_file(gpkg_file, driver="GPKG", crs=f"EPSG:{coord_system}")
                timing_logger.log(f"FLO-2D Points GeoPackage created at: {gpkg_file}")
            except Exception as e:
                logger.error(f"Failed to create GeoPackage: {str(e)}")

    # Step 6: Extract and Process SUPER.OUT Data
    super_out_file = os.path.join(file_path, 'SUPER.OUT')
    if os.path.exists(super_out_file):
        timing_logger.log("Extracting data from SUPER.OUT")
        super_data = extract_super_data(file_path)
        timing_logger.log("SUPER.OUT data extraction completed")

        # Ensure grid_id is of the same type in both DataFrames
        super_data['grid_id'] = super_data['grid_id'].astype(geo_df['grid_id'].dtype)

        # Merge the super_data with the main GeoDataFrame
        super_geo_df = geo_df.merge(super_data, on='grid_id', how='left')
        print("Columns in super_geo_df after merge:", super_geo_df.columns)  # Debug print

        # Filter rows to include only those with non-null values in the super_data columns
        super_geo_df = super_geo_df.dropna(subset=['max_froude_no', 'depth_super', 'time_super', 'num_supercritical_timesteps'])

        # Select only the relevant columns for the output file
        columns_to_select = ['grid_id', 'max_froude_no', 'depth_super', 'time_super', 'num_supercritical_timesteps', 'geometry']
        super_geo_df = super_geo_df[columns_to_select]

        # Create a points shapefile or GeoPackage for the SUPER.OUT data
        if output_format == "Shapefile":
            super_file = os.path.join(shp_outpath, 'super_out_points.shp')
            driver = "ESRI Shapefile"
        else:
            super_file = os.path.join(shp_outpath, 'super_out_points.gpkg')
            driver = "GPKG"

        try:
            super_geo_df.to_file(super_file, driver=driver, crs=f"EPSG:{coord_system}")
            timing_logger.log(f"SUPER.OUT Points {output_format} created at: {super_file}")
        except Exception as e:
            logger.error(f"Failed to create SUPER.OUT Points {output_format}: {str(e)}")
    else:
        logger.info("SUPER.OUT file not found. Skipping SUPER.OUT data extraction.")

    # New Step: Extract and Process EVACUATEDFP.OUT Data
    evacuatedfp_file = os.path.join(file_path, 'EVACUATEDFP.OUT')
    if os.path.exists(evacuatedfp_file):
        timing_logger.log("Extracting data from EVACUATEDFP.OUT")
        evacuatedfp_data = extract_evacuatedfp_data(evacuatedfp_file)
        timing_logger.log("EVACUATEDFP.OUT data extraction completed")

        # Ensure grid_id is of the same type in both DataFrames
        evacuatedfp_data['grid_id'] = evacuatedfp_data['grid_id'].astype(geo_df['grid_id'].dtype)

        # Merge the evacuatedfp_data with the main GeoDataFrame
        evacuatedfp_geo_df = geo_df.merge(evacuatedfp_data, on='grid_id', how='left')
        print("Columns in evacuatedfp_geo_df after merge:", evacuatedfp_geo_df.columns)  # Debug print

        # Filter rows to include only those with non-null values in the evacuatedfp_data columns
        evacuatedfp_geo_df = evacuatedfp_geo_df.dropna(subset=['num_evacuations'])

        # Select only the relevant columns for the output file
        columns_to_select = ['grid_id', 'num_evacuations', 'geometry']
        evacuatedfp_geo_df = evacuatedfp_geo_df[columns_to_select]

        # Create a points shapefile or GeoPackage for the EVACUATEDFP.OUT data
        if output_format == "Shapefile":
            evacuatedfp_file = os.path.join(shp_outpath, 'evacuatedfp_out_points.shp')
            driver = "ESRI Shapefile"
        else:
            evacuatedfp_file = os.path.join(shp_outpath, 'evacuatedfp_out_points.gpkg')
            driver = "GPKG"

        try:
            evacuatedfp_geo_df.to_file(evacuatedfp_file, driver=driver, crs=f"EPSG:{coord_system}")
            timing_logger.log(f"EVACUATEDFP.OUT Points {output_format} created at: {evacuatedfp_file}")
        except Exception as e:
            logger.error(f"Failed to create EVACUATEDFP.OUT Points {output_format}: {str(e)}")
    else:
        logger.info("EVACUATEDFP.OUT file not found. Skipping EVACUATEDFP.OUT data extraction.")

    # New Step: Extract and Process TIME.OUT Data
    time_out_file = os.path.join(file_path, 'TIME.OUT')
    if os.path.exists(time_out_file):
        timing_logger.log("Extracting data from TIME.OUT")
        time_out_data = extract_time_out_data(time_out_file)
        timing_logger.log("TIME.OUT data extraction completed")

        # Ensure grid_id is of the same type in both DataFrames
        time_out_data['grid_id'] = time_out_data['grid_id'].astype(geo_df['grid_id'].dtype)

        # Merge the time_out_data with the main GeoDataFrame
        time_out_geo_df = geo_df.merge(time_out_data, on='grid_id', how='left')
        print("Columns in time_out_geo_df after merge:", time_out_geo_df.columns)  # Debug print

        # Filter rows to include only those with non-null values in the time_out_data columns
        time_out_geo_df = time_out_geo_df.dropna(subset=['num_time_decrements'])

        # Select only the relevant columns for the output file
        columns_to_select = ['grid_id', 'num_time_decrements', 'geometry']
        time_out_geo_df = time_out_geo_df[columns_to_select]

        # Create a points shapefile or GeoPackage for the TIME.OUT data
        if output_format == "Shapefile":
            time_out_file = os.path.join(shp_outpath, 'time_out_points.shp')
            driver = "ESRI Shapefile"
        else:
            time_out_file = os.path.join(shp_outpath, 'time_out_points.gpkg')
            driver = "GPKG"

        try:
            time_out_geo_df.to_file(time_out_file, driver=driver, crs=f"EPSG:{coord_system}")
            timing_logger.log(f"TIME.OUT Points {output_format} created at: {time_out_file}")
        except Exception as e:
            logger.error(f"Failed to create TIME.OUT Points {output_format}: {str(e)}")
    else:
        logger.info("TIME.OUT file not found. Skipping TIME.OUT data extraction.")

    # Step 7: Process Inflow Data
    if "INFLOW.DAT" in os.listdir(file_path):
        timing_logger.log("Extracting inflow data")
        inflow_data = extract_inflow_hydrographs(file_path)
        output_excel_path = os.path.join(plots_outpath, 'inflow_data.xlsx')
        export_hydrograph_to_excel(inflow_data, output_excel_path)
        timing_logger.log(f"Inflow data spreadsheet created: {output_excel_path}")
        # Uncomment below lines if PDF plots are desired
        # create_pdf_plots(inflow_data, os.path.join(plots_outpath, 'inflow_plots.pdf'))
        # timing_logger.log(f"Inflow plots PDF created: {os.path.join(plots_outpath, 'inflow_plots.pdf')}")

    # Step 8: Process Floodplain Cross Sections
    if "FPXSEC.DAT" in os.listdir(file_path) and "HYCROSS.OUT" in os.listdir(file_path):
        timing_logger.log("Processing Floodplain Cross Sections")
        fpxsec_results = extract_fpxsec_results(file_path)
        fpxsec_shp = create_fpxsec_shapefile(f_path=file_path, coord_system=coord_system, model_data=model_data, fpxsec_results=fpxsec_results, output_format=output_format)
        timing_logger.log(f"Floodplain Cross Sections Output created at: {fpxsec_shp}")
        hycross_files = hycross_spreadsheet_and_plots(file_path)
        timing_logger.log(f"HYCROSS Spreadsheet and Plots generated: {hycross_files}")
    else:
        logger.info("Floodplain Cross Sections data not found. Skipping this step.")

    # Step 9: Process Hydraulic Structures
    if "HYSTRUC.DAT" in os.listdir(file_path):
        timing_logger.log("Processing Hydraulic Structures")
        hystruc_df, rating_curves = extract_hystruc_results(file_path)
        hystruc_shp = create_hystruc_shapefile(hystruc_df, model_data, coord_system, shp_outpath, output_format=output_format)
        timing_logger.log(f"Hydraulic Structures Output created at: {hystruc_shp}")
        hydrograph_data = parse_hydrograph_data(file_path)
        hydrostruct_files = hydrostruct_spreadsheet_and_plots(file_path, hydrograph_data)
        timing_logger.log(f"Hydrostruct Spreadsheet and Plots generated: {hydrostruct_files}")
        rating_curve_excel = os.path.join(plots_outpath, 'hystruc_rating_curves.xlsx')
        rating_curve_pdf = os.path.join(plots_outpath, 'hystruc_rating_curves.pdf')
        create_rating_curve_spreadsheet(rating_curves, rating_curve_excel)
        timing_logger.log(f"Rating Curves Spreadsheet created at: {rating_curve_excel}")
        plot_rating_curves_to_pdf(rating_curves, rating_curve_pdf)
        timing_logger.log(f"Rating Curves PDF report generated at: {rating_curve_pdf}")
    else:
        logger.info("Hydraulic Structures data not found. Skipping this step.")

    # Step 10: Create Rainfall Spreadsheet and Plot
    if "RAIN.DAT" in os.listdir(file_path):
        timing_logger.log("Generating Rainfall Spreadsheet and Plot")
        rain_files = rain_spreadsheet_and_plot(file_path)
        timing_logger.log(f"Rainfall Spreadsheet and Plot created at: {rain_files}")
    else:
        logger.info("Rainfall data not found. Skipping this step.")

    # Step 11: Process SWMM Data
    swmm_file = os.path.join(file_path, 'SWMM.inp')
    if os.path.exists(swmm_file):
        timing_logger.log("Extracting SWMM Data from SWMM.inp")
        swmm_data = extract_swmm_data(swmm_file, coord_system)

        swmm_qin_file = os.path.join(file_path, 'SWMMQIN.OUT')
        if os.path.exists(swmm_qin_file):
            timing_logger.log("Generating SWMM Inlet Spreadsheets and PDF")
            swmm_inlet_files = swmm_inlet_spreadsheets_and_pdf(file_path)
            timing_logger.log(f"SWMM Inlet Spreadsheets and PDF created at: {swmm_inlet_files}")
        else:
            logger.warning(f"SWMMQIN.OUT file not found at {swmm_qin_file}. Skipping SWMM Inlet Spreadsheet and PDF creation.")

        # Create SWMM Shapefiles and GeoPackages
        timing_logger.log("Creating SWMM Shapefiles and GeoPackages")
        swmm_files = create_swmm_shapefiles(swmm_data, shp_outpath, output_format=output_format)
        for swmm_file_created in swmm_files:
            timing_logger.log(f"SWMM File created at: {swmm_file_created}")
        timing_logger.log("SWMM Data Extraction and File Creation completed successfully")
    else:
        logger.info("SWMM Input File (SWMM.inp) not found. Skipping SWMM Data Extraction.")

    # Step 12: Extract SWMM Rating Tables
    if "SWMMFLORT.DAT" in os.listdir(file_path):
        timing_logger.log("Extracting SWMM Rating Tables")
        swmm_rating_tables = extract_swmm_rating_tables(file_path)
        timing_logger.log("SWMM Rating Tables extraction completed")
        swmm_rating_tables_and_plots(file_path, swmm_rating_tables)
        rating_tables_excel = os.path.join(plots_outpath, 'swmm_rating_tables.xlsx')
        timing_logger.log(f"SWMM Rating Tables Spreadsheet created at: {rating_tables_excel}")
    else:
        logger.info("SWMM Rating Tables data not found. Skipping this step.")

    # Step 13: Calculate Cell Size for Raster Creation
    timing_logger.log("Calculating cell size for raster generation")
    cell_size = calculate_cell_size(geo_df)
    timing_logger.log(f"Calculated cell size: {cell_size} units")

    # Step 14: Create Rasters for Specified Columns
    desired_columns = [
        'depth_max', 'xksat', 'psif', 'dtheta', 'abstrinf', 'rtimpf', 'soil_depth',
        'velocity', 'q_max', 'wse_max', 'infil_depth', 'infil_stop', 'time_of_oneft',
        'time_of_twoft', 'time_to_peak', 'mannings_n', 'topo', 'final_velocity',
        'final_depth', 'rain_depth', 'arf'
    ]
    raster_columns = [col for col in desired_columns if col in model_data.columns]

    timing_logger.log("Initiating raster creation for available data columns")
    logger.debug(f"Available Columns in GeoDataFrame: {list(geo_df.columns)}")

    for column in raster_columns:
        logger.debug(f"Processing Column: '{column}' (Data Type: {geo_df[column].dtype})")
        raster_file = os.path.join(raster_outpath, f'{column}.tif')
        try:
            create_raster_from_gdf(geo_df, column, raster_file, cell_size, logger)
            timing_logger.log(f"Raster successfully created: {raster_file}")
        except Exception as e:
            logger.error(f"Failed to create raster for column '{column}'. Error: {e}")

    # Step 15: Apply Styles to Shapefiles and Rasters (if provided)
    if style_folder:
        timing_logger.log("Applying style files to shapefiles and rasters")
        apply_styles(file_path, style_folder, logger)
    else:
        logger.info("No style folder provided. Skipping style application.")

    timing_logger.log("=== FLO-2D Postprocessor Completed Successfully ===")
    return "FLO-2D Postprocessing completed successfully."

def apply_styles(file_path, style_folder, logger):
    """
    Applies QML style files to shapefiles and rasters.

    Args:
        file_path (str): Path to the FLO-2D project directory.
        style_folder (str): Path to the folder containing style files.
        logger (logging.Logger): Logger instance.
    """
    output_folders = [
        os.path.join(file_path, 'flo2d_rasters'),
        os.path.join(file_path, 'flo2d_shp')
    ]

    for folder in output_folders:
        if not os.path.isdir(folder):
            logger.warning(f"Output folder does not exist: {folder}. Skipping style application for this folder.")
            continue

        for file in os.listdir(folder):
            file_name, file_ext = os.path.splitext(file)
            style_file = os.path.join(style_folder, f"{file_name}.qml")
            if os.path.exists(style_file):
                destination = os.path.join(folder, f"{file_name}.qml")
                try:
                    shutil.copy(style_file, destination)
                    logger.info(f"Applied style file: {style_file} to {file}")
                except Exception as e:
                    logger.error(f"Failed to apply style file: {style_file} to {file}. Error: {e}")
            else:
                logger.warning(f"Style file not found for: {file_name}. Skipping style application for this file.")

    logger.info("Style application process completed.")

def batch_process_flo2d(file_paths, coord_system, create_flo2d_points, verbose=False, style_folder=None, output_format="Shapefile"):
    """
    Processes multiple FLO-2D project directories sequentially.

    Args:
        file_paths (list): List of FLO-2D project directory paths.
        coord_system (int): EPSG code for the coordinate system.
        create_flo2d_points (bool): Flag to create FLO-2D points shapefile.
        verbose (bool): Flag to enable verbose logging.
        style_folder (str): Path to the folder containing style files.
        output_format (str): Desired output format ("Shapefile" or "GeoPackage").

    Returns:
        str: Aggregated status messages for all processed directories.
    """
    results = []
    for file_path in file_paths:
        logger = logging.getLogger('FLO2D_Postprocessor')
        logger.info(f"Initiating processing for project directory: {file_path}")
        result = process_flo2d(
            file_path,
            coord_system,
            create_flo2d_points,
            verbose,
            style_folder=style_folder,
            output_format=output_format  # Pass output_format
        )
        results.append(f"{file_path}: {result}")
    return "\n".join(results)

def main():
    """
    The main entry point of the FLO-2D Postprocessor script.
    Parses command-line arguments and initiates processing.
    """
    parser = argparse.ArgumentParser(description="FLO-2D Postprocessor: Automate FLO-2D Data Extraction and Processing.")
    parser.add_argument(
        "file_paths",
        nargs='+',
        help="Paths to the input directories containing FLO-2D project files."
    )
    parser.add_argument(
        "--epsg",
        type=int,
        default=2224,
        help="EPSG code for the coordinate system (default: 2224)."
    )
    parser.add_argument(
        "--create_flo2d_points",
        action="store_true",
        help="Flag to create FLO-2D points shapefile."
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging for detailed output."
    )
    parser.add_argument(
        "--style_folder",
        help="Path to the folder containing QML style files for shapefiles and rasters."
    )
    parser.add_argument(
        "--output_format",
        choices=["Shapefile", "GeoPackage"],
        default="Shapefile",
        help="Desired output format for vector data (default: Shapefile)."
    )
    args = parser.parse_args()

    if args.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    # Set up root logger
    setup_logger(level=log_level)

    logger = logging.getLogger('FLO2D_Postprocessor')
    logger.info("=== FLO-2D Postprocessor Execution Started ===")

    result = batch_process_flo2d(
        args.file_paths,
        args.epsg,
        args.create_flo2d_points,
        verbose=args.verbose,
        style_folder=args.style_folder,
        output_format=args.output_format  # Pass output_format
    )
    logger.info("=== FLO-2D Postprocessor Execution Completed ===")
    logger.info(result)

if __name__ == "__main__":
    main()
