# hystruc_vectorization.py

import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
import os
from modules.utilities import time_function
import logging

@time_function
def create_hystruc_shapefile(hystruc_df, model_data_df, coord_system, output_path, output_format="Shapefile"):
    """
    Create a shapefile or geopackage for hydraulic structures based on the specified output format.

    Args:
        hystruc_df (DataFrame): Hydraulic structures data.
        model_data_df (DataFrame): Model data with 'grid_id', 'x', 'y' columns.
        coord_system (int): EPSG code for spatial reference.
        output_path (str): Directory path to save the output file.
        output_format (str): Desired output format ("Shapefile" or "GeoPackage").

    Returns:
        str: Path to the created shapefile or geopackage.
    """
    logger = logging.getLogger('FLO2D_Postprocessor')

    # Merge the hystruc dataframe with the model data dataframe to get x, y coordinates for inflow and outflow nodes
    # This assumes the model_data_df has 'grid_id', 'x', 'y' columns
    merged_df = pd.merge(hystruc_df, model_data_df[['grid_id', 'x', 'y']], left_on='Inflow Node', right_on='grid_id', how='left')
    merged_df.rename(columns={'x': 'inflow_x', 'y': 'inflow_y'}, inplace=True)
    merged_df = pd.merge(merged_df, model_data_df[['grid_id', 'x', 'y']], left_on='Outflow Node', right_on='grid_id', how='left', suffixes=('', '_outflow'))
    merged_df.rename(columns={'x': 'outflow_x', 'y': 'outflow_y'}, inplace=True)

    # Create a GeoDataFrame with a LineString from inflow to outflow for each structure
    geometry = [
        LineString([
            (row['inflow_x'], row['inflow_y']),
            (row['outflow_x'], row['outflow_y'])
        ]) for index, row in merged_df.iterrows()
        if not pd.isna(row['inflow_x']) and not pd.isna(row['outflow_x'])
    ]

    gdf = gpd.GeoDataFrame(merged_df, geometry=geometry, crs=f"EPSG:{coord_system}")

    if gdf.empty:
        logger.warning("No Hydraulic Structures data to save. GeoDataFrame is empty.")
        return None

    if output_format == "Shapefile":
        output_file = os.path.join(output_path, 'hydraulic_structures.shp')
        try:
            gdf.to_file(output_file, driver="ESRI Shapefile", crs=f"EPSG:{coord_system}")
            logger.info(f"Hydraulic Structures Shapefile created at: {output_file}")
        except Exception as e:
            logger.error(f"Failed to create Shapefile: {str(e)}")
            raise
    elif output_format == "GeoPackage":
        output_file = os.path.join(output_path, 'hydraulic_structures.gpkg')
        try:
            gdf.to_file(output_file, driver="GPKG", crs=f"EPSG:{coord_system}")
            logger.info(f"Hydraulic Structures GeoPackage created at: {output_file}")
        except Exception as e:
            logger.error(f"Failed to create GeoPackage: {str(e)}")
            raise
    else:
        logger.error(f"Unsupported output format: {output_format}. Expected 'Shapefile' or 'GeoPackage'.")
        raise ValueError(f"Unsupported output format: {output_format}")

    return output_file
