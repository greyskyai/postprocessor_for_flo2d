# fpxsec_vectorization.py

import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
from modules.utilities import time_function
import logging

def filter_model_data(model_data):
    """Filter model data for non-zero fpxsec values and return sorted unique fpxsec IDs."""
    fpxsec_grids = model_data[pd.notna(model_data['fpxsec'])]
    fpxsec_grids['fpxsec'] = fpxsec_grids['fpxsec'].astype(int)
    fpxsec_ids = fpxsec_grids['fpxsec'].drop_duplicates().sort_values()
    return fpxsec_ids


def create_linestring_from_data(df, fpxsec_id):
    """Create a LineString geometry from dataframe coordinates."""
    points = [Point(float(row['x']), float(row['y'])) for _, row in df.iterrows()]
    if len(points) > 1:
        return LineString(points)
    else:
        return None


def create_geodataframe(fpxsec_ids, model_data, fpxsec_results):
    """Create a GeoDataFrame with LineStrings and corresponding attributes."""
    rows = []
    for fpxsec_id in fpxsec_ids:
        df = model_data[model_data['fpxsec'] == fpxsec_id]
        line = create_linestring_from_data(df, fpxsec_id)
        if line is None:
            continue
        if fpxsec_id not in fpxsec_results['fpxs_id'].values:
            continue
        result_row = fpxsec_results.loc[fpxsec_results['fpxs_id'] == fpxsec_id].iloc[0]
        row = result_row.to_dict()
        row['geometry'] = line
        rows.append(row)

    gdf = gpd.GeoDataFrame(rows, columns=fpxsec_results.columns.tolist() + ['geometry'])
    return gdf


def save_geodataframe(gdf, f_path, coord_system, output_format, logger):
    """
    Save the GeoDataFrame to the specified format (Shapefile or GeoPackage).

    Args:
        gdf (GeoDataFrame): The GeoDataFrame to save.
        f_path (str): Base path to save the file.
        coord_system (int): EPSG code for the coordinate system.
        output_format (str): "Shapefile" or "GeoPackage".
        logger (logging.Logger): Logger instance.
    
    Returns:
        str: Path to the saved file.
    """
    output_dir = os.path.join(f_path, 'FLO2D_SHP')
    os.makedirs(output_dir, exist_ok=True)

    if output_format == "Shapefile":
        output_file = os.path.join(output_dir, 'fpxsec.shp')
        try:
            gdf.to_file(output_file, driver="ESRI Shapefile", crs=f"EPSG:{coord_system}")
            logger.info(f"FLO-2D FPXSEC Shapefile created at: {output_file}")
        except Exception as e:
            logger.error(f"Failed to create Shapefile: {str(e)}")
            raise
    elif output_format == "GeoPackage":
        output_file = os.path.join(output_dir, 'fpxsec.gpkg')
        try:
            gdf.to_file(output_file, driver="GPKG", crs=f"EPSG:{coord_system}")
            logger.info(f"FLO-2D FPXSEC GeoPackage created at: {output_file}")
        except Exception as e:
            logger.error(f"Failed to create GeoPackage: {str(e)}")
            raise
    else:
        logger.error(f"Unsupported output format: {output_format}. Expected 'Shapefile' or 'GeoPackage'.")
        raise ValueError(f"Unsupported output format: {output_format}")

    return output_file


@time_function
def create_fpxsec_shapefile(f_path, coord_system, model_data, fpxsec_results, output_format="Shapefile"):
    """
    Main function to create a shapefile or geopackage from model data and fpxsec results.

    Args:
        f_path (str): Path to the FLO-2D project directory.
        coord_system (int): EPSG code for the coordinate system.
        model_data (DataFrame): Extracted model data.
        fpxsec_results (DataFrame): Extracted FPXSEC results.
        output_format (str): Desired output format ("Shapefile" or "GeoPackage").

    Returns:
        str: Path to the created shapefile or geopackage.
    """
    logger = logging.getLogger('FLO2D_Postprocessor')

    fpxsec_ids = filter_model_data(model_data)
    gdf = create_geodataframe(fpxsec_ids, model_data, fpxsec_results)
    gdf.crs = f"EPSG:{coord_system}"

    if gdf.empty:
        logger.warning("No FPXSEC data to save. GeoDataFrame is empty.")
        return None

    shp_path = save_geodataframe(gdf, f_path, coord_system, output_format, logger)
    return shp_path
