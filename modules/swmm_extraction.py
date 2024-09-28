# swmm_extraction.py

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
import os
import logging
import re
from modules.utilities import time_function

def extract_swmm_data(file_path, epsg):
    """
    Extracts SWMM data from an input file and returns processed GeoDataFrames.

    Parameters:
    - file_path: str, path to the SWMM .inp file.
    - epsg: int, EPSG code for the coordinate reference system.

    Returns:
    - dict of GeoDataFrames for junctions, outfalls, and conduits.
    """
    sections = {
        'JUNCTIONS': [], 'OUTFALLS': [], 'CONDUITS': [], 'XSECTIONS': [],
        'COORDINATES': [], 'LOSSES': [], 'INFLOWS': []
    }

    current_section = None

    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            # Identify section headers
            if line.startswith('[') and line.endswith(']'):
                current_section = line[1:-1].upper()
                continue
            if not line or line.startswith(';'):  # Skip empty lines and comments
                continue
            # Only extract data for relevant sections
            if current_section in sections:
                sections[current_section].append(line)

    results = {}
    if sections['JUNCTIONS']:
        results['junctions'] = process_junctions(sections['JUNCTIONS'], sections['COORDINATES'], epsg)
    if sections['OUTFALLS']:
        results['outfalls'] = process_outfalls(sections['OUTFALLS'], sections['COORDINATES'], epsg)
    if sections['CONDUITS']:
        results['conduits'] = process_conduits(sections['CONDUITS'], sections['XSECTIONS'], sections['COORDINATES'], epsg)

    return results

def process_junctions(junctions_data, coordinates_data, coord_system):
    """
    Processes the junctions data into a GeoDataFrame using the coordinates from the COORDINATES section.

    Parameters:
    - junctions_data: List of junction data lines.
    - coordinates_data: List of coordinates data lines.
    - coord_system: EPSG code for the coordinate reference system.

    Returns:
    - GeoDataFrame with junction points.
    """
    columns = ['Name', 'Invert_Elevation', 'Max_Depth', 'Init_Depth', 'Surcharge_Depth', 'Ponded_Area']
    junctions = []

    for line in junctions_data:
        parts = re.split(r'\s+', line.strip())
        if len(parts) < len(columns):
            parts += [None] * (len(columns) - len(parts))  # Pad missing values with None
        junctions.append(parts[:len(columns)])

    df_junctions = pd.DataFrame(junctions, columns=columns)
    df_junctions = df_junctions.apply(pd.to_numeric, errors='ignore')

    coords_columns = ['Name', 'X_Coord', 'Y_Coord']
    coords = []

    for line in coordinates_data:
        parts = re.split(r'\s+', line.strip())
        if len(parts) < len(coords_columns):
            parts += [None] * (len(coords_columns) - len(parts))
        coords.append(parts[:len(coords_columns)])

    df_coords = pd.DataFrame(coords, columns=coords_columns)
    df_coords[['X_Coord', 'Y_Coord']] = df_coords[['X_Coord', 'Y_Coord']].apply(pd.to_numeric, errors='coerce')

    df_merged = pd.merge(df_junctions, df_coords, on='Name', how='left')
    geometry = [Point(xy) for xy in zip(df_merged['X_Coord'], df_merged['Y_Coord'])]
    gdf = gpd.GeoDataFrame(df_merged, geometry=geometry, crs=f"EPSG:{coord_system}")

    return gdf

def process_outfalls(outfalls_data, coordinates_data, coord_system):
    """
    Processes the outfalls data into a GeoDataFrame using the coordinates from the COORDINATES section.

    Parameters:
    - outfalls_data: List of outfalls data lines.
    - coordinates_data: List of coordinates data lines.
    - coord_system: EPSG code for the coordinate reference system.

    Returns:
    - GeoDataFrame with outfall points.
    """
    columns = ['Name', 'Invert_Elevation', 'Outfall_Type', 'Stage_Data', 'Tide_Gate']
    outfalls = []

    for line in outfalls_data:
        parts = re.split(r'\s+', line.strip())
        if len(parts) < len(columns):
            parts += [None] * (len(columns) - len(parts))  # Pad missing values with None
        outfalls.append(parts[:len(columns)])

    df_outfalls = pd.DataFrame(outfalls, columns=columns)
    df_outfalls = df_outfalls.apply(pd.to_numeric, errors='ignore')

    coords_columns = ['Name', 'X_Coord', 'Y_Coord']
    coords = []

    for line in coordinates_data:
        parts = re.split(r'\s+', line.strip())
        if len(parts) < len(coords_columns):
            parts += [None] * (len(coords_columns) - len(parts))
        coords.append(parts[:len(coords_columns)])

    df_coords = pd.DataFrame(coords, columns=coords_columns)
    df_coords[['X_Coord', 'Y_Coord']] = df_coords[['X_Coord', 'Y_Coord']].apply(pd.to_numeric, errors='coerce')

    df_merged = pd.merge(df_outfalls, df_coords, on='Name', how='left')
    geometry = [Point(xy) for xy in zip(df_merged['X_Coord'], df_merged['Y_Coord'])]
    gdf = gpd.GeoDataFrame(df_merged, geometry=geometry, crs=f"EPSG:{coord_system}")

    return gdf

def process_conduits(conduits_data, xsections_data, coordinates_data, coord_system):
    """
    Processes the conduits data into a GeoDataFrame using the coordinates from the COORDINATES section.

    Parameters:
    - conduits_data: List of conduits data lines.
    - xsections_data: List of cross-section data lines.
    - coordinates_data: List of coordinates data lines.
    - coord_system: EPSG code for the coordinate reference system.

    Returns:
    - GeoDataFrame with conduit lines.
    """
    conduit_columns = ['Name', 'From_Node', 'To_Node', 'Length', 'Manning_N', 'Inlet_Offset', 'Outlet_Offset', 'Init_Flow', 'Max_Flow']
    conduits = []

    for line in conduits_data:
        parts = re.split(r'\s+', line.strip())
        if len(parts) < len(conduit_columns):
            parts += [None] * (len(conduit_columns) - len(parts))  # Pad missing values with None
        conduits.append(parts[:len(conduit_columns)])

    df_conduits = pd.DataFrame(conduits, columns=conduit_columns)
    df_conduits = df_conduits.apply(pd.to_numeric, errors='ignore')

    coords_columns = ['Name', 'X_Coord', 'Y_Coord']
    coords = []

    for line in coordinates_data:
        parts = re.split(r'\s+', line.strip())
        if len(parts) < len(coords_columns):
            parts += [None] * (len(coords_columns) - len(parts))
        coords.append(parts[:len(coords_columns)])

    df_coords = pd.DataFrame(coords, columns=coords_columns)
    df_coords[['X_Coord', 'Y_Coord']] = df_coords[['X_Coord', 'Y_Coord']].apply(pd.to_numeric, errors='coerce')

    # Merge conduit start (From_Node) and end (To_Node) coordinates
    df_merged_from = pd.merge(df_conduits, df_coords, left_on='From_Node', right_on='Name', how='left', suffixes=('', '_from'))
    df_merged_to = pd.merge(df_merged_from.drop(columns=['Name']), df_coords, left_on='To_Node', right_on='Name', how='left', suffixes=('_from', '_to'))

    # Create LineString geometries for the conduits
    geometry = [
        LineString([
            (row['X_Coord_from'], row['Y_Coord_from']),
            (row['X_Coord_to'], row['Y_Coord_to'])
        ]) for index, row in df_merged_to.iterrows()
        if not pd.isna(row['X_Coord_from']) and not pd.isna(row['X_Coord_to'])
    ]

    gdf = gpd.GeoDataFrame(df_merged_to, geometry=geometry, crs=f"EPSG:{coord_system}")

    return gdf

def save_geodataframe(gdf, output_path, layer_name, coord_system, output_format, logger):
    """
    Save the GeoDataFrame to the specified format (Shapefile or GeoPackage).

    Args:
        gdf (GeoDataFrame): The GeoDataFrame to save.
        output_path (str): Base path to save the file.
        layer_name (str): Name of the layer/file.
        coord_system (int): EPSG code for the coordinate system.
        output_format (str): "Shapefile" or "GeoPackage".
        logger (logging.Logger): Logger instance.
    
    Returns:
        str: Path to the saved file.
    """

    if output_format == "Shapefile":
        output_file = os.path.join(output_path, f'{layer_name}.shp')
        try:
            gdf.to_file(output_file, driver="ESRI Shapefile", crs=f"EPSG:{coord_system}")
            logger.info(f"SWMM {layer_name.capitalize()} Shapefile created at: {output_file}")
        except Exception as e:
            logger.error(f"Failed to create Shapefile for {layer_name}: {str(e)}")
            raise
    elif output_format == "GeoPackage":
        output_file = os.path.join(output_path, f'swmm_{layer_name}.gpkg')
        try:
            gdf.to_file(output_file, layer=layer_name, driver="GPKG", crs=f"EPSG:{coord_system}")
            logger.info(f"SWMM {layer_name.capitalize()} GeoPackage created at: {output_file}")
        except Exception as e:
            logger.error(f"Failed to create GeoPackage for {layer_name}: {str(e)}")
            raise
    else:
        logger.error(f"Unsupported output format: {output_format}. Expected 'Shapefile' or 'GeoPackage'.")
        raise ValueError(f"Unsupported output format: {output_format}")

    return output_file

@time_function
def create_swmm_shapefiles(swmm_data, output_path, output_format="Shapefile"):
    """
    Creates shapefiles or geopackages for SWMM junctions, conduits, and outfalls.

    Parameters:
    - swmm_data: dict of GeoDataFrames from extract_swmm_data
    - output_path: str, path to save the shapefiles or geopackages
    - output_format: str, "Shapefile" or "GeoPackage"

    Returns:
    - list of created file paths
    """
    logger = logging.getLogger('FLO2D_Postprocessor')
    shapefile_paths = []

    if 'junctions' in swmm_data:
        junctions_gdf = swmm_data['junctions']
        if not junctions_gdf.empty:
            shp_path = save_geodataframe(junctions_gdf, output_path, 'junctions', junctions_gdf.crs.to_epsg(), output_format, logger)
            shapefile_paths.append(shp_path)
        else:
            logger.warning("No junctions data to save.")

    if 'outfalls' in swmm_data:
        outfalls_gdf = swmm_data['outfalls']
        if not outfalls_gdf.empty:
            shp_path = save_geodataframe(outfalls_gdf, output_path, 'outfalls', outfalls_gdf.crs.to_epsg(), output_format, logger)
            shapefile_paths.append(shp_path)
        else:
            logger.warning("No outfalls data to save.")

    if 'conduits' in swmm_data:
        conduits_gdf = swmm_data['conduits']
        if not conduits_gdf.empty:
            shp_path = save_geodataframe(conduits_gdf, output_path, 'conduits', conduits_gdf.crs.to_epsg(), output_format, logger)
            shapefile_paths.append(shp_path)
        else:
            logger.warning("No conduits data to save.")

    return shapefile_paths

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 4:
        print("Usage: python swmm_extraction.py <path_to_swmm_file> <epsg_code> <output_format>")
        sys.exit(1)

    file_path = sys.argv[1]
    epsg = int(sys.argv[2])
    output_format = sys.argv[3].capitalize()

    # Validate output_format
    if output_format not in ["Shapefile", "Geopackage"]:
        print("Output format must be 'Shapefile' or 'GeoPackage'.")
        sys.exit(1)

    swmm_data = extract_swmm_data(file_path, epsg)

    output_path = os.path.dirname(file_path)
    shapefile_paths = create_swmm_shapefiles(swmm_data, output_path, output_format)

    print("Created files:", shapefile_paths)
