import dask.dataframe as dd
import geopandas as gpd
import dask_geopandas as dgpd

def convert_gdf_to_shapefile(geo_df, output_path, coord_system):
    """
    Convert a GeoDataFrame to a shapefile with only grid_id and flow_direction fields.
    :param geo_df: GeoDataFrame to be converted.
    :param output_path: Output file path for the shapefile.
    :param coord_system: Coordinate system EPSG code.
    """
    # Ensure the initial GeoDataFrame has a CRS
    if geo_df.crs is None:
        geo_df.set_crs(epsg=coord_system, inplace=True)
    elif geo_df.crs.to_epsg() != coord_system:
        geo_df.to_crs(epsg=coord_system, inplace=True)

    # Select only grid_id and flow_direction columns
    geo_df = geo_df[['grid_id', 'flow_direction', 'geometry']]

    # Convert to Dask GeoDataFrame for parallel processing
    npartitions = 10  # Adjust the number of partitions based on your data size and available memory
    dask_geo_df = dgpd.from_geopandas(geo_df, npartitions=npartitions)

    # Compute the Dask GeoDataFrame to get a GeoDataFrame
    computed_df = dask_geo_df.compute()

    # Write to file
    computed_df.to_file(output_path, driver='ESRI Shapefile')

    return computed_df
