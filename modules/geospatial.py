import geopandas as gpd
from shapely.geometry import Point
from modules.utilities import time_function

@time_function
def convertToGeoDataFrame(df):
    geometry = [Point(xy) for xy in zip(df.x, df.y)]
    geo_df = gpd.GeoDataFrame(df, geometry=geometry)
    return geo_df

@time_function
def calculate_cell_size(geo_df):
    if len(geo_df) < 2:
        raise ValueError("The GeoDataFrame should contain at least two points.")
    point1 = geo_df.geometry.iloc[0]
    point2 = geo_df.geometry.iloc[1]
    cell_size = point1.distance(point2)
    return cell_size
