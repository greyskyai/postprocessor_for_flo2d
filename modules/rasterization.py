import numpy as np
import rasterio
from rasterio.transform import from_origin
from modules.utilities import time_function

@time_function
def create_raster_from_gdf(geo_df, column, raster_file, cell_size, logger):
    logger.info(f"Creating raster for column: {column}")
    logger.info(f"Column data type: {geo_df[column].dtype}")
    logger.info(f"First few values of the column: {geo_df[column].head()}")

    try:
        x_coords = geo_df.geometry.x.to_numpy()
        y_coords = geo_df.geometry.y.to_numpy()
        values = geo_df[column].to_numpy()

        xmin, ymin, xmax, ymax = geo_df.total_bounds
        xmin -= cell_size / 2
        xmax += cell_size / 2
        ymin -= cell_size / 2
        ymax += cell_size / 2
        nrows = int(np.ceil((ymax - ymin) / cell_size))
        ncols = int(np.ceil((xmax - xmin) / cell_size))

        transform = from_origin(xmin, ymax, cell_size, cell_size)
        raster = np.full((nrows, ncols), np.nan)

        col_idx = ((x_coords - xmin - cell_size / 2) / cell_size).astype(int)
        row_idx = ((ymax - y_coords - cell_size / 2) / cell_size).astype(int)

        valid_mask = (row_idx >= 0) & (row_idx < nrows) & (col_idx >= 0) & (col_idx < ncols)
        raster[row_idx[valid_mask], col_idx[valid_mask]] = values[valid_mask]

        with rasterio.open(
            raster_file, 'w',
            driver='GTiff',
            height=raster.shape[0],
            width=raster.shape[1],
            count=1,
            dtype=raster.dtype,
            crs=geo_df.crs,
            transform=transform,
        ) as dst:
            dst.write(raster, 1)
        return raster_file
    except Exception as e:
        logger.error(f"Error creating raster for column {column}: {str(e)}")
        raise