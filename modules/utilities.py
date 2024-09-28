# utilities.py

from functools import wraps
import time
import os

# Decorator for timing functions
def time_function(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Finished {func.__name__!r} in {end_time - start_time:.3f} seconds")
        return result
    return wrapper

# Create folders for shapefile, raster and spreadsheet outputs
def create_required_folders(folders):
    for folder in folders:
        os.makedirs(folder, exist_ok=True)
