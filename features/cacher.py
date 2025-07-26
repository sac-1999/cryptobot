import os
import pandas as pd
from functools import wraps
import hashlib
import pickle
import datetime

cache_dir = '/Users/macuser/Documents/Cache/cryptobot'

class FileDict:
    def __init__(self, path):
        self.path = path
        os.makedirs(path, exist_ok=True)  # Create the directory if it doesn't exist
    
    def _get_file_path(self, func_key, arg_hash):
        """Generate the file path for a given function key and argument hash."""
        filename = f"{func_key}_{arg_hash}.pkl"
        return os.path.join(self.path, filename)
    
    def get(self, func_key, arg_hash):
        """Retrieve the value from the cache if it exists, otherwise raise KeyError."""
        file_path = self._get_file_path(func_key, arg_hash)
        if not os.path.exists(file_path):
            raise KeyError(f"No cache found for key: {func_key} with hash: {arg_hash}")
        with open(file_path, 'rb') as file:
            return pickle.load(file)
    
    def set(self, func_key, arg_hash, value):
        """Store the value in the cache."""
        file_path = self._get_file_path(func_key, arg_hash)
        with open(file_path, 'wb') as file:
            pickle.dump(value, file)
    
    def contains(self, func_key, arg_hash):
        """Check if a cached value exists for the given function key and argument hash."""
        file_path = self._get_file_path(func_key, arg_hash)
        return os.path.exists(file_path)

def load_or_save_dataframe(subdir=None, save_type='pkl', non_empty=False):
    file_cache = FileDict(os.path.join(cache_dir, subdir))

    def decorator(func):
        @wraps(func)
        def wrapper(symbol, date_or_datetime, *args, **kwargs):
            # print(symbol, date_or_datetime, *args, **kwargs)
            try:
                # Normalize the date
                date = pd.to_datetime(date_or_datetime)
                if date == date.normalize():
                    date = date.date()

                # Create the function key based on its module and name
                func_key = f"{func.__module__}.{func.__name__}"

                # Create a unique hash based on args and kwargs
                arg_key = f"{symbol}:{date}:{args}:{kwargs}"
                arg_hash = f"{symbol}:{date}" + hashlib.md5(arg_key.encode()).hexdigest()

                # Check if the DataFrame is in the file cache
                if file_cache.contains(func_key, arg_hash):
                    existing_df = file_cache.get(func_key, arg_hash)
                    # Check if the DataFrame is empty and if non_empty flag is set
                    if not non_empty or (non_empty and not existing_df.empty):
                        print(f"Reading cache for {func.__module__}.{func.__name__}{arg_key}")
                        return existing_df
                    else:
                        print(f"Cached DataFrame is empty, rerunning function...")

                # If not, call the function and store the result in the file cache
                df = func(symbol, date_or_datetime, *args, **kwargs)
                file_cache.set(func_key, arg_hash, df)
                print(f"Cached.")
                return df

            except Exception as e:
                raise ValueError(f"Error occurred: {e}")

        return wrapper

    return decorator