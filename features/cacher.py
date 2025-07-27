import os
import hashlib
import pickle
import sys
import traceback
from functools import wraps
from datetime import datetime

cache_dir = '/Users/macuser/Documents/Cache/cryptobot_v2'

class FileDict:
    def __init__(self, path):
        self.path = path
        os.makedirs(path, exist_ok=True)
    
    def _get_file_path(self, func_key, arg_hash):
        filename = f"{func_key}_{arg_hash}.pkl"
        return os.path.join(self.path, filename)
    
    def get(self, func_key, arg_hash):
        file_path = self._get_file_path(func_key, arg_hash)
        if not os.path.exists(file_path):
            raise KeyError(f"No cache found for key: {func_key} with hash: {arg_hash}")
        with open(file_path, 'rb') as file:
            return pickle.load(file)
    
    def set(self, func_key, arg_hash, value):
        file_path = self._get_file_path(func_key, arg_hash)
        with open(file_path, 'wb') as file:
            pickle.dump(value, file)
    
    def contains(self, func_key, arg_hash):
        return os.path.exists(self._get_file_path(func_key, arg_hash))
    
    def get_file_path(self, func_key, arg_hash):
        return self._get_file_path(func_key, arg_hash)

def load_or_save_pickle(subdir=None, check_valid=lambda x: True, verbose=0):
    """
    Decorator to cache function output to a pickle file.
    
    Args:
        subdir (str): Subdirectory under cache_dir.
        check_valid (callable): Function to validate cached value. Return False to re-run function.
        verbose (int): Verbosity level.
    """
    file_cache = FileDict(os.path.join(cache_dir, subdir))

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                # Create a hashable key
                func_key = f"{func.__module__}.{func.__name__}"
                arg_key = f"{args}:{kwargs}"
                arg_hash = hashlib.md5(arg_key.encode()).hexdigest()

                if file_cache.contains(func_key, arg_hash):
                    file_path = file_cache.get_file_path(func_key, arg_hash)
                    result = file_cache.get(func_key, arg_hash)
                    if check_valid(result):
                        if verbose:
                            print(f"[CACHE READ] {func_key}{arg_key}\n → from: {file_path}")
                        return result
                    elif verbose:
                        print(f"[CACHE INVALID] {file_path} → rerunning function...")

                # Run the actual function
                result = func(*args, **kwargs)
                file_cache.set(func_key, arg_hash, result)
                if verbose:
                    print(f"[CACHE WRITE] {func_key}{arg_key}")
                return result

            except Exception:
                traceback.print_exc()
                sys.exit(1)

        return wrapper

    return decorator
