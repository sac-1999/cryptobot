import os
import hashlib
import pickle
import traceback
from functools import wraps
from datetime import datetime
from contextlib import redirect_stdout, redirect_stderr
import io
import pandas as pd  # Used for DataFrame empty check

# Base cache path
CACHEPATH = '/home/debjitparia/Cache/cryptobot_fut_check_v2'


def _hash_args(func_key, args, kwargs):
    """Generate a stable hash key for the function call including args and kwargs."""
    try:
        sorted_kwargs = tuple(sorted(kwargs.items()))
        try:
            raw = pickle.dumps((func_key, args, sorted_kwargs))
        except Exception:
            raw = repr((func_key, args, sorted_kwargs)).encode("utf-8", errors="backslashreplace")
        return hashlib.md5(raw).hexdigest()
    except Exception as e:
        raise ValueError(f"Cannot hash arguments for {func_key}: {e}")


def _make_cache_dir(base_dir, folder_name):
    full_path = os.path.join(base_dir, folder_name)
    os.makedirs(full_path, exist_ok=True)
    return full_path


def _write_cache_file(path, result, stdout, stderr, success=True, timestamp=None):
    """Write all cache data to a single pickle file."""
    if timestamp is None:
        timestamp = datetime.now()
    
    payload = {
        "result": result,
        "stdout": stdout,
        "stderr": stderr,
        "success": success,
        "timestamp": timestamp,
        "exit_code": 0 if success else 1
    }
    
    with open(path, "wb") as f:
        pickle.dump(payload, f)


def _read_cache_file(path):
    """Read cache data from a single pickle file."""
    with open(path, "rb") as f:
        payload = pickle.load(f)
    
    # Handle legacy format or missing fields
    result = payload.get("result")
    stdout = payload.get("stdout", "")
    stderr = payload.get("stderr", "")
    success = payload.get("success", True)
    
    return result, stdout, stderr, success


def _is_empty_df(obj):
    return isinstance(obj, pd.DataFrame) and obj.empty


def _preview_arg(a):
    """Safe preview of an argument for folder name (shortened, safe chars)."""
    if isinstance(a, datetime):
        return a.strftime("%Y%m%d-%H%M%S")
    s = str(a)
    # Replace unsafe chars for folder names
    s = s.replace("/", "-").replace(" ", "_").replace(":", "-")
    return s[:20]  # truncate to avoid very long names

def persistent_cache(subdir=None, non_empty=False):
    """
    Decorator to cache function output persistently with logs.
    Creates a unique folder per function call based on its arguments.
    Now uses a single file per cache entry to reduce inode usage.

    Args:
        subdir (str): Subfolder under base cache path.
        non_empty (bool): If True, treats empty DataFrames as invalid cache.
    """
    base_path = os.path.join(CACHEPATH, subdir or "default")
    os.makedirs(base_path, exist_ok=True)

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            func_key = f"{func.__module__}.{func.__name__}"
            hash_key = _hash_args(func_key, args, kwargs)

            # Human-readable prefix (handle datetime + kwargs safely)
            func_name = func.__name__

            arg_previews = []
            for a in args[:2]:
                arg_previews.append(_preview_arg(a))

            for k, v in list(kwargs.items())[:2]:  # take first 2 kwargs for readability
                arg_previews.append(f"{_preview_arg(v)}")

            arg_preview = "_".join(arg_previews)
            arg_preview = arg_preview.replace("/", "-").replace(" ", "")[:50]

            folder_name = f"{func_name}_{arg_preview}_{hash_key}"
            cache_dir = _make_cache_dir(base_path, folder_name)

            # Single cache file instead of multiple files
            cache_file = os.path.join(cache_dir, "cache.pkl")

            # Check existing cache
            if os.path.exists(cache_file):
                try:
                    result, cached_stdout, cached_stderr, success = _read_cache_file(cache_file)
                    
                    if success and (not non_empty or not _is_empty_df(result)):
                        # Optionally print cached stdout/stderr if you want to see them again
                        # print(cached_stdout, end='')
                        # print(cached_stderr, end='', file=sys.stderr)
                        return result
                    else:
                        if not success:
                            print(f"[Cache Invalidated] Previous execution failed at {folder_name}")
                        elif _is_empty_df(result):
                            print(f"[Cache Invalidated] Empty DataFrame at {folder_name}")
                except Exception as e:
                    print(f"[Cache Error] Failed to read cached output: {e}")

            stdout_buffer = io.StringIO()
            stderr_buffer = io.StringIO()

            try:
                with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
                    result = func(*args, **kwargs)

                stdout_content = stdout_buffer.getvalue()
                stderr_content = stderr_buffer.getvalue()

                try:
                    _write_cache_file(cache_file, result, stdout_content, stderr_content, success=True)
                except Exception as e:
                    print(f"[Cache Warning] Failed to write cache file: {e}")

                return result

            except Exception as e:
                tb = traceback.format_exc()
                stdout_content = stdout_buffer.getvalue()
                stderr_content = stderr_buffer.getvalue() + f"\nException:\n{tb}"

                try:
                    _write_cache_file(cache_file, None, stdout_content, stderr_content, success=False)
                except Exception as ee:
                    print(f"[Cache Warning] Failed to write error cache: {ee}")

                error_msg = (
                    f"Error in {func_key} (hash {hash_key}): {e}\n"
                    f"See cached error at: {cache_file}"
                )
                raise RuntimeError(error_msg)

        return wrapper

    return decorator