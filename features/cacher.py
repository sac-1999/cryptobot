import os
import hashlib
import pickle
import traceback
from functools import wraps
from datetime import datetime
from contextlib import redirect_stdout, redirect_stderr
import io
import pandas as pd  # Used for DataFrame empty check

# Base cache path — change if needed
CACHEPATH = '/home/debjitparia/Cache/cryptobot'


def _hash_args(func_key, args, kwargs):
    """Generate a stable hash key for the function call including args and kwargs."""
    try:
        # Sort kwargs for stability
        sorted_kwargs = tuple(sorted(kwargs.items()))
        
        # Try pickling for compactness
        try:
            raw = pickle.dumps((func_key, args, sorted_kwargs))
        except Exception:
            # Fall back to string repr if pickling fails
            raw = repr((func_key, args, sorted_kwargs)).encode("utf-8", errors="backslashreplace")
        
        # Hash the serialized form
        hash_str = hashlib.md5(raw).hexdigest()
        
        # Optional: include short readable prefix for debugging
        prefix = func_key.split(".")[-1][:8]  # Short function name prefix
        return f"{prefix}_{hash_str}"
    
    except Exception as e:
        raise ValueError(f"Cannot hash arguments for {func_key}: {e}")


def _make_cache_dir(base_dir, hash_key):
    """Ensure cache directory exists."""
    full_path = os.path.join(base_dir, hash_key)
    os.makedirs(full_path, exist_ok=True)
    return full_path


def _write_file(path, content):
    """
    Safe file writer that tolerates unencodable characters.
    Tries multiple encoding strategies before giving up.
    """
    if content is None:
        content = ""

    # If bytes, write directly
    if isinstance(content, (bytes, bytearray)):
        try:
            with open(path, "wb") as f:
                f.write(content)
            return
        except Exception:
            content = content.decode("utf-8", errors="backslashreplace")

    # Ensure string
    if not isinstance(content, str):
        try:
            content = str(content)
        except Exception:
            content = repr(content)

    # Try strict UTF-8 first
    try:
        with open(path, "w", encoding="utf-8", errors="strict") as f:
            f.write(content)
        return
    except (UnicodeEncodeError, OSError):
        pass

    # Try UTF-8 replace
    try:
        with open(path, "w", encoding="utf-8", errors="replace") as f:
            f.write(content)
        return
    except Exception:
        pass

    # Try surrogatepass (keeps weird surrogates intact in UTF-8)
    try:
        b = content.encode("utf-8", errors="surrogatepass")
        with open(path, "wb") as f:
            f.write(b)
        return
    except Exception:
        pass

    # Try backslashreplace (escapes bad chars)
    try:
        b = content.encode("utf-8", errors="backslashreplace")
        with open(path, "wb") as f:
            f.write(b)
        return
    except Exception:
        pass

    # Final fallback: repr()
    try:
        with open(path, "w", encoding="utf-8", errors="replace") as f:
            f.write(repr(content))
    except Exception as ex:
        import sys
        print(f"[Cache Write Error] Could not write file {path}: {ex}", file=sys.stderr)


def _write_pickle(path, obj):
    """Write an object to pickle file."""
    with open(path, "wb") as f:
        pickle.dump(obj, f)


def _read_pickle(path):
    """Read an object from pickle file."""
    with open(path, "rb") as f:
        return pickle.load(f)


def _is_empty_df(obj):
    """Check if the object is an empty DataFrame."""
    return isinstance(obj, pd.DataFrame) and obj.empty


def persistent_cache(subdir=None, non_empty=False):
    """
    Decorator to cache function output persistently with logs.
    Creates a unique folder per function call based on its arguments.

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
            cache_dir = _make_cache_dir(base_path, hash_key)

            output_file = os.path.join(cache_dir, "output.pkl")
            stdout_file = os.path.join(cache_dir, "stdout.txt")
            stderr_file = os.path.join(cache_dir, "stderr.txt")
            success_file = os.path.join(cache_dir, "success.txt")

            # Check existing cache
            if os.path.exists(success_file):
                try:
                    result = _read_pickle(output_file)
                    if not non_empty or not _is_empty_df(result):
                        return result
                    else:
                        print(f"[Cache Invalidated] Empty DataFrame at {hash_key}")
                except Exception as e:
                    print(f"[Cache Error] Failed to read cached output: {e}")

            # Prepare buffers
            stdout_buffer = io.StringIO()
            stderr_buffer = io.StringIO()

            try:
                with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
                    result = func(*args, **kwargs)

                # Write outputs safely
                try:
                    _write_pickle(output_file, result)
                except Exception as e:
                    print(f"[Cache Warning] Failed to write pickle: {e}")

                for file_path, content in [
                    (stdout_file, stdout_buffer.getvalue()),
                    (stderr_file, stderr_buffer.getvalue() + "\nExitCode: 0"),
                    (success_file, f"Success at {datetime.now()}\nExitCode: 0")
                ]:
                    try:
                        _write_file(file_path, content)
                    except Exception as e:
                        print(f"[Cache Warning] Failed to write {file_path}: {e}")

                return result

            except Exception as e:
                tb = traceback.format_exc()
                stderr_output = stderr_buffer.getvalue() + f"\nException:\n{tb}\nExitCode: 1"

                for file_path, content in [
                    (stderr_file, stderr_output),
                    (stdout_file, stdout_buffer.getvalue())
                ]:
                    try:
                        _write_file(file_path, content)
                    except Exception as ee:
                        print(f"[Cache Warning] Failed to write {file_path}: {ee}")

                error_msg = (
                    f"Error in {func_key} (hash {hash_key}): {e}\n"
                    f"See stderr log at: {stderr_file}"
                )
                raise RuntimeError(error_msg)

        return wrapper

    return decorator