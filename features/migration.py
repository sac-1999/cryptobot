import os
import pickle
import gzip
from datetime import datetime

CACHEPATH = "/home/debjitparia/Cache/cryptobot_fut"

def revert_cache(base_path=CACHEPATH, dry_run=True, delete_after=False):
    """
    Revert compact .pkl.gz cache files back into multi-file cache dirs.
    
    - dry_run=True: Only print what would happen, make no changes.
    - delete_after=True: Delete the .pkl.gz file after successful revert.
    """
    reverted, skipped, failed = 0, 0, 0

    for root, dirs, files in os.walk(base_path):
        for f in files:
            if f.endswith(".pkl.gz"):
                gz_path = os.path.join(root, f)
                dir_path = gz_path.replace(".pkl.gz", "")

                if dry_run:
                    print(f"[DRY-RUN] Would revert: {gz_path} -> {dir_path}/")
                    skipped += 1
                    continue

                try:
                    # Load compressed data
                    with gzip.open(gz_path, "rb") as gzf:
                        data = pickle.load(gzf)

                    os.makedirs(dir_path, exist_ok=True)

                    # Restore output.pkl
                    outpath = os.path.join(dir_path, "output.pkl")
                    with open(outpath, "wb") as f_out:
                        pickle.dump(data["result"], f_out, protocol=pickle.HIGHEST_PROTOCOL)

                    # Restore stdout/stderr
                    if data.get("stdout"):
                        with open(os.path.join(dir_path, "stdout.txt"), "w", errors="ignore") as f_out:
                            f_out.write(data["stdout"])
                    if data.get("stderr"):
                        with open(os.path.join(dir_path, "stderr.txt"), "w", errors="ignore") as f_out:
                            f_out.write(data["stderr"])

                    # Restore success flag
                    if data.get("success", False):
                        open(os.path.join(dir_path, "success.txt"), "w").close()

                    # Restore original timestamp (best-effort)
                    if "timestamp" in data:
                        ts = data["timestamp"].timestamp()
                        os.utime(outpath, (ts, ts))

                    print(f"[OK] Reverted: {gz_path} -> {dir_path}/")
                    reverted += 1

                    # Delete gz file if requested
                    if delete_after:
                        os.remove(gz_path)
                        print(f"    [CLEANUP] Deleted {gz_path}")

                except Exception as e:
                    print(f"[FAIL] Could not revert {gz_path}: {e}")
                    failed += 1

    print(f"🔄 Revert complete: {reverted} reverted, {skipped} skipped (dry-run), {failed} failed.")


if __name__ == "__main__":
    # First run in dry-run mode
    revert_cache(dry_run=True, delete_after=False)

    # When confident, run with dry_run=False, delete_after=True
    # revert_cache(dry_run=False, delete_after=True)