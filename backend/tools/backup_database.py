"""Create a consistent, timestamped DuckDB backup and apply retention."""
import argparse
from services.runtime_service import backup_database

parser = argparse.ArgumentParser()
parser.add_argument("--target-dir", default="")
args = parser.parse_args()
print(backup_database(args.target_dir))
