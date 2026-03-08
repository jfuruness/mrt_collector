import argparse
from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path

from .time_goodizer import latest_avail_dump_time
from .mrt_collector import MRTCollector


def main():
    parser = argparse.ArgumentParser(prog="MRT Collector")
    # for use with running with limited files
    parser.add_argument(
        "-lf",
        "--limit_files",
        type=int,
        help="Number of files to process; Leave blank for all",
    )

    parser.add_argument(
        "-sp",
        "--single_process",
        action="store_true",
        help="Limits to single processining",
    )

    args = parser.parse_args()
    limit_files_to = 0 if args.limit_files is None else args.limit_files

    dl_time = latest_avail_dump_time()

    # dl_time=datetime(2025, 3, 20, 0, 0, 0)
    # dl_time = datetime(2026, 2, 26, 0, 0, 0)
    output_path = Path.home() / "mrt_data" / dl_time.strftime("%Y_%m_%d")

    # I (Satchel) use this for testing on my machine
    output_path = (
        Path("/Volumes/Crucial X8/") / "mrt_data" / dl_time.strftime("%Y_%m_%d")
    )
    collector = MRTCollector(
        dl_time=dl_time,
        cpus=1 if args.single_process else cpu_count(),
        base_dir=output_path,
    )

    mrt_files = collector.run(limit_files_to=limit_files_to)  # noqa

# MHExportAnalyzer().run(mrt_files)
# MHExportAnalyzer().create_graphs()

if __name__ == "__main__":
    main()
