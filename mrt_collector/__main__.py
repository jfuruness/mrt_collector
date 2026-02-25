import argparse
from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path

from .mrt_collector import MRTCollector


def main():
    print("HERE")

    parser = argparse.ArgumentParser(prog='MRT Collector')
    #for use with running with limited files
    parser.add_argument(
        '-lf',
        '--limit_files',
        type=int,
        help='Number of files to process; Leave blank for all'
    )

    parser.add_argument(
        '-sp',
        '--single_process',
        action='store_true',
        help='Limits to single processining'
    )

    args = parser.parse_args()
    limit_files_to = 0 if args.limit_files is None else args.limit_files
    dl_time=datetime(2025, 3, 20, 0, 0, 0)
    # dl_time = datetime.now() #figure we probably want now in this context?
    collector = MRTCollector(
        dl_time=dl_time,
        cpus= 1 if args.single_process else cpu_count(),
        base_dir=Path.home() / "mrt_data" / dl_time.strftime("%Y_%m_%d"),
    )

    mrt_files = collector.run(limit_files_to = limit_files_to)
    print("total files created, awaiting download: " + str(len(mrt_files)))
    for mrt_file in mrt_files:
        print(str(mrt_file) + "\n----")
    # for mrt_file in mrt_files:
    #     print(mrt_file.download_succeeded)

    # mrt_files = list(sorted(collector.run()))
    # mrt_files = [mrt_files[0]]
    # MHExportAnalyzer().run(mrt_files)
    # MHExportAnalyzer().create_graphs()

if __name__ == "__main__":
    main()
