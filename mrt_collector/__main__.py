import argparse
from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path

from .analyzers import MHExportAnalyzer
from .mrt_collector import MRTCollector


def main():
    print("HERE")

    parser = argparse.ArgumentParser(prog='MRT Collector')
    #for use with running with limited files
    parser.add_argument('-lf', '--limit_files', type=int, help='Number of files to process; Leave blank for all')
    parser.add_argument('-sp', '--single_process', action='store_true', help='Limits to single processining')
    args = parser.parse_args()
    limit_files_to = 0 if args.limit_files is None else args.limit_files
    # dl_time=datetime(2025, 3, 20, 0, 0, 0)
    dl_time = datetime.now() #figure we probably want now in this context?
    collector = MRTCollector(
        dl_time=dl_time,
        cpus= 1 if args.single_process else cpu_count(),
        base_dir=Path.home() / "mrt_data" / dl_time.strftime("%Y_%m_%d"),
        limit_files_to=limit_files_to
    )

    mrt_files = collector.run()

    for mrt_file in mrt_files: print(mrt_file.download_succeeded) 

    # for now, I'm going to avoid trying to even run the multihome analyzer
    # there appear to be multiple program breaking bugs, such as in create_graphs (no definition
    # for f, I'm assuming thats supposed to be the file path of the json we dump to) and in mh.run()
    # I'll focus on implementing the file size validation stuff, and compile a list of locations within
    # the program that appear to have critical errors

    # mrt_files = list(sorted(collector.run()))
    # mrt_files = [mrt_files[0]]
    # MHExportAnalyzer().run(mrt_files)
    # MHExportAnalyzer().create_graphs()

if __name__ == "__main__":
    main()
