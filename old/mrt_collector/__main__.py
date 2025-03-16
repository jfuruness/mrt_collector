import argparse

from datetime import datetime

from .mrt_collector import MRTCollector

dl_time = datetime(2023, 12, 12, 0, 0, 0)

def main():
    parser = argparse.ArgumentParser(description="Run the MRT Collector")
    parser.add_argument("--quick", action="store_true", help="Enable quick mode")
    args = parser.parse_args()

    if args.quick:
        # For the quick version, just run it with a single MRT file
        collector = MRTCollector(dl_time=dl_time)
        mrt_files = collector.get_mrt_files()
        # This one has a fast download time
        mrt_files = tuple([mrt_files[x] for x in (-2, -3, -4, -5)])
        # print(mrt_files[0].url)
        collector.run(mrt_files=mrt_files)
    else:
        MRTCollector(dl_time=dl_time).run()


if __name__ == "__main__":
    main()
