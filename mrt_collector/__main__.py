import argparse

from .mrt_collector import MRTCollector


def main():
    parser = argparse.ArgumentParser(description="Run the MRT Collector")
    parser.add_argument("--quick", action="store_true", help="Enable quick mode")
    args = parser.parse_args()

    if args.quick:
        # For the quick version, just run it with a single MRT file
        collector = MRTCollector()
        mrt_files = collector.get_mrt_files()
        # This one has a fast download time
        mrt_files = tuple([mrt_files[x] for x in (-2, -3, -4, -5)])
        # print(mrt_files[0].url)
        collector.run(mrt_files=mrt_files)
    else:
        MRTCollector().run()


if __name__ == "__main__":
    main()
