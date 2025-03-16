from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path

from .mrt_collector import MRTCollector


def main():
    dl_time=datetime(2025, 3, 1, 0, 0, 0)
    collector = MRTCollector(
        dl_time=dl_time,
        cpus=cpu_count(),
        base_dir=Path.home() / "mrt_data" / dl_time.strftime("%Y_%m_%d")
    )
    collector.run()

if __name__ == "__main__":
    main()
