from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path

from .analyzers import MHExportAnalyzer
from .mrt_collector import MRTCollector


def main():
    dl_time=datetime(2025, 3, 1, 0, 0, 0)
    collector = MRTCollector(
        dl_time=dl_time,
        cpus=cpu_count(),
        base_dir=Path.home() / "mrt_data" / dl_time.strftime("%Y_%m_%d")
    )
    mrt_files = list(sorted(collector.run()))
    # mrt_files = [mrt_files[0]]
    MHExportAnalyzer().run(mrt_files)

if __name__ == "__main__":
    main()
