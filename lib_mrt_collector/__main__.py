from datetime import datetime

from pathlib import Path

from .mrt_collector import MRTCollector

def main():
    mrt_path = Path("/tmp/mrt_dev/")
    MRTCollector(dir_=mrt_path).timed_run()
