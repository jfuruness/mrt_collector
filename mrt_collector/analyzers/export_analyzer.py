import csv
from pathlib import Path

from ABC import ABC, abstractmethod
from tqdm import tqdm

from mrt_collector.mrt_collector import sort_mrt_files_by_parsed_file_size
from mrt_collector.mrt_file import MRTFile


class ExportAnalyzer(ABC):
    def __init__(
        self,
        base_dir: Path
    ) -> None:

        self.base_dir = base_dir
        self.desc = "Analyzing some data"

    def run(
        self,
        mrt_files: tuple[MRTFile, ...]
    ) -> None:
        """Lifecycle of the export analyzer"""

        mrt_files = sort_mrt_files_by_parsed_file_size(mrt_files)
        self.get_data(mrt_files)
        self.dump_json()

    def get_data(
        self,
        mrt_files: tuple[MRTFile, ...]
    ) -> None:
        """Iterates through each parsed mrt file for performing analysis"""
        total_lines = sum(x.total_parsed_lines for x in mrt_files)

        with tqdm(
            total=total_lines,
            desc = self.desc
        ) as pbar:
            for mrt_file in mrt_files:
                if mrt_file.parsed_path_psv.exists():
                    with mrt_file.parsed_path_psv.open() as f:
                        reader = csv.DictReader(f, delimiter="|")
                        for row in reader:
                            pbar.update()
                            if row["type"] == "A":
                                continue
                            self.analyze(row)
    @abstractmethod
    def analyze(
        self,
        row: dict[str, ...]
    ) -> None:
        pass

    @abstractmethod
    def dump_json(
        self
    ) -> None:
        pass

