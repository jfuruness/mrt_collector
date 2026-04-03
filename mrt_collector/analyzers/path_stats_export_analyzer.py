import json
from pathlib import Path

from .export_analyzer import ExportAnalyzer
from .json_set_encoder import JSONSetEncoder

import bisect

class PathStatsExportAnalyzer(ExportAnalyzer):
    def __init__(
        self,
        base_dir: Path
    ) -> None:

        super().__init__(base_dir)
        self.desc = "Extracting Mean, Median, Max and Min of all AS Path Lengths"
        self.as_path_lengths = []
        self.max = 0
        self.min = 0

    def analyze(
        self,
        row: dict[str, ...]
    ) -> None:
        """Collects full set of bgp communities"""

        try:
            as_path = [int(x) for x in row["as_path"].split()]
        except ValueError:
            # print("Encountered AS set")
            return
        
        as_path_length = len(as_path)
        if as_path_length == 0:
            return

        if as_path_length > self.max or self.max == 0:
            self.max = as_path_length
        if as_path_length < self.min or self.min == 0:
            self.min = as_path_length
        bisect.insert(self.as_path_lengths, as_path_length)

    def dump_json(self) -> None:
        sum_ = sum(self.as_path_lengths)
        len_ = len(self.as_path_lengths)
        mean = float(sum_) / len_
        median = self.as_path_lengths[len_/2]
        filepath = self.json_path
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w") as f:
            json.dump(self.otc_data, f, indent=4, cls=JSONSetEncoder)

    @property
    def json_path(self) -> Path:
        return self.base_dir / "analysis" / "path_stats.json"
