import json
from collections import defaultdict
from pathlib import Path
from dataclasses import asdict, dataclass

from .export_analyzer import ExportAnalyzer

from bgpy.as_graphs import CAIDAASGraphConstructor

@dataclass
class PathData:
    path: str
    path_topo_aligns_with_bgpy: bool
    sent_to_providers: bool | None

class SlashzeroPrefixAnalyzer(ExportAnalyzer):
    def __init__(
        self, 
        base_dir: Path
    )->None:
        
        super().__init__(base_dir)
        self.desc = "Extracting /0 prefix data"
        self.prefix_data = defaultdict(dict)
        self.bgp_dag = CAIDAASGraphConstructor().run()

    def analyze(
        self, 
        row: dict[str, ...]
    ) -> None:
        """Collects /0 prefix data from mrt file"""

        prefix = row["prefix"]
        if "/0" not in prefix:
            return
        
        origin = int(row["origin_asns"])
        data = self.get_pathdata_from_path(row["as_path"])

        source = self.current_source
        self.prefix_data[origin][source] = data

    def get_pathdata_from_path(
        self,
        path: str
    ) -> PathData:
        """Removes any prepending from given AS path"""

        split_asns = list(map(int, path.split()))
        # lazy method of removing prepending; dicts in python preserve insertion order
        path_no_prepending = list(dict.fromkeys(split_asns))



        path_no_prepending = " ".join(map(str, path_no_prepending))

        return PathData(path_no_prepending, False, False)

    def dump_json(
        self
    ) -> None:
        """JSON dump for /0 prefix data"""
        self.json_prefix_data_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.json_prefix_data_path, "w") as f:
            json.dump(self.prefix_data, f, indent=4)
    
    @property
    def json_prefix_data_path(self) -> Path:
        return self.base_dir / "analysis" / "slashzero_prefix_data.json"