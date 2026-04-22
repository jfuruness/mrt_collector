import json
from collections import defaultdict
from pathlib import Path

from .export_analyzer import ExportAnalyzer

from .json_set_encoder import JSONSetEncoder

class SlashzeroPrefixAnalyzer(ExportAnalyzer):
    def __init__(
        self, 
        base_dir: Path
    )->None:
        
        super().__init__(base_dir)
        self.desc = "Extracting /0 prefix data"
        self.prefix_data = defaultdict(set)

    def analyze(
        self, 
        row: dict[str, ...]
    ) -> None:
        """Collects /0 prefix data from mrt file"""

        prefix = row["prefix"]
        if "/0" not in prefix:
            return
        
        origin = row["origin"]
        path = self.strip_prepending(row["as_path"])

        entry = (self.current_source, path)
        self.prefix_data[origin].add(entry)


    def strip_prepending(
        self,
        path: str
    ) -> str:
        """Removes any prepending from given AS path"""

        temp = set(path.split())
        return " ".join(temp)
    
    def dump_json(
        self
    ) -> None:
        """JSON dump for atomic aggregate data"""
        self.json_prefix_data_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.json_prefix_data_path, "w") as f:
            json.dump(self.prefix_data, f, indent=4, cls=JSONSetEncoder)
    
    @property
    def json_prefix_data_path(self) -> Path:
        return self.base_dir / "analysis" / "slashzero_prefix_data.json"