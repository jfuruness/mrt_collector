import json
from pathlib import Path

from .export_analyzer import ExportAnalyzer
from .json_set_encoder import JSONSetEncoder


class CommunitiesExportAnalyzer(ExportAnalyzer):
    def __init__(
        self,
        base_dir: Path
    ) -> None:

        super().__init__(base_dir)
        self.desc = "Extracting set of all BGP communities"
        self.comm_data = set()

    def analyze(
        self,
        row: dict[str, ...]
    ) -> None:
        """Collects full set of bgp communities"""

        communities = row["communities"]
        if communities == "":
            return

        self.comm_data.update(communities)

    def dump_json(self) -> None:
        filepath = self.json_path
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w") as f:
            json.dump(self.comm_data, f, indent=4, cls=JSONSetEncoder)

    @property
    def json_path(self) -> Path:
        return self.base_dir / "analysis" / "communities.json"
