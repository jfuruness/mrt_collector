import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

from .export_analyzer import ExportAnalyzer

# prefix atomic data will be formatted as:
# defaultdict<prefix: str, set{data: AtomicData}>

@dataclass(frozen=True)
class AtomicData:
    atomic: bool
    aggr_asn: int

class AtomicExportAnalyzer(ExportAnalyzer):
    def __init__(
        self,
        base_dir: Path
    ) -> None:

        super().__init__(base_dir)
        self.desc = "Extracting atomic aggregate data"
        self.atomic_data = defaultdict(set)
        self.atomic_prefixes = set()
        self.aggr_asn_prefixes = set()

    def analyze(
        self,
        row: dict[str, ...]
    ) -> None:
        """Collects atomic data from an mrt file"""

        atomic = row["atomic"] == "true"
        aggr_asn = row["aggr_asn"] or "None"
        # skip rows without atomic and without aggregate data
        # some rows can have atomic=false but still have data
        if not atomic and aggr_asn == "None":
            return

        prefix = row["prefix"]
        if atomic:
            self.atomic_prefixes.add(prefix)

        if aggr_asn != "None":
            self.aggr_asn_prefixes.add(prefix)

        self.atomic_data[prefix].add(
            AtomicData(atomic, aggr_asn)
        )

    def dump_json(
        self
    ) -> None:

        self.dump_atomic_data_json(
            self.json_atomic_data_path
        )
        self.dump_prefix_sets_json(
            self.json_prefixes_path
        )

    def dump_atomic_data_json(
        self,
        filepath: Path #= self.json_atomic_data_path
    ) -> None:
        """JSON dump for atomic aggregate data"""
        filepath.parent.mkdir(parents=True, exist_ok=True)

        serializable = {
            prefix: [asdict(ad) for ad in ad_set]
            for prefix, ad_set in self.atomic_data.items()
        }

        with open(filepath, "w") as f:
            json.dump(serializable, f, indent=4)

    def dump_prefix_sets_json(
        self,
        filepath: Path #= self.json_prefixes_path
    ) -> None:
        """JSON dump for prefix sets"""

        output = {
            "prefixes where atomic=true": list(self.atomic_prefixes),
            "prefixes with aggregator asn": list(self.aggr_asn_prefixes),
            "prefixes where atomic=true AND with aggregator asn": list(
                self.atomic_and_aggr_asn_prefixes
            )
        }

        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w") as f:
            json.dump(output, f, indent=4)

    @property
    def atomic_and_aggr_asn_prefixes(self):
        """Property for an optional third set of prefixes;
        where atomic=true and aggr_asn is not empty
        """

        return self.atomic_prefixes & self.aggr_asn_prefixes

    @property
    def json_atomic_data_path(self) -> Path:
        return self.base_dir / "analysis" / "atomic_data.json"

    @property
    def json_prefixes_path(self) -> Path:
        return self.base_dir / "analysis" / "atomic_prefixes.json"

