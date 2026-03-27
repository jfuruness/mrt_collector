import csv
import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

from tqdm import tqdm

from mrt_collector.mrt_collector import sort_mrt_files_by_parsed_file_size
from mrt_collector.mrt_file import MRTFile

# prefix atomic data will be formatted as:
# defaultdict<prefix: str, set{data: AtomicData}>

@dataclass(frozen=True)
class AtomicData:
    atomic: bool
    aggr_asn: int

class AtomicExportAnalyzer:
    def __init__(
        self,
        base_dir: Path
    ) -> None:

        self.atomic_data = defaultdict(set)
        self.atomic_prefixes = set()
        self.aggr_asn_prefixes = set()
        self.base_dir = base_dir

    def run(
        self,
        mrt_files: tuple[MRTFile, ...]
    ) -> None:
        """Lifecycle of the export analyzer"""

        mrt_files = sort_mrt_files_by_parsed_file_size(mrt_files)
        self.get_atomic_data(mrt_files)
        self.dump_atomic_data_json(
            self.json_atomic_data_path
        )
        self.dump_prefix_sets_json(
            self.json_prefixes_path
        )


    def get_atomic_data(
        self,
        mrt_files: tuple[MRTFile, ...],
    ):
        """Creates Atomic Export Data from parsed MRTs"""

        total_lines = sum(x.total_parsed_lines for x in mrt_files)
        desc = "Extracting atomic aggregate data"
        with tqdm(
            total=total_lines,
            desc=desc
        ) as pbar:
            for mrt_file in mrt_files:
                if not mrt_file.parsed_path_psv.exists():
                    continue
                self._collect_from_file(
                    mrt_file,
                    pbar
                )

    def _collect_from_file(
        self,
        mrt_file: MRTFile,
        pbar
    ):
        """Collects atomic data from an mrt file"""

        with mrt_file.parsed_path_psv.open() as f:
            reader = csv.DictReader(f, delimiter="|")
            for row in reader:
                pbar.update()
                if row["type"] != "A":
                    continue

                atomic = row["atomic"] == "true"
                aggr_asn = row["aggr_asn"] or "None"
                # skip rows without atomic and without aggregate data
                # some rows can have atomic=false but still have data
                if not atomic and aggr_asn == "None":
                    continue

                prefix = row["prefix"]
                if atomic:
                    self.atomic_prefixes.add(prefix)

                if aggr_asn != "None":
                    self.aggr_asn_prefixes.add(prefix)

                self.atomic_data[prefix].add(
                    AtomicData(atomic, aggr_asn)
                )

    def dump_atomic_data_json(
        self,
        filepath: Path #= self.json_atomic_data_path
    ) -> None:
        """Manual-ish json dump for atomic data. Doing it this way
        helps with both my defaultdict formatting and conserving
        memory, since we aren't loading the entire dump all at once
        """

        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w") as f:
            f.write("{\n")
            items = list(self.atomic_data.items())
            for i, (prefix, data_set) in enumerate(items):
                f.write(f'    "{prefix}": [\n')
                data_list = list(data_set)
                for j, item in enumerate(data_list):
                    line = json.dumps(asdict(item))
                    comma = "," if j < len(data_list) - 1 else ""
                    f.write(f'        {line}{comma}\n')
                comma = "," if i < len(item) - 1 else ""
                f.write(f'    ]{comma}\n')
            f.write("}\n")

    def dump_prefix_sets_json(
        self,
        filepath: Path #= self.json_prefixes_path
    ) -> None:
        """json dump for prefix sets"""

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

