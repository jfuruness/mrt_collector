import json
from collections import defaultdict
from pathlib import Path
from dataclasses import asdict, dataclass

from .export_analyzer import ExportAnalyzer

@dataclass
class PathData:
    path: str
    path_topo_aligns_with_bgpy: bool
    sent_to_providers: bool
    key_error: bool

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, PathData):
            return asdict(obj)
        return super().default(obj)

class SlashzeroPrefixAnalyzer(ExportAnalyzer, analyzer_id="slashzero"):
    desc = "Extracts /0 announcement data, cross references against BGPy graph"

    def __init__(
        self, 
        base_dir: Path
    )->None:
        
        super().__init__(base_dir)
        self.prefix_data = defaultdict(dict)
        self.uses_bgpy_graph = True

    def analyze(
        self, 
        row: dict[str]
    ) -> None:
        """Collects /0 prefix data from mrt file"""

        prefix = row["prefix"]
        if "/0" not in prefix:
            return
        
        origin = int(row["origin_asns"])
        data = self._get_pathdata_from_path(row["as_path"])

        source = self.current_source
        self.prefix_data[origin][source] = data

    def _get_pathdata_from_path(
        self,
        path: str
    ) -> PathData:
        """Removes any prepending from given AS path"""

        split_asns = list(map(int, path.split()))
        # lazy method of removing prepending; dicts in python preserve insertion order
        path_no_prepending = list(dict.fromkeys(split_asns))

        topo_aligns = True
        sent_to_providers = False
        
        # need a default
        key_error = path_no_prepending[0] not in self.bgp_dag.as_dict

        path_length = len(path_no_prepending)
        if path_length > 1:
            # as path reads right to left, thank you BGP
            for idx in range(path_length-1, 0, -1):
                cur_asn = path_no_prepending[idx]
                next_asn = path_no_prepending[idx-1]

                # we are getting key errors, this is interesting because
                # not only does this mean topology is not reflected
                # but the actual ASs themselves are not reflected
                # thus I take note of this separate from topological
                # consistency
                key_error = cur_asn not in self.bgp_dag.as_dict
                if(key_error == False):
                    cur_as = self.bgp_dag.as_dict[cur_asn]
                    if(next_asn in cur_as.customer_asns):
                        continue
                    elif(next_asn in cur_as.provider_asns):
                        sent_to_providers = True
                        continue
                    elif(next_asn in cur_as.peer_asns):
                        continue

                # if this is ever reached, the path does not align w/ bgpy topology
                topo_aligns = False
                break

        final_path = " ".join(map(str, path_no_prepending))

        return PathData(final_path, topo_aligns, sent_to_providers, key_error)

    def dump_json(
        self
    ) -> None:
        """JSON dump for /0 prefix data"""
        self.json_prefix_data_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.json_prefix_data_path, "w") as f:
            json.dump(self.prefix_data, f, indent=4, cls=CustomEncoder)
    
    @property
    def json_prefix_data_path(self) -> Path:
        return self.base_dir / "slashzero_prefix_data.json"      