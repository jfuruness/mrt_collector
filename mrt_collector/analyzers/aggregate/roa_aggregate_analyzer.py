import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

import ipaddress

from mrt_collector.analyzers.export_analyzer import ExportAnalyzer

from roa_collector.roa_collector import ROACollector
from roa_checker.roa_checker import ROAChecker

class ROAAggregateAnalyzer(ExportAnalyzer, analyzer_id="agg_roa"):
    def __init__(
        self, 
        base_dir: Path
    ) -> None:
        super().__init__(base_dir)
        self.aggregated_prefixes = set()

        # obtain roas
        roa_collector = ROACollector()
        self.roas = roa_collector._parse_roa_json(roa_collector._get_json_roas())

        # build ROA checker, includes v4 and v6 roa trie
        self.roa_checker = ROAChecker()
        for roa in self.roas:
            self.roa_checker.insert(roa.prefix, roa)

    def analyze(
        self, 
        row
    ) -> None:
        
        atomic = row["atomic"] == "true"
        aggr_asn = row["aggr_asn"] or "None"
        if not atomic and aggr_asn == "None":
            return

        raw_prefix = row["prefix"]
        prefix = ipaddress.ip_network(raw_prefix, strict=False)

        self.aggregated_prefixes.add(prefix)

    def post_process(
        self
    ) -> None:
        pass