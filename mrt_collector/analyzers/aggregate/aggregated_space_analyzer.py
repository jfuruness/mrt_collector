import json
from collections import defaultdict
from pathlib import Path

from typing import Iterator

import ipaddress

from mrt_collector.mrt_collector import sort_mrt_files_by_parsed_file_size
from mrt_collector.mrt_file import MRTFile

from mrt_collector.analyzers.export_analyzer import ExportAnalyzer
from lib_cidr_trie.cidr_tries import IPv4CIDRTrie, IPv6CIDRTrie
from .atomic_cidr_node import AtomicCIDRNode

class AggregatedSpaceAnalyzer(ExportAnalyzer, analyzer_id="agg_space"):
    desc = "Extracts percentage of aggregated network space (v4 and v6)"

    def __init__(
        self, 
        base_dir: Path
    )->None:
        
        super().__init__(base_dir)

        self.v4_trie = IPv4CIDRTrie(AtomicCIDRNode)
        self.agg_v4_space = 0

        # default to max
        self.lowest_v4_subnet = 32

        self.v6_trie = IPv6CIDRTrie(AtomicCIDRNode)
        self.agg_v6_space = 0

        # default to max
        self.lowest_v6_subnet = 128

    def analyze(
        self,
        row: dict[str]
    )->None:
        """Builds CIDR Tries, for v4 and v6"""
        
        atomic_aggregate = row["atomic"] == "true" or row["aggr_asn"] != ""
        prefix = ipaddress.ip_network(row["prefix"], strict=False)

        if isinstance(prefix, ipaddress.IPv4Network):
            self.v4_trie.insert(prefix, atomic_aggregate)
        else:
            self.v6_trie.insert(prefix, atomic_aggregate)

    def post_process(self)->None:
        """Calculates percentages aggregated network space (for v4 and v6)"""
        
        v4_prefixes = [
            node.prefix for node in self.dfs(self.v4_trie.root, v4=True)
        ]
        ann_v4_space = sum(
            net.num_addresses for net in ipaddress.collapse_addresses(v4_prefixes)
        )

        self.per_agg_ann_v4_space = self.agg_v4_space / ann_v4_space
        self.per_agg_total_v4_space = self.agg_v4_space / 2**32

        v6_prefixes = [
            node.prefix for node in self.dfs(self.v6_trie.root, v4=False)
        ]
        ann_v6_space = sum(
            net.num_addresses for net in ipaddress.collapse_addresses(v6_prefixes)
        )

        self.per_agg_ann_v6_space = self.agg_v6_space / ann_v6_space
        self.per_agg_total_v6_space = self.agg_v6_space / 2**128

    def dfs(
        self, 
        node:AtomicCIDRNode,
        v4:bool = True,
    )->Iterator[AtomicCIDRNode]:
        """Performs depth-first search, calculating aggregated space
            simultaneously returns generator to be used to calculate
            announced space
        """
        if node is None:
            return
        
        if node.prefix is not None and node.prefix.prefixlen != 0:
            yield node
            if node.atomic_aggregate == True:
                subnet = node.prefix.prefixlen
                if v4:
                    self.lowest_v4_subnet = min(self.lowest_v4_subnet, subnet)
                    self.agg_v4_space += 2**(32-subnet)
                else:
                    self.lowest_v6_subnet = min(self.lowest_v6_subnet, subnet)
                    self.agg_v6_space += 2**(128-subnet)
                return
        
        yield from self.dfs(node.left, v4)
        yield from self.dfs(node.right, v4)


    def dump_json(
        self
    )->None:
        
        filepath = self.json_path
        filepath.parent.mkdir(parents=True, exist_ok=True)

        serializable = {
            "Percent aggregated total v4 space": f"{100*self.per_agg_total_v4_space:.10f}",
            "Percent aggregated announced v4 space": f"{100*self.per_agg_ann_v4_space:.10f}",
            "Lowest value v4 subnet mask": self.lowest_v4_subnet,
            "Percent aggregated total v6 space": f"{100*self.per_agg_total_v6_space:.10f}",
            "Percent aggregated announced v6 space": f"{100*self.per_agg_ann_v6_space:.10f}",
            "Lowest value v6 subnet mask": self.lowest_v6_subnet
        }

        with open(filepath, "w") as f:
            json.dump(serializable, f, indent=4)

    @property
    def json_path(self) -> Path:
        return self.base_dir / "analysis" / "per_aggregated_space.json"