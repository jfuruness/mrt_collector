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

class AggregatedSpaceAnalyzer(ExportAnalyzer):
    def __init__(
        self, 
        base_dir: Path
    )->None:
        
        super().__init__(base_dir)
        self.desc = "Extracting percentage of aggregated network space (v4 and v6)"
        self.v4_trie = IPv4CIDRTrie(AtomicCIDRNode)
        self.agg_v4_space = 0
        self.v6_trie = IPv6CIDRTrie(AtomicCIDRNode)
        self.agg_v6_space = 0

    def analyze(
        self,
        row: dict[str]
    )->None:
        """Builds CIDR Tries, for v4 and v6"""
        
        atomic_aggregate = row["atomic"] == "true" or row["aggr_asn"] is not ""
        prefix = ipaddress.ip_network(row["prefix"], strict=False)

        if isinstance(prefix, ipaddress.IPv4Network):
            self.v4_trie.insert(prefix, atomic_aggregate)
        else:
            self.v6_trie.insert(prefix, atomic_aggregate)

    def post_process(self)->None:
        """Calculates percentages aggregated network space (for v4 and v6)"""
        
        v4_prefixes = [
            node.prefix for node in self.dfs(self.v4_trie.root, v4=True) if "/0" not in node.prefix
        ]
        ann_v4_space = sum(
            net.num_addresses for net in ipaddress.collapse_addresses(v4_prefixes)
        )

        self.per_agg_ann_v4_space = self.agg_v4_space / ann_v4_space
        self.per_agg_total_v4_space = self.agg_v4_space / 2**32

        v6_prefixes = [
            node.prefix for node in self.dfs(self.v6_trie.root, v4=False) if "/0" not in node.prefix
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
        
        if node.prefix is not None:
            yield node
            if node.atomic_aggregate == True:
                if v4:
                    self.agg_v4_space += 2**(32-node.prefix.prefixlen)
                else:
                    self.agg_v6_space += 2**(128-node.prefix.prefixlen)
                return
        
        yield from self.dfs(node.left, v4)
        yield from self.dfs(node.right, v4)


    def dump_json(
        self
    )->None:
        
        filepath = self.json_path
        filepath.parent.mkdir(parents=True, exist_ok=True)

        serializable = {
            "Percent aggregated total v4 space": f"{self.per_agg_total_v4_space:.10f}",
            "Percent aggregated announced v4 space": f"{self.per_agg_ann_v4_space:.10f}",
            "Percent aggregated total v6 space": f"{self.per_agg_total_v6_space:.2e}",
            "Percent aggregated announced v6 space": f"{self.per_agg_ann_v6_space:.10f}",
        }

        with open(filepath, "w") as f:
            json.dump(serializable, f, indent=4)

    @property
    def json_path(self) -> Path:
        return self.base_dir / "analysis" / "per_aggregated_space.json"