import json
from collections import defaultdict
from pathlib import Path

from mrt_collector.mrt_collector import sort_mrt_files_by_parsed_file_size

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
        self.v6_trie = IPv6CIDRTrie(AtomicCIDRNode)

    def run(
        self,
        mrt_files: tuple[MRTFile, ...]
    ) -> None:
        """Lifecycle of the export analyzer"""

        mrt_files = sort_mrt_files_by_parsed_file_size(mrt_files)
        self.get_data(mrt_files)
        self.post_process()
        self.dump_json()

    def analyze(
        self,
        row: dict[str, ...]
    )->None:
        """Builds CIDR Tries, for v4 and v6"""
        pass
        #atomic_aggregate = 


    def post_process(self)->None:
        """Calculates percentages aggregated network space (for v4 and v6)"""
        pass