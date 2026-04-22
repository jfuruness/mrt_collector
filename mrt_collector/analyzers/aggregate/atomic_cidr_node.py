from ipaddress import IPv4Network, IPv6Network

from lib_cidr_trie.cidr_node import CIDRNode

class AtomicCIDRNode(CIDRNode):
    def add_data(
        self, 
        prefix: IPv4Network | IPv6Network,
        *args
    )->None:
        super().add_data(prefix)
        self.atomic_aggregate = args[0] if args else False