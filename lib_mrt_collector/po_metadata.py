from multiprocessing import Lock

from ipaddress import ip_network


class POMetadata:
    """Stores prefix origin metadata"""

    __slots__ = ["prefix_ids", "roa_checker", "next_prefix_id",
                 "next_block_id", "max_block_size", "next_prefix_block_id",
                 "po_meta"]

    def __init__(self, uniq_prefixes, max_block_size, roa_checker):
        self.prefix_ids = dict()
        self.po_meta = dict()
        self.roa_checker = roa_checker
        self.next_prefix_id = 0
        self.next_block_id = 0
        self.max_block_size = max_block_size
        self.next_prefix_block_id = 0
        for prefix in uniq_prefixes:
            self._add_prefix(prefix)
 
    def get_meta(self, prefix: str, origin: int):
        """Adds a prefix if it was not already. Returns prefix metadata"""

        # Prefix_ids is created upon init. Only origin is missing.
        if (prefix, origin) not in self.po_meta:
            validity = self._get_roa_validity(prefix, origin)

            # Prefix metadata
            # Using tuples because managers have trouble with nested mutables
            # And they are also faster than lists (slightly)
            meta = (*self.prefix_ids[prefix], validity)
            self.po_meta[(prefix, origin)] = meta
        
        return self.po_meta[(prefix, origin)]

    def _add_prefix(self, prefix: str):
        """Adds a prefix to the data structure and calculates meta"""

        prefix_meta = tuple([self.next_prefix_id,
                             self.next_block_id,
                             self.next_prefix_block_id])

        self.prefix_ids[prefix] = prefix_meta
        self.next_prefix_id += 1
        self.next_prefix_block_id += 1
        if self.next_prefix_block_id == self.max_block_size:
            self.next_prefix_block_id = 0
            self.next_block_id += 1

    def _get_roa_validity(self, prefix: str, origin: int):
        """returns roa validity"""

        if self.roas:
            return self.roas.get_validity(ip_network(prefix), origin).value
        else:
            # NOTE: precompute the prefixes!! Only compute the origin shit!
            return None
