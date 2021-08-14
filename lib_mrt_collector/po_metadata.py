from ipaddress import ip_network


class POMetadata:
    """Stores prefix origin metadata"""

    __slots__ = ["prefix_ids", "origin_ids", "po_roa_validities",
                 "roas", "next_prefix_id", "next_block_id",
                 "max_block_size", "next_prefix_block_id", "next_origin_id",
                 "po_meta"]

    def __init__(self, roa_checker, max_block_size):
        self.prefix_ids = dict()
        self.origin_ids = dict()
        self.po_roa_validities = dict()
        self.po_meta = dict()
        self.roas = roa_checker
        self.next_prefix_id = 0
        self.next_block_id = 0
        self.max_block_size = max_block_size
        self.next_prefix_block_id = 0
        self.next_origin_id = 0

    def get_meta(self, prefix: str, origin: int):
        """Adds a prefix if it was not already. Returns prefix metadata"""

        if (prefix, origin) not in self.po_meta:
            if prefix not in self.prefix_ids:
                self._add_prefix(prefix)
            if origin not in self.origin_ids:
                self._add_origin(origin)
            if (prefix, origin) not in self.po_roa_validities:
                self._add_prefix_origin(prefix, origin)

            # Prefix metadata
            meta = self.prefix_ids[prefix].copy()
            # origin metadata
            meta.append(self.origin_ids[origin])
            # prefix origin meta
            meta.append(self.po_roa_validities[(prefix, origin)])
            self.po_meta[(prefix, origin)] = tuple(meta)

        return self.po_meta[(prefix, origin)]

    def _add_prefix(self, prefix: str):
        """Adds a prefix to the data structure and calculates meta"""

        prefix_meta = [self.next_prefix_id,
                       self.next_block_id,
                       self.next_prefix_block_id]

        self.prefix_ids[prefix] = prefix_meta
        self.next_prefix_id += 1
        if self.next_prefix_block_id == self.max_block_size:
            self.next_prefix_block_id = 0
            self.next_block_id += 1

    def _add_origin(self, origin: int):
        """Adds the origin to the origin ids"""

        self.origin_ids[origin] = self.next_origin_id
        self.next_origin_id += 1

    def _add_prefix_origin(self, prefix: str, origin: int):
        """Adds the prefix origin metadata, mainly roa validity"""

        if self.roas:
            valid = self.roas.get_validity(ip_network(prefix), origin).value
        else:
            valid = None

        self.po_roa_validities[(prefix, origin)] = valid
