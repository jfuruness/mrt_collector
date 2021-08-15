from multiprocessing import Lock

from ipaddress import ip_network




class POMetadata:
    """Stores prefix origin metadata"""

    __slots__ = ["prefix_ids", "origin_ids", "prefix_ids_lock",
                 "origin_ids_lock", "po_lock",
                 "roas", "next_prefix_id", "next_block_id",
                 "max_block_size", "next_prefix_block_id", "next_origin_id",
                 "po_meta"]

    def __init__(self, roa_checker, max_block_size):
        self.prefix_ids = dict()
        self.prefix_ids_lock = Lock()
        self.origin_ids = dict()
        self.origin_ids_lock = Lock()
        self.po_meta = dict()
        self.po_lock = Lock()
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
            validity = self._get_roa_validity(prefix, origin)

            with self.po_lock:
                # Prefix origin was added while waiting for the lock
                if (prefix, origin) in self.po_meta:
                    return self.po_meta[(prefix, origin)]
                # Prefix metadata
                # Using tuples because managers have trouble with nested mutables
                # And they are also faster than lists (slightly)
                meta = (*self.prefix_ids[prefix], self.origin_ids[origin], validity)
                self.po_meta[(prefix, origin)] = meta
        
        return self.po_meta[(prefix, origin)]

    def _add_prefix(self, prefix: str):
        """Adds a prefix to the data structure and calculates meta"""

        with self.prefix_ids_lock:
            # If while acquiring lock the prefix was added, noop
            if prefix in self.prefix_ids:
                return
            prefix_meta = tuple([self.next_prefix_id,
                                 self.next_block_id,
                                 self.next_prefix_block_id])

            self.prefix_ids[prefix] = prefix_meta
            self.next_prefix_id += 1
            self.next_prefix_block_id += 1
            if self.next_prefix_block_id == self.max_block_size:
                self.next_prefix_block_id = 0
                self.next_block_id += 1

    def _add_origin(self, origin: int):
        """Adds the origin to the origin ids"""

        with self.origin_ids_lock:
            # If while acquiring the lock the origin was added, noop
            if origin in self.origin_ids:
                return
            self.origin_ids[origin] = self.next_origin_id
            self.next_origin_id += 1

    def _get_roa_validity(self, prefix: str, origin: int):
        """returns roa validity"""

        if self.roas:
            return self.roas.get_validity(ip_network(prefix), origin).value
        else:
            return None
