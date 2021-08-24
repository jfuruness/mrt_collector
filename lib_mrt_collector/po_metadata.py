import logging
from multiprocessing import Lock

from ipaddress import ip_network
import pandas as pd
from tqdm import tqdm

from lib_roa_checker import ROAChecker, ROAValidity


class POMetadata:
    """Stores prefix origin metadata"""

    __slots__ = ["prefix_ids", "roa_checker", "next_prefix_id",
                 "next_block_id", "max_block_size", "next_prefix_block_id",
                 "po_meta", "roa_info"]

    def __init__(self, prefixes, max_block_size, roas_path):
        self.prefix_ids = dict()
        self.po_meta = dict()
        self.roa_checker = self._init_roas_checker(roas_path)
        self.roa_info = dict()
        self.next_prefix_id = 0
        self.next_block_id = 0
        self.max_block_size = max_block_size
        self.next_prefix_block_id = 0
        for prefix in tqdm(prefixes, "Adding prefixes", total=len(prefixes)):
            self._add_prefix(prefix)

    def _init_roas_checker(self, roas_path):
        """ROA checker.

        We can get the roa based on the prefix, so we do this in advance
        Afterwards we want fast lookups based on just the origin
        so we construct a dict for this
        """

        roa_checker = ROAChecker()
        df = pd.read_csv(roas_path, delimiter="\t")
        # https://stackoverflow.com/a/55557758/8903959
        for prefix, origin, max_length in tqdm(zip(df["prefix"],
                                                   df["asn"],    
                                                   df["max_length"]),
                                               total=len(df),
                                               desc="Filling ROA trie"):
            try:
                max_length = int(max_length)
            # Sometimes max length is nan
            except ValueError:
                max_length = None
            roa_checker.insert(ip_network(prefix), origin, max_length)
        return roa_checker

    def get_meta(self, prefix, prefix_obj: ip_network, origin: int):
        """Adds a prefix if it was not already. Returns prefix metadata

        The reason we can't use prefix_obj here for the string
        is because ipv6 might be exploded or compressed
        and that might vary by collector - I haven't checked
        so we must just go with the strings from the mrt rib dumps
        """

        # Prefix_ids is created upon init. Only origin is missing.
        if (prefix, origin) not in self.po_meta:
            validity = self._get_roa_validity(prefix, prefix_obj, origin)

            # Prefix metadata
            # Using tuples because managers have trouble with nested mutables
            # And they are also faster than lists (slightly)
            meta = (*self.prefix_ids[prefix], validity.value)
            self.po_meta[(prefix, origin)] = meta
        
        return self.po_meta[(prefix, origin)]

    def _add_prefix(self, prefix: str):
        """Adds a prefix to the data structure and calculates meta"""

        try:
            prefix_obj = ip_network(prefix)
        # Occurs when host bits are set. We throw these out
        except ValueError:
            logging.warning(f"{prefix} had host bits set")
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
        roa = self.roa_checker.get_roa(prefix_obj)
        if roa:
            # Only one ROA for this prefix
            if len(roa.origin_max_lengths) == 1:
                origin, max_length = list(roa.origin_max_lengths)[0]
                if prefix_obj.prefixlen > max_length:
                    self.roa_info[prefix] = ROAValidity.INVALID
                else:
                    self.roa_info[prefix] = roa
            # There are some cases we could optimize out
            # But that would probably be rather confusing
            # Without much speed improvement
            # So let's just leave it as is
            else:
                self.roa_info[prefix] = roa
        else:
            self.roa_info[prefix] = ROAValidity.UNKNOWN

    def _get_roa_validity(self, prefix, prefix_obj: ip_network, origin: int):
        """returns roa validity"""

        result = self.roa_info[prefix]
        if isinstance(result, ROAValidity):
            return result
        else:
            return result.get_validity(prefix_obj, origin)
