import csv
import logging
from multiprocessing import Lock

from ipaddress import ip_network
from tqdm import tqdm

from lib_bgpstream_website_collector import Row

from lib_roa_checker import ROAChecker, ROAValidity


class POMetadata:
    """Stores prefix origin metadata"""

    __slots__ = ["prefix_ids", "roa_checker", "next_prefix_id",
                 "next_block_id", "max_block_size", "next_prefix_block_id",
                 "po_meta", "roa_info", "bgpstream_po_dict", "bgpstream_origin_dict"]

    def __init__(self,
                 prefixes,
                 max_block_size,
                 roas_path,
                 bgpstream_website_tsv_path):
        self.prefix_ids = dict()
        self.po_meta = dict()
        bgpstream_po_dict, bgpstream_origin_dict = self.get_bgpstream_dict(bgpstream_website_tsv_path)
        self.bgpstream_po_dict = bgpstream_po_dict
        self.bgpstream_origin_dict = bgpstream_origin_dict
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
        with open(roas_path, mode="r") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                origin = int(row["asn"])
                try:
                    max_length = int(row["max_length"])
                # Sometimes max length is nan
                except ValueError:
                    max_length = None
                roa_checker.insert(ip_network(row["prefix"]), origin, max_length)
        return roa_checker

    def get_bgpstream_dict(self, bgpstream_website_tsv_path):
        bgpstream_po_info = dict()
        bgpstream_origin_info = dict()
        with open(bgpstream_website_tsv_path, mode="r") as f:
            reader = csv.DictReader(f, delimiter="\t")
            print("Fix leaks here")
            for row in reader:
                # Hijack
                if row["hijack_detected_origin_number"] not in [None, ""]:
                    po = (row["hijack_more_specific_prefix"],
                          int(row["hijack_detected_origin_number"]),)
                    bgpstream_po_info[po] = tuple(list(row.values()))
                    po = (row["hijack_expected_prefix"],
                          int(row["hijack_expected_origin_number"]),)
                    bgpstream_po_info[po] = tuple(list(row.values()))
                # Leak
                elif row["leaked_prefix"] not in [None, ""]:
                    po = (row["leaked_prefix"],
                          int(row["leaker_as_number"]),)
                    bgpstream_po_info[po] = tuple(list(row.values()))

                    #po = (row["leaked_prefix"],
                    #      int(row["leak_origin_as_number"]),)
                    #bgpstream_po_info[po] = tuple(list(row.values()))
                    #po = (row["leaked_prefix"],
                    #      int(row["leaked_to_number"]),)
                    #bgpstream_po_info[po] = tuple(list(row.values()))

                elif row["outage_as_number"] not in [None, ""]:
                    bgpstream_origin_info[int(row["outage_as_number"])] = tuple(row.values())
        return bgpstream_po_info, bgpstream_origin_info

    def _get_bgpstream_vals(self, prefix_str, prefix_obj, origin):
        po_info = self.bgpstream_po_dict.get((prefix_str, origin))
        if po_info is not None:
            return po_info
        else:
            default = tuple([None for x in Row.columns])
            return self.bgpstream_origin_dict.get(origin, default)

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
            bgpstream_vals = self._get_bgpstream_vals(prefix, prefix_obj, origin)
            # Prefix metadata
            # Using tuples because managers have trouble with nested mutables
            # And they are also faster than lists (slightly)
            meta = bgpstream_vals + (validity.value,) + self.prefix_ids[prefix]
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
