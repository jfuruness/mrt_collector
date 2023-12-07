from datetime import datetime
from functools import cache
from ipaddress import ip_network, IPv4Network, IPv6Network
from pathlib import Path
from typing import Any, Optional

from tqdm import tqdm

from bgpstream_website_collector import BGPStreamWebsiteCollector
from roa_collector import ROACollector
from roa_checker import ROAChecker, ROAValidity, ROARouted
from roa_checker.roa import ROA


class PrefixOriginMetadata:
    """Stores prefix origin metadata"""

    ##############
    # Init Funcs #
    ##############

    def __init__(
        self,
        dl_time: datetime,
        requests_cache_db_path: Path,
        prefixes_path: Path,
        max_block_size: int,
    ) -> None:
        self.dl_time: datetime = dl_time
        self.requests_cache_db_path: Path = requests_cache_db_path
        # Stores prefix and it's corresponding ID for block
        self.extrapolator_meta: dict[str, dict[str, int]] = dict()
        self.bgpstream_po_meta: dict[
            tuple[str, int], dict[str, Any]
        ] = self._get_bgpstream_po_meta()
        self.bgpstream_origin_meta: dict[
            int, dict[str, Any]
        ] = self._get_bgpstream_origin_meta()
        print("initializing roa checker")
        self.roa_checker: ROAChecker = self._init_roa_checker()
        self.prefix_roa_dict: dict[str, ROA] = dict()
        # prefix mapped to an ID
        self.next_prefix_id: int = 0
        # Block number (used in extrapolator)
        self.next_block_id: int = 0
        # Maximum number of prefixes per block
        self.max_block_size: int = max_block_size
        # Prefix ID within it's given block
        self.next_block_prefix_id: int = 0
        print("roa checker done")
        print("reading prefixes")
        with prefixes_path.open() as f:
            # Don't include header
            prefixes = [x.strip() for x in f if x.strip() != "prefix"]
        print("prefixes read")

        for prefix in tqdm(prefixes, "Adding extrapolator meta", total=len(prefixes)):
            self._add_prefix_to_extrapolator_meta(prefix)

    def _get_bgpstream_po_meta(self) -> dict[tuple[str, int], dict[str, Any]]:
        rows = self._get_bgpstream_rows()
        bgpstream_po_info: dict[tuple[str, int], dict[str, Any]] = dict()
        for row in rows:
            # Hijack
            if row["hijack_detected_origin_number"] not in [None, ""]:
                po = (
                    row["hijack_more_specific_prefix"],
                    int(row["hijack_detected_origin_number"]),
                )
                bgpstream_po_info[po] = row
                po = (
                    row["hijack_expected_prefix"],
                    int(row["hijack_expected_origin_number"]),
                )
                bgpstream_po_info[po] = row
            # Leak
            elif row["leaked_prefix"] not in [None, ""]:
                po = (
                    row["leaked_prefix"],
                    int(row["leaker_as_number"]),
                )
                bgpstream_po_info[po] = row

                po = (
                    row["leaked_prefix"],
                    int(row["leak_origin_as_number"]),
                )
                bgpstream_po_info[po] = row

                asns_str = row["leaked_to_number"].replace("{", "").replace("}", "")
                asns_str = asns_str.replace(" ", "")
                leaked_to_asns = asns_str.split(",")
                for leaked_to_asn in leaked_to_asns:
                    po = (
                        row["leaked_prefix"],
                        int(leaked_to_asn),
                    )
                    bgpstream_po_info[po] = row
        return bgpstream_po_info

    def _get_bgpstream_origin_meta(self) -> dict[int, dict[str, Any]]:
        rows = self._get_bgpstream_rows()
        bgpstream_origin_info: dict[int, dict[str, Any]] = dict()
        for row in rows:
            if row["outage_as_number"] not in [None, ""]:
                bgpstream_origin_info[int(row["outage_as_number"])] = row
        return bgpstream_origin_info

    def _get_bgpstream_rows(self) -> list[dict[str, Any]]:
        """Returns all bgpstream rows"""

        # TODO: have this as a property somewhere...
        pickle_path = Path("/tmp/bgpstream_rows.pickle")

        # Cache result if not exists. requests_cache too slow for this...
        if not pickle_path.exists():
            collector = BGPStreamWebsiteCollector(
                csv_path=None,
                requests_cache_db_path=self.requests_cache_db_path,
            )

            rows: list[dict[str, Any]] = collector.run(self.dl_time.date())
            with pickle_path.open("wb") as f:
                pickle.dump(rows, f)

        with pickle_path.open("rb") as f:
            rows: list[dict[str, Any]] = pickle.load(f)
            for row in rows:
                # Can't save URL since that would overwrite the source url
                row["bgpstream_url"] = row.pop("url")
            return rows

    def _init_roa_checker(self) -> ROAChecker:
        """Downloads ROAs and returns ROAChecker"""

        roa_checker = ROAChecker()
        # TODO: Change this to historical roas
        for roa in ROACollector(csv_path=None).run():
            roa_checker.insert(ip_network(roa.prefix), roa.origin, roa.max_length)
        return roa_checker

    def _add_prefix_to_extrapolator_meta(self, prefix: str) -> None:
        """Adds prefix data to various data structures"""

        try:
            ip_network(prefix)
        # Occurs when host bits are set. We throw these out
        except ValueError as e:
            print(f"{prefix} had host bits set {e}, throwing it out")
            return

        # Set extrapolator metadata
        extrapolator_meta = {
            "prefix_id": self.next_prefix_id,
            "block_id": self.next_block_id,
            "block_prefix_id": self.next_block_prefix_id,
        }

        self.extrapolator_meta[prefix] = extrapolator_meta

        # Increment extrapolator metadata
        self.next_prefix_id += 1
        self.next_block_prefix_id += 1
        if self.next_block_prefix_id == self.max_block_size:
            self.next_block_prefix_id = 0
            self.next_block_id += 1

        # There used to be an optimization here to save in advance
        # all ROAs that had unknown for prefixes that weren't covered
        # but since 90+% of prefixes are covered, this no longer makers sense
        # additionally, previously we didn't differentiate between
        # invalid by origin, path length, or both
        # so saving just prefix info if prefix is invalid doesn't make sense
        # so I've removed this optimization for now

    ##################
    # Get Meta Funcs #
    ##################

    # Consumes too much memory. Also lol disabling this barely affected speed
    # it might even be faster now, and before took up so much RAM my machine would crash
    # @cache
    def get_meta(
        self, prefix: str, prefix_obj: IPv4Network | IPv6Network, origin: int
    ) -> dict[str, Any]:
        """Returns prefix origin metadata"""

        # This should always return, since we prepopulate this with all prefixes
        meta = self.extrapolator_meta[prefix].copy()

        # Add validity meta
        prefix_obj = ip_network(prefix)
        roa = self._get_roa(prefix_obj)
        if roa:
            validity, routed = roa.get_validity(prefix_obj, origin)
        else:
            validity, routed = ROAValidity.UNKNOWN, ROARouted.UNKNOWN
        meta["roa_validity"] = validity.value
        meta["roa_routed"] = routed.value

        # Add bgpstream meta
        meta.update(
            self.bgpstream_po_meta.get(
                (
                    prefix,
                    origin,
                ),
                # mypy being dumb
                dict(),  # type: ignore
            )
        )
        # mypy doesn't understand the second argument of get
        meta.update(self.bgpstream_origin_meta.get(origin, dict()))  # type: ignore

        return meta

    # consumes too much memory. Also lol disabling this barely affected speed.
    # it might even be faster now, and before took up so much RAM my machine would crash
    # @cache
    def _get_roa(self, prefix_obj: IPv4Network | IPv6Network) -> Optional[ROA]:
        """Cached function around returning ROAs"""
        return self.roa_checker.get_roa(prefix_obj)
