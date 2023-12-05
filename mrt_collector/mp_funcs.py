"""This file contains functions used when multiprocessing"""

import csv
from ipaddress import ip_network
import json
from subprocess import check_call
from typing import Any, Callable

from .mrt_file import MRTFile
from .prefix_origin_metadata import PrefixOriginMetadata

MAX_ASN: int = 401308


def download_mrt(mrt_file: MRTFile) -> None:
    mrt_file.download_raw()


def store_prefixes(mrt_file: MRTFile) -> None:
    mrt_file.store_unique_prefixes()


###############
# Parse funcs #
###############

PARSE_FUNC = Callable[[MRTFile], None]


def bgpkit_parser_json(mrt_file: MRTFile) -> None:
    """Extracts info from raw dumps into parsed path

    For this particular parser, I output both to CSV and to JSON
    I know it makes it take twice as long, but you need the CSV for the prefixes,
    and the JSON is just so convenient.

    You only have to run it once to analyze it any number of ways, so whatevs

    For other funcs of this kind, note that you MUST always pipe to PSV
    """

    # This takes up so much space that it's not even possible on 1 TB machine
    # if not mrt_file.parsed_path_psv.exists():
    #     check_call(
    #         f"bgpkit-parser {mrt_file.raw_path} > {mrt_file.parsed_path_psv}",
    #         shell=True,
    #     )
    if not mrt_file.parsed_path_json.exists():
        check_call(
            f"bgpkit-parser {mrt_file.raw_path} --json > {mrt_file.parsed_path_json}",
            shell=True,
        )


def bgpkit_parser(mrt_file: MRTFile) -> None:
    """Extracts info from raw dumps into parsed path"""

    if not mrt_file.parsed_path_psv.exists():
        check_call(
            f"bgpkit-parser {mrt_file.raw_path} > {mrt_file.parsed_path_psv}",
            shell=True,
        )


################
# Format Funcs #
################


FORMAT_FUNC = Callable[[MRTFile, PrefixOriginMetadata], None]


def format_json_into_tsv(
    mrt_file: MRTFile, prefix_origin_metadata: PrefixOriginMetadata
) -> None:
    """Formats JSON into a PSV"""

    # Open all blocks for all files
    mrt_file.formatted_dir.mkdir(parents=True, exist_ok=True)
    block_nums = list(range(prefix_origin_metadata.next_block_id + 1))
    wfiles = [(mrt_file.formatted_dir / f"{i}.tsv").open("w") for i in block_nums]

    rfile = mrt_file.parsed_path_json.open()
    writers = [csv.writer(x, delimiter="\t") for x in wfiles]

    non_public_asns = get_non_public_asns()

    for line in rfile:
        meta = json.loads(line)

        # VALIDATION ###
        try:
            prefix_obj = ip_network(meta["prefix"])
        # This occurs whenever host bits are set
        except ValueError:
            continue
        assert meta["type"] == "ANNOUNCE", f"Not an announcement? {meta}"

        # No AS sets or empty AS paths
        if "}" in str(meta["as_path"]) or meta["as_path"] in [None, []]:
            continue

        meta = _get_path_data(meta, non_public_asns)
        meta.update(
            prefix_origin_metadata.get_meta(
                meta["prefix"],
                prefix_obj,
                meta["origin_asn"]
            )
        )
        meta["url"] = mrt_file.url
        values = [meta[x] for x in fieldnames()]
        writers[meta["block_id"]].writerow(values)

    rfile.close()
    for f in wfiles:
        f.close()


def get_non_public_asns() -> frozenset[int]:

    return frozenset(
        # Later make this not hardcoded
        # https://www.iana.org/assignments/iana-as-numbers-special-registry/
        # iana-as-numbers-special-registry.xhtml
        # https://www.iana.org/assignments/as-numbers/as-numbers.xhtml
        [0, 112, 23456, 65535]
        + list(range(64496, 64511))
        + list(range(64512, 65534))
        + list(range(65536, 65551))
        + list(range(65552, 131071))
        # Unallocated
        + list(range(153914, 196607))
        + list(range(216476, 262143))
        + list(range(273821, 327679))
        + list(range(329728, 393215))
    )


def _get_path_data(
    meta: dict[str, Any],
    non_public_asns: frozenset[int]
) -> dict[str, Any]:
    as_path = meta["as_path"]
    meta["origin_asn"] = as_path[-1]
    meta["collector_asn"] = as_path[0]
    meta["invalid_as_path_asns"] = list()
    meta["ixps_in_as_path"] = list()
    meta["prepending"] = False
    meta["as_path_loop"] = False
    # TODO: Check these
    meta["valley_free_caida_path"] = None
    # ASNs not part of CAIDA
    meta["non_caida_asns"] = None
    meta["input_clique_split"] = None

    # TODO: Actually get IXPs from CAIDA
    ixps = set()
    as_path_set = set()
    last_asn = None
    last_non_ixp = None
    for asn in as_path:
        asn = int(asn)
        if asn in non_public_asns or asn > MAX_ASN:
            meta["invalid_as_path_asns"].append(asn)
        if last_asn == asn:
            meta["prepending"] = True
            meta["as_path_loop"] = True
        elif asn in as_path_set:
            meta["as_path_loop"] = True
        as_path_set.add(asn)
        last_asn = asn
        if asn in ixps:
            meta["ixps_in_as_path"].append(asn)
        else:
            last_non_ixp = asn  # noqa

    return meta


def fieldnames() -> tuple[str]:

    return (
        # from bgpkit parser ###
        "aggr_asn",
        "aggr_ip",
        "as_path",
        "atomic",  # true if prefix aggregation occured, even if aggr_asn is False
        "communities",
        "deprecated",
        "local_pref",
        # "med",  useless imo
        # "next_hop",  # ip addr
        "only_to_customer",
        "origin",  # IGP or
        "origin_asns",
        # "peer_asn",  # ASN that is connected to vantage point
        # "peer_ip",  # IP of AS connected to vantage point
        "prefix",
        "timestamp",
        # "type",  # always announce
        "unknown",
        # Extrapolator ###
        "prefix_id",
        "block_id",
        "block_prefix_id",
        # AS Path data ###
        "origin_asn",
        "collector_asn",
        "invalid_as_path_asns",
        "ixps_in_as_path",
        "prepending",
        "valley_free_caida_path",
        "non_caida_asns",
        "input_clique_split",
        "as_path_loop",
        "ixps_in_as_path",
        # Other ###
        "url",
    )
