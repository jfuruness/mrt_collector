"""This file contains functions used when multiprocessing"""

import csv
from ipaddress import ip_network
from pathlib import Path
import json
import os
import re
from subprocess import check_call
from typing import Any, Callable, Optional

from bgpy.caida_collector import CaidaCollector
from bgpy.enums import Relationships, ASGroups

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


# NOTE: DO NOT USE! No point, now that PSV flag exists that includes the JSON info
def bgpkit_parser_csv(mrt_file: MRTFile) -> None:
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
    if not mrt_file.parsed_path_csv.exists():
        check_call(
            f"bgpkit-parser {mrt_file.raw_path} --json > {mrt_file.parsed_path_json}",
            shell=True,
        )
        # Unfortunately, the JSON files are so large that my computer runs out of space
        # when trying to parse them. So instead we need to immediatly read them in,
        # write them to CSV, and delete
        # Must leave this here to keep in scope
        fieldnames = []
        with mrt_file.parsed_path_json.open() as json_f:
            for line in json_f:
                fieldnames = list(json.loads(line).keys())
                break

        with mrt_file.parsed_path_csv.open("w") as csv_f:
            # NOTE: not using DictWriter because this is much faster
            writer = csv.writer(csv_f)
            # Sometimes JSON files are empty
            if fieldnames:
                writer.writerow(fieldnames)
                with mrt_file.parsed_path_json.open() as json_f:
                    for line in json_f:
                        line_as_dict = json.loads(line)
                        row = [str(line_as_dict[x]) for x in fieldnames]
                        writer.writerow(row)
        os.remove(str(mrt_file.parsed_path_json))


def bgpkit_parser(mrt_file: MRTFile) -> None:
    """Extracts info from raw dumps into parsed path"""

    if not mrt_file.parsed_path_psv.exists():
        check_call(
            f"bgpkit-parser {mrt_file.raw_path} --psv > {mrt_file.parsed_path_psv}",
            shell=True,
        )


################
# Format Funcs #
################


FORMAT_FUNC = Callable[[MRTFile, PrefixOriginMetadata], None]


def format_psv_into_tsv(
    mrt_file: MRTFile, prefix_origin_metadata: PrefixOriginMetadata
) -> None:
    """Formats PSV into a TSV"""

    bgp_dag = CaidaCollector().run(tsv_path=None)
    ixps = bgp_dag.ixp_asns

    count = 0
    # Open all blocks for all files
    mrt_file.formatted_dir.mkdir(parents=True, exist_ok=True)
    block_nums = list(range(prefix_origin_metadata.next_block_id + 1))
    # Must store in the block num, or else if you change the block num and then
    # overwrite, this will have a mix of files and it will be wrong
    format_dir = mrt_file.formatted_dir / str(prefix_origin_metadata.max_block_size)
    if format_dir.exists():
        return
    else:
        format_dir.mkdir(parents=True, exist_ok=True)
    count_file_path: Path = format_dir / "count.txt"

    wfiles = [(format_dir / f"{i}.tsv").open("w") for i in block_nums]

    rfile = mrt_file.parsed_path_psv.open()
    reader = csv.DictReader(rfile, delimiter="|")
    writers = [csv.writer(x, delimiter="\t") for x in wfiles]
    for writer in writers:
        writer.writerow(fieldnames())

    non_public_asns = get_non_public_asns()
    print(mrt_file.url)
    # This is only temporary
    from tqdm import tqdm  # noqa

    # for meta in tqdm(reader):
    for meta in reader:
        # VALIDATION ###
        try:
            prefix_obj = ip_network(meta["prefix"])
        # This occurs whenever host bits are set
        # Should never happen, but might occur sometimes
        except ValueError:
            print("Can't set prefix: {prefix_obj}")
            count += 1
            continue

        assert meta["type"] == "A", f"Not an announcement? {meta}"

        meta = _get_path_data(meta, non_public_asns, bgp_dag, ixps)
        meta.update(
            prefix_origin_metadata.get_meta(
                meta["prefix"], prefix_obj, meta["origin_asn"]
            )
        )
        meta["url"] = mrt_file.url
        # Must use .get here, since not all values return something from bgpstream
        # Really this should be changed to ensure we don't make any typos
        values = [meta.get(x) for x in fieldnames()]
        writers[meta["block_id"]].writerow(values)
        count += 1
        if count % 10000 == 0:
            with count_file_path.open("w") as f:
                f.write(str(count))

    with count_file_path.open("w") as f:
        f.write(str(count))

    for f in wfiles + [rfile]:
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


# Non greedy regex match for AS sets
as_set_re = re.compile("{.+?}")


def _get_path_data(
    meta: dict[str, Any], non_public_asns: frozenset[int], bgp_dag, ixps: set[int]
) -> dict[str, Any]:
    as_path = convert_as_path_str(meta["as_path"])
    as_set_strs = as_set_re.findall(meta["as_path"])

    origin_asn_or_set = as_path[-1]
    if isinstance(origin_asn_or_set, list):
        # Just take the first one. Best guess.
        meta["origin_asn"] = origin_asn_or_set[0]
    elif isinstance(origin_asn_or_set, int):
        meta["origin_asn"] = origin_asn_or_set
    else:
        raise NotImplementedError("Case not accounted for")

    meta["as_sets"] = as_set_strs if as_set_strs else None
    meta["collector_asn"] = as_path[0]
    meta["invalid_as_path_asns"] = list()
    meta["ixps_in_as_path"] = list()
    meta["prepending"] = False
    meta["as_path_loop"] = False
    # TODO: Check these
    meta["valley_free_caida_path"] = True
    # ASNs not part of CAIDA
    meta["non_caida_asns"] = list()
    meta["input_clique_split"] = False
    meta["missing_caida_relationship"] = False
    relationships = list()
    input_clique_asns: set[int] = bgp_dag.asn_groups[ASGroups.INPUT_CLIQUE.value]

    input_clique_asn_in_path = False
    as_path_set = set()
    last_asn = None
    # MUST reverse this, since you must get relationships in order
    # in order to check for route leaks
    for asn_or_set in reversed(as_path):
        # AS SET
        # TODO: Refactor, this is duplicated code in the AS set
        # and in the non as set
        if isinstance(asn_or_set, list):
            for asn in asn_or_set:
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
                if asn not in bgp_dag.as_dict:
                    meta["non_caida_asns"].append(asn)
                    meta["missing_caida_relationship"] = True

                if asn in input_clique_asns:
                    # if this isn't the first input clique ASN
                    if (
                        input_clique_asn_in_path
                        and last_asn is not None
                        and last_asn not in input_clique_asns
                    ):
                        meta["input_clique_split"] = True
                    input_clique_asn_in_path = True

                if (
                    last_asn is not None
                    and asn in bgp_dag.as_dict
                    and last_asn in bgp_dag.as_dict
                ):
                    current_as = bgp_dag.as_dict[asn]
                    last_as = bgp_dag.as_dict[last_asn]
                    # Go left to right
                    # From last asn (origin) to next AS (provider), last as is customer
                    if last_as in current_as.providers:
                        rel = Relationships.CUSTOMERS
                    elif last_as in current_as.customers:
                        rel = Relationships.PROVIDERS
                    elif last_as in current_as.peers:
                        rel = Relationships.PEER
                    else:
                        rel = None
                        meta["missing_caida_relationship"] = True
                    relationships.append(rel)

        else:
            asn = asn_or_set
            assert isinstance(asn, int)
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
            if asn not in bgp_dag.as_dict:
                meta["non_caida_asns"].append(asn)

            if asn in input_clique_asns:
                # if this isn't the first input clique ASN
                if (
                    input_clique_asn_in_path
                    and last_asn is not None
                    and last_asn not in input_clique_asns
                ):
                    meta["input_clique_split"] = True
                input_clique_asn_in_path = True

            if (
                last_asn is not None
                and asn in bgp_dag.as_dict
                and last_asn in bgp_dag.as_dict
            ):
                current_as = bgp_dag.as_dict[asn]
                last_as = bgp_dag.as_dict[last_asn]
                # Go left to right
                # From last asn (origin) to next AS (provider), last as is customer
                if last_as in current_as.providers:
                    rel = Relationships.CUSTOMERS
                elif last_as in current_as.customers:
                    rel = Relationships.PROVIDERS
                elif last_as in current_as.peers:
                    rel = Relationships.PEER
                else:
                    rel = None
                    meta["missing_caida_relationship"] = True
                relationships.append(rel)

    if relationships:
        no_more_customers = False
        no_more_peers = False
        last_relationship = relationships[0]
        if last_relationship == Relationships.PEERS:
            no_more_peers = True

        for relationship in relationships[1:]:
            if no_more_peers and relationship == Relationships.PEERS:
                meta["valley_free_caida_path"] = False
                break
            if no_more_customers and relationship == Relationships.CUSTOMERS:
                meta["valley_free_caida_path"] = False
                break

            if relationship == Relationships.PEERS:
                no_more_peers = True
            if (
                last_relationship == Relationships.CUSTOMERS
                and relationship != last_relationship
            ):
                no_more_customers = True

            last_relationship = relationship

    return meta


def convert_as_path_str(as_path_str: str) -> list[int | list[int]]:
    """Converts as path string to as path

    NOTE: must account for AS sets
    """

    as_path: list[int | list[int]] = list()
    as_set: Optional[list[int]] = None
    for chars in as_path_str.split(" "):
        # Start of AS set
        if "{" in chars:
            as_set = [int(chars.replace("{", ""))]
        # End of AS set
        elif "}" in chars:
            assert as_set
            as_set.append(int(chars.replace("}", "")))
            as_path.append(as_set)
            as_set = None
        # We're in AS set
        elif as_set is not None:
            as_set.append(int(chars.replace("}", "")))
        else:
            as_path.append(int(chars))
    return as_path


def fieldnames() -> tuple[str, ...]:
    return (
        # from bgpkit parser ###
        "aggr_asn",
        "aggr_ip",
        "as_path",
        "atomic",  # true if prefix aggregation occured, even if aggr_asn is False
        "communities",
        # "deprecated",            ############ No longer present
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
        # "unknown",                   ### No longer present
        # Extrapolator ###
        "prefix_id",
        "block_id",
        "block_prefix_id",
        # ROA Validity (from po metadata) ###
        "roa_validity",
        "roa_routed",
        # BGPStream (from po metadata) ###
        "country",
        "start_time",
        "end_time",
        "event_number",
        "event_type",
        "url",
        "hijack_detected_as_path",
        "hijack_detected_by_bgpmon_peers",
        "hijack_detected_origin_name",
        "hijack_detected_origin_number",
        "hijack_expected_origin_name",
        "hijack_expected_origin_number",
        "hijack_expected_prefix",
        "hijack_more_specific_prefix",
        "leak_detected_by_bgpmon_peers",
        "leak_example_as_path",
        "leaked_prefix",
        "leaked_to_name",
        "leaked_to_number",
        "leaker_as_name",
        "leaker_as_number",
        "leak_origin_as_name",
        "leak_origin_as_number",
        "outage_as_name",
        "outage_as_number",
        "outage_number_prefixes_affected",
        "outage_percent_prefixes_affected",
        # AS Path data ###
        "collector_asn",
        "invalid_as_path_asns",
        "ixps_in_as_path",
        "prepending",
        "valley_free_caida_path",
        "non_caida_asns",
        "input_clique_split",
        "as_path_loop",
        "ixps_in_as_path",
        "as_sets",
        "missing_caida_relationship",
        # Other ###
        "url",
    )


def analyze(mrt_file, max_block_size):
    for formatted_path in (mrt_file.formatted_dir / str(max_block_size)).glob("*.tsv"):
        with formatted_path.open() as f:
            reader = csv.DictReader(f)
            print(reader)
