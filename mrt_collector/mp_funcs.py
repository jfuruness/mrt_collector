"""This file contains functions used when multiprocessing"""

import csv
from ipaddress import ip_network
from pathlib import Path
from pprint import pprint
import json
import os
import re
from subprocess import check_call
import subprocess
from typing import Any, Callable, Optional
import typing

from bgpy.caida_collector import CaidaCollector
from bgpy.enums import Relationships, ASGroups
from roa_checker import ROARouted, ROAValidity

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
    mrt_file: MRTFile,
    prefix_origin_metadata: PrefixOriginMetadata,
    single_proc: bool = False,
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

    if single_proc:
        # Define the command to be executed
        command = f"wc -l {mrt_file.parsed_path_psv}"

        # Run the command
        result = subprocess.run(command, shell=True, text=True, capture_output=True)

        # Check if the command was successful
        if result.returncode != 0:
            print("Error running command:", result.stderr)
            raise Exception

        # Process the output to get the total number of lines
        output = result.stdout.strip()
        lines = output.split("\n")
        total_lines = int(lines[-1].strip().split(" ")[0])
        iterable = tqdm(reader, total=total_lines, desc="Formatting file")
    else:
        iterable = reader
    # for meta in tqdm(reader):
    for meta in iterable:
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

        if not meta["as_path"]:
            print("missing_as_path")
            meta["url"] = mrt_file.url
            pprint(meta)
            continue

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
                assert isinstance(asn, int)
                if asn in non_public_asns or asn > MAX_ASN:
                    meta["invalid_as_path_asns"].append(asn)
                if last_asn == asn:
                    meta["prepending"] = True
                    meta["as_path_loop"] = True
                elif asn in as_path_set:
                    meta["as_path_loop"] = True
                as_path_set.add(asn)

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
                    # Don't do this if there's prepending
                    and last_asn != asn
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
                        rel = Relationships.PEERS
                    else:
                        rel = None
                        meta["missing_caida_relationship"] = True
                    relationships.append(rel)

                last_asn = asn

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
                and last_asn != asn
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
                    rel = Relationships.PEERS
                else:
                    rel = None
                    meta["missing_caida_relationship"] = True
                # print("")
                # print("")
                # print("")
                # print(rel)
                # print(list(reversed(as_path)))
                # print(f"current asn of {current_as.asn}")
                # print(f"current as providers {[x.asn for x in current_as.providers]}")
                # print(f"current as peers {[x.asn for x in current_as.peers]}")
                # print(f"current as customers {[x.asn for x in current_as.customers]}")
                # print(f"last is {last_asn}")

                relationships.append(rel)

            last_asn = asn

    # Remove None from this
    relationships = [x for x in relationships if x is not None]
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
    try:
        # Normal ASNs are separated by spaces. AS sets are: {1,2,3}
        for chars in as_path_str.replace(",", " ").split(" "):
            # Start of AS set
            if "{" in chars:
                as_set = [int(chars.replace("{", "").replace("}", ""))]
                # Must account for the case of a single ASN
                if "}" in chars:
                    as_path.append(as_set)
                    as_set = None
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
    except ValueError as e:
        raise ValueError(f"{as_path_str} {e}")
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
        "bgpstream_url",
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


# TODO: Fix this later. split stats into sets and ints
@typing.no_type_check
def analyze(mrt_file, max_block_size, single_proc: bool = False):
    count_file_path = Path(str(mrt_file.analysis_path).replace(".json", "_count.txt"))

    stats: dict[str, int | set[Any]] = {
        "ann_not_covered_by_roa": 0,
        "ann_covered_by_roa": 0,
        "ann_valid_by_roa": 0,
        "ann_invalid_by_roa": 0,
        "ann_invalid_by_routed_roa": 0,
        "ann_invalid_by_length_routed_roa": 0,
        "ann_invalid_by_origin_routed_roa": 0,
        "ann_invalid_by_origin_routed_and_length_roa": 0,
        # non routed
        "ann_invalid_by_non_routed_roa": 0,
        "ann_invalid_by_length_non_routed_roa": 0,
        "ann_invalid_by_origin_non_routed_roa": 0,
        "ann_invalid_by_origin_and_length_non_routed_roa": 0,
        # bgpstream
        "hijacker_ann_on_bgpstream_hijacks": 0,
        "hijacker_ann_on_bgpstream_hijacks_set": set(),
        "hijacker_ann_invalid_by_roa_and_on_bgpstream_hijacks": 0,
        "hijacker_ann_invalid_by_roa_and_on_bgpstream_hijacks_set": set(),
        "hijacker_ann_valid_by_roa_and_on_bgpstream_hijacks": 0,
        "hijacker_ann_valid_by_roa_and_on_bgpstream_hijacks_set": set(),
        "hijacker_ann_not_covered_by_roa_and_on_bgpstream_hijacks": 0,
        "hijacker_ann_not_covered_by_roa_and_on_bgpstream_hijacks_set": set(),
        # Leaks
        "ann_on_bgpstream_route_leaks": 0,
        "ann_on_bgpstream_route_leaks_set": set(),
        "ann_on_bgpstream_route_leaks_and_not_caida_valley_free": 0,
        "ann_on_bgpstream_route_leaks_and_not_caida_valley_free_set": set(),
        # Prefix aggregation
        "aggregator_asns": 0,
        "aggregator_asns_set": set(),
        "ann_atomic": 0,
        # community is a tuple of ASN that set it, and community_attr
        "communities": 0,
        "communities_set": set(),
        "local_pref_set": set(),
        "only_to_customer": 0,
        "only_to_customer_set": set(),
        # Origin
        "origin_is_ibgp": 0,
        "origin_is_ebgp": 0,
        "origin_is_incomplete": 0,
        "invalid_as_path_asns": 0,
        "invalid_as_path_asns_set": set(),
        "ixps_in_as_path": 0,
        "ixps_in_as_path_set": set(),
        "prepending": 0,
        "not_valley_free_caida_path": 0,
        "non_caida_asns": 0,
        "non_caida_asns_set": set(),
        "input_clique_split": 0,
        "as_path_loop": 0,
        "as_s_ets": 0,
        "as_s_ets_set": set(),
        "missing_caida_relationship": 0,
        "missing_as_path": 0,
        "total_anns": 0,
    }
    for formatted_path in (mrt_file.formatted_dir / str(max_block_size)).glob("*.tsv"):
        with formatted_path.open() as f:
            for row in csv.DictReader(f, delimiter="\t"):
                ################
                # ROA Validity #
                ################

                roa_validity = row["roa_validity"]
                if roa_validity == str(ROAValidity.UNKNOWN.value):
                    stats["ann_not_covered_by_roa"] += 1
                elif roa_validity == str(ROAValidity.VALID.value):
                    stats["ann_covered_by_roa"] += 1
                    stats["ann_valid_by_roa"] += 1
                elif roa_validity in (
                    str(ROAValidity.INVALID_LENGTH.value),
                    str(ROAValidity.INVALID_ORIGIN.value),
                    str(ROAValidity.INVALID_LENGTH_AND_ORIGIN.value),
                ):
                    stats["ann_covered_by_roa"] += 1
                    stats["ann_invalid_by_roa"] += 1
                    if row["roa_routed"] == str(ROARouted.ROUTED.value):
                        stats["ann_invalid_by_routed_roa"] += 1
                        if roa_validity == str(ROAValidity.INVALID_LENGTH.value):
                            stats["ann_invalid_by_length_routed_roa"] == 1
                        elif roa_validity == str(ROAValidity.INVALID_ORIGIN.value):
                            stats["ann_invalid_by_origin_routed_roa"] += 1
                        elif (
                            roa_validity == str(ROAValidity.INVALID_LENGTH_AND_ORIGIN.value)
                        ):
                            stats["ann_invalid_by_origin_routed_and_length_roa"] += 1
                    elif row["roa_routed"] == str(ROARouted.NON_ROUTED.value):
                        stats["ann_invalid_by_non_routed_roa"] += 1
                        if roa_validity == str(ROAValidity.INVALID_LENGTH.value):
                            stats["ann_invalid_by_length_non_routed_roa"] == 1
                        elif roa_validity == str(ROAValidity.INVALID_ORIGIN.value):
                            stats["ann_invalid_by_origin_non_routed_roa"] += 1
                        elif (
                            roa_validity == str(ROAValidity.INVALID_LENGTH_AND_ORIGIN.value)
                        ):
                            stats[
                                "ann_invalid_by_origin_and_length_non_routed_roa"
                            ] += 1
                    else:
                        raise NotImplementedError("this should never happen")
                else:
                    raise NotImplementedError(f"This should never happen {roa_validity}")

                #############
                # BGPStream #
                #############
                if (
                    row["bgpstream_url"]
                    # We don't care about outages
                    and (
                        row["hijack_detected_origin_number"]
                        # We don't care if it's the victim's ann
                        and row["hijack_detected_origin_number"] in row["origin_asns"]
                    )
                ):
                    stats["hijacker_ann_on_bgpstream_hijacks"] += 1
                    stats["hijacker_ann_on_bgpstream_hijacks_set"].add(row["bgpstream_url"])
                    if row["roa_validity"] == ROAValidity.VALID.value:
                        stats["hijacker_ann_valid_by_roa_and_on_bgpstream_hijacks"] += 1
                        stats["hijacker_ann_valid_by_roa_and_on_bgpstream_hijacks_set"].add(
                            row["bgpstream_url"]
                        )
                    elif row["roa_validity"] == ROAValidity.UNKNOWN.value:
                        stats["hijacker_ann_not_covered_by_roa_and_on_bgpstream_hijacks"] += 1
                        stats["hijacker_ann_not_covered_by_roa_and_on_bgpstream_hijacks_set"] += 1
                    else:
                        stats["hijacker_ann_invalid_by_roa_and_on_bgpstream_hijacks"] += 1
                        stats["hijacker_ann_invalid_by_roa_and_on_bgpstream_hijacks_set"].add(
                            row["bgpstream_url"]
                        )

                if (row["bgpstream_url"]
                    and row["leaked_prefix"]
                        and row["leaker_as_number"] in row["as_path"]):

                    stats["ann_on_bgpstream_route_leaks"] += 1
                    stats["ann_on_bgpstream_route_leaks_set"].add(row["bgpstream_url"])
                    if row["valley_free_caida_path"] == "False":
                        stats["ann_on_bgpstream_route_leaks_and_not_caida_valley_free"] += 1
                        stats["ann_on_bgpstream_route_leaks_and_not_caida_valley_free_set"].add(
                            row["bgpstream_url"]
                        )

                ######################
                # Prefix Aggregation #
                ######################
                if row["aggr_asn"]:
                    stats["aggregator_asns"] += 1
                    stats["aggregator_asns_set"].add(row["aggr_asn"])
                if row["atomic"] == "true":
                    stats["ann_atomic"] += 1
                elif row["atomic"] == "false":
                    pass
                else:
                    raise NotImplementedError("Should never happen")

                ###############
                # Communities #
                ###############
                if row["communities"]:
                    stats["communities"] += 1
                    stats["communities_set"].update(row["communities"].split(" "))
                if row["only_to_customer"]:
                    stats["only_to_customer"] += 1
                    # THE ORIGINATOR OF THE COMMUNITY!!!!
                    stats["only_to_customer_set"].add(row["only_to_customer"])

                ########
                # Misc #
                ########
                stats["local_pref_set"].add(row["local_pref"])
                stats["total_anns"] += 1
                if row["origin"].lower() in ["ibgp", "igp"]:
                    stats["origin_is_ibgp"] += 1
                elif row["origin"].lower() in ["ebgp", "egp"]:
                    stats["origin_is_ebgp"] += 1
                elif row["origin"].lower() in ["incomplete"]:
                    stats["origin_is_incomplete"] += 1
                else:
                    raise NotImplementedError(str(row["origin"]))

                ###########
                # AS Path #
                ###########
                if row["invalid_as_path_asns"] not in ["[]", "", None]:
                    stats["invalid_as_path_asns"] += 1
                    stats["invalid_as_path_asns_set"].update(
                        row["invalid_as_path_asns"][1:-1].split(",")
                    )
                if row["ixps_in_as_path"] not in ["[]", "", None]:
                    stats["ixps_in_as_path"] += 1
                    stats["ixps_in_as_path_set"].update(row["ixps_in_as_path"][1:-1].split(","))

                if row["prepending"] == "True":
                    stats["prepending"] += 1
                elif row["prepending"] == "False":
                    pass
                else:
                    raise NotImplementedError

                if row["valley_free_caida_path"] == "True":
                    pass
                elif row["valley_free_caida_path"] == "False":
                    stats["not_valley_free_caida_path"] += 1
                else:
                    raise NotImplementedError

                if row["non_caida_asns"] not in ["[]", "", None]:
                    stats["non_caida_asns"] += 1
                    stats["non_caida_asns_set"].update(row["non_caida_asns"][1:-1].split(","))

                if row["input_clique_split"] == "True":
                    stats["input_clique_split"] += 1
                elif row["input_clique_split"] == "False":
                    pass
                else:
                    raise NotImplementedError

                if row["as_path_loop"] == "True":
                    stats["as_path_loop"] += 1
                elif row["as_path_loop"] == "False":
                    pass
                else:
                    raise NotImplementedError

                if row["as_sets"] not in ["[]", "", None]:
                    # Remove brackets []; remove single quotes
                    # Then split based on ', '
                    # Can' split just on comma, since that includes the AS
                    # set itself
                    # but you can split on ', ' which separates each AS set
                    # from the other AS sets
                    as_sets = row["as_sets"][1:-1].replace("'", "").split(", ")
                    stats["as_s_ets"] += 1
                    stats["as_s_ets_set"].update(as_sets)

                if row["missing_caida_relationship"] == "True":
                    stats["missing_caida_relationship"] += 1
                elif row["missing_caida_relationship"] == "False":
                    pass
                else:
                    raise NotImplementedError

                if stats["total_anns"] % 10000 == 0:
                    with count_file_path.open("w") as f:
                        f.write(str(stats["total_anns"]))

    with count_file_path.open("w") as f:
        f.write(str(stats["total_anns"]))

    with mrt_file.analysis_path.open("w") as f:
        # https://stackoverflow.com/a/22281062/8903959
        def set_default(obj):
            if isinstance(obj, set):
                return list(obj)
            raise TypeError

        json.dump(stats, f, indent=4, default=set_default)
