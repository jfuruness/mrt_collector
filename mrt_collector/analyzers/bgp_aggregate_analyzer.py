import json
import ipaddress
from collections import defaultdict
from pathlib import Path
from typing import Optional

import pytricia

from .export_analyzer import ExportAnalyzer


class BGPAggregateAnalyzer(ExportAnalyzer):
    """
    Comprehensive BGP aggregation analyzer that performs five tasks:

    Task 0: Collect all /0 default route announcements per source, recording
            AS path length (distance to collector) and the full announcement line.

    Task 1: Compute what percentage of IPv4 and IPv6 address space is covered
            by structural aggregates (prefixes that have more-specifics also
            announced). Uses "space that has an aggregate over it" definition.
            Ignores /0 prefixes.

    Task 2: For each structural aggregate, check whether the origin AS of the
            aggregate matches the origin AS(es) of its sub-prefixes. Report the
            percentage of aggregates announced by a non-owner.

    Task 3: Cross-reference aggregated vs non-aggregated prefixes against ROA
            data. Count how many ROA-covered sub-prefixes are swallowed by an
            aggregate lacking a ROA, and how many of those aggregates can be
            cleanly decomposed back into their ROA-covered sub-prefixes.

    Task 4: Find prefixes that have a ROA but are NOT announced, where a
            super-prefix IS announced without a ROA. Count occurrences.

    IMPORTANT: The base class ExportAnalyzer.get_data() currently has a bug on
    the line `if row["type"] == "A": continue` — this skips announcements
    instead of keeping them. This should be `if row["type"] != "A": continue`.
    This subclass assumes that bug is fixed and that analyze() only receives
    announcement ("A") rows.

    ROA data must be loaded separately before calling run(), via load_roas().
    """

    def __init__(
        self,
        base_dir: Path
    ) -> None:

        super().__init__(base_dir)
        self.desc = "Analyzing BGP aggregation across all tasks"

        # ── Current MRT file tracking ──
        # Set by overridden get_data so analyze() knows the source
        self._current_source: str = ""

        # ── Task 0: Default route collection ──
        # {source_name: [announcement_line, ...]}
        self.default_routes: dict[str, list[str]] = defaultdict(list)

        # ── Tasks 1-4: Prefix data structures ──
        # Separate tries for IPv4 and IPv6
        # Each trie maps prefix -> set of origin ASNs seen announcing it
        self.v4_trie: pytricia.PyTricia = pytricia.PyTricia()
        self.v6_trie: pytricia.PyTricia = pytricia.PyTricia(128)

        # Track all announced prefixes with their origin ASNs
        # {prefix_str: set(origin_asn_str, ...)}
        self.prefix_origins: dict[str, set[str]] = defaultdict(set)

        # Track atomic/aggregator flagged prefixes separately
        # (for the complementary attribute-based detection)
        self.flagged_aggregates: set[str] = set()

        # ── Task 3 & 4: ROA data ──
        # Must be loaded before run() via load_roas()
        # {prefix_str: set(authorized_asn_str, ...)}
        self.roa_entries: dict[str, set[str]] = {}
        self._roas_loaded: bool = False

    # ──────────────────────────────────────────────
    # Override get_data to track current source file
    # ──────────────────────────────────────────────

    def get_data(
        self,
        mrt_files
    ) -> None:
        """
        Overrides parent to track which MRT file is being processed,
        so analyze() can attribute /0 routes to their source.
        """
        from mrt_collector.mrt_collector import sort_mrt_files_by_parsed_file_size
        from tqdm import tqdm
        import csv

        total_lines = sum(x.total_parsed_lines for x in mrt_files)

        self.load_roas(Path(__file__).parent / "roas.json")

        with tqdm(
            total=total_lines,
            desc=self.desc
        ) as pbar:
            for mrt_file in mrt_files:
                if mrt_file.parsed_path_psv.exists():
                    # Extract source name from filename
                    self._current_source = mrt_file.parsed_path_psv.stem

                    with mrt_file.parsed_path_psv.open() as f:
                        reader = csv.DictReader(f, delimiter="|")
                        for row in reader:
                            pbar.update()
                            if row["type"] != "A":
                                continue
                            self.analyze(row)

    # ──────────────────────────────────────────────
    # Core analysis — called once per announcement row
    # ──────────────────────────────────────────────

    def analyze(
        self,
        row: dict[str, ...]
    ) -> None:
        """Process a single BGP announcement row across all tasks."""

        prefix = row["prefix"]
        as_path = row.get("as_path", "")
        origin_asns = row.get("origin_asns", "")
        atomic = row.get("atomic", "") == "true"
        aggr_asn = row.get("aggr_asn", "")

        # ── Task 0: Collect /0 default routes ──
        if prefix in ("0.0.0.0/0", "::/0"):
            line = self._reconstruct_line(row)
            self.default_routes[self._current_source].append(line)
            # ignore /0s for aggregation analysis
            return

        # ── Populate prefix trie and origin tracking ──
        trie = self._get_trie(prefix)
        if trie is None:
            return  # malformed prefix, skip

        # Store origin ASN(s) for this prefix
        if origin_asns:
            for asn in origin_asns.split():
                self.prefix_origins[prefix].add(asn)

        # Insert into the appropriate trie
        # Value: True (we just need presence; origin data is in prefix_origins)
        if prefix not in trie:
            trie[prefix] = True

        # ── Track attribute-flagged aggregates ──
        if atomic or aggr_asn:
            self.flagged_aggregates.add(prefix)

    # ──────────────────────────────────────────────
    # ROA loading (call before run())
    # ──────────────────────────────────────────────

    def load_roas(
        self,
        roa_filepath: Path
    ) -> None:
        """
        Load ROA/VRP data from a JSON file.

        Expected format (e.g., from rpki-client or Routinator export):
        {
            "roas": [
                {"prefix": "10.0.0.0/8", "asn": "AS12345", "maxLength": 24},
                ...
            ]
        }

        Adjust parsing below to match your actual ROA dump format.
        """
        self.roa_entries = defaultdict(set)

        with open(roa_filepath) as f:
            data = json.load(f)

        for entry in data.get("roas", []):
            prefix = entry["prefix"]
            # Strip "AS" prefix if present
            asn = str(entry["asn"]).replace("AS", "").replace("as", "")
            self.roa_entries[prefix].add(asn)

        self._roas_loaded = True

    # ──────────────────────────────────────────────
    # Post-processing: run after get_data completes
    # ──────────────────────────────────────────────

    def run(
        self,
        mrt_files
    ) -> None:
        """Override run to insert post-processing between data collection and dump."""
        from mrt_collector.mrt_collector import sort_mrt_files_by_parsed_file_size

        mrt_files = sort_mrt_files_by_parsed_file_size(mrt_files)
        self.get_data(mrt_files)
        self._results = self._compute_all_tasks()
        self.dump_json()

    def _compute_all_tasks(self) -> dict:
        """
        After all MRT rows have been processed, compute results for all tasks
        using the populated tries and prefix data.
        """
        results = {}

        results["task0_default_routes"] = self._task0_default_routes()
        results["task1_aggregated_space"] = self._task1_aggregated_space()
        results["task2_non_owner_aggregation"] = self._task2_non_owner_aggregation()

        if self._roas_loaded:
            results["task3_roa_coverage_gaps"] = self._task3_roa_coverage_gaps()
            results["task4_unannounced_roa_superprefixes"] = (
                self._task4_unannounced_roa_superprefixes()
            )
        else:
            results["task3_roa_coverage_gaps"] = "ROA data not loaded — skipped"
            results["task4_unannounced_roa_superprefixes"] = (
                "ROA data not loaded — skipped"
            )

        return results

    # ──────────────────────────────────────────────
    # Task 0: Default routes
    # ──────────────────────────────────────────────

    def _task0_default_routes(self) -> dict:
        """Summarize /0 announcements per source with AS path lengths."""
        summary = {}
        for source, lines in self.default_routes.items():
            entries = []
            for line in lines:
                parts = line.split("|")
                # parts layout: A|ts|peer_ip|peer_asn|prefix|as_path|...
                as_path = parts[5] if len(parts) > 5 else ""
                hops = len(as_path.split()) if as_path else 0
                entries.append({
                    "line": line,
                    "as_path": as_path,
                    "as_path_length": hops
                })
            summary[source] = {
                "count": len(lines),
                "entries": entries
            }
        return summary

    # ──────────────────────────────────────────────
    # Task 1: Percentage of IP space aggregated
    # ──────────────────────────────────────────────

    def _task1_aggregated_space(self) -> dict:
        """
        Compute percentage of IPv4/IPv6 space covered by structural aggregates.
        A structural aggregate is any prefix in the trie that has at least one
        more-specific also present in the trie.

        Uses "space that has an aggregate over it" definition: the full address
        range of every aggregate counts, regardless of coexisting more-specifics.
        """
        v4_agg_addresses = 0
        v6_agg_addresses = 0
        v4_total_announced = 0
        v6_total_announced = 0
        v4_aggregate_count = 0
        v6_aggregate_count = 0

        # IPv4
        for prefix in self.v4_trie:
            net = ipaddress.ip_network(prefix, strict=False)
            num = net.num_addresses
            v4_total_announced += num

            children = self.v4_trie.children(prefix)
            if children:
                v4_agg_addresses += num
                v4_aggregate_count += 1

        # IPv6
        for prefix in self.v6_trie:
            net = ipaddress.ip_network(prefix, strict=False)
            num = net.num_addresses
            v6_total_announced += num

            children = self.v6_trie.children(prefix)
            if children:
                v6_agg_addresses += num
                v6_aggregate_count += 1

        total_v4 = 2**32
        total_v6 = 2**128

        return {
            "ipv4": {
                "aggregated_addresses": v4_agg_addresses,
                "total_announced_addresses": v4_total_announced,
                "pct_of_announced": (
                    (v4_agg_addresses / v4_total_announced * 100)
                    if v4_total_announced else 0
                ),
                "pct_of_total_space": v4_agg_addresses / total_v4 * 100,
                "aggregate_prefix_count": v4_aggregate_count,
            },
            "ipv6": {
                "aggregated_addresses": str(v6_agg_addresses),
                "total_announced_addresses": str(v6_total_announced),
                "pct_of_announced": (
                    (v6_agg_addresses / v6_total_announced * 100)
                    if v6_total_announced else 0
                ),
                "pct_of_total_space": float(v6_agg_addresses / total_v6 * 100),
                "aggregate_prefix_count": v6_aggregate_count,
            },
            "flagged_aggregate_count": len(self.flagged_aggregates),
            "note": (
                "Definition: 'space that has an aggregate over it'. "
                "/0 prefixes excluded per professor instruction."
            ),
        }

    # ──────────────────────────────────────────────
    # Task 2: Aggregation by non-owners
    # ──────────────────────────────────────────────

    def _task2_non_owner_aggregation(self) -> dict:
        """
        For each structural aggregate, compare its origin AS(es) against the
        origin AS(es) of its immediate children. If there's no overlap, the
        aggregate is announced by a non-owner.

        This is the origin-AS comparison approach (cheap first pass).
        A stronger check would use RIR delegation or ROA data.
        """
        total_aggregates = 0
        non_owner_aggregates = 0
        non_owner_details = []

        for trie in (self.v4_trie, self.v6_trie):
            for prefix in trie:
                children = trie.children(prefix)
                if not children:
                    continue  # not a structural aggregate

                total_aggregates += 1

                agg_origins = self.prefix_origins.get(prefix, set())
                if not agg_origins:
                    continue

                # Collect all origin ASNs from immediate children
                child_origins = set()
                for child in children:
                    child_origins.update(
                        self.prefix_origins.get(child, set())
                    )

                # Non-owner if the aggregate's origins don't overlap
                # with any of its children's origins
                if not agg_origins & child_origins:
                    non_owner_aggregates += 1
                    non_owner_details.append({
                        "aggregate_prefix": prefix,
                        "aggregate_origins": sorted(agg_origins),
                        "child_origins": sorted(child_origins),
                    })

        return {
            "total_structural_aggregates": total_aggregates,
            "non_owner_aggregates": non_owner_aggregates,
            "pct_non_owner": (
                (non_owner_aggregates / total_aggregates * 100)
                if total_aggregates else 0
            ),
            "non_owner_details": non_owner_details,
            "note": (
                "Ownership determined by origin AS overlap between aggregate "
                "and its immediate children. Stronger check: use RIPEstat or "
                "RIR delegation data."
            ),
        }

    # ──────────────────────────────────────────────
    # Task 3: ROA coverage gaps in aggregation
    # ──────────────────────────────────────────────

    def _task3_roa_coverage_gaps(self) -> dict:
        """
        Find structural aggregates that lack a ROA but contain sub-prefixes
        that do have ROAs. Also check whether the ROA-covered children
        cleanly tile the aggregate (can it be decomposed without gaps).
        """
        roa_prefixes = set(self.roa_entries.keys())

        total_aggregates_without_roa = 0
        aggregates_swallowing_roa_children = 0
        cleanly_decomposable = 0
        gap_details = []

        for trie in (self.v4_trie, self.v6_trie):
            for prefix in trie:
                children = trie.children(prefix)
                if not children:
                    continue  # not an aggregate

                # Check if aggregate itself has a ROA
                if prefix in roa_prefixes:
                    continue  # aggregate is ROA-protected, skip

                total_aggregates_without_roa += 1

                # Find children (at any depth) that have ROAs
                roa_children = []
                self._collect_roa_descendants(trie, prefix, roa_prefixes, roa_children)

                if not roa_children:
                    continue

                aggregates_swallowing_roa_children += 1

                # Check tiling: do the ROA-covered descendants cleanly
                # cover the aggregate's address space?
                agg_net = ipaddress.ip_network(prefix, strict=False)
                agg_size = agg_net.num_addresses

                child_coverage = 0
                for child_pfx in roa_children:
                    child_net = ipaddress.ip_network(child_pfx, strict=False)
                    child_coverage += child_net.num_addresses

                coverage_pct = (child_coverage / agg_size * 100) if agg_size else 0
                is_clean = coverage_pct >= 99.9  # near-100% tiling

                if is_clean:
                    cleanly_decomposable += 1

                gap_details.append({
                    "aggregate": prefix,
                    "roa_children_count": len(roa_children),
                    "coverage_pct": round(coverage_pct, 2),
                    "cleanly_decomposable": is_clean,
                })

        return {
            "aggregates_without_roa": total_aggregates_without_roa,
            "aggregates_swallowing_roa_children": aggregates_swallowing_roa_children,
            "cleanly_decomposable": cleanly_decomposable,
            "pct_swallowing": (
                (aggregates_swallowing_roa_children / total_aggregates_without_roa * 100)
                if total_aggregates_without_roa else 0
            ),
            "details": gap_details,
        }

    def _collect_roa_descendants(
        self,
        trie: pytricia.PyTricia,
        prefix: str,
        roa_prefixes: set[str],
        result: list[str]
    ) -> None:
        """
        Recursively collect all descendants of `prefix` in the trie
        that have a ROA. Stops descending past a ROA-covered node
        to avoid double-counting nested ROA coverage.
        """
        children = trie.children(prefix)
        for child in children:
            if child in roa_prefixes:
                result.append(child)
                # Don't descend further — this child's space is covered
            else:
                # Keep looking deeper
                self._collect_roa_descendants(trie, child, roa_prefixes, result)

    # ──────────────────────────────────────────────
    # Task 4: ROA'd but unannounced, super-prefix without ROA
    # ──────────────────────────────────────────────

    def _task4_unannounced_roa_superprefixes(self) -> dict:
        """
        Find prefixes that have a ROA but are NOT announced in BGP,
        where a covering super-prefix IS announced without a ROA.

        This is the SAV incentive pattern.
        """
        roa_prefixes = set(self.roa_entries.keys())

        # Build set of all announced prefixes for fast lookup
        announced = set()
        for prefix in self.v4_trie:
            announced.add(prefix)
        for prefix in self.v6_trie:
            announced.add(prefix)

        occurrences = []
        affected_asns = set()
        affected_address_count_v4 = 0
        affected_address_count_v6 = 0

        for roa_prefix in roa_prefixes:
            if roa_prefix in announced:
                continue  # It's announced, skip

            # ROA'd prefix is NOT announced — look for a covering super-prefix
            trie = self._get_trie(roa_prefix)
            if trie is None:
                continue

            # Find the closest announced ancestor
            try:
                parent = trie.parent(roa_prefix)
            except KeyError:
                parent = None

            if parent is None:
                continue  # No covering announcement at all

            # Check if that parent has a ROA
            if parent in roa_prefixes:
                continue  # Parent is ROA-covered, not the pattern we want

            # Found the pattern: ROA'd prefix not announced,
            # super-prefix announced without ROA
            net = ipaddress.ip_network(roa_prefix, strict=False)
            roa_asns = self.roa_entries.get(roa_prefix, set())
            parent_origins = self.prefix_origins.get(parent, set())

            occurrences.append({
                "roa_prefix": roa_prefix,
                "roa_authorized_asns": sorted(roa_asns),
                "covering_superprefix": parent,
                "superprefix_origins": sorted(parent_origins),
            })

            affected_asns.update(roa_asns)
            if net.version == 4:
                affected_address_count_v4 += net.num_addresses
            else:
                affected_address_count_v6 += net.num_addresses

        return {
            "total_occurrences": len(occurrences),
            "affected_distinct_asns": len(affected_asns),
            "affected_v4_addresses": affected_address_count_v4,
            "affected_v6_addresses": str(affected_address_count_v6),
            "occurrences": occurrences,
            "note": (
                "Each occurrence is a ROA-holding prefix not announced in BGP, "
                "covered by an announced super-prefix that lacks a ROA. "
                "This pattern creates a direct SAV deployment incentive for "
                "the ROA holder."
            ),
        }

    # ──────────────────────────────────────────────
    # JSON output
    # ──────────────────────────────────────────────

    def dump_json(self) -> None:
        """Dump all task results to JSON files."""

        output_dir = self.base_dir / "analysis"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Full results
        filepath = output_dir / "bgp_aggregate_analysis.json"
        with open(filepath, "w") as f:
            json.dump(self._results, f, indent=4, default=str)

        # Task 0 separately (since it can be large)
        t0_path = output_dir / "default_routes.json"
        with open(t0_path, "w") as f:
            json.dump(
                self._results.get("task0_default_routes", {}),
                f, indent=4, default=str
            )

    # ──────────────────────────────────────────────
    # Utility methods
    # ──────────────────────────────────────────────

    def _get_trie(
        self,
        prefix: str
    ) -> Optional[pytricia.PyTricia]:
        """Return the correct trie (v4 or v6) for a given prefix string."""
        try:
            net = ipaddress.ip_network(prefix, strict=False)
        except ValueError:
            return None

        if net.version == 4:
            return self.v4_trie
        else:
            return self.v6_trie

    @staticmethod
    def _reconstruct_line(row: dict) -> str:
        """Reconstruct a PSV announcement line from a parsed dict row."""
        fields = [
            "type", "timestamp", "peer_ip", "peer_asn", "prefix",
            "as_path", "origin_asns", "origin", "next_hop", "local_pref",
            "med", "communities", "atomic", "aggr_asn", "aggr_ip",
            "only_to_customer"
        ]
        return "|".join(row.get(f, "") for f in fields)

    # ──────────────────────────────────────────────
    # Output path properties
    # ──────────────────────────────────────────────

    @property
    def json_analysis_path(self) -> Path:
        return self.base_dir / "analysis" / "bgp_aggregate_analysis.json"

    @property
    def json_default_routes_path(self) -> Path:
        return self.base_dir / "analysis" / "default_routes.json"
