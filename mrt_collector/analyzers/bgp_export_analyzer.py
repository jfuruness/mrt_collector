from collections import defaultdict
from dataclasses import dataclass
import gc
import csv
import json
from pathlib import Path
import time

from tqdm import tqdm
import matplotlib as mpl
import matplotlib.pyplot as plt

from bgpy.as_graphs import CAIDAASGraphConstructor
from ..mrt_file import MRTFile


mpl.use("Agg")


@dataclass(frozen=True)
class NextHopData:
    asn: int
    prepending: bool


# https://stackoverflow.com/a/8230505/8903959
class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


class BGPExportAnalyzer:
    def run(self, mrt_files: tuple[MRTFile, ...]):
        og_start = time.perf_counter()
        start = og_start
        # Aggregates data into {current_asn: {prefix: set_of_next_hops}}
        as_path_data = self.get_as_path_data(mrt_files)
        print("Make the above multiprocessing")
        print(f"got AS path data in {time.perf_counter() - start}")
        start = time.perf_counter()
        as_path_data_w_only_providers = self.remove_non_providers(as_path_data)
        print(f"filtered AS path data in {time.perf_counter() - start}")
        self.create_graphs(as_path_data_w_only_providers)
        print(time.perf_counter() - og_start)

    def get_as_path_data(
        self, mrt_files: tuple[MRTFile, ...]
    ) -> defaultdict[int, defaultdict[str, set[int]]]:
        """Aggregates data into {current_asn: {prefix: set_of_next_hops}}"""

        data = defaultdict(lambda: defaultdict(set))
        total_lines = sum(x.total_parsed_lines for x in mrt_files)
        with tqdm(total=total_lines, desc="Extracting AS-Path data") as pbar:
            for mrt_file in sorted(mrt_files):
                if not mrt_file.parsed_path_psv.exists():
                    continue
                with mrt_file.parsed_path_psv.open() as f:
                    reader = csv.DictReader(f, delimiter="|")
                    for row in reader:
                        pbar.update()
                        if row["type"] == "A":
                            try:
                                as_path = [int(x) for x in row["as_path"].split()]
                            except ValueError:
                                # print("Encountered AS set")
                                continue
                            # print(as_path)
                            reversed_as_path = list(reversed(as_path))
                            prepending = len(set(as_path)) != len(as_path)
                            for i, asn in enumerate(reversed_as_path):
                                try:
                                    next_asn = reversed_as_path[i + 1]
                                    # This will add an empty set to the end
                                    data[asn][row["prefix"]]
                                except IndexError:
                                    break
                                data[asn][row["prefix"]].add(
                                    NextHopData(asn=next_asn, prepending=prepending)
                                )
        return data

    def remove_non_providers(
        self,
        as_path_data: defaultdict[int, defaultdict[str, set[int]]],
    ) -> defaultdict[int, defaultdict[str, set[int]]]:
        filtered_data = defaultdict(lambda: defaultdict(set))
        bgp_dag = CAIDAASGraphConstructor().run()
        for asn, inner_dict in tqdm(
            as_path_data.items(),
            total=len(as_path_data),
            desc="Filtering AS path data",
        ):
            as_obj = bgp_dag.as_dict.get(asn)
            if as_obj is None:
                continue
            provider_asns = as_obj.provider_asns
            if not provider_asns:
                continue
            for prefix, set_of_next_hops in inner_dict.items():
                filtered_data[asn][prefix] = set(
                    [x for x in set_of_next_hops if x.asn in provider_asns]
                )
        return filtered_data

    def create_graphs(
        self,
        filtered_as_path_data: defaultdict[int, defaultdict[str, set[int]]],
    ) -> None:
        total = 0
        total_export_to_some = 0
        total_export_to_some_prefix = 0
        total_export_to_some_prepending = 0
        total_export_to_all = 0
        total_only_one_provider = 0
        bgp_dag = CAIDAASGraphConstructor().run()
        export_to_some_ases = set()
        for asn, prefix_dict in filtered_as_path_data.items():
            total += 1
            provider_lengths = [len(v) for v in prefix_dict.values()]
            prepending = False
            for prefix, set_of_next_hops in prefix_dict.items():
                prepending_list = [x.prepending for x in set_of_next_hops]
                if len(set(prepending_list)) == 2:
                    prepending = True
                    total_export_to_some_prepending += 1
                    total_export_to_some += 1
                    export_to_some_ases.add(asn)
                    break
            assert any(len(x) > 0 for x in provider_lengths), "No providers?"
            if len(set(provider_lengths)) > 1:
                if not prepending:
                    total_export_to_some += 1
                total_export_to_some_prefix += 1
                export_to_some_ases.add(asn)
            if len(bgp_dag.as_dict[asn].provider_asns) == 1:
                total_only_one_provider += 1
            if len(set(provider_lengths)) <= 1:
                total_export_to_all += 1
        with Path("~/Desktop/export_to_some.json").expanduser().open("w") as f:
            json.dump(list(export_to_some_ases), f, indent=4, cls=SetEncoder)

        categories = [
            "Export to Some Prepending",
            "Export to Some Prefix",
            "Export to Some",
            "Export to All (more than one provider)",
            "Only One Provider",
        ]
        values = [
            total_export_to_some_prepending,
            total_export_to_some_prefix,
            total_export_to_some,
            total_export_to_all,
            total_only_one_provider,
        ]
        percentages = [v / total * 100 for v in values]
        fig, ax = plt.subplots()
        ax.set_xticklabels(categories, rotation=90, ha="center")
        bars = ax.bar(categories, percentages, color=["blue", "green", "red"])
        for bar, value in zip(bars, values):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height(),
                str(value),
                ha="center",
                va="bottom",
                fontsize=12,
                fontweight="bold",
            )
        ax.set_ylabel("Percentage (%)")
        ax.set_title("Graph of Exporting Behaviors")
        ax.set_ylim(0, 100)
        plt.tight_layout()
        plt.savefig(Path("~/Desktop/export_graphs.png").expanduser())
        # https://stackoverflow.com/a/33343289/8903959
        ax.cla()
        plt.cla()
        plt.clf()
        # If you just close the fig, on machines with many CPUs and trials,
        # there is some sort of a memory leak that occurs. See stackoverflow
        # comment above
        plt.close(fig)
        gc.collect()
