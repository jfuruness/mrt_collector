import gc
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
from bgpy.as_graphs import CAIDAASGraphConstructor
from tqdm import tqdm

from mrt_collector.mrt_file import MRTFile

from .export_analyzer import ExportAnalyzer
from .json_set_encoder import JSONSetEncoder as SetEncoder

mpl.use("Agg")


@dataclass(frozen=True)
class NextHopData:
    asn: int
    prepending: bool

class BGPExportAnalyzer(ExportAnalyzer):
    def __init__(
        self,
        base_dir: Path
    ) -> None:

        super().__init__(base_dir)
        self.desc = "Extracting AS-Path data"
        self.bgp_data = defaultdict(lambda: defaultdict(set))

    def run(self, mrt_files: tuple[MRTFile, ...]):
        super().run(mrt_files)
        as_path_data_w_only_providers = self.remove_non_providers(self.bgp_data)
        self.create_graphs(as_path_data_w_only_providers)

    def analyze(
        self,
        row: dict[str, ...]
    ) -> None:

        try:
            as_path = [int(x) for x in row["as_path"].split()]
        except ValueError:
            # print("Encountered AS set")
            return
        # print(as_path)
        reversed_as_path = list(reversed(as_path))
        prepending = len(set(as_path)) != len(as_path)
        for i, asn in enumerate(reversed_as_path):
            try:
                next_asn = reversed_as_path[i + 1]
                # This will add an empty set to the end
                self.bgp_data[asn][row["prefix"]]
            except IndexError:
                break
            self.bgp_data[asn][row["prefix"]].add(
                NextHopData(asn=next_asn, prepending=prepending)
            )

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
                filtered_data[asn][prefix] = {
                    x for x in set_of_next_hops if x.asn in provider_asns
                }
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
            for prefix, set_of_next_hops in prefix_dict.items():  # noqa
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
        for bar, value in zip(bars, values, strict=False):
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

