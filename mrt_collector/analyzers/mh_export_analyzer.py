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
class PrefixData:
    prefix: str
    prepending: bool


# https://stackoverflow.com/a/8230505/8903959
class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


class MHExportAnalyzer:
    def run(self, mrt_files: tuple[MRTFile, ...]):
        print("This takes about an hour")
        og_start = time.perf_counter()
        start = og_start
        # mh_data = self._init_data()
        # Aggregates data into {current_asn: {provider_asn: {set of prefix data}}
        print("Turn this back on")
        # mh_data = self.get_mh_data(mrt_files, mh_data)
        print("Make the above multiprocessing")
        print(f"got AS path data in {time.perf_counter() - start}")
        start = time.perf_counter()
        print("Turn this back on")
        # self.dump_json(mh_data)
        self.create_graphs()
        print(f"got graph data in {time.perf_counter() - start}")
        print(time.perf_counter() - og_start)

    def _init_data(self):
        bgp_dag = CAIDAASGraphConstructor().run()
        data = dict()
        for as_obj in bgp_dag:
            if as_obj.multihomed and len(as_obj.providers) >= 2:
                data[as_obj.asn] = {x: set() for x in as_obj.provider_asns}
        return data

    def get_mh_data(
        self, mrt_files: tuple[MRTFile, ...], mh_data
    ) -> defaultdict[int, defaultdict[str, set[int]]]:
        """Aggregates data into {current_asn: {prefix: set_of_next_hops}}"""

        print("NOTE: this takes up about 5GB of RAM")
        print("Add multiprocessing? Potentially? If you have enough ram?")
        total_lines = sum(x.total_parsed_lines for x in mrt_files)
        with tqdm(
            total=total_lines,
            desc="Extracting Mulithomed 2+Provider Export data"
        ) as pbar:
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

                            if len(as_path) <= 1:
                                continue
                            else:
                                origin = as_path[-1]
                                if origin not in mh_data:
                                    continue
                                provider_asn = as_path[-2]
                                prepending = (provider_asn == origin)
                                if prepending:
                                    reversed_as_path = list(reversed(as_path))
                                    for asn in reversed_as_path:
                                        if asn != origin:
                                            provider_asn = asn
                                            break
                                # This was just prepending and nothing else
                                if provider_asn == origin:
                                    continue
                                # Provider is not in CAIDA, skip
                                if provider_asn not in mh_data[origin]:
                                    continue
                                mh_data[origin][provider_asn].add(
                                    PrefixData(
                                        prefix=row["prefix"], prepending=prepending
                                    )
                                )
        return mh_data

    def dump_json(
        self,
        mh_data,
    ) -> None:
        with self.json_prefixes_path.open("w") as f:
            # This is horrible, fix
            export_to_some_prefixes = {
                origin: {
                    k: set([q.prefix for q in v]) for k, v in inner_dict.items()
                } for origin, inner_dict in mh_data.items()
            }
            json.dump(export_to_some_prefixes, f, indent=4, cls=SetEncoder)
        with self.json_prepending_path.open("w") as f:
            # This is horrible, fix
            export_to_some_prepending = {
                origin: {
                    k: set([q.prepending for q in v]) for k, v in inner_dict.items()
                } for origin, inner_dict in mh_data.items()
            }
            json.dump(export_to_some_prepending, f, indent=4, cls=SetEncoder)

    def create_graphs(
        self,
    ) -> None:
        with self.json_prefixes_path.open() as f:
            export_to_some_prefixes = json.load(f)
        with self.json_prepending_path.open() as f:
            export_to_some_prepending = json.load(f)
        relevant_origins = sorted(
            set(
                list(export_to_some_prefixes) + list(export_to_some_prepending)
            )
        )

        total = 0
        total_export_to_some = 0
        total_export_to_some_prefix = 0
        total_export_to_some_prepending = 0
        total_export_to_all = 0
        total_only_one_provider = 0
        total_zero_export = 0
        bgp_dag = CAIDAASGraphConstructor().run()
        for origin in relevant_origins:
            provider_prefix_dict = export_to_some_prefixes[origin]
            provider_prepending_dict = export_to_some_prepending[origin]
            total += 1
            provider_lengths = [len(v) for v in provider_prefix_dict.values()]
            export_to_some_prefix = False
            export_to_some = False
            prepending = False
            export_to_all = False
            for provider, set_of_prepending_bools in provider_prepending_dict.items():
                if any(x for x in set_of_prepending_bools):
                    prepending = True
                    export_to_some = True
                    break
            if not any(x > 0 for x in provider_lengths):
                total_zero_export += 1
            if len(set(provider_lengths)) > 1:
                export_to_some = True
                export_to_some_prefix = True
            if len(bgp_dag.as_dict[int(origin)].provider_asns) == 1:
                total_only_one_provider += 1
                continue
            if len(set(provider_lengths)) <= 1:
                export_to_all = True

            total_export_to_some += int(export_to_some)
            total_export_to_some_prefix += int(export_to_some_prefix)
            total_export_to_some_prepending += int(prepending)
            # Sometimes both can be true if there is prepending
            total_export_to_all += int(export_to_all and not export_to_some)

        categories = [
            "Export to Some Prepending",
            "Export to Some Prefix",
            "Export to Some",
            "Export to All (more than one provider)",
            "Export to None"
        ]
        values = [
            total_export_to_some_prepending,
            total_export_to_some_prefix,
            total_export_to_some,
            total_export_to_all,
            total_zero_export,
            # total_only_one_provider,
        ]
        percentages = [0 if total == 0 else v / total * 100 for v in values]
        fig, ax = plt.subplots()
        ax.set_xticklabels(categories, rotation=90, ha="center")
        bars = ax.bar(categories, percentages, color=["blue", "green", "red", "yellow", "brown"])
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
        ax.set_title("Multihomed 2+ Providers Export-To-Some Behaviors")
        ax.set_ylim(0, 100)
        plt.tight_layout()
        plt.savefig(Path("~/Desktop/mh_2p_export_graphs.png").expanduser())
        # https://stackoverflow.com/a/33343289/8903959
        ax.cla()
        plt.cla()
        plt.clf()
        # If you just close the fig, on machines with many CPUs and trials,
        # there is some sort of a memory leak that occurs. See stackoverflow
        # comment above
        plt.close(fig)
        gc.collect()

    @property
    def json_prefixes_path(self) -> Path:
        return Path(
            "~/Desktop/mh_2p_export_to_some_prefixes.json"
        ).expanduser()

    @property
    def json_prepending_path(self) -> Path:
        return Path(
            "~/Desktop/mh_2p_export_to_some_prepending.json"
        ).expanduser()
