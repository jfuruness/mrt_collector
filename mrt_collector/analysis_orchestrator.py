import csv
from pathlib import Path
from tqdm import tqdm
from mrt_collector.mrt_collector import sort_mrt_files_by_parsed_file_size
from mrt_collector.mrt_file import MRTFile
from mrt_collector.analyzers.export_analyzer import ExportAnalyzer

from bgpy.as_graphs import CAIDAASGraphConstructor

class AnalysisOrchestrator():
    # need to pass list of str args, each corresponds to a module
    # and initialize each module on init
    def __init__(
        self,
        base_dir: Path,
        mrt_files: tuple[MRTFile],
        analyzer_ids: list[str],
    ) -> None:

        self.base_dir = base_dir
        self.desc = self.format_desc(analyzer_ids)
        self.analyzers = [
            ExportAnalyzer.from_id(id, base_dir) for id in analyzer_ids
        ]
        # only create the graph if we really need it
        if any(a.uses_bgpy_graph for a in self.analyzers):
            self.bgp_dag = CAIDAASGraphConstructor().run()
            for a in self.analyzers:
                if a.uses_bgpy_graph:
                    a.set_bgpy_graph(self.bgp_dag)

        self.run(mrt_files)

    def run(
        self,
        mrt_files: tuple[MRTFile, ...]
    ) -> None:
        """Lifecycle of the export analyzer"""

        mrt_files = sort_mrt_files_by_parsed_file_size(mrt_files)
        self.get_data(mrt_files)
        # call post process on analyzers
        for a in self.analyzers:
            a.post_process()
        # call dump json on analyzers
        for a in self.analyzers:
            a.dump_json()

    def get_data(
        self,
        mrt_files: tuple[MRTFile, ...]
    ) -> None:
        """Iterates through each parsed mrt file for performing analysis"""
        total_lines = sum(x.total_parsed_lines for x in mrt_files)

        with tqdm(
            total=total_lines,
            desc = self.desc
        ) as pbar:
            for mrt_file in mrt_files:
                if mrt_file.parsed_path_psv.exists():
                    self.current_source = mrt_file.parsed_path_psv.stem
                    with mrt_file.parsed_path_psv.open() as f:
                        reader = csv.DictReader(f, delimiter="|")
                        for row in reader:
                            pbar.update()
                            if row["type"] != "A":
                                continue
                            # call analyze on analyzers
                            for a in self.analyzers:
                                a.set_current_source(self.current_source)
                                a.analyze(row)                      

    def format_desc(
        self, 
        analyzers: list[str]
    ) -> str:
        """Formats tqdm bar description to reflect all selected analysis modules"""
        tmp = "Running "
        for s in analyzers:
            tmp = tmp + s + ', '
        tmp += "analyzers."
        return tmp
