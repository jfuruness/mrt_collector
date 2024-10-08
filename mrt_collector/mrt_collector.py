from concurrent.futures import ProcessPoolExecutor, as_completed
import csv
from datetime import datetime
import gc
import json
from multiprocessing import cpu_count  # noqa
from pathlib import Path
from subprocess import check_call
import subprocess
import time
from typing import Any, Callable, Optional

from tqdm import tqdm

from bgpy.as_graphs import CAIDAASGraphConstructor

from .mp_funcs import PARSE_FUNC, bgpkit_parser
from .mp_funcs import download_mrt
from .mp_funcs import store_prefixes
from .mp_funcs import FORMAT_FUNC, format_psv_into_tsv
from .mp_funcs import analyze
from .prefix_origin_metadata import PrefixOriginMetadata
from .mrt_file import MRTFile
from .sources import Source

from mrt_collector import mrtc

def get_vantage_point_json(vantage_point, dirs, as_rank, max_block_size, get_path_poisoning):
    # NOTE: iterating over all the files is simply wayyy to slow
    # we need to use awk to get only the necessary file info FIRST
    # and only afterwards iterate over it
    from tempfile import TemporaryDirectory
    with TemporaryDirectory() as tmp_dir:
        agg_path = Path(f"/tmp/agg_{vantage_point}.tsv")
        agg_path.unlink(missing_ok=True)

        assert dirs
        # Get header
        for dir_ in dirs:
            for formatted_path in (Path(dir_) / str(max_block_size)).glob("*.tsv"):
                with formatted_path.open() as f:
                    with agg_path.open("w") as agg_path_f:
                        for line in f:
                            agg_path_f.write(line)
                            break
        from subprocess import check_call
        # Use awk to write temporary files containing the vantage point
        for dir_ in dirs:
            print(f"using awk on {dir_}")
            cmd = ("""find "$ROOT_DIRECTORY" -name '*.tsv' | """
                    # NOTE: using just 1 core for this
                    # since we are multiprocessing elsewhere
                   """xargs -P 1 -I {} """
                   """sh -c 'awk -F"\t" "\$3 ~ /^asn($| )/" {} """
                   """> '"$TMP_DIR"'"/"""
                   """$(echo {} | md5sum | cut -d" " -f1).tmp"'""")
            cmd = cmd.replace("$ROOT_DIRECTORY", str(dir_))
            cmd = cmd.replace("asn", str(vantage_point))
            cmd = cmd.replace("$TMP_DIR", str(Path(tmp_dir)))
            check_call(cmd, shell=True)
        print("combining files")
        # Combine the files
        # Get only unique lines or else it will screw up the counts
        # uniq too slow https://unix.stackexchange.com/a/581324/477240
        # check_call(f"cat {tmp_dir}/*.tmp | sort | uniq >> {agg_path}", shell=True)
        check_call(f"cat {tmp_dir}/*.tmp | awk '!x[$0]++' >> {agg_path}", shell=True)
        print("combined files")
        # input(agg_path)
        vantage_point_stat = mrtc.get_vantage_point_stat(
            int(vantage_point), int(as_rank), [str(agg_path)], get_path_poisoning
        )
        agg_path.unlink()
        return {
            "asn": vantage_point,
            "as_rank": as_rank,
            "num_prefixes": len(vantage_point_stat.prefix_id_set),
            "num_anns": vantage_point_stat.ann_count,
            "no_path_poisoning_prefix_ids_set": list(vantage_point_stat.no_path_poisoning_prefix_id_set)
        }



class MRTCollector:
    def __init__(
        self,
        dl_time: datetime = datetime(2023, 11, 1, 0, 0, 0),
        # for a full run, just using 1 core since we don't want to run out of RAM
        # now that we're using the CAIDA collector for pypy
        # jk, use 2 since we want the nice counter, and each core appears to take 5gb
        cpus: int = 1,  # 2,  # cpu_count(),
        base_dir: Optional[Path] = None,
    ) -> None:
        """Creates directories"""

        self.dl_time: datetime = dl_time
        self.cpus: int = cpus

        # Set base directory
        if base_dir is None:
            t_str = str(dl_time).replace(" ", "_")
            self.base_dir: Path = Path.home() / "mrt_data" / t_str
        else:
            self.base_dir = base_dir

        self._initialize_dirs()

    def run(
        self,
        sources: tuple[Source, ...] = tuple(
            [Cls() for Cls in Source.sources]  # type: ignore
        ),
        # Steps
        mrt_files: Optional[tuple[MRTFile, ...]] = None,
        download_raw_mrts: bool = True,
        parse_mrt_func: PARSE_FUNC = bgpkit_parser,
        store_prefixes: bool = True,
        max_block_size: int = 100,  # Used for extrapolator
        format_parsed_dumps_func: FORMAT_FUNC = format_psv_into_tsv,
        analyze_formatted_dumps: bool = True,
    ) -> None:
        """See README package description"""

        mrt_files = mrt_files if mrt_files else self.get_mrt_files(sources)
        ###############################################
        if download_raw_mrts:
            self.download_raw_mrts(mrt_files)
        self.parse_mrts(mrt_files, parse_mrt_func)
        if store_prefixes:
            self.store_prefixes(mrt_files)
        self.format_parsed_dumps(mrt_files, max_block_size, format_parsed_dumps_func)

        if analyze_formatted_dumps:
            done_path = self.analysis_dir / "done.txt"
            if not done_path.exists():
                self._get_vantage_point_stats(mrt_files, max_block_size)
                self.analyze_formatted_dumps(mrt_files, max_block_size)
                self.get_multihomed_preference(mrt_files, max_block_size)
            with done_path.open("w") as f:
                f.write("done!")

    def get_mrt_files(
        self,
        sources: tuple[Source, ...] = tuple(
            [Cls() for Cls in Source.sources]  # type: ignore
        ),
    ) -> tuple[MRTFile, ...]:
        """Gets URLs from sources (cached) and returns MRT File objects"""

        mrt_files = list()
        for source in tqdm(sources, total=len(sources), desc=f"Parsing {sources}"):
            for url in source.get_urls(self.dl_time, self.requests_cache_dir):
                print(f"Adding {url}")
                mrt_files.append(
                    MRTFile(
                        url,
                        source,
                        raw_dir=self.raw_dir,
                        parsed_dir=self.parsed_dir,
                        prefixes_dir=self.prefixes_dir,
                        formatted_dir=self.formatted_dir,
                        analysis_dir=self.analysis_dir,
                    )
                )
        return tuple(mrt_files)

    def download_raw_mrts(self, mrt_files: tuple[MRTFile, ...]) -> None:
        """Downloads raw MRT RIB dumps into raw_dir"""

        args = tuple([(x,) for x in mrt_files])
        self._mp_tqdm(args, download_mrt, desc="Downloading MRTs (~12m)")

    def parse_mrts(
        self, mrt_files: tuple[MRTFile, ...], parse_func: PARSE_FUNC
    ) -> None:
        """Runs a tool to extract information from a dump"""

        # Remove MRT files that failed to download, and sort by file size
        mrt_files = tuple(list(sorted(x for x in mrt_files if x.download_succeeded)))
        args = tuple([(x,) for x in mrt_files])
        desc = f"Parsing MRTs (largest first) {self.parse_times.get(parse_func, '')}"
        self._mp_tqdm(args, parse_func, desc=desc)

    def store_prefixes(self, mrt_files: tuple[MRTFile, ...]) -> None:
        """Stores unique prefixes from MRT Files"""

        # If this file exists, don't redo
        completed_path = self.prefixes_dir / "completed.txt"
        if completed_path.exists():
            with completed_path.open() as f:
                urls = set(f.read().split("\n"))
                mrt_file_urls = set(x.url for x in mrt_files)
                if urls != mrt_file_urls:
                    print("Already stored prefixes, not redoing")
                    return
                else:
                    print("prefix urls don't match, redoing")
                    print(urls)
                    print(mrt_file_urls)
        args = tuple([(x,) for x in mrt_files])
        # First with multiprocessing store for each file
        self._mp_tqdm(args, store_prefixes, desc="Storing prefixes")

        successful_mrts = [x for x in mrt_files if x.unique_prefixes_path.exists()]
        assert successful_mrts, "No prefixes?"

        file_paths = " ".join([str(x.unique_prefixes_path) for x in successful_mrts])
        # Concatenate all files, fastest with cat
        # https://unix.stackexchange.com/a/118248/477240
        cmd = f"cat {file_paths} >> {self.all_non_unique_prefixes_path}"
        check_call(cmd, shell=True)
        # Make lines unique, fastest with awk
        # it uses a hash map while all others require sort
        # https://unix.stackexchange.com/a/128782/477240
        check_call(
            f"awk '!x[$0]++' {self.all_non_unique_prefixes_path} "
            f"> {self.all_unique_prefixes_path}",
            shell=True,
        )
        # Write this file so that we don't redo this step
        with completed_path.open("w") as f:
            for mrt_file in mrt_files:
                f.write(mrt_file.url + "\n")
        print(f"prefixes written to {self.all_unique_prefixes_path}")

    def format_parsed_dumps(
        self,
        mrt_files: tuple[MRTFile, ...],
        # Used by the extrapolator
        max_block_size: int,
        format_func: FORMAT_FUNC,
    ) -> None:
        """Formats the parsed BGP RIB dumps and add metadata from other sources"""

        # If this file exists, don't redo
        completed_path = self.formatted_dir / f"{max_block_size}_completed.txt"
        if completed_path.exists():
            with completed_path.open() as f:

                existing_mrt_files = tuple(
                    [x for x in mrt_files if x.unique_prefixes_path.exists()]
                )
                urls = [x for x in f.read().split("\n") if x.strip()]
                if set(urls) == set(x.url for x in existing_mrt_files):
                    print("Already formatted, not reformatting")
                    return
                else:
                    print(set(urls) == set(x.url for x in existing_mrt_files))
                    print(set(x.url for x in existing_mrt_files).difference(set(urls)))
                    print(set(urls).difference(set(x.url for x in existing_mrt_files)))
                    input("redoing this? waiting for input")

        print("Starting prefix origin metadata")
        # Initialize prefix origin metadata
        # ROA checker is .7GB
        # exr data is .4GB
        # format_func adds no RAM, just .1GB that doesn't accumulate
        # jk it does add ram, seems to do so on large files, it accumulates up to 1gb...
        # wow, even more than 1GB is consumed, like 1.5GB now, and just going to kill it
        # wow, went from like 1gb total preprogram,
        # to 2gb total pre loop, to 4gb post loop
        # and python isn't giving the memory back...
        # AHA - it is thge prefix origin metadata caching mechanism
        # just gonna disable that, don't mind me
        # except for potentially pickling/unpickling for mp
        prefix_origin_metadata = PrefixOriginMetadata(
            self.dl_time,
            self.requests_cache_dir / "other_collector_cache.db",
            self.all_unique_prefixes_path,
            max_block_size,
        )
        print("prefix origin metadata complete")
        print("caching caida")
        CAIDAASGraphConstructor(tsv_path=None).run()
        # Collect CAIDA collector (number of unreachable objects is returned
        print(gc.collect())
        print("cached caida")

        mrt_files = tuple([x for x in mrt_files if x.unique_prefixes_path.exists()])
        args = tuple([(x, prefix_origin_metadata) for x in mrt_files])
        iterable = args
        desc = "Formatting (~3hrs)"
        func = format_func

        # Directories containing the count files
        # must do it this way since there's no central dir for a max block size
        count_dirs = [x.formatted_dir / str(max_block_size) for x in mrt_files]
        # Starts the progress bar in another thread
        if self.cpus == 1 and False:
            for args in tqdm(iterable, total=len(iterable), desc=desc):
                func(*args, single_proc=True)  # type: ignore
        else:
            total = self._get_parsed_lines()
            # https://stackoverflow.com/a/63834834/8903959
            with ProcessPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(func, *x) for x in iterable]
                with tqdm(total=total, desc=desc) as pbar:
                    while futures:
                        # Non blocking check for completion
                        completed = [x for x in futures if x.done()]
                        futures = [x for x in futures if x not in completed]
                        for future in completed:
                            # reraise any exceptions from the processes
                            future.result()

                        # Increment pbar
                        pbar.n = sum(self._get_count(x) for x in count_dirs)
                        pbar.refresh()
                        time.sleep(10)

        # Write this file so that we don't redo this step
        with completed_path.open("w") as f:
            for mrt_file in mrt_files:
                f.write(mrt_file.url + "\n")

    def analyze_formatted_dumps(
        self, mrt_files: tuple[MRTFile, ...], max_block_size: int
    ) -> None:
        """Analyzes the formatted BGP dumps"""

        # NOTE: not putting a thing to not run this part of the pipeline here
        # since why would you even run this func if you didn't want analysis
        # simply set the bool to do analysis to False
        # Remove MRT files that failed to format
        mrt_files = tuple([x for x in mrt_files if x.unique_prefixes_path.exists()])
        args = tuple([(x, max_block_size) for x in mrt_files])
        iterable = args
        desc = "Analyzing MRTs"
        func = analyze
        # Starts the progress bar in another thread
        if self.cpus == 1:
            for args in tqdm(iterable, total=len(iterable), desc=desc):
                func(*args, single_proc=True)  # type: ignore
        else:
            total = self._get_formatted_lines(mrt_files, max_block_size)
            # https://stackoverflow.com/a/63834834/8903959
            with ProcessPoolExecutor(max_workers=self.cpus // 2) as executor:
                futures = [executor.submit(func, *x) for x in iterable]
                with tqdm(total=total, desc=desc) as pbar:
                    while futures:
                        # Non blocking check for completion
                        completed = [x for x in futures if x.done()]
                        futures = [x for x in futures if x not in completed]
                        for future in completed:
                            # reraise any exceptions from the processes
                            future.result()
                        # Increment pbar
                        pbar.n = self._get_count(self.analysis_dir)
                        pbar.refresh()
                        time.sleep(5)

        stats = dict()  # type: ignore
        for mrt_file in tqdm(mrt_files, total=len(mrt_files), desc="Joining stats"):
            with mrt_file.analysis_path.open() as f:
                for k, v in json.load(f).items():
                    if isinstance(v, int):
                        stats[k] = stats.get(k, 0) + v  # type: ignore
                    else:
                        stats[k] = stats.get(k, set()) | set(v)  # type: ignore
        with (self.analysis_dir / "final_analysis.json").open("w") as f:
            # https://stackoverflow.com/a/22281062/8903959
            def set_default(obj):
                if isinstance(obj, set):
                    return list(obj)
                raise TypeError

            json.dump(stats, f, indent=4, default=set_default)

    def get_multihomed_preference(
        self, mrt_files: tuple[MRTFile, ...], max_block_size: int
    ) -> None:
        """Analyzes the formatted BGP dumps for multihomed preference"""

        mrt_files = tuple([x for x in mrt_files if x.unique_prefixes_path.exists()])
        print("caching caida")
        bgp_dag = CAIDAASGraphConstructor(tsv_path=None)
        mh_as_pref_dict = dict()
        for as_obj in bgp_dag.as_dict.values():
            if len(as_obj.providers) > 1 and len(as_obj.customers) == 0:
                mh_as_pref_dict[as_obj.asn] = {
                    x.asn: {"general": 0, "no_path_poison": 0} for x in as_obj.providers
                }
        print("cached caida")

        total_files = 0
        for mrt_file in mrt_files:
            for formatted_path in (
                mrt_file.formatted_dir / str(max_block_size)
                    ).glob("*.tsv"):
                total_files += 1

        # TODO: refactor this, this should be multiprocessed
        with tqdm(total=total_files, desc="getting mh pref") as pbar:
            for mrt_file in mrt_files:
                for formatted_path in (
                    mrt_file.formatted_dir / str(max_block_size)
                        ).glob("*.tsv"):
                    with formatted_path.open() as f:
                        for row in csv.DictReader(f, delimiter="\t"):
                            # No AS sets
                            if "}" in row["as_path"]:
                                continue
                            as_path = [int(x) for x in row["as_path"][1:-1].split()]
                            # Origin only
                            if len(as_path) < 2:
                                continue
                            if as_path[-1] not in mh_as_pref_dict:
                                continue
                            if as_path[-2] not in mh_as_pref_dict[as_path[-1]]:
                                continue

                            # If no path poisoning
                            if (
                                row["invalid_as_path_asns"] in [None, "", "[]"]
                                # and row["ixps_in_as_path"] not in [None, "", "[]"]
                                and row["prepending"] == "False"
                                and row["as_path_loop"] == "False"
                                and row["input_clique_split"] == "False"
                            ):
                                dct = mh_as_pref_dict[as_path[-1]][as_path[-2]]
                                dct["no_path_poison"] += 1
                            mh_as_pref_dict[as_path[-1]][as_path[-2]]["general"] += 1

                        pbar.update()
        for k, inner_dict in mh_as_pref_dict.copy().items():
            # This multihomed AS never originated an announcement
            # So we aren't interested in it
            values = [v["general"] for v in inner_dict.values()]
            if sum(values) == 0:
                del mh_as_pref_dict[k]
        with (self.analysis_dir / "mh_pref.json").open("w") as f:
            json.dump(mh_as_pref_dict, f, indent=4)

    def _get_vantage_point_stats(
        self, mrt_files: tuple[MRTFile, ...], max_block_size: int
    ) -> None:
        """Analyzes the formatted BGP dumps for various vantage point statistics"""

        vantage_points_to_dirs = self.get_vantage_points_to_dirs(
            mrt_files, max_block_size
        )

        print("Getting statistics on each vantage point")
        print("caching caida")
        bgp_dag = CAIDAASGraphConstructor(tsv_path=None).run()
        print("cached caida")

        stat_path = self.analysis_dir / "vantage_point_stats.json"
        try:
            with stat_path.open() as f:
                data = {int(k): v for k, v in json.load(f).items()}
        except Exception as e:
            print(e)
            data = dict()
            input("ensure that this exception is handled")

        iterable = list()
        for vantage_point, dirs in tqdm(vantage_points_to_dirs.items(), total=len(vantage_points_to_dirs), desc="getting stats for vantage points"):
            if int(vantage_point) in data:
                print(f"skipping {vantage_point} since it's already been parsed")
                continue
            # print(f"{vantage_point} has {len(dirs)} dirs")
            # Get AS Rank, by default higher than total number of ASes by far
            as_obj = bgp_dag.as_dict.get(vantage_point)
            as_rank = as_obj.as_rank if as_obj else 500000
            get_path_poisoning = False
            iterable.append([vantage_point, dirs, as_rank, max_block_size, get_path_poisoning])

        func = get_vantage_point_json
        if self.cpus == 1 and False:
            for args in tqdm(iterable, total=len(iterable), desc="getting vpoint stats"):
                vantage_point_json = func(*args)
                try:
                    with stat_path.open() as f:
                        data = json.load(f)
                except Exception as e:
                    print(e)
                    data = dict()
                data[vantage_point_json["asn"]] = vantage_point_json
                with stat_path.open("w") as f:
                    json.dump(data, f, indent=4)

        else:
            # https://stackoverflow.com/a/63834834/8903959
            with ProcessPoolExecutor(max_workers=9) as executor:
                futures = [executor.submit(func, *x) for x in iterable]
                for future in tqdm(
                    as_completed(futures),
                    total=len(iterable),
                    desc="Getting vantage point stats",
                ):
                    # reraise any exceptions from the processes
                    vantage_point_json = future.result()
                    try:
                        with stat_path.open() as f:
                            data = json.load(f)
                    except Exception as e:
                        print(e)
                        data = dict()
                    data[vantage_point_json["asn"]] = vantage_point_json
                    with stat_path.open("w") as f:
                        json.dump(data, f, indent=4)

    def get_vantage_points_to_dirs(
        self, mrt_files: tuple[MRTFile, ...], max_block_size: int
    ) -> dict[int, list[str]]:
        mrt_files = tuple([x for x in mrt_files if x.unique_prefixes_path.exists()])

        dir_to_tsv_paths = dict()
        for mrt_file in mrt_files:
            dir_to_tsv_paths[str(mrt_file.formatted_dir)] = list()
            for formatted_path in (
                mrt_file.formatted_dir / str(max_block_size)
                    ).glob("*.tsv"):
                dir_to_tsv_paths[str(mrt_file.formatted_dir)].append(
                    str(formatted_path)
                )

        print("Getting relevant paths")
        relevant_paths = mrtc.get_relevant_paths(dir_to_tsv_paths)
        # from pprint import pprint
        # pprint(relevant_paths)
        # input("waiting")

        print("Getting vantage points")
        # NOTE: this assumes that vantage points only exist at one
        # collector, this should have an assertion in the future
        vantage_points_json = self.analysis_dir / "vantage_points.json"
        if not vantage_points_json.exists():
            data = mrtc.get_vantage_points(relevant_paths)
            # sort the keys for consistent runs
            data = {k: data[k] for k in sorted(data)}
            vantage_points_to_dirs = dict()
            for vantage_point, dirs in data.items():
                vantage_points_to_dirs[int(vantage_point)] = list(sorted(dirs))
            with vantage_points_json.open("w") as f:
                json.dump(vantage_points_to_dirs, f, indent=4)
        with vantage_points_json.open() as f:
            vantage_points_to_dirs = {int(k): v for k, v in json.load(f).items()}
            assert all(isinstance(k, int) for k in vantage_points_to_dirs), "not ints"
        return vantage_points_to_dirs

    def _get_parsed_lines(self) -> int:
        """Gets the total number of lines in parsed dir"""

        parsed_count_path = self.parsed_dir / "total_lines.txt"
        if parsed_count_path.exists():
            with parsed_count_path.open() as f:
                return int(f.read().strip())

        print("Getting parsed lines")
        # Define the command to be executed
        command = f"find {self.parsed_dir} -name '*.psv' | xargs wc -l"

        # Run the command
        result = subprocess.run(command, shell=True, text=True, capture_output=True)

        # Check if the command was successful
        if result.returncode != 0:
            print("Error running command:", result.stderr)
            raise Exception

        # Process the output to get the total number of lines
        output = result.stdout.strip()
        lines = output.split("\n")
        count = int(lines[-1].strip().split(" ")[0])

        with parsed_count_path.open("w") as f:
            f.write(str(count))
        return count

    def _get_formatted_lines(self, mrt_files, max_block_size: int) -> int:
        """Gets the total number of lines in formatted dir"""

        formatted_dirs = [
            mrt_file.formatted_dir / str(max_block_size) for mrt_file in mrt_files
        ]
        formatted_count_path = self.formatted_dir / f"total_lines_{max_block_size}.txt"
        if formatted_count_path.exists():
            with formatted_count_path.open() as f:
                return int(f.read().strip())

        print("Getting formatted lines")
        count = 0
        for dir_ in formatted_dirs:
            # Define the command to be executed
            command = f"find {dir_} -name '*.tsv' | xargs wc -l"

            # Run the command
            result = subprocess.run(command, shell=True, text=True, capture_output=True)

            # Check if the command was successful
            if result.returncode != 0:
                print("Error running command:", result.stderr)
                raise Exception

            # Process the output to get the total number of lines
            output = result.stdout.strip()
            lines = output.split("\n")
            # Subtract the header line from every file
            num_files = len([x for x in dir_.glob('*') if x.is_file()])
            count += int(lines[-1].strip().split(" ")[0]) - num_files

        # Not sure why we need to add one extra line per dir, but we do
        count += len(formatted_dirs)

        with formatted_count_path.open("w") as f:
            f.write(str(count))
        return count

    def _get_count(self, dir_) -> int:
        """Returns the total number of lines in a directories count files"""

        total_sum = 0
        # TODO: fix
        paths = list(dir_.rglob("*count.txt"))
        if len(paths) == 0:
            paths = list(dir_.rglob("count.txt"))
        for file_path in paths:
            try:
                with file_path.open() as f:
                    number = int(f.read().strip())
                    total_sum += number
            except ValueError:
                pass
            except Exception as e:
                print(f"Error reading file {file_path}: {e}")
        return total_sum

    def _mp_tqdm(
        self,
        # args to func in a list of lists
        iterable: tuple[tuple[Any, ...], ...],
        func: Callable[..., Any],
        desc: str,
    ) -> None:
        """Runs tqdm with multiprocessing"""

        # Starts the progress bar in another thread
        if self.cpus == 1:
            for args in tqdm(iterable, total=len(iterable), desc=desc):
                func(*args)
        else:
            # https://stackoverflow.com/a/63834834/8903959
            with ProcessPoolExecutor(max_workers=self.cpus) as executor:
                futures = [executor.submit(func, *x) for x in iterable]
                for future in tqdm(
                    as_completed(futures),
                    total=len(iterable),
                    desc=desc,
                ):
                    # reraise any exceptions from the processes
                    future.result()

    @property
    def parse_times(self) -> dict[PARSE_FUNC, str]:
        """Useful for printing the times it will take to parse files"""

        return {bgpkit_parser: "~??m"}

    @property
    def all_non_unique_prefixes_path(self) -> Path:
        """Returns the path to all prefixes"""
        return self.prefixes_dir / "all_non_unique_prefixes.csv"

    @property
    def all_unique_prefixes_path(self) -> Path:
        """Returns the path to all prefixes"""
        return self.prefixes_dir / "all_unique_prefixes.csv"

    ###############
    # Directories #
    ###############

    def _initialize_dirs(self) -> None:
        """Initializes dirs. Does this now to prevent issues when multiprocessing"""

        for dir_ in (
            self.base_dir,
            self.requests_cache_dir,
            self.raw_dir,
            self.parsed_dir,
            self.prefixes_dir,
            self.formatted_dir,
            self.analysis_dir,
        ):
            dir_.mkdir(parents=True, exist_ok=True)

    @property
    def requests_cache_dir(self) -> Path:
        """Returns dir that is sometimes used to cache requests

        For example, when getting URLs from RIPE and Route Views,
        this directory is used
        """

        return self.base_dir / "requests_cache"

    @property
    def raw_dir(self) -> Path:
        """Returns directory into which raw MRTs are downloaded"""
        return self.base_dir / "raw"

    @property
    def parsed_dir(self) -> Path:
        """Directory in which MRTs are parsed using available tools"""
        return self.base_dir / "parsed"

    @property
    def prefixes_dir(self) -> Path:
        """Dir in which prefixes are extracted from MRTs"""
        return self.base_dir / "prefixes"

    @property
    def formatted_dir(self) -> Path:
        """Dir into which parsed files are formatted into useful CSVs"""
        return self.base_dir / "formatted"

    @property
    def analysis_dir(self) -> Path:
        """Returns directory into which analysis files are stored"""
        return self.base_dir / "analysis"
