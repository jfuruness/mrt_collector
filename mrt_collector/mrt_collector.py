from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path
from subprocess import check_call
from typing import Any, Callable, Optional

from tqdm import tqdm

from .parse_funcs import PARSE_FUNC, bgpkit_parser_json
from .mrt_file import MRTFile
from .sources import Source


def download_mrt(mrt_file: MRTFile) -> None:
    mrt_file.download_raw()

def store_prefixes(mrt_file: MRTFile) -> None:
    mrt_file.store_unique_prefixes()


class MRTCollector:
    def __init__(
        self,
        dl_time: datetime = datetime(2023, 11, 1, 0, 0, 0),
        cpus: int = cpu_count(),
        base_dir: Optional[Path] = None,
    ) -> None:
        """Creates directories"""

        self.dl_time: datetime = dl_time
        self.cpus: int = cpus

        # Set base directory
        if base_dir is None:
            self.base_dir: Path = Path.home() / "mrt_data" / str(dl_time.date())
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
        parse_mrt_func: PARSE_FUNC = bgpkit_parser_json,
        store_prefixes: bool = True,
        format_parsed_dumps: bool = True,
        analyze_formatted_dumps: bool = True,
    ) -> None:
        """See README package description"""

        mrt_files = mrt_files if mrt_files else self.get_mrt_files(sources)
        if download_raw_mrts:
            self.download_raw_mrts(mrt_files)
        self.parse_mrts(mrt_files, parse_mrt_func)
        if store_prefixes:
            self.store_prefixes(mrt_files)
        if format_parsed_dumps:
            self.format_parsed_dumps(mrt_files)
        if analyze_formatted_dumps:
            self.analyze_formatted_dumps(mrt_files)

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
                mrt_files.append(
                    MRTFile(
                        url,
                        source,
                        raw_dir=self.raw_dir,
                        parsed_dir=self.parsed_dir,
                        prefixes_dir=self.prefixes_dir,
                        formatted_dir=self.formatted_dir,
                    )
                )
        return tuple(mrt_files)

    def download_raw_mrts(self, mrt_files: tuple[MRTFile, ...]) -> None:
        """Downloads raw MRT RIB dumps into raw_dir"""

        self._mp_tqdm(mrt_files, download_mrt, desc="Downloading MRTs (~12m)")

    def parse_mrts(
        self, mrt_files: tuple[MRTFile, ...], parse_func: PARSE_FUNC
    ) -> None:
        """Runs a tool to extract information from a dump"""

        # Remove MRT files that failed to download, and sort by file size
        mrt_files = tuple(list(sorted(x for x in mrt_files if x.download_succeeded)))
        desc = f"Parsing MRTs (largest first) {self.parse_times.get(parse_func, '')}"

        self._mp_tqdm(mrt_files, parse_func, desc=desc)

    def store_prefixes(self, mrt_files: tuple[MRTFile, ...]) -> None:
        """Stores unique prefixes from MRT Files"""

        # First with multiprocessing store for each file
        self._mp_tqdm(mrt_files, store_prefixes, desc="Storing prefixes")

        file_paths = ' '.join(
            str(x.unique_prefixes_path) for x in mrt_files if x.unique_prefixes_path.exists()
        )
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

    def format_parsed_dumps(
        self,
        mrt_files: tuple[MRTFile, ...],
        # Used by the extrapolator
        max_block_size: int = 2000,
    ) -> None:
        """Formats the parsed BGP RIB dumps and add metadata from other sources"""

        po_metadata = POMetadata(
            self.dl_time,
            self.all_unique_prefixes_path,
            max_block_size,

        )
        raise NotImplementedError("Add metadata to anns and output into blocks")
        raise NotImplementedError("Also output the po_metadata into it's own file")

    def analyze_formatted_dumps(self, mrt_files: tuple[MRTFile, ...]) -> None:
        """Analyzes the formatted BGP dumps"""

        raise NotImplementedError

    def _mp_tqdm(
        self, iterable: tuple[MRTFile, ...], func: Callable[[MRTFile], Any], desc: str
    ) -> None:
        """Runs tqdm with multiprocessing"""

        # Starts the progress bar in another thread
        if self.cpus == 1:
            for item in tqdm(iterable, total=len(iterable), desc=desc):
                func(item)
        else:
            # https://stackoverflow.com/a/63834834/8903959
            with ProcessPoolExecutor(max_workers=self.cpus) as executor:
                futures = [executor.submit(func, x) for x in iterable]
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

        return {bgpkit_parser_json: "~35m"}

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
