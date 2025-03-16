from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from multiprocessing import cpu_count  # noqa
from pathlib import Path
from typing import Any, Callable

from tqdm import tqdm

from .rib_dump_parse_funcs import PARSE_FUNC, bgpkit_parser
from .mrt_file import MRTFile
from .sources import Source


def download_mrt(mrt_file: MRTFile) -> None:
    mrt_file.download_raw()


def count_parsed_lines(mrt_file: MRTFile) -> None:
    mrt_file.count_parsed_lines()


class MRTCollector:
    def __init__(
        self,
        dl_time: datetime = datetime(2025, 3, 1, 0, 0, 0),
        cpus: int = cpu_count(),
        base_dir: Path | None = None,
    ) -> None:
        """Creates directories"""

        self.dl_time: datetime = dl_time
        self.cpus: int = cpus

        # Set base directory
        if base_dir is None:
            t_str = dl_time.strftime("%Y_%m_%d")
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
        mrt_files: tuple[MRTFile, ...] = (),
    ) -> None:
        """Downloads MRTs and then extracts data from them"""

        mrt_files = mrt_files if mrt_files else self.get_mrt_files(sources)
        self.download_raw_mrts(mrt_files)
        self.parse_mrts(mrt_files)
        self.count_parsed_lines(mrt_files)
        return mrt_files

    def get_mrt_files(
        self,
        sources: tuple[Source, ...] = tuple(
            [Cls() for Cls in Source.sources]  # type: ignore
        ),
    ) -> tuple[MRTFile, ...]:
        """Gets URLs from sources (cached) and returns MRT File objects"""

        mrt_files = list()
        for source in tqdm(sources, total=len(sources), desc=f"Getting URLs {sources}"):
            for url in source.get_urls(self.dl_time, self.requests_cache_path):
                mrt_files.append(
                    MRTFile(
                        url,
                        source,
                        raw_dir=self.raw_dir,
                        parsed_dir=self.parsed_dir,
                        parsed_line_count_dir=self.parsed_line_count_dir,
                    )
                )
        return tuple(mrt_files)

    def download_raw_mrts(self, mrt_files: tuple[MRTFile, ...]) -> None:
        """Downloads raw MRT RIB dumps into raw_dir"""

        print("This can be optimized, get file size before downloading and order")
        args = tuple([(x,) for x in mrt_files])
        self._mp_tqdm(args, download_mrt, desc="Downloading MRTs (~5m)")

    def parse_mrts(
        self, mrt_files: tuple[MRTFile, ...], parse_func: PARSE_FUNC = bgpkit_parser
    ) -> None:
        """Runs a tool to extract information from a dump"""

        # Remove MRT files that failed to download, and sort by file size
        mrt_files = tuple(list(sorted(x for x in mrt_files if x.download_succeeded)))
        args = tuple([(x,) for x in mrt_files])
        desc = "Parsing MRTs (largest first), ~13m"
        self._mp_tqdm(args, parse_func, desc=desc)

    def count_parsed_lines(self, mrt_files: tuple[MRTFile, ...]) -> None:
        """Counts parsed lines from MRT files and stores them"""

        mrt_files = tuple(list(sorted(x for x in mrt_files if x.download_succeeded)))
        args = tuple([(x,) for x in mrt_files])
        desc = "Counting lines in MRTs (largest first), ~2m"
        self._mp_tqdm(args, count_parsed_lines, desc=desc)

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

    ###############
    # Directories #
    ###############

    def _initialize_dirs(self) -> None:
        """Initializes dirs. Does this now to prevent issues when multiprocessing"""

        for dir_ in (
            self.base_dir,
            self.raw_dir,
            self.parsed_dir,
            self.parsed_line_count_dir,
        ):
            dir_.mkdir(parents=True, exist_ok=True)

    @property
    def requests_cache_path(self) -> Path:
        """Returns dir that is sometimes used to cache requests

        For example, when getting URLs from RIPE and Route Views,
        this directory is used
        """

        return self.base_dir / "requests_cache.db"

    @property
    def raw_dir(self) -> Path:
        """Returns directory into which raw MRTs are downloaded"""
        return self.base_dir / "raw"

    @property
    def parsed_dir(self) -> Path:
        """Directory in which MRTs are parsed using available tools"""
        return self.base_dir / "parsed"

    @property
    def parsed_line_count_dir(self) -> Path:
        """Directory in which MRTs are parsed using available tools"""
        return self.base_dir / "parsed_line_count"
