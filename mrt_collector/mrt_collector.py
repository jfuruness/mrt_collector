from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path
from typing import Any, Callable

import time # this is for temp solution to our new connection error diagnostics
import warnings

from tqdm import tqdm

from .mrt_file import MRTFile
from .rib_dump_parse_funcs import PARSE_FUNC, bgpkit_parser
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
            [Cls() for Cls in Source.sources]
        ),
        limit_files_to: int = 0,
        mrt_files: tuple[MRTFile, ...] = (),
    ) -> tuple[MRTFile, ...]:
        """Downloads MRTs and then extracts data from them"""

        mrt_files = mrt_files or self.get_mrt_files()

        self.set_mrt_ec_file_sizes(mrt_files)
        
        mrt_files = self.sort_mrt_files_by_ec_file_size(mrt_files)
        mrt_files = self.strip_unavail_sources(mrt_files)
        return mrt_files #temp while we test which sources are bad
       
        
        if limit_files_to != 0:
            mrt_files = self.limit_mrt_files(mrt_files, limit_files_to)
        self.download_raw_mrts(mrt_files)
        mrt_files = self.strip_failed_downloads(mrt_files)
        # TODO:  method to get rid of bad downloads, should also allow us to
        # remove some of the download checking logic from parse_mrts() and count_parsed_lines()
        #self.parse_mrts(mrt_files)
        #self.count_parsed_lines(mrt_files)
        return mrt_files

    def get_mrt_files(
        self,
        sources: tuple[Source, ...] = tuple(
            [Cls() for Cls in Source.sources]
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

    def set_mrt_ec_file_sizes(
        self,
        mrt_files: tuple[MRTFile, ...]
    ) -> None:
        """Gets the expected file size of each MRT"""
         
        for mrt_file in mrt_files:
            mrt_file.fetch_ec_file_size()            
            # need minimum 3 sec delay between requests, otherwise rate limit exceeded
            time.sleep(5) 

    def sort_mrt_files_by_ec_file_size(
        self,
        mrt_files: tuple[MRTFile, ...]
    ) -> tuple[MRTFile, ...]:
        """Sorts mrt_files by expected compressed file size (descending)"""

        return tuple(sorted(
            mrt_files,
            key= lambda mrt_file: getattr(mrt_file, "ec_file_size"),
            reverse=True
        ))

    def sort_mrt_files_by_ac_file_size(
        self,
        mrt_files: tuple[MRTFile, ...]
    ) -> tuple[MRTFile, ...]:
        """Sorts mrt_files by actual compressed file size (descending)"""

        return tuple(sorted(
            mrt_files,
            key= lambda mrt_file: getattr(mrt_file, "ac_file_size"),
            reverse=True
        ))
    
    def sort_mrt_files_by_parsed_file_size(
        self,
        mrt_files: tuple[MRTFile, ...]
    ) -> tuple[MRTFile, ...]:
        """Sorts mrt_files by parsed file size (descending)"""

        return tuple(sorted(
            mrt_files,
            key= lambda mrt_file: getattr(mrt_file, "parsed_file_size"),
            reverse=True
        ))

    def strip_unavail_sources(
        self,
        mrt_files: tuple[MRTFile, ...],
        ) -> tuple[MRTFile, ...]:
        """
        Removes all MRTFile with ec_file_size of 0 from mrt_files
        """
        
        mrt_files = tuple(
            [mrt_file for mrt_file in mrt_files if mrt_file.ec_file_size != 0]
        )
        return mrt_files
        
    def limit_mrt_files(
        self,
        mrt_files: tuple[MRTFile, ...],
        num_files: int
    ) -> tuple[MRTFile, ...]:
        """Creates a new tuple containing as many files as defined by limit_files_to"""
        
        strip_at = len(mrt_files) - num_files
        return mrt_files[strip_at:]

    def get_total_expected_mrt_file_size(
        self,
        mrt_files: tuple[MRTFile, ...]
    ) -> int:
        """Returns in bytes (int) the total sum of expected mrt file sizes"""

        total_bytes = 0

        for mrt_file in mrt_files:
            file_size = mrt_file.ec_file_size
            total_bytes += file_size

        return total_bytes

    def strip_failed_downloads(
        self,
        mrt_files: tuple[MRTFile, ...]
    ) -> tuple[MRTFile, ...]:
        """Removes any MRTFile where download_succeeded is false"""

        return tuple([mrt_file for mrt_file in mrt_files if mrt_file.download_succeeded])

    def download_raw_mrts(self, mrt_files: tuple[MRTFile, ...]) -> None:
        """Downloads raw MRT RIB dumps into raw_dir"""

        args = tuple([(x,) for x in mrt_files])
        self._mp_tqdm(args, download_mrt, desc="Downloading MRTs (~5m)")

    def parse_mrts(
        self, mrt_files: tuple[MRTFile, ...], parse_func: PARSE_FUNC = bgpkit_parser
    ) -> None:
        """Runs a tool to extract information from a dump"""

        # Remove MRT files that failed to download, and sort by file size
        mrt_files = tuple(sorted(x for x in mrt_files if x.download_succeeded))
        args = tuple([(x,) for x in mrt_files])
        desc = "Parsing MRTs (largest first), ~13m"
        self._mp_tqdm(args, parse_func, desc=desc)

    def count_parsed_lines(self, mrt_files: tuple[MRTFile, ...]) -> None:
        """Counts parsed lines from MRT files and stores them"""

        mrt_files = tuple(sorted(x for x in mrt_files if x.download_succeeded))
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
