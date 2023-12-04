from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path
from typing import Optional

from tqdm import tqdm

from .parse_funcs import PARSE_FUNC, bgp_kit_parser
from .mrt_file import MRTFile
from .sources import Source


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
        parse_mrt_func: PARSE_FUNC = bgp_kit_parser,
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

        # Starts the progress bar in another thread
        if self.cpus == 1:
            for mrt_file in tqdm(mrt_files, total=len(mrt_files), desc="Downloading"):
                mrt_file.download_raw()
        else:
            # https://stackoverflow.com/a/63834834/8903959
            with ProcessPoolExecutor(max_workers=self.cpus) as executor:
                futures = [executor.submit(x.download_raw) for x in mrt_files]
                for future in tqdm(
                    as_completed(futures),
                    total=len(mrt_files),
                    desc="Downloading MRTs (~12m)",
                ):
                    # reraise any exceptions from the processes
                    future.result()

    def parse_mrts(
        self, mrt_files: tuple[MRTFile, ...], parse_func: PARSE_FUNC
    ) -> None:
        """Runs a tool to extract information from a dump"""

        # Starts the progress bar in another thread
        if self.cpus == 1:
            for mrt_file in tqdm(mrt_files, total=len(mrt_files), desc="Parsing"):
                parse_func(mrt_file)
        else:
            # https://stackoverflow.com/a/63834834/8903959
            with ProcessPoolExecutor(max_workers=self.cpus) as executor:
                futures = [executor.submit(parse_func, x) for x in mrt_files]
                for future in tqdm(
                    as_completed(futures),
                    total=len(mrt_files),
                    desc="Parsing MRTs (~12m)",
                ):
                    # reraise any exceptions from the processes
                    future.result()

    def store_prefixes(self, mrt_files: tuple[MRTFile, ...]) -> None:
        """Stores unique prefixes from MRT Files"""

        raise NotImplementedError

    def format_parsed_dumps(self, mrt_files: tuple[MRTFile, ...]) -> None:
        """Formats the parsed BGP RIB dumps"""

        raise NotImplementedError

    def analyze_formatted_dumps(self, mrt_files: tuple[MRTFile, ...]) -> None:
        """Analyzes the formatted BGP dumps"""

        raise NotImplementedError

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
