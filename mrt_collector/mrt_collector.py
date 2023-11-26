from copy import deepcopy
import csv
from datetime import datetime
import logging
from multiprocessing import cpu_count
from multiprocessing.managers import BaseManager
import os
from pathlib import Path
import re
from urllib.parse import quote

from tqdm import tqdm

from lib_bgpstream_website_collector import BGPStreamWebsiteCollector
from lib_caida_collector import CaidaCollector
from lib_roa_collector import ROACollector
from lib_utils.base_classes import Base
from lib_utils.file_funcs import download_file, delete_paths
from lib_utils.helper_funcs import mp_call, run_cmds

from .mrt_file import MRTFile
from .po_metadata import POMetadata
from .sources import Source
from .tools import BGPGrep


class MRTCollector(Base):
    """This class downloads, parses, and stores MRT Rib dumps

    NOTE: this library uses https://git.doublefourteen.io/bgp/ubgpsuite
    The creator of bgpscanner moved on to this library since Isolario
    reached it's end of life. bgpscanner contained bugs that were never
    fixed
    """

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.raw_dir = self.dir_ / "raw"
        self.dumped_dir = self.dir_ / "dumped"
        self.prefix_dir = self.dir_ / "prefix"
        self.parsed_dir = self.dir_ / "parsed"

        for path in [self.raw_dir,
                     self.dumped_dir,
                     self.prefix_dir,
                     self.parsed_dir]:
            path.mkdir(parents=True, exist_ok=self.dir_exist_ok)

        other_collector_kwargs = deepcopy(self.kwargs)
        other_collector_kwargs.pop("dir_", None)
        other_collector_kwargs.pop("base_dir", None)

        # Gets ROAs
        self.roa_collector = ROACollector(**deepcopy(other_collector_kwargs), dir_=self.dir_ / ROACollector.__name__)
        # Gets relationships
        self.caida_collector = CaidaCollector(**deepcopy(other_collector_kwargs), dir_=self.dir_ / CaidaCollector.__name__)
        # Gets hijacks, leaks, outage info from bgpstream.com
        self.bgpstream_website_collector = BGPStreamWebsiteCollector(**deepcopy(other_collector_kwargs),
                                                                     dir_=self.dir_ / BGPStreamWebsiteCollector.__name__)

        # Temporary placeholder for AS designations (reserved, private, etc)
        class IANACollector(Base):
            def __init__(*args, **kwargs):
                pass
            def run(*args, **kwargs):
                pass
        self.iana_collector = IANACollector(**deepcopy(other_collector_kwargs), dir_=self.dir_ / IANACollector.__name__)

    def run(self,
            sources=Source.sources.copy(),
            tool=BGPGrep,
            max_block_size=2000,
            local_files=None):
        """Downloads and parses the latest RIB dumps from sources.

        First all downloading is done so as to efficiently multiprocess
        the parsing. This was found to have significant speedup.

        In depth explanation in readme
        """


        # Downloads all other collectors that we need to process MRTs
        self._download_collectors()

        mrt_files = self._init_mrt_files(sources=sources,
                                         local_files=local_files)
        try:
            # Get downloaded instances of mrt files
            mrt_files = self._download_mrts(mrt_files)
            # Saves files to CSVs. csv_paths is sorted for largest first
            self._dump_mrts(mrt_files, tool=tool)
            # Free up some disk space
            delete_paths([self.raw_dir])
            # Get all prefixes so you can assign prefix ids,
            # which must be done sequentially
            prefix_path: list = self._get_uniq_prefixes(mrt_files)
            # Parse CSVs. Must be done sequentially for block/prefix id
            self._parse_dumps(mrt_files, max_block_size, prefix_path)
            # Remove unnessecary dirs
            delete_paths([self.dumped_dir, self.prefix_dir])
        # So much space, always clean up upon error
        except Exception as e:
            dirs = [x.dir_ for x in [self,
                                     self.roa_collector,
                                     self.caida_collector,
                                     self.bgpstream_website_collector,]]
                                     #self.iana_collector]]
            delete_paths(dirs)
            raise e

    def _download_collectors(self):
        """Runs collectors which are needed to process MRTs"""

        # Roa validity, relationships/reserved ASNs for path poisoning
        for collector in [self.roa_collector,
                          self.caida_collector,
                          self.iana_collector,
                          self.bgpstream_website_collector]:
            collector.run()

    def _init_mrt_files(self, sources=Source.sources.copy(), local_files=None):
        """Gets MRT files for downloading from URLs of sources"""

        logging.info(f"Sources: {[x.__class__.__name__ for x in sources]}")

        path_kwargs = {"raw_dir": self.raw_dir,
                       "dumped_dir": self.dumped_dir,
                       "prefix_dir": self.prefix_dir,
                       "parsed_dir": self.parsed_dir}

        # Initialize MRT files from URLs of sources
        mrt_files = list()
        for source in sources:
            for url in source.get_urls(self.dl_time):
                mrt_files.append(MRTFile(url, source, **path_kwargs))
        if local_files is not None:
            assert isinstance(local_files, list)
            for path in local_files:
                mrt_files.append(MRTFile(str(path), "local_file", **path_kwargs))

        return mrt_files[:2] if self.debug else mrt_files

    def _download_mrts(self, mrt_files):
        """Downloads MRT files from URLs into paths using multiprocessing"""

        self.download_mp(lambda x: x.download(), [mrt_files])

        return [x for x in mrt_files if x.downloaded]

    def _dump_mrts(self, mrt_files, tool=BGPGrep):
        """Processes files in parallel and inserts into db"""

        # Make sure to install deps for the tool
        tool.install_deps()
        # Sort MRT and CSV paths to parse the largest first
        self.parse_mp(tool.parse, [sorted(mrt_files)], "Dumping MRTs")

    def _get_uniq_prefixes(self, mrt_files):
        """Gets all prefixes and assigns prefix IDs

        This must be done sequentially, and is done here so that other things
        can be run very fast
        """

        self.parse_mp(MRTFile.get_prefixes,
                      [sorted(mrt_files)],
                      "Getting prefixes")

        prefix_path = self.prefix_dir / "all_prefixes.txt"
        parsed_path = self.prefix_dir / "parsed.txt"
        delete_paths([prefix_path, parsed_path])
        # awk is fastest tool for unique lines
        # it uses a hash map while all others require sort
        # https://unix.stackexchange.com/a/128782/477240
        # cat is also the fastest way to combine files
        # https://unix.stackexchange.com/a/118248/477240
        cmds = [f"cd {self.prefix_dir}",
                f"cat ./* >> {prefix_path}",
                f"awk '!x[$0]++' {prefix_path} > {parsed_path}"]
        logging.info("Extracting prefix IDs")
        run_cmds(cmds)
        print(parsed_path)
        # Returns a path here so that I can skip this function for development
        return parsed_path

    def _parse_dumps(self, mrt_files, max_block_size, uniq_prefixes_path):
        """Parses all CSVs

        Note that if cpu count is = cpus than you have on the machine,
        the progress bar doesn't update very well at all

        NOTE: a data structure where concurrent reads can occur for the
        prefix origin metadata would benifit this greatly
        however python does not have such a way to do this, even with a manager
        I created custom proxy objects, but even these are actually one at a time,
        and slowed it down to an insane degree.

        Instead I read prefixes beforehand. It's very fast, takes <10m. Origins
        we must do later, because that would not be possible in cut and the regex
        is too slow even with sed or perl
        """

        # Return a list of prefixes
        # Reads them in here so I can skip the uniq prefixes func for dev
        with open(uniq_prefixes_path, "r") as f:
            uniq_prefixes = [x.strip() for x in f]
        meta = POMetadata(uniq_prefixes,
                          max_block_size,
                          self.roa_collector.tsv_path,
                          self.bgpstream_website_collector.tsv_path)
        # Later make this not hardcoded
        # https://www.iana.org/assignments/iana-as-numbers-special-registry/iana-as-numbers-special-registry.xhtml
        # https://www.iana.org/assignments/as-numbers/as-numbers.xhtml
        logging.warning("Make non public asns not hardcoded")
        non_public_asns = set([0, 112, 23456, 65535]
                              + list(range(64496, 64511))
                              + list(range(64512, 65534))
                              + list(range(65536, 65551))
                              # Unallocated
                              + list(range(147770, 196607))
                              + list(range(213404, 262143))
                              + list(range(272797, 327679))
                              + list(range(329728, 393215)))
        max_asn = 401308

        print("read caida df and pass to parse funcs. Do the same with iana")
        #for mrt_file in sorted(mrt_files):
        #    mrt_file.parse(meta)
        #input("remove above after caida and iana for mp")
        logging.info("logging about to shutdown")
        # Done here to avoid conflict when creating dirs
        # TODO: move this to use the parsed_path of MRT file
        parse_dirs = [self.parsed_dir / str(i) for i in range(meta.next_block_id + 1)]
        for parse_dir in parse_dirs:
            parse_dir.mkdir(exist_ok=self.dir_exist_ok)
        logging.shutdown()
        self.parse_mp(MRTFile.parse,
                      [sorted(mrt_files),
                       [meta] * len(mrt_files),
                       [non_public_asns] * len(mrt_files),
                       [max_asn] * len(mrt_files),
                       ],
                      "Adding metadata to MRTs")
        # Concatenate all chunk dirs into 1 file per chunk
        output_files = []
        for parse_dir in parse_dirs:
            output_file = Path(str(parse_dir) + ".tsv")
            output_files.append(output_file)
            parsed_file = next(parse_dir.iterdir())
            cmd = (f"head -n 1 {parsed_file} > {output_file} && "
                   f"tail -n+2 -q {parse_dir}/* >> {output_file}")
            run_cmds([cmd])
            delete_paths([parse_dir])

        # Concatenate all chunks together
        # Useful for statistics
        output_file = Path(str(self.parsed_dir) + ".tsv")
        parsed_file = next(self.parsed_dir.iterdir())
        cmd = (f"head -n 1 {parsed_file} > {output_file} && "
               f"tail -n+2 -q {self.parsed_dir}/* >> {output_file}")
        run_cmds([cmd])
