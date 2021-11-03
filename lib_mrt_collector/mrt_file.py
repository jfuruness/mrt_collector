import csv
import logging
from os import path
from urllib.parse import quote

from ipaddress import ip_network

from lib_utils import helper_funcs, file_funcs


class MRTFile:
    """This class contains functionality associated with MRT Files"""

    def __init__(self,
                 url,
                 source,
                 raw_dir=None,
                 dumped_dir=None,
                 prefix_dir=None,
                 parsed_dir=None):
        """Inits MRT File and the paths at which to write to"""

        self.url = url
        self.source = source
        self.parsed_dir = parsed_dir
        self.raw_path = raw_dir / self._url_to_path()
        self.dumped_path = dumped_dir / self._url_to_path(ext=".csv")
        self.prefix_path = prefix_dir / self._url_to_path(ext=".txt")

    def parsed_path(self, block_id):
        """Returns parsed path for that specific block id"""

        block_dir = self.parsed_dir / str(block_id)
        block_dir.mkdir(exist_ok=True)
        return block_dir / self._url_to_path(ext=".tsv")

    def __lt__(self, other):
        """Returns the file that is smaller"""

        if isinstance(other, MRTFile):
            for path_attr in ["dumped_path", "raw_path"]:
                # Save the paths to variables
                self_path = getattr(self, path_attr)
                other_path = getattr(other, path_attr) 
                # If both parsed paths exist
                if self_path.exists() and other_path.exists():
                    # Check the file size, sort in descending order
                    # That way largest files are done first
                    # https://stackoverflow.com/a/2104107/8903959
                    if self_path.stat().st_size > other_path.stat().st_size:
                        return True
                    else:
                        return False
        return NotImplemented


    def download(self):
        """Downloads raw MRT file"""

        file_funcs.download_file(self.url, self.raw_path)

    def get_prefixes(self):
        """Gets all prefixes within the MRT files"""

        # unique instead of awk here because it's sometimes ribs in
        # so may prefix origin pairs are next to each other
        # By adding uniq here. mrt_collector._get_prefix_ids has a 3x speedup
        # Even the bash cmd speeds up because it doesn't write as much
        cmd = f'cut -d "|" -f 2 {self.dumped_path} | uniq > {self.prefix_path}'
        helper_funcs.run_cmds([cmd])

    def parse(self, po_metadata, non_public_asns: set, max_asn: int):
        """Parses MRT file and adds metadata

        Things I've tried to make this faster that didn't work
        /dev/shm
        writing to lists then writing all at once
        pypy3
        etc

        unfortunately, I think it's largely csv writing that makes it slow
        Note that using dicts over lists is wayyyy slower
        """

        # TYPE|PREFIXES|PATH ATTRIBUTES|PEER|TIMESTAMP|ASN32BIT
        # PATH ATTRIBUtES:
        # AS_PATH|NEXT_HOP|ORIGIN|ATOMIC_AGGREGATE|AGGREGATOR|COMMUNITIES

        # File that will be read from
        rfile = self.dumped_path.open(mode="r")
        # Opens all files for the block ids
        wfiles = [open(self.parsed_path(i), "w")
                  for i in range(po_metadata.next_block_id + 1)]
        # CSV reader
        reader = csv.reader(rfile, delimiter="|")
        writers = [csv.writer(x, delimiter="\t") for x in wfiles]
        wfields = ("prefix", "as_path", "atomic_aggregate", "aggregator",
                   "communities", "timestamp", "origin", "collector",
                   "prepending", "loops", "ixps", "gao_rexford", "new_asns",
                   "path_poisoning", "roa_validity", "prefix_id", "block_id",
                   "prefix_block_id")
        for writer in writers:
            writer.writerow(wfields)

        for i, ann in enumerate(reader):
            try:
                (_type,
                 prefix,
                 as_path,
                 next_hop,
                 bgp_type,
                 atomic_aggregate,
                 aggregator,
                 communities,
                 peer,
                 timestamp,
                 asn_32b) = ann
            except ValueError as e:
                print(f"Problem with ann line {i} for {self.dumped_path}, fix later")
                continue

            try:
                prefix_obj = ip_network(prefix)
            # This occurs whenever host bits are set
            except ValueError:
                continue

            if _type != "=":
                continue

            if atomic_aggregate:
                if atomic_aggregate != "AT":
                    print("ann doesn't have AT for attomic aggregate")
                    input(ann)
                else:
                    atomic_aggregate = True
            # AS set in the path
            if "{" in as_path:
                continue
            # There is no AS path
            if not as_path:
                continue
            _as_path = as_path.split(" ")
            path_data = self._get_path_data(_as_path,
                                            non_public_asns,
                                            max_asn,
                                            set())
            origin = _as_path[-1]
            collector = _as_path[0]
            if aggregator:
                # (aggregator_asn aggregator_ip_address)
                aggregator = aggregator.split(" ")[0]

            # Adding:
            # prefix_id
            # block_id
            # prefix_block_id
            # origin_id
            # NOTE: This is a shallow copy for speed! Do not modify!
            meta = po_metadata.get_meta(prefix, prefix_obj, int(origin))
            roa_validity, prefix_id, block_id, prefix_block_id = meta
            # NOT SAVING:
            # type of announcement
            # next_hop - an ipaddress
            # bgp_type (i vs ebgp)
            # peer (peer-address, collector)
            # Aggregator ip address
            # asn_32_bit - 1 if yes 0 if no
            # Feel free to add these later, it won't break things
            # Just also add them to the table
            wfields = (prefix,
                       as_path,
                       atomic_aggregate,
                       aggregator,
                       communities,
                       timestamp,
                       origin,
                       collector,) + path_data + (roa_validity,
                                                  prefix_id,
                                                  block_id,
                                                  prefix_block_id,)

            # Saving rows to a list then writing is slower
            writers[block_id].writerow(wfields)
        for f in wfiles + [rfile]:
            f.close()

    def _get_path_data(self, as_path, non_public_asns, max_asn, ixps):
        """Returns as path data"""

        # NOTE: make sure this matches the header!!
        prepending = False
        loop = False
        ixp = False

        as_path_set = set()
        last_asn = None
        last_non_ixp = None
        for asn in as_path:
            if last_asn == asn:
                prepending = True
                loop = True
            if asn in as_path_set:
                loop = True
            as_path_set.add(asn)
            last_asn = asn
            if asn in ixps:
                ixp = True
            else:
                last_non_ixp = asn

        # doesn't follow Gao rexford according to Caida
        # Contains ASNs that Caida doesn't have (that aren't non public)
        # path poisoning by reserved asn, non public asn, or clique being split
        return (int(prepending), int(loop), int(ixp), int(False), int(False), int(False),)

    def _url_to_path(self, ext=""):
        _path = quote(self.url).replace("/", "_")
        if ext:
            _path = _path.replace(".gz", ext).replace(".bz2", ext)
        return _path

    @property
    def downloaded(self):
        """Returns true if the raw file was downloaded"""

        return self.raw_path.exists()
