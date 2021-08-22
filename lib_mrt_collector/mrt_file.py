import csv
import logging
from os import path
from urllib.parse import quote

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
        self.raw_path = path.join(raw_dir, self._url_to_path())
        self.dumped_path = path.join(dumped_dir, self._url_to_path(ext=".csv"))
        self.prefix_path = path.join(prefix_dir, self._url_to_path(ext=".txt"))
        self.parsed_path = path.join(parsed_dir, self._url_to_path(ext=".tsv"))

    def __lt__(self, other):
        """Returns the file that is smaller"""

        if isinstance(other, MRTFile):
            for path_attr in ["dumped_path", "raw_path"]:
                # Save the paths to variables
                self_path = getattr(self, path_attr)
                other_path = getattr(other, path_attr) 
                # If both parsed paths exist
                if path.exists(self_path) and path.exists(other_path):
                    # Check the file size
                    if path.getsize(self_path) < path.getsize(other_path):
                        return True
                    else:
                        return False
        raise NotImplementedError


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
        helper_funcs.run_cmds(cmd)

    def parse(self, po_metadata):
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

        with open(self.dumped_path, "r") as rf,\
            open(self.parsed_path, "w") as wf:
            # Initialize reader and writer
            reader = csv.reader(rf, delimiter="|")
            writer = csv.writer(wf, delimiter="\t")
            for ann in reader:
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
                meta = po_metadata.get_meta(prefix, int(origin))
 
                # NOT SAVING:
                # type of announcement
                # next_hop - an ipaddress
                # bgp_type (i vs ebgp)
                # peer (peer-address, collector)
                # Aggregator ip address
                # asn_32_bit - 1 if yes 0 if no
                # Feel free to add these later, it won't break things
                # Just also add them to the table
                # Taken care of in dict reader initialize (ignore extras)
                #for k in ["type", "next_hop", "bgp_type", "peer", "asn_32b"]:
                #    del ann[k]
                wfields = (prefix,
                           as_path,
                           atomic_aggregate,
                           aggregator,
                           communities,
                           timestamp,
                           origin,
                           collector,
                           *meta)


                # Saving rows to a list then writing is slower
                writer.writerow(wfields)

    def _url_to_path(self, ext=""):
        _path = quote(self.url).replace("/", "_")
        if ext:
            _path = _path.replace(".gz", ext).replace(".bz2", ext)
        return _path

    @property
    def downloaded(self):
        """Returns true if the raw file was downloaded"""

        return True if path.exists(self.raw_path) else False
