import csv
from os import path
from urllib.parse import quote

from lib_utils import helper_funcs, file_funcs


ann_count = 0
as_set_count = 0

class MRTFile:
    """This class contains functionality associated with MRT Files"""

    def __init__(self, url, source, mrt_dir, csv_dir, prefix_dir, parsed_dir):
        self.url = url
        self.source = source
        self.mrt_dir = mrt_dir
        self.csv_dir = csv_dir
        self.prefix_dir = prefix_dir
        self.parsed_dir = parsed_dir

    def __lt__(self, other):
        """Checks various file sizes"""

        if isinstance(other, MRTFile):
            for path_attr in ["parsed_path", "csv_path", "mrt_path"]:
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
        else:
            raise NotImplementedError


    def download(self):
        """Downloads raw MRT file"""

        file_funcs.download_file(self.url, self.mrt_path)

    def get_prefixes(self):
        """Gets all prefixes within the MRT files"""

        # unique instead of awk here because it's sometimes ribs in
        # so may prefix origin pairs are next to each other
        # By adding uniq here. the mrt_collector._get_prefix_ids has a 3x speedup
        # Even the bash cmd speeds up because it doesn't write as much
        bash = f'cut -d "|" -f 2 {self.csv_path} | uniq > {self.prefix_path}'
        helper_funcs.run_cmds(bash)

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

        # I know this is slower than saving a counter, but it only happens
        # once for file on lists that are <100k in size, so whatever

        global ann_count
        global as_set_count

        with open(self.csv_path, "r") as rf, open(self.parsed_path, "w") as wf:
            rfields = ["type",
                       "prefix",
                       "as_path",
                       "next_hop",
                       "bgp_type",
                       "atomic_aggregate",
                       "aggregator",
                       "communities",
                       "peer",
                       "timestamp",
                       "asn_32b"]

            wfields = ["prefix",
                       "as_path",
                       "atomic_aggregate",
                       "aggregator",
                       "communities",
                       "timestamp",
                       "origin",
                       "collector",
                       "prefix_id",
                       "block_id",
                       "prefix_block_id",
                       "origin_id",
                       "roa_validity"]

            
            #reader = csv.DictReader(rf, delimiter="|", fieldnames=rfields)
            reader = csv.reader(rf, delimiter="|")
            writer = csv.writer(wf, delimiter="\t")
            #writer = csv.DictWriter(wf, delimiter="\t", fieldnames=wfields, extrasaction='ignore')
            from tqdm import tqdm
            from datetime import datetime
            start = datetime.now()
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


                ann_count += 1
                if _type != "=":
                    continue
 
                if atomic_aggregate:
                    if atomic_aggregate != "AT":
                        print("ann doesn't have AT for attomic aggregate")
                        input(ann)
                    else:
                        atomic_aggregate = True
                if "{" in as_path:
                    as_set_count += 1
                    #print(f"AS set in as_path {as_set_count}/{ann_count}")
                    continue
                if not as_path:
                    #print("as path is none")
                    continue
                _as_path = as_path.split(" ")
                origin = _as_path[-1]
                collector = as_path[0]
                if aggregator:
                    # (aggregator_asn aggregator_ip_address)
                    aggregator = aggregator.split(" ")[0]

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
                wfields = [prefix,
                           as_path,
                           atomic_aggregate,
                           aggregator,
                           communities,
                           timestamp,
                           origin,
                           collector]


                # Adding:
                # prefix_id
                # block_id
                # prefix_block_id
                # origin_id
                # roa_validity
                # NOTE: This is a shallow copy for speed! Do not modify!
                meta = po_metadata.get_meta(prefix, int(origin))
                wfields.extend(meta)
                # Saving rows to a list then writing is slower
                writer.writerow(wfields)
            print((datetime.now() - start).total_seconds() / 60, "minutes", self._url_to_path())
                
######################
### Path functions ###
######################

    def _url_to_path(self, ext=""):
        path = quote(self.url).replace("/", "_")
        if ext:
            path = path.replace(".gz", ext).replace(".bz2", ext)
        return path

    @property
    def mrt_path(self):
        return path.join(self.mrt_dir, self._url_to_path())

    @property
    def csv_path(self):
        return path.join(self.csv_dir, self._url_to_path(ext=".csv"))

    @property
    def prefix_path(self):
        return path.join(self.prefix_dir, self._url_to_path(ext=".txt"))

    @property
    def parsed_path(self):
        return path.join(self.parsed_dir, self._url_to_path(ext=".csv"))

    @property
    def dirs(self):
        return [self.mrt_dir, self.csv_dir, self.prefix_dir, self.parsed_dir]
