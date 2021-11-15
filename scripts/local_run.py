from datetime import datetime
from pathlib import Path

from lib_mrt_collector import MRTCollector, Source, BGPGrep

def run(dl_time, sources=Source.sources.copy(), tool=BGPGrep, max_block_size=2000):

    t_str = dl_time.strftime("%Y_%m_%d")
    path = Path(f"/data/mrt_cache/{t_str}")
    local_file_path = Path(f"/data/mrt_cache/local_files/{t_str}.mrt")
    assert local_file_path.exists(), local_file_path
    collector = MRTCollector(dir_=path, dir_exist_ok=True, dl_time=dl_time)
    collector._download_collectors()  ##
    mrt_files = collector._init_mrt_files(sources=[], local_files=[str(local_file_path)])
    print("num mrt files", str(len(mrt_files)))
    mrt_files = collector._download_mrts(mrt_files)  ##
    mrt_files = [x for x in mrt_files if x.downloaded]
    # TO TEST - JUST USE 4!!
    mrt_files = list(sorted(mrt_files))####################[-4:]
    print("num mrt files downloaded", str(len(mrt_files)))
    collector._dump_mrts(mrt_files, tool=tool)  ##
    prefix_path = collector._get_uniq_prefixes(mrt_files)  ##
    prefix_path = str(path / "prefix" / "parsed.txt")
    collector._parse_dumps(mrt_files, max_block_size, prefix_path)

if __name__ == "__main__":
    # Timing test
    # #######################################################input("Ensure com is on performance mode")

    kwargs = {"year": 2021, "hour": 0, "minute": 0, "second": 0}
    dl_times = [datetime(month=1, day=22, **kwargs),
                datetime(month=3, day=19, **kwargs),
                datetime(month=4, day=29, **kwargs),
                datetime(month=6, day=2, **kwargs),
                datetime(month=8, day=21, **kwargs)]
    for dl_time in dl_times:
        start = datetime.now()
        run(dl_time)
        print((datetime.now() - start).total_seconds())
