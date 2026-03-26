# mrt\_collector

[Informational Badges]

* [Description](#package-description)
* [Usage](#usage)
* [Installation](#installation)
* [License](#license)

## Package Description

This package downloads and parses raw MRT data from a given time, sourced from **RIPE** and **Routeview** archives, and supports various analysis of those parsed MRT files (currently supports only atomic aggregate analysis). This package supports single and multiprocessing.

When run, the user must provide a date and time (24-hour) from which to download MRT dumps. By default data is written to the home directory, but the user can optionally provide a custom location. The directory is formatted as:
```
ROOT
├── mrt_data
│   └── yyyy_mm_dd_hh
│       ├── analysis
│       │   └── analysis files…
│       ├── parsed
│       │   └── parsed files…
│       ├── parsed_line_count
│       │   └── line count files…
│       ├── raw
│       │   └── raw files…
│       └── head_req.json
```

Parsed files are `.psv` formatted as:
```
type|timestamp|peer_ip|peer_asn|prefix|as_path|origin_asns|origin|next_hop|local_pref|med|communities|atomic|aggr_asn|aggr_ip|only_to_customer
```

On an M2 MacBook Air with 16 GB of RAM, with ~40 MB/s download speeds, multiprocess runtime is about 30 minutes, singleprocess runtime is about 60 minutes. Atomic aggregate analysis runtime is about 30 minutes.

## Usage

From the command line:
```bash
mrt_collector -dt=mm/dd/yyyy/hh
```

> **NOTE:** RIPE dumps every 8 hours, Routeview every 2 hours, so `hh` must be `00`, `08`, or `16`.

| Flag | Long Form | Description |
|------|-----------|-------------|
| `-dt` | `--datetime` | Specifies the desired MRT dump time to download from (**required**) |
| `-p` | `--path` | Specifies the directory to place `mrt_data/…` in |
| `-sp` | `--single_process` | Forces singleprocess use on multi-core machines |
| `-lf` | `--limit_files` | Limits the number of files to process, uses *n* smallest files |

### Atomic Aggregate Analysis

The atomic aggregate analysis module outputs two JSON files, `atomic_data.json` and `atomic_prefixes.json`. Atomic prefixes includes the set of "prefixes where atomic=true", the set of "prefixes with aggregator ASN", and the set of "prefixes where atomic=true AND with aggregator ASN" (prefixes can have atomic=false and still have an aggregator ASN). Atomic data lists all prefixes that appear in atomic prefixes along with their atomic status and aggregator ASN.

## Installation

Install Python and pip if you have not already. Then run:
```bash
pip3 install pip --upgrade
pip3 install wheel
```

> *Haven't published this to PyPI yet, so this is TODO.*

## License

BSD License (see license file)
