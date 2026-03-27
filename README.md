Informational Badges:

[![PyPI version](https://badge.fury.io/py/mrt_collector.svg)](https://badge.fury.io/py/mrt_collector)
![PyPy](https://img.shields.io/badge/PyPy-7.3.17-blue)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/mrt_collector)](https://pypi.org/project/mrt_collector/)
![Tests](https://github.com/jfuruness/mrt_collector/actions/workflows/tests.yml/badge.svg)
![Linux](https://img.shields.io/badge/os-Linux-blue.svg)
![macOS Intel](https://img.shields.io/badge/os-macOS_Intel-lightgrey.svg)
![macOS ARM](https://img.shields.io/badge/os-macOS_ARM-lightgrey.svg)

Some Linting Badges (Where I could find them):

[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Checked with mypy](https://img.shields.io/badge/mypy-checked-2A6DBA.svg)](http://mypy-lang.org/)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![Pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/pylint-dev/pylint/tree/main)
[![try/except style: tryceratops](https://img.shields.io/badge/try%2Fexcept%20style-tryceratops%20%F0%9F%A6%96%E2%9C%A8-black)](https://github.com/guilatrova/tryceratops)

# mrt\_collector

### If you like the repo, it would be awesome if you could add a star to it! It really helps out the visibility. Also for any questions at all we'd love to hear from you at jfuruness@gmail.com

* [Description](#package-description)
* [Usage](#usage)
* [Installation](#installation)
* [Testing](#testing)
* [Development/Contributing](#developmentcontributing)
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

Install python and pip if you have not already.
Then run:

```bash
# Needed for graphviz and Pillow
pip3 install pip --upgrade
pip3 install wheel
```

For production:

```bash
pip3 install mrt_collector
```

This will install the package and all of it's python dependencies.

If you want to install the project for development:

```bash
git clone https://github.com/jfuruness/mrt_collector.git
cd mrt_collector
pip3 install -e .
pre-commit install
```

To test the development package: [Testing](#testing)

## Testing

To test the package after installation:

```
cd mrt_collector
pytest mrt_collector
ruff check mrt_collector
ruff format mrt_collector
mypy mrt_collector
```

If you want to run it across multiple environments, and have python 3.10 and 3.11 installed:

```
cd mrt_collector
tox --skip-missing-interpreters
```

## Development/Contributing

1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Test it
5. Run tox
6. Commit your changes: `git commit -am 'Add some feature'`
7. Push to the branch: `git push origin my-new-feature`
8. Ensure github actions are passing tests
9. Email me at jfuruness@gmail.com if it's been a while and I haven't seen it

## License

BSD License (see license file)
