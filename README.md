[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/release/python-3100/)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)
![Tests](https://github.com/jfuruness/mrt_collector/actions/workflows/tests.yml/badge.svg)

# mrt\_collector

* [Description](#package-description)
* [Usage](#usage)
* [Installation](#installation)
* [Testing](#testing)
* [Credits](#credits)
* [History](#history)
* [Development/Contributing](#developmentcontributing)
* [Licence](#license)


## Package Description

This package performs the following:

0. Initializes all of the directories
1. Get all the URLs from the MRT data sources for the MRT RIB dumps and init MRTs
    * into the requests\_cache
    * in the init_mrt_files function
1. Downloads raw MRT RIB dumps from RIPE RIS and Route Views
    * into base\_dir / raw
    * using download\_raw\_mrts func
2. Parses these MRT RIB dumps using any number of tools built to do this into base\_dir / parsed
3. Gets the unique prefixes contained within the MRT files into base\_dir / prefixes
4. Format the dumps into something meaningful and store in a CSV into base\_dir / formatted
5. Analyze the CSVs into base\_dir / analysis

## Usage
* [mrt\_collector](#mrt\_collector)

TODO

from a script:

From the command line:

## Installation
* [mrt\_collector](#mrt\_collector)

Install python and pip if you have not already.

Then run:

```bash
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
pip3 install -e .[test]
pre-commit install
```

After installing the python package, you must also install whatever BGPDumpTool
that you want to use.
These are used to extract data from the raw MRT files, and are external packages for this.
Installation for these changes from time to time, and supporting automatic installs of
these tools doesn't make much sense for this repo.
So instead users need to install them if they want to use them.
By default, this is the tool that is used https://github.com/bgpkit/bgpkit-parser
but if you want to use a different tool you must install it separately.
At the time of this writing (Dec 4 2023) this can be installed with:

```bash
sudo apt install cargo -y
git clone git@github.com:bgpkit/bgpkit-parser.git
cargo install --path ./bgpkit-parser --features cli
cp bgpkit-parser/target/release/bgpkit-parser ~/.local/bin
```

but refer to the docs to make sure it hasn't changed.

To test the development package: [Testing](#testing)


## Testing
* [mrt\_collector](#mrt\_collector)

To test the package after installation:

```
cd mrt_collector
pytest mrt_collector
ruff mrt_collector
black mrt_collector
mypy mrt_collector
```

If you want to run it across multiple environments, and have python 3.10 and 3.11 installed:

```
cd mrt_collector
tox
```

## Credits
* [mrt\_collector](#mrt\_collector)

Various people have worked on this repo.
It was originally in lib_bgp_data.
Huge credits to Matt Jaccino, Tony Zheng, Nicholas Shpetner

The sources also pull from RIPE and Route Views and can be seen in the code.
The BGP parsing tools can also be seen in the code.

## History
* [mrt\_collector](#mrt\_collector)

TODO

## Development/Contributing
* [mrt\_collector](#mrt\_collector)

1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Test it
5. Run tox
6. Commit your changes: `git commit -am 'Add some feature'`
7. Push to the branch: `git push origin my-new-feature`
8. Ensure github actions are passing tests
9. Email me at jfuruness@gmail.com if it's been a while and I haven't seen it

## License
* [mrt\_collector](#mrt\_collector)

BSD License (see license file)
