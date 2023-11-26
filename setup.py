from setuptools import setup, find_packages
import sys

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# https://stackoverflow.com/a/58534041/8903959
setup(
    name='mrt_collector',
    author="Justin Furuness, Matt Jaccino, Tony Zheng, Nicholas Shpetner",
    author_email="jfuruness@gmail.com",
    version="0.0.1",
    url='https://github.com/jfuruness/mrt_collector.git',
    license="BSD",
    description="Downloads public MRT RIB dumps into a database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords=["Furuness", "BGP", "Caida", "MRT", "bgpgrep", "RIB", "MRTCollector"],
    include_package_data=True,
    python_requires=">=3.10",
    packages=find_packages(),
    install_requires=[
        'ip_address',
        'tqdm',
    ],
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3'],
    entry_points={
        'console_scripts': 'mrt_collector = mrt_collector.__main__:main'
    },
)
