import pathlib
from setuptools import setup, find_packages

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "readme.md").read_text()

# This call to setup() does all the work
setup(
    name="ninjasql",
    version="0.0.1",
    description="Create and extract SQL DDL, DMLs and scd2 blueprints.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/dondaum/ninjasql",
    author="Sebastian Daum",
    author_email="sebastian.daum89@gmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",

        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',

        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    packages=find_packages(exclude=["tests", "tests.*"]),
    include_package_data=True,
    install_requires=[
        'decorator==4.4.2',
        'networkx==2.4',
        'numpy==1.18.2',
        'pandas==1.0.3',
        'python-dateutil==2.8.1',
        'pytz==2019.3',
        'six==1.14.0',
        'sqlalchemy==1.3.15'
    ],
    zip_safe=False,
)
