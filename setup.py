import os
from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

# get the dependencies and installs
with open("requirements.txt", "r", encoding="utf-8") as f:
    requires = [x.strip() for x in f if x.strip()]

# get test dependencies and installs
with open("test_requirements.txt", "r", encoding="utf-8") as f:
    test_requires = [x.strip() for x in f if x.strip() and not x.startswith("-r")]

setup(
    name="kedro-popmon",
    version=os.getenv("VERSION", "0.1.0"),
    url="https://github.com/stephanecollot/kedro-popmon",
    author="Marian Dabrowski",
    author_email="marian.dabrowski95@gmail.com",
    description="Kedro Popmon makes integrating Popmon with Kedro easy!",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=["kedro_popmon"],
    zip_safe=False,
    include_package_data=True,
    license="MIT",
    install_requires=requires,
    tests_require=test_requires,
    extras_require={
       "spark": ["pyspark>=2.4.0", "py4j", "kedro[spark]==0.17.3"]
    },
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    entry_points={
        "kedro.project_commands": ["kedro_popmon = kedro_popmon.framework.cli.cli:commands"],
        "kedro.hooks": ["kedro_popmon = kedro_popmon.framework.kedro_popmon:hooks"],
    }
)
