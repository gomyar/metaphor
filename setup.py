#!/usr/bin/env python

from setuptools import setup, find_packages

requirements = open("requirements.txt")

setup(
    name='metaphor',
    version='0.1',
    description='API Layer for MongoDB.',
    packages=find_packages("src"),
    include_package_data=True,
    package_dir={ '': 'src'},
    install_requires=[req.strip() for req in requirements],
    package_data={'static': ['*'], 'templates': ['*']},
)
