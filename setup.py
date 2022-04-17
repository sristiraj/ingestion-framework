#!/usr/bin/env python

from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

setup(name='ingestion_framework',
      version='1.0',
      description='Python ingestion framework Distribution Utilities',
      author='Authored',
      author_email='hello@hola.com',
      url='www.test.com',
      packages=find_packages(include=['ingestion_framework', 'ingestion_framework.*']),
      install_requires=[],
     )