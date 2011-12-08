#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sesql

from setuptools import setup, find_packages
import glob

install_requires = ['GenericCache == 1.0.2', ]

long_description = (open('README', "r").read())

setup(
    name='sesql',
    version=sesql.__version__,
    description='SeSQL (Search Engine SQL) handle advanced SQL search on huge databases of documents/articles from Django applications',
    long_description=long_description,
    author='Pilot Systems',
    author_email='gael@pilotsystems.net',
    url='https://bitbucket.org/liberation/sesql/',
    license='GPL v2+',
    keywords="sql search postgresql django",
    platforms=["any"],
    packages=find_packages(),
    include_package_data=True,
    install_requires=install_requires,
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU GPL",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.4",
        "Programming Language :: Python :: 2.5",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
    ],
)
