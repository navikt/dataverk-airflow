# -*- coding: utf-8 -*-
from setuptools import setup, find_packages


with open('requirements.txt') as f:
    install_requires = f.read().strip().split('\n')

setup(
    name='dataverk-airflow',
    version='1.0.0',
    readme = "README.md"
    packages=find_packages('src'),
    package_dir={'': 'src'},
    python_requires='>=3.8',
    install_requires=install_requires,
    # metadata to display on PyPI
    author="NAV IKT",
    url="https://github.com/navikt/dataverk-airflow",
    classifiers=[
        'DEVELOPMENT STATUS :: 5 - PRODUCTION/STABLE',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3'
    ],
)
