#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'redis==2.10.3',
    'python-redis-lock[django]==2.3.0',
]

test_requirements = [
    # Tox gets them from requirements file
]

setup(
    name='stoneredis',
    version='0.6.1',
    description="Redis client based on redis-py client with some added features",
    long_description=readme + '\n\n' + history,
    author="Pedro Ten",
    author_email='pedroten@gmail.com',
    url='https://github.com/stoneworksolutions/stoneredis',
    packages=[
        'stoneredis',
    ],
    package_dir={'stoneredis':
                 'stoneredis'},
    include_package_data=True,
    install_requires=requirements,
    license="ISCL",
    zip_safe=False,
    keywords='stoneredis',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
