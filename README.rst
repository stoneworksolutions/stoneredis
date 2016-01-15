===============================
StoneRedis
===============================

.. image:: https://img.shields.io/pypi/v/stoneredis.svg
        :target: https://pypi.python.org/pypi/stoneredis

.. image:: https://img.shields.io/travis/stoneworksolutions/stoneredis.svg
        :target: https://travis-ci.org/stoneworksolutions/stoneredis

.. image:: https://readthedocs.org/projects/stoneredis/badge/?version=latest
        :target: https://readthedocs.org/projects/stoneredis/?badge=latest
        :alt: Documentation Status


Redis client based on redis-py client with some added features

* Free software: ISC license
* Documentation: https://stoneredis.readthedocs.org.

Features
--------

* Fully compatible with redis-py implementation
* Added some convenient features:
+ Multi lpop: Pops multiple elements from a queue in an atomic way
+ Multi rpush: Pushes multiple elements to a list. If bulk_size is set it will execute the pipeline every bulk_size elements. This operation will be atomic if transaction=True is passed
+ Multi rpush limit: Pushes multiple elements to a list in an atomic way until it reaches certain size. Once limit is reached, the function will lpop the oldest elements. This operation runs in LUA, so is always atomic



Credits
---------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
