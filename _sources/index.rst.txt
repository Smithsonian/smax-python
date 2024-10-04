.. smax documentation master file, created by
   sphinx-quickstart on Fri Oct  4 07:35:36 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Python for SMA-X documentation
==================

The `SMA Exchange (SMA-X) <https://docs.google.com/document/d/1eYbWDClKkV7JnJxv4MxuNBNV47dFXuUWu7C4Ve_YTf0/edit?usp=sharing>`_
is a high performance and versatile real-time data sharing platform for distributed software systems. It is built 
around a central Redis database, and provides atomic access to structured data, including specific branches and/or 
leaf nodes, with associated metadata. SMA-X was developed at the Submillimeter Array (SMA) observatory, where we use 
it to share real-time data among hundreds of computers and nearly a thousand individual programs.

SMA-X consists of a set of server-side `LUA <https://lua.org/>`_ scripts that run on `Redis <https://redis.io>`_ (or one 
of its forks / clones such as `Valkey <https://valkey.io>`_ or `Dragonfly <https://dragonfly.io>`_); a set of libraries to 
interface client applications; and a set of command-line tools built with them. Here we provide the API documentation
for the Python 3 client library, available on GitHub as `Smithsonian/smax-python <https://github.com/Smithsonian/smax-python>`_.


.. toctree::
   :maxdepth: 2
   :caption: Contents:
   
   smax

