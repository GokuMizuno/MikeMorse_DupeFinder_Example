There are two files, both are separate scripts.  One, Duplicate_sample.py, is not persistent and is designed for a single folder tree, the other, SQL_sample.py, is persistent and designed for multiple folders across a network.

I also have a C++ version of sql_sample.py, which runs 20x faster, and has a few enhancements, designed to handle multi-TB data pools.  It is, not quite as portable as the python script.  However, I can easily make it work for both Debian and Windows based machines.
I also have a PowerShell version of this script; it is, however, greatly stripped down, as PowerShell is not as performant as C++, or Python.
