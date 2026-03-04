There are two files, both are separate scripts.  One, Duplicate_sample.py, is not persistent and is designed for a single folder tree, the other, SQL_sample.py, is persistent and designed for multiple folders across a network.

I also have a C++ version of sql_sample.py, which runs 20x faster, and has a few enhancements, designed to handle multi-TB data pools.  It is, not quite as portable as the python script.  However, I can easily make it work for both Debian and Windows based machines, depending on your server makeup.

The full program, besides being faster, is more memory-performant, and uses streams to read once, compute many, conserving SSD reads; useful for suspected failing drives or full arrays.  It also can persist across multiple network folders, has the option to compare two folders, and of course, can output to CSV or TXT, not simply print the duplicates.  And of course, it can filter out file extensions, so if you wanted, for example, to scan only PDFs, it will.

And of course, being a CLI only script, it can be used by automation tools such as MoveIT, or run as a cron job, again depending on the tools in use.

I also have a PowerShell version of this script; it is, however, greatly stripped down, as PowerShell is not as speedy as C++ or Python.
