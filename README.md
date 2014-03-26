# Recon File Format

The goal of the `recon` file format (or more precisely, suite of file
formats) is to provide a compact format for storing (typically
time-series oriented) simulation data in a network friendly way.
Along the way, we've added support for embedding metadata at the file,
table, object and signal level as well.

Obviously compactness is important to keep the size of files down.
Disk space is less an issue these days than it has been in the past.
Nevertheless, one our requirements was to be competitive in terms of
how much disk space is required to store signals.

In trying to develop a network friendly format, we quickly found
ourselves facing two conflicting goals.  On the one hand, we wanted
the ability to stream data from simulations to files.  This meant,
essentially, having a format that was "append friendly".  On the other
hand, we wanted a format where data for an individual signal could be
accomplished with a "single" read (i.e. single request over the
network).  Note, our definition of "single" has some caveats.

We recognized that we could not accomplish this goal with a single
format (at least not without making the implementation extremely
complicated and involving lots of extra writes).  So we opted to
specify two file formats that could easily be converted.

## Dependencies

This package requires an implementation of `msgpack`.  However, there
are several implementation in Python.  During development, the
`msgpack-python` package was used.  It is possible that other packages
might work as well.

## The "Wall" Format

The wall format is the "append friendly" format.  You can think of the
wall format as a series of "bricks" being laid down.  Each "brick"
represents some data being added to the file.  One of the nice
advantages of this format is that it allows concurrent writing to
multiple tables of data.  So in a simulation where different results
are reported at different intervals, this file format can be used to
append different results to different tables.  In other words, it
supports appending to multiple tables, not just one.

## The "Meld" Format

The meld format is mainly an archival format.  It rearranges the data
so that individual signals can be easily extracted.  This is what
enables data to be extracted with a minimal number of "reads" from the
data source.  The key issue with reads is the case where the data is
being read over a network.

As simulation moves to cloud based systems, it will be come
increasingly cumbersome to move entire files back and forth between
the cloud and the desktop/browser.  Having a format that supports
"pulling" just the information that is required on demand facilitates
utilizing cloud/remote storage solutions which will lead to more
responsive interfaces and better data management practices and
capacity.  The meld format is designed for this use case.

# Specification

The current specification for both formats can be found [here](https://github.com/xogeny/recon/blob/master/docs/SPEC.md#introduction).


