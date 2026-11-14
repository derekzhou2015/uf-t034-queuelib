========
queuelib
========

.. image:: https://img.shields.io/pypi/v/queuelib.svg
   :target: https://pypi.python.org/pypi/queuelib

.. image:: https://img.shields.io/pypi/pyversions/queuelib.svg
   :target: https://pypi.python.org/pypi/queuelib

.. image:: https://github.com/scrapy/queuelib/actions/workflows/tests-ubuntu.yml/badge.svg
   :target: https://github.com/scrapy/queuelib/actions/workflows/tests-ubuntu.yml

.. image:: https://img.shields.io/codecov/c/github/scrapy/queuelib/master.svg
   :target: http://codecov.io/github/scrapy/queuelib?branch=master
   :alt: Coverage report


Queuelib is a Python library that implements object collections which are stored
in memory or persisted to disk, provide a simple API, and run fast.

Queuelib provides collections for queues_ (FIFO), stacks_ (LIFO), queues
sorted by priority and queues that are emptied in a round-robin_ fashion.

.. note:: Queuelib collections are not thread-safe.

Queuelib supports Python 3.10+ and has no dependencies.

.. _queues: https://en.wikipedia.org/wiki/FIFO_(computing_and_electronics)
.. _round-robin: https://en.wikipedia.org/wiki/Round-robin_scheduling
.. _stacks: https://en.wikipedia.org/wiki/Stack_(abstract_data_type)

Installation
============

You can install Queuelib either via the Python Package Index (PyPI) or from
source.

To install using pip::

    $ pip install queuelib

To install using easy_install::

    $ easy_install queuelib

If you have downloaded a source tarball you can install it by running the
following (as root)::

    # python setup.py install

FIFO/LIFO memory queues
=======================

``FifoMemoryQueue`` and ``LifoMemoryQueue`` hold their items in memory, take no
path, and accept objects of any type::

    >>> from queuelib import FifoMemoryQueue
    >>> q = FifoMemoryQueue()
    >>> q.push({'a': 1})
    >>> q.pop()
    {'a': 1}

FIFO/LIFO disk queues
=====================

Queuelib provides four disk queue classes, all of which store bytes:
``FifoDiskQueue`` and ``LifoDiskQueue``, which use a file format of their own,
and ``FifoSQLiteQueue`` and ``LifoSQLiteQueue``, which use SQLite. See
`Choosing a disk queue class`_ for the trade-off.

Here is an example usage of the FIFO queue::

    >>> from queuelib import FifoDiskQueue
    >>> q = FifoDiskQueue("queuefile")
    >>> q.push(b'a')
    >>> q.push(b'b')
    >>> q.push(b'c')
    >>> q.pop()
    b'a'
    >>> q.close()
    >>> q = FifoDiskQueue("queuefile")
    >>> q.pop()
    b'b'
    >>> q.pop()
    b'c'
    >>> q.pop()
    >>>

The other disk queue classes have the same API, and the LIFO ones pop the item
that was pushed last.

PriorityQueue
=============

A discrete-priority queue implemented by combining multiple FIFO/LIFO queues
(one per priority).

First, select the type of queue to be used per priority (FIFO or LIFO)::

    >>> from queuelib import FifoDiskQueue
    >>> qfactory = lambda priority: FifoDiskQueue('queue-dir-%s' % priority)

Then instantiate the Priority Queue with it::

    >>> from queuelib import PriorityQueue
    >>> pq = PriorityQueue(qfactory)

And use it::

    >>> pq.push(b'a', 3)
    >>> pq.push(b'b', 1)
    >>> pq.push(b'c', 2)
    >>> pq.push(b'd', 2)
    >>> pq.pop()
    b'b'
    >>> pq.pop()
    b'c'
    >>> pq.pop()
    b'd'
    >>> pq.pop()
    b'a'

RoundRobinQueue
===============

Has nearly the same interface and implementation as a Priority Queue except
that each element must be pushed with a (mandatory) key.  Popping from the
queue cycles through the keys "round robin".

Instantiate the Round Robin Queue similarly to the Priority Queue::

    >>> from queuelib import RoundRobinQueue
    >>> rr = RoundRobinQueue(qfactory)

And use it::

    >>> rr.push(b'a', '1')
    >>> rr.push(b'b', '1')
    >>> rr.push(b'c', '2')
    >>> rr.push(b'd', '2')
    >>> rr.pop()
    b'a'
    >>> rr.pop()
    b'c'
    >>> rr.pop()
    b'b'
    >>> rr.pop()
    b'd'

Clearing a queue
================

``clear()`` removes every item from a queue, freeing the disk space that they
used, and leaves the queue open and usable::

    >>> q.clear()
    >>> len(q)
    0

``PriorityQueue`` and ``RoundRobinQueue`` also close their internal queues, the
same way that ``pop()`` does when one of them becomes empty.

Disk persistence
================

Disk queues write their items to the path they get on instantiation, so that a
queue can be resumed later, even by a different process.

Each class uses that path differently:

-   ``FifoDiskQueue`` uses a directory, which it creates, together with any
    missing parent directory. Items go into chunk files (``q00000``,
    ``q00001``, etc.), each holding up to ``chunksize`` items, and the queue
    also keeps an ``info.json`` file there for its own bookkeeping.

-   ``LifoDiskQueue``, ``FifoSQLiteQueue`` and ``LifoSQLiteQueue`` use a single
    file, whose parent directory must already exist.

The layout and the contents of those files, including the database schema of
the SQLite queues, are an implementation detail that may change in any release.
Do not read or write them yourself, and do not expect a queue written by one
version of Queuelib to be readable by a different one.

Choosing a disk queue class
---------------------------

``FifoDiskQueue`` and ``LifoDiskQueue`` are fast and lose the items that a
crash did not account for. Their bookkeeping is only written on ``close()``
(and, since 1.11, atomically), so a crash costs every item pushed since the
last ``close()`` call. Reopening one of those queues after a crash is safe:
the stale items are discarded and never overtake the items of the recovering
session, and a failed write leaves the queue either recoverable or unopenable.

``FifoSQLiteQueue`` and ``LifoSQLiteQueue`` commit every ``push()`` and
``pop()`` call, so a crash or a failed write costs nothing. Each commit waits
for a disk flush, which on common storage takes about a millisecond and limits
these queues to a few hundred operations per second, against hundreds of
thousands for the file queues.

Measure that cost next to the work that your code does per item before letting
it decide: a millisecond is most of the budget when items are cheap to produce,
and noise when each one comes from the network. The subsections below cover
each failure mode in detail.

Always close disk queues
------------------------

Closing a disk queue is still mandatory, but no longer closing one is no longer
corrupting. ``FifoDiskQueue`` and ``LifoDiskQueue`` keep an on-disk marker
(inside ``info.json``, or in a sidecar ``.qmeta`` file) that records whether
the queue was open the last time it was used, together with the position of
the last clean ``close()``::

    from contextlib import closing

    with closing(FifoDiskQueue("queuedir")) as q:
        q.push(b'a')

If a process is killed or the machine loses power before ``close()`` runs,
reopening the queue reconciles it against that last clean state:

- every item pushed by the crashed session becomes unreachable, exactly as
  documented below; stale items never jump ahead of, or replace, items pushed
  by the recovering session;
- items that the recovering session has not delivered yet survive a later
  ``close()`` or ``clear()``;
- this holds over any number of repeated crash/recovery rounds in the same
  process.

There are two unavoidable limits. Queuelib never calls ``fsync()``, and
``LifoDiskQueue`` writes items through a buffered file object, so the most
recent writes may never have reached the disk at all. Also, items that the
crashed session had already popped may be delivered again once the queue is
reopened: the bookkeeping only advances on ``close()``, so the recovery point
predates those pops. Treat delivery as at-least-once across a crash.

A SQLite queue that is not closed keeps every item that was pushed. It still
needs ``close()`` to delete the file of an empty queue.

Recovering from torn bookkeeping
--------------------------------

The bookkeeping files are replaced atomically, so a power loss halfway through
a write leaves the previous version intact instead of a half-written file.
Torn or otherwise invalid bookkeeping can still happen if the underlying
storage loses whole pages, or if a ``LifoDiskQueue`` ``pop()`` had truncated
the data file before the crash, in which case accounted and stale bytes can no
longer be told apart.

In those cases opening the queue raises :class:`~queuelib.QueueRecoveryError`,
a ``ValueError`` subclass. Queuelib never silently discards the affected data,
and the error is raised from the constructor, before any read or write can
happen, so there is no half-initialized queue. Remove the queue's path and use
it again to create a new, empty queue::

    from queuelib import QueueRecoveryError
    import shutil

    try:
        q = FifoDiskQueue("queuedir")
    except QueueRecoveryError:
        shutil.rmtree("queuedir")
        q = FifoDiskQueue("queuedir")

A failed push corrupts a file queue
-----------------------------------

``push()`` on ``FifoDiskQueue`` or ``LifoDiskQueue`` may fail part way through
writing an item, for example with ``OSError`` when the disk is full. Queuelib
does not undo the part that was written. On the next reopen, the recovery
described above truncates the file back to the last clean state, so the
half-written item becomes unreachable instead of being returned by ``pop()``;
the rare cases where that is not possible raise
:class:`~queuelib.QueueRecoveryError`. Stop using a queue whose ``push()``
call raised until it has been reopened.

On a SQLite queue the failed ``push()`` call is rolled back and the queue stays
usable.

Empty queues delete their files
-------------------------------

``close()`` on an empty queue deletes its file, or, in the case of
``FifoDiskQueue``, its chunk files and its ``info.json`` file, and also its
directory if nothing else remains in it. Using that same path again creates a
new, empty queue.

FifoDiskQueue frees disk space one chunk at a time
--------------------------------------------------

``FifoDiskQueue`` deletes a chunk file once every item in it has been popped.
Until then, popped items keep using disk space, so a queue uses up to
``chunksize`` items worth of disk space on top of the items that it holds.

Lower ``chunksize`` to lower that overhead, at the cost of more chunk files and
more file operations. For example, a queue that holds 400 items of 1 MB each
uses about 100 GB of disk space with the default ``chunksize`` of 100000, and
about 800 MB with a ``chunksize`` of 400.

Reopening a FifoDiskQueue keeps its chunk size
----------------------------------------------

``FifoDiskQueue`` stores its ``chunksize`` when creating a queue, and reuses
the stored value when reopening one, ignoring the ``chunksize`` parameter.

Use one queue object per path at a time
---------------------------------------

On top of not being thread-safe, a given path must not be used by more than one
open queue object at a time, in the same process or not.

``FifoDiskQueue`` and ``LifoDiskQueue`` do not lock the files that they use, so
such queue objects overwrite each other's items and bookkeeping; for example,
two ``FifoDiskQueue`` objects on the same directory return the same items, and
their ``close()`` calls may raise ``FileNotFoundError``.

SQLite serializes access to its file, but ``pop()`` on a SQLite queue reads and
deletes an item in separate steps, so concurrent queue objects on the same path
may return the same item more than once.

Persisting a PriorityQueue or a RoundRobinQueue
-----------------------------------------------

``PriorityQueue`` and ``RoundRobinQueue`` do not write anything to disk
themselves; their persistence comes entirely from the queues that ``qfactory``
builds, and it is up to ``qfactory`` to map a priority or a key to a valid
path.

Their ``close()`` method returns the priorities or keys whose underlying queue
was not empty. Storing that value is your responsibility, and so is passing it
back as ``startprios`` or ``start_domains`` on the next run::

    >>> import json
    >>> from queuelib import FifoDiskQueue, PriorityQueue
    >>> qfactory = lambda priority: FifoDiskQueue('queue-dir-%s' % priority)
    >>> pq = PriorityQueue(qfactory)
    >>> pq.push(b'a', 3)
    >>> active = pq.close()
    >>> with open('active.json', 'w') as f:
    ...     json.dump(active, f)
    ...
    >>> with open('active.json') as f:
    ...     startprios = json.load(f)
    ...
    >>> pq = PriorityQueue(qfactory, startprios)
    >>> pq.pop()
    b'a'

Priorities and keys that you do not pass back are not detected, and the items
in their queues stay on disk, unreachable.

Bug tracker
===========

If you have any suggestions, bug reports or annoyances please report them to
our issue tracker at: http://github.com/scrapy/queuelib/issues/

Contributing
============

Development of Queuelib happens at GitHub: http://github.com/scrapy/queuelib

You are highly encouraged to participate in the development. If you don't like
GitHub (for some reason) you're welcome to send regular patches.

All changes require tests to be merged.

Tests
=====

Tests are located in `queuelib/tests` directory. They can be run using
`nosetests`_ with the following command::

    nosetests

The output should be something like the following::

    $ nosetests
    .............................................................................
    ----------------------------------------------------------------------
    Ran 77 tests in 0.145s

    OK

License
=======

This software is licensed under the BSD License. See the LICENSE file in the
top distribution directory for the full license text.

Versioning
==========

This software follows `Semantic Versioning`_

.. _Scrapy framework: http://scrapy.org
.. _scrapy-users: http://groups.google.com/group/scrapy-users
.. _Semantic Versioning: http://semver.org/
.. _nosetests: https://nose.readthedocs.org/en/latest/
