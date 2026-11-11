.. _spider_inflight_limit:

In-flight Capacity Limit
========================

By default the spider does not limit how many downloaded documents are
held in memory at the same time. If network is fast but parsing (or
cache writing) is slow, downloaded bodies pile up in the internal
queues and memory usage grows without bound.

The `inflight_limit` option bounds the number of in-flight task
attempts, i.e. the number of tasks whose content has been read (from
the network or from the cache) and is still being processed:

.. code:: python

    bot = ExampleSpider(inflight_limit=10)
    bot.run()

The option accepts a positive integer and is disabled by default
(`None`). Any other value (zero, negative numbers, non-numbers) raises
`SpiderMisuseError` in the `Spider` constructor, before any service is
started. The option can also be set via the spider config dict with
the `inflight_limit` key.

How it works
------------

* A slot is taken right before each attempt to read the task content
  from the cache or from the network.
* The slot is released only when the output of the task handler (the
  parse callback) has been completely consumed AND the cache write of
  the response (if any) has finished.
* A failed attempt releases its slot immediately. A retry is a new
  attempt: it acquires a new slot when it re-enters the cache reader
  or the network service.
* A cache miss followed by a network request does NOT take a second
  slot: the slot of the cache read attempt is carried over to the
  network attempt.
* When all slots are in use, only new attempts are paused. Dispatching
  of results, parsing of already downloaded documents and cache
  writes keep making progress, so handlers yielding new tasks,
  retries and the `only_cache` mode do not deadlock.
* The limit is per Spider instance and works with both network
  services (`multicurl` and `threaded`).
* `spider.stop()` wakes up threads waiting for a free slot, so the
  spider shuts down promptly even when the capacity is exhausted.
