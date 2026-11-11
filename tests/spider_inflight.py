"""
Tests for the Spider in-flight capacity limit (inflight_limit).

All tests use local stand-ins only:
* the local test HTTP server (test_server) with gated responses,
* an in-memory dict cache backend with a gated save_response,
so no external services are required.
"""
import sys
import threading
import time
from unittest import TestCase

import six

from grab.cookie import CookieManager
from grab.document import Document
from grab.spider import Spider, Task
from grab.spider.cache_service import CacheReaderService, CacheWriterService
from grab.spider.error import SpiderMisuseError
from grab.spider.inflight import InFlightLimiter, InFlightToken
from test_server import Response
from tests.util import BaseGrabTestCase, build_spider

JOIN_TIMEOUT = 30


class DictCacheBackend(object):
    """
    In-memory cache backend stand-in with optional gates to
    simulate slow cache writes/reads.
    """

    def __init__(
        self,
        storage=None,
        save_gate=None,
        save_started=None,
        spider=None,
        **kwargs
    ):
        self.storage = storage if storage is not None else {}
        self.save_gate = save_gate
        self.save_started = save_started
        self.spider = spider

    def close(self):
        pass

    def get_item(self, url):
        return self.storage.get(url)

    def load_response(self, grab, cache_item):
        grab.setup_document(cache_item["body"])

        def custom_prepare_response_func(transport, grab):
            doc = Document()
            doc.head = cache_item["head"]
            doc.body = cache_item["body"]
            doc.code = cache_item["response_code"]
            doc.download_size = len(cache_item["body"])
            doc.upload_size = 0
            doc.download_speed = 0
            doc.url = cache_item["response_url"]
            doc.parse(charset=grab.config["document_charset"])
            doc.cookies = CookieManager(transport.extract_cookiejar())
            doc.from_cache = True
            return doc

        grab.process_request_result(custom_prepare_response_func)

    def save_response(self, url, grab):
        if self.save_started is not None:
            self.save_started.set()
        if self.save_gate is not None:
            self.save_gate.wait(15)
        self.storage[url] = build_cache_item(url, grab.doc.body)


def build_cache_item(url, body, code=200):
    return {
        "url": url,
        "response_url": url,
        "body": body,
        "head": b"",
        "response_code": code,
    }


def setup_dict_cache(bot, storage=None, **backend_kwargs):
    """Wire the dict cache stand-in into the spider."""
    storage = storage if storage is not None else {}
    reader_backend = DictCacheBackend(storage=storage, spider=bot, **backend_kwargs)
    writer_backend = DictCacheBackend(storage=storage, spider=bot, **backend_kwargs)
    bot.cache_reader_service = CacheReaderService(bot, reader_backend)
    bot.cache_writer_service = CacheWriterService(bot, writer_backend)
    return storage


def run_spider_async(bot):
    """Run bot.run() in a thread, capturing exceptions."""
    exc_info_list = []

    def target():
        try:
            bot.run()
        except Exception:  # pylint: disable=broad-except
            exc_info_list.append(sys.exc_info())

    thread = threading.Thread(target=target)
    thread.daemon = True
    thread.start()
    return thread, exc_info_list


def join_spider(thread, exc_info_list, timeout=JOIN_TIMEOUT):
    thread.join(timeout)
    if thread.is_alive():
        raise AssertionError(
            "Spider has not finished in %d seconds (possible deadlock)" % timeout
        )
    if exc_info_list:
        six.reraise(*exc_info_list[0])


class CollectSpider(Spider):
    def task_page(self, grab, task):
        self.stat.collect("handled", task.url)


class InFlightLimiterTestCase(TestCase):
    def test_acquire_release(self):
        limiter = InFlightLimiter(1)
        self.assertTrue(limiter.acquire())
        self.assertEqual(limiter.inflight, 1)
        limiter.release()
        self.assertEqual(limiter.inflight, 0)

    def test_stop_wakes_up_waiters(self):
        limiter = InFlightLimiter(1)
        self.assertTrue(limiter.acquire())
        results = []

        def waiter():
            results.append(limiter.acquire())

        thread = threading.Thread(target=waiter)
        thread.start()
        time.sleep(0.3)
        self.assertEqual(results, [])
        limiter.stop()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        # The waiter has been woken up but got no slot
        self.assertEqual(results, [False])
        # New acquires fail too
        self.assertFalse(limiter.acquire())

    def test_token_releases_slot_once(self):
        limiter = InFlightLimiter(1)
        limiter.acquire()
        token = InFlightToken(limiter)
        token.expect_cache_write()
        token.finish_parse()
        # Cache write is still pending: slot is not released
        self.assertEqual(limiter.inflight, 1)
        token.finish_cache()
        self.assertEqual(limiter.inflight, 0)
        # All further calls are no-ops
        token.finish_parse()
        token.release()
        self.assertEqual(limiter.inflight, 0)

    def test_token_force_release(self):
        limiter = InFlightLimiter(1)
        limiter.acquire()
        token = InFlightToken(limiter)
        token.expect_cache_write()
        token.release()
        self.assertEqual(limiter.inflight, 0)
        token.finish_parse()
        token.finish_cache()
        self.assertEqual(limiter.inflight, 0)


class InFlightConfigTestCase(TestCase):
    def test_default_is_disabled(self):
        bot = build_spider(CollectSpider)
        self.assertIsNone(bot.inflight_limit)
        self.assertIsNone(bot.inflight_limiter)

    def test_valid_values(self):
        bot = build_spider(CollectSpider, inflight_limit=2)
        self.assertEqual(bot.inflight_limit, 2)
        self.assertEqual(bot.inflight_limiter.size, 2)
        bot = build_spider(CollectSpider, config={"inflight_limit": 3})
        self.assertEqual(bot.inflight_limit, 3)
        bot = build_spider(CollectSpider, config={"inflight_limit": "4"})
        self.assertEqual(bot.inflight_limit, 4)

    def test_invalid_values_fail_before_start(self):
        for bad_value in (0, -1, -100, "foo", "1.5", 1.5, 0.5, True, False, [], object()):
            with self.assertRaises(SpiderMisuseError, msg=repr(bad_value)):
                build_spider(CollectSpider, inflight_limit=bad_value)
            with self.assertRaises(SpiderMisuseError, msg=repr(bad_value)):
                build_spider(CollectSpider, config={"inflight_limit": bad_value})


class InFlightLimitTestCase(BaseGrabTestCase):
    def test_slow_parse_bounds_inflight(self):
        """
        With a slow parse callback and inflight_limit=1 the spider
        must not start new network attempts while a downloaded
        document is still being parsed.
        """
        for service in ("multicurl", "threaded"):
            with self.subTest(network_service=service):
                self.server.reset()
                self.server.add_response(Response(data=b"ok"), count=-1)
                handler_entered = threading.Event()
                handler_release = threading.Event()

                class SlowParseSpider(Spider):
                    def task_page(self, grab, task):
                        handler_entered.set()
                        handler_release.wait(15)
                        self.stat.collect("handled", task.url)

                bot = build_spider(
                    SlowParseSpider,
                    network_service=service,
                    thread_number=3,
                    inflight_limit=1,
                )
                bot.setup_queue()
                for _ in range(3):
                    bot.add_task(Task("page", self.server.get_url()))
                thread, exc_info_list = run_spider_async(bot)
                try:
                    self.assertTrue(handler_entered.wait(15))
                    # Give the spider a chance to over-fetch
                    time.sleep(1)
                    # Only one attempt is in-flight: no new network
                    # requests while the parse callback is blocked
                    self.assertEqual(bot.inflight_limiter.inflight, 1)
                    self.assertEqual(
                        bot.stat.counters.get("spider:request-network", 0), 1
                    )
                    handler_release.set()
                    join_spider(thread, exc_info_list)
                finally:
                    handler_release.set()
                    bot.stop()
                    thread.join(15)
                self.assertEqual(len(bot.stat.collections["handled"]), 3)
                self.assertEqual(bot.inflight_limiter.inflight, 0)

    def test_slow_cache_write_holds_slot(self):
        """
        The in-flight slot is released only when the cache write of
        the response has finished, even if parsing is already done.
        """
        self.server.add_response(Response(data=b"ok"), count=-1)
        save_gate = threading.Event()
        save_started = threading.Event()
        storage = {}
        bot = build_spider(CollectSpider, thread_number=3, inflight_limit=1)
        setup_dict_cache(
            bot, storage=storage, save_gate=save_gate, save_started=save_started
        )
        bot.setup_queue()
        url = self.server.get_url()
        bot.add_task(Task("page", url))
        bot.add_task(Task("page", url))
        thread, exc_info_list = run_spider_async(bot)
        try:
            self.assertTrue(save_started.wait(15))
            # Give the parser a chance to finish the first task
            time.sleep(1)
            # Parsing of the first task is done but the cache write
            # is blocked: the slot is still held and the second task
            # has not been read from the cache yet
            self.assertEqual(len(bot.stat.collections["handled"]), 1)
            self.assertEqual(bot.inflight_limiter.inflight, 1)
            self.assertEqual(bot.stat.counters.get("cache:req-hit", 0), 0)
            save_gate.set()
            join_spider(thread, exc_info_list)
        finally:
            save_gate.set()
            bot.stop()
            thread.join(15)
        self.assertEqual(len(bot.stat.collections["handled"]), 2)
        # The second task was served from the cache written
        # by the first task
        self.assertEqual(bot.stat.counters["cache:req-miss"], 1)
        self.assertEqual(bot.stat.counters["cache:req-hit"], 1)
        self.assertEqual(bot.stat.counters["spider:request-network"], 1)
        self.assertEqual(bot.inflight_limiter.inflight, 0)

    def test_cache_hit_and_miss_do_not_double_acquire(self):
        """
        A cache miss followed by the network request takes a single
        slot. With inflight_limit=1 a double acquire would deadlock,
        so plain completion of this test proves the slot is reused.
        """
        self.server.add_response(Response(data=b"from-network"), count=-1)
        url_cached = self.server.get_url("/cached")
        url_fresh = self.server.get_url("/fresh")
        storage = {url_cached: build_cache_item(url_cached, b"from-cache")}
        bot = build_spider(CollectSpider, thread_number=2, inflight_limit=1)
        setup_dict_cache(bot, storage=storage)
        bot.setup_queue()
        bot.add_task(Task("page", url_cached))
        bot.add_task(Task("page", url_fresh))
        thread, exc_info_list = run_spider_async(bot)
        try:
            join_spider(thread, exc_info_list)
        finally:
            bot.stop()
            thread.join(15)
        self.assertEqual(
            sorted(bot.stat.collections["handled"]), sorted([url_cached, url_fresh])
        )
        self.assertEqual(bot.stat.counters["cache:req-hit"], 1)
        self.assertEqual(bot.stat.counters["cache:req-miss"], 1)
        self.assertEqual(bot.stat.counters["spider:request-network"], 1)
        # The network response has been stored into the cache
        self.assertIn(url_fresh, storage)
        self.assertEqual(bot.inflight_limiter.inflight, 0)

    def test_failed_attempt_releases_slot_and_retry_reenters(self):
        """
        A failed attempt releases its slot immediately; the retried
        task acquires a new slot. With inflight_limit=1 a leaked
        slot would deadlock the retry.
        """
        self.server.add_response(Response(data=b"bad", status=500), count=1)
        self.server.add_response(Response(data=b"ok"), count=-1)
        bot = build_spider(
            CollectSpider,
            thread_number=2,
            inflight_limit=1,
            network_try_limit=2,
        )
        bot.setup_queue()
        bot.add_task(Task("page", self.server.get_url()))
        thread, exc_info_list = run_spider_async(bot)
        try:
            join_spider(thread, exc_info_list)
        finally:
            bot.stop()
            thread.join(15)
        self.assertEqual(bot.stat.counters["spider:request-network"], 2)
        self.assertEqual(len(bot.stat.collections["handled"]), 1)
        self.assertEqual(bot.inflight_limiter.inflight, 0)

    def test_handler_subtasks_do_not_deadlock(self):
        """
        Tasks yielded by the parse callback are dispatched and
        processed while the inflight limit is enabled.
        """

        class SubtaskSpider(Spider):
            def task_page(self, grab, task):
                self.stat.collect("handled", task.url)
                for _ in range(2):
                    yield Task("child", url=self.meta["server"].get_url())

            def task_child(self, grab, task):
                self.stat.collect("child", task.url)

        self.server.add_response(Response(data=b"ok"), count=-1)
        bot = build_spider(
            SubtaskSpider,
            thread_number=2,
            inflight_limit=1,
            meta={"server": self.server},
        )
        bot.setup_queue()
        bot.add_task(Task("page", self.server.get_url()))
        thread, exc_info_list = run_spider_async(bot)
        try:
            join_spider(thread, exc_info_list)
        finally:
            bot.stop()
            thread.join(15)
        self.assertEqual(len(bot.stat.collections["handled"]), 1)
        self.assertEqual(len(bot.stat.collections["child"]), 2)
        self.assertEqual(bot.inflight_limiter.inflight, 0)

    def test_only_cache_mode_does_not_deadlock(self):
        """
        In only_cache mode cache hits are parsed and cache misses
        are dropped without network requests; the inflight limit
        must not deadlock either path.
        """
        url_cached = self.server.get_url("/cached")
        url_missed = self.server.get_url("/missed")
        storage = {url_cached: build_cache_item(url_cached, b"from-cache")}
        bot = build_spider(
            CollectSpider, thread_number=2, inflight_limit=1, only_cache=True
        )
        setup_dict_cache(bot, storage=storage)
        bot.setup_queue()
        bot.add_task(Task("page", url_cached))
        bot.add_task(Task("page", url_missed))
        thread, exc_info_list = run_spider_async(bot)
        try:
            join_spider(thread, exc_info_list)
        finally:
            bot.stop()
            thread.join(15)
        self.assertEqual(bot.stat.collections["handled"], [url_cached])
        self.assertEqual(
            bot.stat.counters["spider:request-network-disabled-only-cache"], 1
        )
        self.assertEqual(bot.stat.counters.get("spider:request-network", 0), 0)
        self.assertEqual(bot.inflight_limiter.inflight, 0)

    def test_stop_wakes_up_capacity_waiters(self):
        """
        spider.stop() wakes up services blocked on the inflight
        capacity, so the spider shuts down promptly.
        """
        self.server.add_response(Response(data=b"ok"), count=-1)
        handler_entered = threading.Event()
        handler_release = threading.Event()

        class SlowParseSpider(Spider):
            def task_page(self, grab, task):
                handler_entered.set()
                handler_release.wait(15)
                self.stat.collect("handled", task.url)

        bot = build_spider(
            SlowParseSpider, thread_number=3, inflight_limit=1
        )
        bot.setup_queue()
        for _ in range(3):
            bot.add_task(Task("page", self.server.get_url()))
        thread, exc_info_list = run_spider_async(bot)
        try:
            self.assertTrue(handler_entered.wait(15))
            # Let other tasks pile up on the inflight capacity
            time.sleep(1)
            bot.stop()
            handler_release.set()
            join_spider(thread, exc_info_list, timeout=20)
        finally:
            handler_release.set()
            bot.stop()
            thread.join(15)
        self.assertFalse(thread.is_alive())
        self.assertTrue(bot.inflight_limiter.stopped)
