"""Unit tests for resource_cache."""

import shutil
import tempfile
from pathlib import Path

import pytest
import requests.exceptions as requests_exceptions
from google.api_core.exceptions import BadRequest
from google.cloud.storage.retry import _should_retry

from pudl.workspace import resource_cache
from pudl.workspace.resource_cache import PudlResourceKey, extend_gcp_retry_predicate

# Unit tests for the GoogleCloudStorageCache class


def test_bad_request_predicate():
    """Check extended predicate catches BadRequest and default exceptions."""
    bad_request_predicate = extend_gcp_retry_predicate(_should_retry, BadRequest)
    # Check default exceptions.
    assert not _should_retry(BadRequest(message="Bad request!"))
    assert _should_retry(requests_exceptions.Timeout())

    # Check extended predicate handles default exceptionss and BadRequest.
    assert bad_request_predicate(requests_exceptions.Timeout())
    assert bad_request_predicate(BadRequest(message="Bad request!"))


# Unit tests for the LocalFileCache class.
@pytest.fixture
def testcache():
    """setup and teardown fixture"""
    # Prepares temporary directory for storing cache contents
    test_dir = tempfile.mkdtemp()
    cache = resource_cache.LocalFileCache(Path(test_dir))
    yield (test_dir, cache)
    # Deletes content of the temporary directories
    shutil.rmtree(test_dir)


def test_add_single_resource(testcache):
    """Adding resource has expected effect on later get() and contains() calls."""
    cache = testcache[1]
    res = PudlResourceKey("ds", "doi", "file.txt")
    assert not cache.contains(res)
    cache.add(res, b"blah")
    assert cache.contains(res)
    assert cache.get(res) == b"blah"


def test_that_two_cache_objects_share_storage(testcache):
    """Two LocalFileCache instances with the same path share the object storage."""
    test_dir = testcache[0]
    cache = testcache[1]
    second_cache = resource_cache.LocalFileCache(Path(test_dir))
    res = PudlResourceKey("dataset", "doi", "file.txt")
    assert not cache.contains(res)
    assert not second_cache.contains(res)
    cache.add(res, b"testContents")
    assert cache.contains(res)
    assert second_cache.contains(res)
    assert second_cache.get(res) == b"testContents"


def test_deletion(testcache):
    """Deleting resources has expected effect on later get() / contains() calls."""
    cache = testcache[1]
    res = PudlResourceKey("a", "b", "c")
    assert not cache.contains(res)
    cache.add(res, b"sampleContents")
    assert cache.contains(res)
    cache.delete(res)
    assert not cache.contains(res)


def test_read_only_add_and_delete_do_nothing(testcache):
    """Test that in read_only mode, add() and delete() calls are ignored."""
    test_dir = testcache[0]
    cache = testcache[1]
    res = PudlResourceKey("a", "b", "c")
    ro_cache = resource_cache.LocalFileCache(Path(test_dir), read_only=True)
    assert ro_cache.is_read_only()

    ro_cache.add(res, b"sample")
    assert not ro_cache.contains(res)

    # Use read-write cache to insert resource
    cache.add(res, b"sample")
    assert not cache.is_read_only()
    assert ro_cache.contains(res)

    # Deleting via ro cache should not happen
    ro_cache.delete(res)
    assert ro_cache.contains(res)


# Unit tests for LayeredCache class


@pytest.fixture
def layeredcachetestdir():
    """Construct localfilecache layers and remove temp dirs when finished"""
    # Constructs two LocalFileCache layers pointed at temporary directories
    layered_cache = resource_cache.LayeredCache()
    test_dir_1 = tempfile.mkdtemp()
    test_dir_2 = tempfile.mkdtemp()
    cache_1 = resource_cache.LocalFileCache(Path(test_dir_1))
    cache_2 = resource_cache.LocalFileCache(Path(test_dir_2))
    yield (layered_cache, cache_1, cache_2, test_dir_1, test_dir_2)
    # Remove temporary directories storing the cache contents
    shutil.rmtree(test_dir_1)
    shutil.rmtree(test_dir_2)


def test_add_caching_layers(layeredcachetestdir):
    """Adding layers has expected effect on the subsequent num_layers() calls."""
    layered_cache = layeredcachetestdir[0]
    cache_1 = layeredcachetestdir[1]
    cache_2 = layeredcachetestdir[2]
    assert layered_cache.num_layers() == 0
    layered_cache.add_cache_layer(cache_1)
    assert layered_cache.num_layers() == 1
    layered_cache.add_cache_layer(cache_2)
    assert layered_cache.num_layers() == 2


def test_add_to_first_layer(layeredcachetestdir):
    """Adding to layered cache by default stores entires in the first layer."""
    layered_cache = layeredcachetestdir[0]
    cache_1 = layeredcachetestdir[1]
    cache_2 = layeredcachetestdir[2]
    layered_cache.add_cache_layer(cache_1)
    layered_cache.add_cache_layer(cache_2)
    res = PudlResourceKey("a", "b", "x.txt")
    assert not layered_cache.contains(res)
    layered_cache.add(res, b"sampleContent")
    assert layered_cache.contains(res)
    assert cache_1.contains(res)
    assert not cache_2.contains(res)


def test_get_uses_innermost_layer(layeredcachetestdir):
    """Resource is retrieved from the leftmost layer that contains it."""
    res = PudlResourceKey("a", "b", "x.txt")
    layered_cache = layeredcachetestdir[0]
    cache_1 = layeredcachetestdir[1]
    cache_2 = layeredcachetestdir[2]
    layered_cache.add_cache_layer(cache_1)
    layered_cache.add_cache_layer(cache_2)
    # cache_1.add(res, "firstLayer")
    cache_2.add(res, b"secondLayer")
    assert layered_cache.get(res) == b"secondLayer"

    cache_1.add(res, b"firstLayer")
    assert layered_cache.get(res) == b"firstLayer"
    # Set on layered cache updates innermost layer
    layered_cache.add(res, b"newContents")
    assert layered_cache.get(res) == b"newContents"
    assert cache_1.get(res) == b"newContents"
    assert cache_2.get(res) == b"secondLayer"

    # Deletion also only affects innermost layer
    layered_cache.delete(res)
    assert layered_cache.contains(res)
    assert not cache_1.contains(res)
    assert cache_2.contains(res)
    assert layered_cache.get(res) == b"secondLayer"


def test_add_with_no_layers_does_nothing(layeredcachetestdir):
    """When add() is called on cache with no layers nothing happens."""
    layered_cache = layeredcachetestdir[0]
    res = PudlResourceKey("a", "b", "c")
    assert not layered_cache.contains(res)
    layered_cache.add(res, b"sample")
    assert not layered_cache.contains(res)
    layered_cache.delete(res)


def test_read_only_layers_skipped_when_adding(layeredcachetestdir):
    """When add() is called, layers that are marked as read_only are skipped."""
    test_dir_1 = layeredcachetestdir[3]
    test_dir_2 = layeredcachetestdir[4]
    c1 = resource_cache.LocalFileCache(Path(test_dir_1), read_only=True)
    c2 = resource_cache.LocalFileCache(Path(test_dir_2))
    lc = resource_cache.LayeredCache(c1, c2)

    res = PudlResourceKey("a", "b", "c")

    assert not lc.contains(res)
    assert not c1.contains(res)
    assert not c2.contains(res)

    lc.add(res, b"test")
    assert lc.contains(res)
    assert not c1.contains(res)
    assert c2.contains(res)

    lc.delete(res)
    assert not lc.contains(res)
    assert not c1.contains(res)
    assert not c2.contains(res)


def test_read_only_cache_ignores_modifications(layeredcachetestdir):
    """When cache is marked as read_only, add() and delete() calls are ignored."""
    cache_1 = layeredcachetestdir[1]
    cache_2 = layeredcachetestdir[2]
    r1 = PudlResourceKey("a", "b", "r1")
    r2 = PudlResourceKey("a", "b", "r2")
    cache_1.add(r1, b"xxx")
    cache_2.add(r2, b"yyy")
    assert cache_1.contains(r1)
    assert cache_2.contains(r2)
    lc = resource_cache.LayeredCache(cache_1, cache_2, read_only=True)

    assert lc.contains(r1)
    assert lc.contains(r2)

    lc.delete(r1)
    lc.delete(r2)
    assert lc.contains(r1)
    assert lc.contains(r2)
    assert cache_1.contains(r1)
    assert cache_2.contains(r2)

    r_new = PudlResourceKey("a", "b", "new")
    lc.add(r_new, b"xyz")
    assert not lc.contains(r_new)
    assert not cache_1.contains(r_new)
    assert not cache_2.contains(r_new)
