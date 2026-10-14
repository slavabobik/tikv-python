"""How the client copes with a cluster that changes or misbehaves under it."""

from __future__ import annotations

import time
from concurrent import futures

import grpc
import pytest

import tikv
from tikv._proto import errorpb_pb2 as errorpb

from .fake_cluster import FakeCluster


def busy(backoff_ms: int = 0) -> errorpb.Error:
    return errorpb.Error(
        message="busy", server_is_busy=errorpb.ServerIsBusy(reason="test", backoff_ms=backoff_ms)
    )


class TestRouting:
    def test_follows_region_split(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        client.put(b"a", b"1")
        client.put(b"z", b"2")  # cached: one region covering everything
        cluster.split(b"m")
        assert client.get(b"a") == b"1"
        assert client.get(b"z") == b"2"

    def test_batch_is_re_split_after_region_split(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        client.get(b"a")  # warm the cache with the single region
        cluster.split(b"c", b"e")
        items = {k: k for k in (b"a", b"b", b"c", b"d", b"e", b"f")}
        client.put_many(items)
        assert client.get_many(items) == items

    def test_split_while_request_in_flight(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        cluster.fail("RawBatchPut", lambda: cluster.split(b"b"))
        client.put_many({b"a": b"1", b"c": b"2"})
        assert client.get_many([b"a", b"c"]) == {b"a": b"1", b"c": b"2"}

    def test_scan_survives_split(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        client.put_many({bytes([c]): b"" for c in b"abcdef"})
        scan = client.scan_keys()
        assert next(scan) == b"a"
        cluster.split(b"c")
        assert list(client.scan_keys()) == [bytes([c]) for c in b"abcdef"]

    def test_follows_leader_hint_without_asking_pd(
        self, client: tikv.RawClient, cluster: FakeCluster
    ) -> None:
        client.put(b"k", b"v")
        lookups = cluster.calls["GetRegion"]
        cluster.transfer_leader(b"k", 2)
        assert client.get(b"k") == b"v"
        assert cluster.calls["GetRegion"] == lookups

    def test_reverse_scan_from_region_boundary_with_cold_cache(self, cluster: FakeCluster) -> None:
        cluster.split(b"m", b"t")
        with tikv.RawClient(cluster.address) as writer:
            writer.put_many({b"a": b"", b"l": b"", b"m": b"", b"s": b"", b"z": b""})
        for end, expected in [(b"m", [b"l", b"a"]), (b"t", [b"s", b"m"]), (None, [b"z", b"s"])]:
            with tikv.RawClient(cluster.address) as client:  # nothing cached yet
                assert list(client.scan_keys(end=end, reverse=True, limit=2)) == expected

    def test_reverse_scan_when_regions_merge_during_lookup(self, cluster: FakeCluster) -> None:
        cluster.split(b"m")
        with tikv.RawClient(cluster.address) as writer:
            writer.put_many({b"a": b"", b"z": b""})
        cluster.fail("GetPrevRegion", lambda: cluster.merge(b"m"))
        with tikv.RawClient(cluster.address) as client:
            assert list(client.scan_keys(end=b"m", reverse=True)) == [b"a"]

    def test_delete_range_after_split(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        client.put_many({b"a": b"", b"m": b"", b"z": b""})
        cluster.split(b"f", b"p")
        client.delete_range(b"b", b"y")
        assert list(client.scan_keys()) == [b"a", b"z"]

    def test_prefetches_regions_for_ranges(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        cluster.split(*(bytes([c]) for c in b"bcdefghij"))
        client.put_many({bytes([c]): b"" for c in b"abcdefghij"})
        assert cluster.calls["GetRegion"] == 0
        assert cluster.calls["ScanRegions"] == 1


class TestTransientFailures:
    def test_retries_server_busy(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        cluster.fail("RawPut", busy())  # backs off 1-2s: busy servers need breathing room
        client.put(b"k", b"v")
        assert cluster.calls["RawPut"] == 2

    def test_honours_server_backoff_hint(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        cluster.fail("RawGet", busy(backoff_ms=2500))
        started = time.monotonic()
        client.get(b"k")
        assert time.monotonic() - started >= 2.5

    def test_retries_unavailable_store(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        cluster.fail("RawGet", grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.UNAVAILABLE)
        assert client.get(b"k") is None

    def test_retries_are_logged(
        self, client: tikv.RawClient, cluster: FakeCluster, caplog: pytest.LogCaptureFixture
    ) -> None:
        cluster.fail("RawGet", grpc.StatusCode.UNAVAILABLE)
        with caplog.at_level("DEBUG", logger="tikv"):
            client.get(b"k")
        assert "PD leader is" in caplog.text
        assert "(tikv_rpc): gRPC UNAVAILABLE" in caplog.text

    def test_retries_unavailable_pd(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        cluster.fail("GetRegion", grpc.StatusCode.UNAVAILABLE)
        assert client.get(b"k") is None

    @pytest.mark.parametrize(
        "error",
        [
            errorpb.Error(message="stale", stale_command=errorpb.StaleCommand()),
            errorpb.Error(message="not found", region_not_found=errorpb.RegionNotFound(region_id=1)),
            errorpb.Error(message="no leader", not_leader=errorpb.NotLeader(region_id=1)),
            errorpb.Error(message="mismatch", store_not_match=errorpb.StoreNotMatch()),
            errorpb.Error(message="something new"),
        ],
        ids=lambda e: e.message,
    )
    def test_retries_region_errors(self, client: tikv.RawClient, cluster: FakeCluster, error) -> None:
        cluster.fail("RawPut", error)
        client.put(b"k", b"v")
        assert client.get(b"k") == b"v"

    def test_gives_up_when_time_budget_runs_out(self, cluster: FakeCluster) -> None:
        cluster.fail("RawGet", *[grpc.StatusCode.UNAVAILABLE] * 1000)
        with tikv.RawClient(cluster.address, timeout=0.5) as client:
            started = time.monotonic()
            with pytest.raises(tikv.TimeoutError, match=r"within 0.5s.*UNAVAILABLE") as caught:
                client.get(b"k")
        assert time.monotonic() - started < 1
        assert isinstance(caught.value, TimeoutError)  # also the builtin
        assert isinstance(caught.value.__cause__, grpc.RpcError)

    def test_unreachable_pd_times_out(self) -> None:
        with tikv.RawClient("127.0.0.1:1", timeout=0.5) as client, pytest.raises(tikv.TimeoutError):
            client.get(b"k")


class TestErrorStorms:
    """Long runs of retryable errors must back off, not spin or overflow the stack."""

    STALE = errorpb.Error(message="stale", stale_command=errorpb.StaleCommand())

    @pytest.mark.parametrize(
        ("method", "call"),
        [
            ("RawBatchGet", lambda c: c.get_many([b"a", b"b"])),
            ("RawBatchPut", lambda c: c.put_many({b"a": b"1"})),
            ("RawDeleteRange", lambda c: c.delete_range(b"a", b"z")),
            ("RawChecksum", lambda c: c.checksum(b"a", b"z")),
        ],
    )
    def test_multi_region_operations_escalate_and_time_out(self, cluster: FakeCluster, method, call) -> None:
        cluster.fail(method, *[self.STALE] * 2000)
        with tikv.RawClient(cluster.address, timeout=1.5) as client, pytest.raises(tikv.TimeoutError):
            call(client)
        assert cluster.calls[method] < 30  # exponential backoff, not a tight loop

    def test_recovers_after_a_burst(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        cluster.split(b"m")
        cluster.fail("RawBatchPut", *[self.STALE] * 8)
        client.put_many({b"a": b"1", b"z": b"2"})
        assert client.get_many([b"a", b"z"]) == {b"a": b"1", b"z": b"2"}


class TestFatalFailures:
    def test_oversized_write_is_not_retried(self, client: tikv.RawClient, cluster: FakeCluster) -> None:
        cluster.fail(
            "RawPut", errorpb.Error(message="too large", raft_entry_too_large=errorpb.RaftEntryTooLarge())
        )
        with pytest.raises(tikv.RegionError, match="too large"):
            client.put(b"k", b"v")
        assert cluster.calls["RawPut"] == 1

    def test_cas_is_not_blindly_replayed(self, atomic_client: tikv.RawClient, cluster: FakeCluster) -> None:
        cluster.fail("RawCompareAndSwap", grpc.StatusCode.UNAVAILABLE)
        with pytest.raises(tikv.UndeterminedError):
            atomic_client.compare_and_swap(b"k", None, b"v")

    def test_cas_undetermined_result(self, atomic_client: tikv.RawClient, cluster: FakeCluster) -> None:
        error = errorpb.Error(message="?", undetermined_result=errorpb.UndeterminedResult(message="lost"))
        cluster.fail("RawCompareAndSwap", error)
        with pytest.raises(tikv.UndeterminedError, match="lost"):
            atomic_client.compare_and_swap(b"k", None, b"v")

    @pytest.mark.parametrize(
        "error", [TestErrorStorms.STALE, errorpb.Error(region_not_found=errorpb.RegionNotFound())]
    )
    def test_cas_may_have_applied(self, atomic_client: tikv.RawClient, cluster: FakeCluster, error) -> None:
        cluster.fail("RawCompareAndSwap", error)
        with pytest.raises(tikv.UndeterminedError):
            atomic_client.compare_and_swap(b"k", None, b"v")

    def test_cas_retries_safe_region_errors(
        self, atomic_client: tikv.RawClient, cluster: FakeCluster
    ) -> None:
        cluster.fail("RawCompareAndSwap", busy())
        assert atomic_client.compare_and_swap(b"k", None, b"v").swapped

    def test_discovery_skips_endpoints_that_are_not_pd(self, cluster: FakeCluster) -> None:
        other = grpc.server(futures.ThreadPoolExecutor(max_workers=1))  # serves nothing
        port = other.add_insecure_port("127.0.0.1:0")
        other.start()
        try:
            with tikv.RawClient([f"127.0.0.1:{port}", cluster.address]) as client:
                assert client.get(b"k") is None
        finally:
            other.stop(grace=None)

    def test_wrong_service_fails_fast(self, cluster: FakeCluster) -> None:
        cluster.fail("GetMembers", grpc.StatusCode.UNIMPLEMENTED)
        with tikv.RawClient(cluster.address) as client, pytest.raises(tikv.PDError, match="not a PD server"):
            client.get(b"k")

    def test_closed_client(self, cluster: FakeCluster) -> None:
        client = tikv.RawClient(cluster.address)
        client.close()
        client.close()  # idempotent
        with pytest.raises(tikv.TiKVError, match="closed"):
            client.get(b"k")
