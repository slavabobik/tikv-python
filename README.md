<h1 align="center">tikv-python</h1>

<p align="center">
  <b>A pure-Python client for <a href="https://tikv.org">TiKV</a>'s raw key-value API.</b><br>
  Blocking and asyncio. No Rust toolchain. Readable in an afternoon.
</p>

<p align="center">
  <a href="https://github.com/slavabobik/tikv-python/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/slavabobik/tikv-python/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://pypi.org/project/tikv-python/"><img alt="PyPI" src="https://img.shields.io/pypi/v/tikv-python?label=pypi&color=blue"></a>
  <a href="pyproject.toml"><img alt="Python versions" src="https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Fraw.githubusercontent.com%2Fslavabobik%2Ftikv-python%2Fmain%2Fpyproject.toml&query=%24.project.requires-python&label=python&color=3776AB"></a>
  <a href="#compatibility"><img alt="Tested with TiKV 7.5 and 8.5" src="https://img.shields.io/badge/TiKV-7.5%20%7C%208.5-ff5a5f"></a>
  <a href="LICENSE"><img alt="License: Apache 2.0" src="https://img.shields.io/badge/license-Apache%202.0-blue.svg"></a>
</p>

```python
import tikv

with tikv.RawClient("pd-0:2379") as db:
    db.put(b"user:1", b"Ada")
    db.get(b"user:1")                                    # b"Ada"

    for key, value in db.scan(prefix=b"user:"):          # lazy, crosses regions for you
        print(key, value)

    db.put(b"session:42", b"...", ttl=3600)              # expires in an hour
```

## Why

TiKV has excellent clients for Go, Rust and Java. For Python, the option so far has
been [`tikv-client`](https://pypi.org/project/tikv-client/), a set of bindings to the
Rust client. Bindings are a reasonable way to bootstrap a client, but they leave
Python users exposed:

- **Wheels decide whether you can install it.** The latest release (0.0.4, January
  2024) ships wheels only for CPython 3.11 on x86-64 and macOS. On Python 3.12+, on
  ARM Linux (Graviton, Axion, Ampere), or in a slim container, `pip install` turns
  into compiling a Rust crate and its gRPC stack from source.
- **Failures cross a language boundary.** A stuck request or a strange error means
  debugging Rust futures behind an FFI layer, from a Python stack trace that ends
  at the binding.
- **asyncio is bolted on.** Async calls are bridged into a separate Tokio runtime
  instead of running on your event loop.
- **Releases are tied to the binding layer.** A fix in TiKV's Rust client reaches
  Python only after the bindings are rebuilt, re-pinned and republished.

TiKV speaks plain gRPC, and Python already has a production-grade gRPC stack. So
this library talks to PD and TiKV directly, the way the Go and Rust clients do:

- **Installs anywhere `grpcio` does**, which is practically everywhere. It is a
  `py3-none-any` wheel with two dependencies.
- **Your event loop, your threads.** `AsyncRawClient` is native asyncio.
  `RawClient` is a plain thread-safe blocking client.
- **Small enough to own.** About 1,500 lines of typed, commented Python. When
  something goes wrong, you can read exactly what the client did, and every retry
  is logged.
- **Behaves like the official clients.** Region routing, retries and backoff follow
  client-go and client-rust, verified against a real cluster.

## Features

- The full raw API: `get`, `put`, `delete`, batches, range scans (forward and
  reverse), `delete_range`, TTLs, `compare_and_swap` and `checksum`
- Blocking `RawClient` and asyncio `AsyncRawClient` with identical APIs
- Automatic routing across regions, following splits, merges, leader changes and
  PD failover
- A time budget per operation, with exponential backoff and jitter on every
  transient error
- Lazy, constant-memory scans over any number of regions
- Mutual TLS
- Fully typed (`mypy --strict`), `py.typed`, no `Any` in the public API

## Installation

```console
pip install tikv-python        # or: uv add tikv-python
```

The package imports as `tikv`. It needs Python 3.10+, `grpcio>=1.60` and
`protobuf>=4.25`.

## Quickstart

Point the client at one or more PD servers. It discovers the rest of the cluster on
its own.

```python
import tikv

with tikv.RawClient(["pd-0:2379", "pd-1:2379"]) as db:
    db.put_many({b"user:1": b"Ada", b"user:2": b"Grace", b"user:3": b"Linus"})

    db.get(b"user:2")                        # b"Grace"
    db.get(b"user:9")                        # None
    db.get_many([b"user:1", b"user:9"])      # {b"user:1": b"Ada"}

    list(db.scan_keys(prefix=b"user:", reverse=True, limit=2))
    # [b"user:3", b"user:2"]

    db.delete(b"user:1")
    db.delete_range(prefix=b"user:")
```

The asyncio client has the same API. Use `await` and `async for`:

```python
import asyncio
import tikv

async def main() -> None:
    async with tikv.AsyncRawClient("pd-0:2379") as db:
        await asyncio.gather(*(db.put(f"k{i}".encode(), b"v") for i in range(1000)))
        async for key, value in db.scan(b"k", b"l", limit=10):
            print(key, value)

asyncio.run(main())
```

## Guide

### API at a glance

| Method | What it does |
| --- | --- |
| `get(key)` | The value, or `None` if the key does not exist |
| `get_many(keys)` | A `dict` of the keys that exist, in the order requested |
| `put(key, value, *, ttl=None)` | Set a key, optionally with an expiry |
| `put_many(items, *, ttl=None)` | Set many keys from a mapping or `(key, value)` pairs |
| `delete(key)` / `delete_many(keys)` | Delete keys; missing keys are not an error |
| `delete_range(start, end)` / `delete_range(prefix=…)` | Delete a whole range of keys |
| `scan(start, end, *, prefix, limit, reverse)` | Lazily iterate over `(key, value)` pairs |
| `scan_keys(…)` | Like `scan`, without transferring values |
| `ttl(key)` | Seconds until the key expires: `None` if never, `KeyError` if missing |
| `checksum(start, end, *, prefix)` | A `Checksum(crc64_xor, keys, size)` computed by the servers |
| `compare_and_swap(key, expected, new, *, ttl=None)` | Atomic CAS, returning `CasResult(swapped, previous)` |

Keys and values are `bytes` (`bytearray` and `memoryview` are fine too). Passing a
`str` raises `TypeError` instead of silently picking an encoding.

### Ranges and scans

Ranges are half-open, `[start, end)`, and `None` means "unbounded":

```python
db.scan()                                  # every key
db.scan(b"m")                              # from b"m" to the end
db.scan(b"a", b"n")                        # b"a" up to, but excluding, b"n"
db.scan(prefix=b"log:")                    # every key starting with b"log:"
db.scan(prefix=b"log:", reverse=True, limit=100)    # the newest 100 log keys
```

Scans fetch one page at a time from the right region, so iterating over millions
of keys uses constant memory. Stop iterating whenever you like.

`delete_range` refuses to run without `start` or a non-empty `prefix`, so a missing
argument cannot wipe your cluster. Deleting everything takes an explicit
`delete_range(b"")`.

### Atomicity and compare-and-swap

Single-key operations are atomic. Batch operations are atomic within a region but
not across regions; that is how TiKV's raw API works.

`compare_and_swap` needs a client in *atomic mode*. In that mode every write goes
through TiKV's single-row transaction path, which makes CAS linearizable:

```python
locks = tikv.RawClient("pd-0:2379", atomic=True)

swapped, holder = locks.compare_and_swap(b"lock:job-7", None, b"worker-3")
if not swapped:
    print(f"already held by {holder!r}")
```

> **Warning:** Every client that writes the same keys must use the same mode.
> Mixing modes breaks linearizability. `delete_range` is unavailable in atomic
> mode because TiKV cannot perform it atomically.

### TTL

`ttl=` takes seconds, rounded up to a whole second (TiKV's precision), or a
`datetime.timedelta`. Expiry needs `storage.enable-ttl = true` on the TiKV side.

```python
db.put(b"otp:5551234", b"839201", ttl=timedelta(minutes=5))
db.ttl(b"otp:5551234")                     # 300
```

### Errors, timeouts and retries

Every operation gets a time budget: `timeout=10.0` seconds by default, set per
client. Within that budget the client retries every transient failure with
exponential backoff and jitter:

- stale routing after a region split or merge,
- leader changes and elections,
- busy or unreachable stores,
- PD leader failover.

Each scan page gets its own budget. When the budget runs out you get
`tikv.TimeoutError`, which is also a builtin `TimeoutError`, with the last
underlying error chained as `__cause__`.

| Exception | Raised when |
| --- | --- |
| `TiKVError` | Base class of everything below |
| `TimeoutError` | The operation could not complete within `timeout` |
| `ServerError` | TiKV rejected the request, e.g. a key over `max-key-size` |
| `RegionError` | A region error that retrying cannot fix, e.g. a write too large for Raft |
| `PDError` | PD rejected a request, or an endpoint is not PD at all |
| `UndeterminedError` | A `compare_and_swap` failed in flight and may or may not have applied |

`compare_and_swap` is deliberately never replayed after a failure that might have
applied. If a replayed swap had already succeeded, it would report a spurious
failure. Read the key to find out what happened.

Every retry is logged at `DEBUG` level on the `tikv` logger, with the reason and the
delay:

```python
logging.getLogger("tikv").setLevel(logging.DEBUG)
```

### Concurrency

Create one client and share it across your application. It keeps one HTTP/2
connection per server, and requests are multiplexed over it.

- `RawClient` is thread-safe. Multi-region batches fan out over a small thread
  pool.
- `AsyncRawClient` is task-safe and fans out with `asyncio`. Like every gRPC
  asyncio channel, it belongs to the event loop it is first used in.

### TLS

```python
tls = tikv.TLSConfig(ca="ca.pem", cert="client.pem", key="client-key.pem")
db = tikv.RawClient("pd-0:2379", tls=tls)
```

## Performance

The numbers below come from a 3-node TiKV 8.5 cluster on Kubernetes, with the
client running in a pod in the same cluster on Python 3.12. Your hardware will
differ, but the shape holds: per-request overhead stays well under a millisecond,
and batches and scans move hundreds of thousands of keys per second.

| Operation | Result |
| --- | --- |
| `get`, sequential | 0.53 ms median, 0.95 ms p99 |
| `put`, sequential | 1.75 ms median, 3.82 ms p99 |
| `put_many`, 50,000 × 100-byte values | ~263,000 keys/s |
| `get_many`, 50,000 keys | ~344,000 keys/s |
| `scan`, 50,000 keys | ~376,000 keys/s |
| `AsyncRawClient.get`, 10,000 concurrent | ~11,500 ops/s from one event loop |

## How it works

```
  your code ──► RawClient / AsyncRawClient
                     │
                     ├─ region cache ── sorted by start key; maps every key to its region and leader
                     │        ▲
                     │        └─ PD client ── finds the PD leader, follows failover,
                     │                        fetches regions (64 at a time when walking a range)
                     │
                     └─ request executor ── groups keys by region, sends to leaders concurrently,
                                            handles NotLeader / EpochNotMatch / ServerIsBusy / …,
                                            re-groups and retries within the time budget
```

A few design decisions worth knowing:

- **One implementation, two flavors.** The asyncio code in `src/tikv/_async` is
  the source of truth. [`scripts/unasync.py`](scripts/unasync.py) mechanically
  generates the blocking client from it, the technique
  [httpcore](https://github.com/encode/httpcore) uses, and CI fails if they drift.
- **A trimmed protocol.** [`proto/`](proto) is the subset of
  [kvproto](https://github.com/pingcap/kvproto) a raw client needs, with upstream
  names and field numbers. Fields the client never reads are omitted, and protobuf
  preserves them as unknown fields on the wire. The generated modules import in
  about a millisecond and work with protobuf 4.25 through 7.x.
- **Region errors drive the cache.** Like client-go, the client never polls for
  topology changes. A routing error invalidates or patches exactly the cache
  entries involved, and the affected keys are re-grouped and retried.
- **Tested against a fake that behaves like the real thing.**
  [`tests/fake_cluster.py`](tests/fake_cluster.py) is an in-process PD + TiKV that
  speaks the real gRPC protocol. It splits and merges regions, moves leaders and
  injects failures, and it validates routing the way TiKV does. CI also runs the
  suite against real TiKV.

## Compatibility

| | Supported |
| --- | --- |
| Python | 3.10 – 3.14 |
| TiKV / PD | 7.5 and 8.5 LTS, tested in CI |
| Storage API | `api-version = 1`, with or without `enable-ttl` |
| Platforms | Anywhere `grpcio` installs: Linux (x86-64, ARM), macOS, Windows |

## Roadmap

These are tracked in [issues](https://github.com/slavabobik/tikv-python/issues):

- API v2 keyspaces (`storage.api-version = 2`)
- Column families other than `default`
- Chaos tests against a multi-node cluster: leader failover, store loss, PD failover
- A transactional (TxnKV) client

Contributions are very welcome. Open an issue to discuss bigger changes first.

## Development

```console
git clone https://github.com/slavabobik/tikv-python && cd tikv-python
uv sync                                          # dev environment in .venv
uv run pytest                                    # unit tests against the fake cluster
uv run ruff check && uv run ruff format --check && uv run mypy
```

Integration tests run against any real cluster. They write only under a unique key
prefix and delete it afterwards:

```console
ci/start-cluster.sh v8.5.8                       # single-node cluster in Docker
TIKV_PD_ENDPOINTS=127.0.0.1:2379 uv run pytest -m integration
```

After changing `src/tikv/_async`, regenerate the blocking client:

```console
uv run python scripts/unasync.py
```

After changing `proto/`, regenerate the messages. The pinned generator keeps the
protobuf floor low:

```console
uv run --with 'grpcio-tools==1.62.3' python scripts/generate_protos.py
```

### Releasing

1. Bump `__version__` in `src/tikv/__init__.py` and merge it to `main`.
2. Tag and push the release: `git tag v0.2.0 && git push origin v0.2.0`.

The [release workflow](.github/workflows/release.yml) checks that the tag matches
the version, publishes to PyPI with
[trusted publishing](https://docs.pypi.org/trusted-publishers/), and creates a
GitHub release with the distributions attached. You can also run it by hand from
the Actions tab to publish to TestPyPI or PyPI, optionally under a different
distribution name; the import name is always `tikv`.

## License

[Apache License 2.0](LICENSE), the same as TiKV.
