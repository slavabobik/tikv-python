"""Mutual TLS, end to end, with throwaway certificates from the openssl CLI."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import grpc
import pytest

import tikv

from .fake_cluster import FakeCluster

pytestmark = pytest.mark.skipif(shutil.which("openssl") is None, reason="needs the openssl CLI")


def openssl(*args: str, cwd: Path) -> None:
    subprocess.run(["openssl", *args], cwd=cwd, check=True, capture_output=True)


@pytest.fixture(scope="module")
def certs(tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp("certs")
    days = ("-days", "1")
    openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", *days, "-subj", "/CN=test-ca",
            "-keyout", "ca.key", "-out", "ca.pem", cwd=path)  # fmt: skip
    (path / "san.ext").write_text("subjectAltName=DNS:localhost\n")
    for name in ("server", "client"):
        openssl("req", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN={name}",
                "-keyout", f"{name}.key", "-out", f"{name}.csr", cwd=path)  # fmt: skip
        openssl("x509", "-req", "-in", f"{name}.csr", "-CA", "ca.pem", "-CAkey", "ca.key", *days,
                "-CAcreateserial", "-extfile", "san.ext", "-out", f"{name}.pem", cwd=path)  # fmt: skip
    return path


def test_mutual_tls(certs: Path) -> None:
    credentials = grpc.ssl_server_credentials(
        [((certs / "server.key").read_bytes(), (certs / "server.pem").read_bytes())],
        root_certificates=(certs / "ca.pem").read_bytes(),
        require_client_auth=True,
    )
    cluster = FakeCluster(credentials=credentials)
    tls = tikv.TLSConfig(ca=certs / "ca.pem", cert=certs / "client.pem", key=certs / "client.key")
    try:
        with tikv.RawClient(cluster.address, tls=tls) as client:
            client.put(b"k", b"v")
            assert client.get(b"k") == b"v"
        with tikv.RawClient(cluster.address, timeout=0.5) as plaintext, pytest.raises(tikv.TimeoutError):
            plaintext.get(b"k")
    finally:
        cluster.stop()
