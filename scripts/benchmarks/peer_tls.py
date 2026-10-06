"""Short-lived static peer credentials for clustered benchmark processes."""

from __future__ import annotations

import base64
import hashlib
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from common import BenchmarkError


@dataclass(frozen=True)
class NodePeerCredentials:
    directory: Path
    trust_bundle: Path
    certificate_chain: Path
    private_key: Path


class PeerCredentials:
    """Own test-only CA material and per-node leaves for one short-lived cluster."""

    def __init__(self, root: Path, cluster_name: str, node_ids: range | list[int]) -> None:
        openssl = shutil.which("openssl")
        if openssl is None:
            raise BenchmarkError("clustered Raft runs require the openssl command")

        self.root = Path(tempfile.mkdtemp(prefix="runnel-peer-test-tls-", dir=root))
        self.cluster_name = cluster_name
        self.node_ids = tuple(node_ids)
        self._generate(openssl)

    def node(self, node_id: int) -> NodePeerCredentials:
        directory = self.root / f"node-{node_id}"
        return NodePeerCredentials(
            directory=directory,
            trust_bundle=directory / "ca.pem",
            certificate_chain=directory / "tls.crt",
            private_key=directory / "tls.key",
        )

    def node_id_for_identity(self, identity: str) -> int | None:
        for node_id in self.node_ids:
            if peer_identity(node_id, self.cluster_name) == identity:
                return node_id
        return None

    def _generate(self, openssl: str) -> None:
        self.root.chmod(0o700)
        ca_key = self.root / "ca-private.pem"
        ca_cert = self.root / "ca.pem"
        ca_config = self.root / "ca.cnf"
        ca_config.write_text(
            "[req]\n"
            "distinguished_name=dn\n"
            "x509_extensions=ca\n"
            "prompt=no\n"
            "[dn]\n"
            "CN=Runnel temporary benchmark peer CA\n"
            "[ca]\n"
            "basicConstraints=critical,CA:TRUE\n"
            "keyUsage=critical,keyCertSign,cRLSign\n"
            "subjectKeyIdentifier=hash\n",
            encoding="utf-8",
        )
        self._run(
            openssl,
            [
                "req",
                "-x509",
                "-newkey",
                "ec",
                "-pkeyopt",
                "ec_paramgen_curve:prime256v1",
                "-nodes",
                "-keyout",
                str(ca_key),
                "-out",
                str(ca_cert),
                "-days",
                "7",
                "-subj",
                "/CN=Runnel temporary benchmark peer CA",
                "-config",
                str(ca_config),
            ],
        )
        ca_key.chmod(0o600)
        ca_cert.chmod(0o644)

        for node_id in self.node_ids:
            node = self.node(node_id)
            node.directory.mkdir(mode=0o700)
            leaf_key = node.private_key
            csr = node.directory / "tls.csr"
            extensions = node.directory / "tls.cnf"
            identity = peer_identity(node_id, self.cluster_name)
            extensions.write_text(
                "[req]\n"
                "distinguished_name=dn\n"
                "prompt=no\n"
                "[dn]\n"
                f"CN=Runnel peer {node_id}\n"
                "[peer]\n"
                "basicConstraints=critical,CA:FALSE\n"
                "keyUsage=critical,digitalSignature\n"
                "extendedKeyUsage=serverAuth,clientAuth\n"
                f"subjectAltName=DNS:{identity}\n",
                encoding="utf-8",
            )
            self._run(
                openssl,
                [
                    "genpkey",
                    "-algorithm",
                    "EC",
                    "-pkeyopt",
                    "ec_paramgen_curve:prime256v1",
                    "-out",
                    str(leaf_key),
                ],
            )
            leaf_key.chmod(0o600)
            self._run(
                openssl,
                [
                    "req",
                    "-new",
                    "-key",
                    str(leaf_key),
                    "-out",
                    str(csr),
                    "-config",
                    str(extensions),
                    "-subj",
                    f"/CN=Runnel peer {node_id}",
                ],
            )
            self._run(
                openssl,
                [
                    "x509",
                    "-req",
                    "-in",
                    str(csr),
                    "-CA",
                    str(ca_cert),
                    "-CAkey",
                    str(ca_key),
                    "-CAcreateserial",
                    "-out",
                    str(node.certificate_chain),
                    "-days",
                    "7",
                    "-extfile",
                    str(extensions),
                    "-extensions",
                    "peer",
                ],
            )
            node.certificate_chain.chmod(0o644)
            (node.directory / "ca.pem").write_bytes(ca_cert.read_bytes())
            (node.directory / "ca.pem").chmod(0o644)
            csr.unlink()
            extensions.unlink()

        ca_key.unlink()
        ca_config.unlink()
        (self.root / "ca.srl").unlink(missing_ok=True)
        ca_cert.unlink()

    @staticmethod
    def _run(openssl: str, arguments: list[str]) -> None:
        try:
            subprocess.run(
                [openssl, *arguments],
                check=True,
                capture_output=True,
                text=True,
            )
        except (OSError, subprocess.CalledProcessError) as error:
            raise BenchmarkError(
                "could not create temporary benchmark peer TLS credentials; "
                "check that openssl supports P-256 certificate generation"
            ) from error


def peer_identity(node_id: int, cluster_name: str) -> str:
    cluster_hash = base64.b32encode(hashlib.sha256(cluster_name.encode("utf-8")).digest())
    encoded_cluster_hash = cluster_hash.decode("ascii").rstrip("=").lower()
    return f"n{node_id}.c-{encoded_cluster_hash}.peer.runnel.invalid"
