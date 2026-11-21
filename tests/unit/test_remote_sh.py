"""deploy/remote.sh with a stub `docker` on PATH: what it writes, refuses and runs."""

import os
import stat
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "deploy" / "remote.sh"
DIGEST = "sha256:" + "a" * 64
ORCHESTRATION = "sha256:" + "c" * 64
# Stand-in for the server's .env: the tests compare bytes, so no credential-shaped lines.
SECRETS = b"FIRST_LINE=unchanged\nSECOND_LINE=also-unchanged\n"
COMPOSE = "compose --env-file .env --env-file release.env -f compose.yaml -f compose.prod.yaml"


@pytest.fixture
def server(tmp_path: Path) -> Path:
    """A deploy directory with a production .env, and a docker stub that logs its arguments."""
    deploy_dir = tmp_path / "ibergrid-lakehouse"
    deploy_dir.mkdir()
    (deploy_dir / ".env").write_bytes(SECRETS)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "docker"
    stub.write_text(f'#!/bin/sh\necho "$*" >> "{tmp_path / "docker.log"}"\n')
    stub.chmod(0o755)
    return deploy_dir


def run(server: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PATH": f"{server.parent / 'bin'}:{os.environ['PATH']}"}
    return subprocess.run(
        ["bash", str(SCRIPT), *args], env=env, capture_output=True, text=True, check=False
    )


def docker_calls(server: Path) -> list[str]:
    log = server.parent / "docker.log"
    return log.read_text().splitlines() if log.exists() else []


def test_pins_the_digest_in_release_env_and_leaves_env_untouched(server: Path) -> None:
    result = run(server, str(server), DIGEST, ORCHESTRATION)

    assert result.returncode == 0, result.stderr
    release = server / "release.env"
    assert release.read_text() == (
        f"IBERGRID_APP_DIGEST={DIGEST}\nIBERGRID_ORCHESTRATION_DIGEST={ORCHESTRATION}\n"
    )
    assert stat.S_IMODE(release.stat().st_mode) == 0o600
    assert (server / ".env").read_bytes() == SECRETS
    assert docker_calls(server) == [
        f"{COMPOSE} pull --quiet",
        f"{COMPOSE} run --rm doctor lake init",
        f"{COMPOSE} up -d --wait --remove-orphans",
        f"{COMPOSE} run --rm --no-deps doctor",
    ]
    assert sorted(p.name for p in server.iterdir()) == [".env", "release.env"]


def test_rerun_with_the_previous_digest_is_the_rollback(server: Path) -> None:
    previous, previous_orchestration = "sha256:" + "b" * 64, "sha256:" + "d" * 64
    assert run(server, str(server), DIGEST, ORCHESTRATION).returncode == 0

    assert run(server, str(server), previous, previous_orchestration).returncode == 0

    assert (server / "release.env").read_text() == (
        f"IBERGRID_APP_DIGEST={previous}\nIBERGRID_ORCHESTRATION_DIGEST={previous_orchestration}\n"
    )


@pytest.mark.parametrize(
    "digest", ["", "sha256:bad;rm -rf /", "sha256:" + "A" * 64, "a" * 64, DIGEST + "\nX=1"]
)
@pytest.mark.parametrize("position", ["app", "orchestration"])
def test_malformed_digest_exits_2_before_touching_anything(
    server: Path, digest: str, position: str
) -> None:
    digests = (digest, ORCHESTRATION) if position == "app" else (DIGEST, digest)
    result = run(server, str(server), *digests)

    assert result.returncode == 2
    assert "not an image digest" in result.stderr
    assert not (server / "release.env").exists()
    assert docker_calls(server) == []


@pytest.mark.parametrize("count", [1, 2, 4])
def test_wrong_number_of_arguments_exits_2(server: Path, count: int) -> None:
    result = run(server, *[str(server), DIGEST, ORCHESTRATION, DIGEST][:count])

    assert result.returncode == 2
    assert result.stderr.startswith("usage: remote.sh")


def test_missing_env_exits_1(server: Path) -> None:
    (server / ".env").unlink()

    result = run(server, str(server), DIGEST, ORCHESTRATION)

    assert result.returncode == 1
    assert ".env is missing" in result.stderr
    assert not (server / "release.env").exists()
    assert docker_calls(server) == []


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads files regardless of their mode")
def test_unreadable_env_fails_and_nothing_is_overwritten(server: Path) -> None:
    env_file = server / ".env"
    env_file.chmod(0)
    try:
        result = run(server, str(server), DIGEST, ORCHESTRATION)
    finally:
        env_file.chmod(0o600)

    assert result.returncode != 0
    assert "not readable" in result.stderr
    assert env_file.read_bytes() == SECRETS
    assert not (server / "release.env").exists()
    assert docker_calls(server) == []
