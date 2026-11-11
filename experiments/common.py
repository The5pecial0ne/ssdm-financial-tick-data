"""
Shared helpers for the experiment scripts.

Only the Python standard library is used, so the scripts run on a bare
Python 3 on macOS, Linux or Windows. Everything else lives in the containers.
"""

import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FLINK = "http://localhost:8081"
JAR = "/opt/flink/job/trading-job.jar"
CONTAINERS = {"taskmanager": "ssdm-taskmanager-1", "jobmanager": "ssdm-jobmanager-1", "kafka": "ssdm-kafka-1"}


def compose(*args, env=None, capture=False, check=True):
    """Runs `docker compose <args>` from the repo root."""
    cmd = ["docker", "compose", *args]
    print("  $ " + " ".join(cmd), flush=True)
    result = subprocess.run(cmd, cwd=ROOT, env={**os.environ, **(env or {})},
                            text=True, encoding="utf-8", errors="replace",
                            stdout=subprocess.PIPE if capture else None,
                            stderr=subprocess.STDOUT if capture else None)
    if check and result.returncode != 0:
        if capture:
            print(result.stdout)
        sys.exit(f"docker compose {args[0]} failed (exit code {result.returncode})")
    return result


def tools(*args, check=True):
    """Runs a command in the Python tools container (producer, collect, compare, summarize)."""
    return compose("run", "--rm", "tools", *args, check=check)


def container_path(path):
    """A path given on the host, relative to the repo root, as the tools container sees it."""
    p = Path(path)
    if p.is_absolute():
        try:
            p = p.resolve().relative_to(ROOT)
        except ValueError:
            sys.exit(f"{path} is outside the repository. Put data files in the repo's data/ folder.")
    if not (ROOT / p).exists():
        sys.exit(f"{ROOT / p} does not exist")
    return p.as_posix()


def host_cpus():
    out = subprocess.run(["docker", "info", "--format", "{{.NCPU}} {{.MemTotal}} {{.OperatingSystem}} {{.Architecture}}"],
                         capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit("Docker is not running. Start Docker Desktop and try again.")
    ncpu, mem, *rest = out.stdout.split()
    return int(ncpu), {"docker_cpus": int(ncpu), "docker_memory_gb": round(int(mem) / 2**30, 1),
                       "docker_os": " ".join(rest)}


def rest(path):
    with urllib.request.urlopen(FLINK + path, timeout=10) as response:
        return json.load(response)


def fresh_infrastructure(partitions=8):
    """Clean Kafka (empty topics) and freshly built images."""
    compose("down", "-v", "--remove-orphans")
    compose("build")
    compose("up", "-d", "kafka")
    compose("run", "--rm", "init-topics", env={"INPUT_PARTITIONS": str(partitions)})


def start_flink(workers):
    """
    Brand-new JobManager and TaskManager, with the TaskManager limited to `workers` CPU cores.
    Starting fresh every time gives each run the same cold start.
    """
    compose("up", "-d", "--force-recreate", "--no-deps", "jobmanager", "taskmanager",
            env={"TM_CPUS": str(workers)})
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            if rest("/overview").get("slots-total", 0) >= workers:
                return
        except OSError:
            pass
        time.sleep(2)
    sys.exit("Flink did not come up with enough task slots within 3 minutes. Try: docker compose logs taskmanager")


def submit_job(parallelism, *job_args):
    out = compose("exec", "-T", "jobmanager", "flink", "run", "-d", "-p", str(parallelism), JAR, *job_args,
                  capture=True).stdout
    match = re.search(r"JobID ([0-9a-f]{32})", out)
    if not match:
        print(out)
        sys.exit("Could not submit the Flink job")
    return match.group(1)


def wait_for_job(job_id, timeout_s):
    """Polls Flink until the job has ended; returns Flink's own record of the job."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        info = rest(f"/jobs/{job_id}")
        if info["state"] in ("FINISHED", "FAILED", "CANCELED"):
            return info
        time.sleep(1)
    sys.exit(f"Job {job_id} was still running after {timeout_s} s. Look at {FLINK} and raise --timeout if it is just slow.")


class StatsSampler(threading.Thread):
    """Samples `docker stats` in the background while a job runs."""

    def __init__(self):
        super().__init__(daemon=True)
        self.samples = {name: [] for name in CONTAINERS}
        self._stop_event = threading.Event()

    def run(self):
        by_container = {v: k for k, v in CONTAINERS.items()}
        while not self._stop_event.is_set():
            out = subprocess.run(["docker", "stats", "--no-stream", "--format",
                                  "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}", *CONTAINERS.values()],
                                 capture_output=True, text=True).stdout
            for line in out.splitlines():
                try:
                    name, cpu, mem = line.split("|")
                    self.samples[by_container[name]].append((float(cpu.rstrip("%")), to_mb(mem.split("/")[0])))
                except (ValueError, KeyError):
                    pass

    def stop(self):
        self._stop_event.set()
        self.join(timeout=15)
        summary = {}
        for name, values in self.samples.items():
            if values:
                cpus, mems = [v[0] for v in values], [v[1] for v in values]
                summary[name] = {"cpu_avg": round(sum(cpus) / len(cpus), 1), "cpu_max": round(max(cpus), 1),
                                 "mem_max_mb": round(max(mems)), "samples": len(values)}
        return summary


def to_mb(text):
    text = text.strip()
    for unit, factor in (("GiB", 1024), ("MiB", 1), ("KiB", 1 / 1024), ("GB", 953.67), ("MB", 0.95367), ("kB", 0.00095367), ("B", 1 / 2**20)):
        if text.endswith(unit):
            return float(text[: -len(unit)]) * factor
    return 0.0


def read_subscriptions():
    lines = (ROOT / "config" / "subscriptions.txt").read_text(encoding="utf-8").splitlines()
    symbols = [line.strip() for line in lines if line.strip() and not line.startswith("#")]
    return ",".join(symbols) or "ALL"
