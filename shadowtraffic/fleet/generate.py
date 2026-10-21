#!/usr/bin/env python3
"""Generates the simulated fleet (docs/design.md §4.4, §7.2): the
ShadowTraffic config shadowtraffic/fleet.json and its rosters.

    python3 shadowtraffic/fleet/generate.py

The roster is the config's fork key: one entry per simulated job, with its
identity, its normal operating values and its behaviour profile as per-tick
transition weights. Everything is deterministic, so regenerating gives the
same fleet. It writes roster-cloud.json (about 1,000 jobs across 40 tenants)
and roster-local.json (a 200-job subset with the same tenants and every
genuine problem); compose.yml and compose.aws.yml mount one of them as
/fleet/roster.json in the st-fleet container.

st-fleet writes each job's steps to fleet.telemetry (one event per 10 s
tick); the fleet-exporter turns them into platform.* events and Prometheus
metrics (crates/platform-model/src/fleet.rs).

Profiles (the noise patterns of docs/design.md §7.2):
- steady: rare deploys (half restart once afterwards), rare transient
  TaskManager losses, rare slow or failed checkpoints;
- peaky: as steady, plus lag peaks at the tenant's busy times that drain;
- flapper: "known noisy": a benign failure every few minutes that recovers;
- crash_loop, stalled, state_growth: genuine problems nobody is watching.

Names, teams and people are fictional.
"""

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

SEED = 20261020
CLOUD_SIZE = 1000
LOCAL_SIZE = 200
HERE = Path(__file__).parent
NOW = datetime(2026, 10, 20, 9, 0, tzinfo=timezone.utc)

TENANTS = [
    "payments-ops", "card-auth", "card-disputes", "fx-pricing", "fx-settlement", "rates-curves",
    "equities-oms", "equities-risk", "fixed-income", "treasury", "liquidity", "collateral-mgmt",
    "margin-calls", "loan-origination", "mortgage-servicing", "credit-scoring", "fraud-detect",
    "aml-screening", "kyc-onboarding", "sanctions", "wealth-advisory", "retail-banking",
    "branch-ops", "atm-network", "mobile-banking", "notifications", "customer-profile",
    "marketing-events", "rewards", "billing", "general-ledger", "regulatory-reporting",
    "finance-close", "trade-surveillance", "market-ops", "ref-data", "pricing-engine",
    "client-onboarding", "investor-reporting", "data-platform",
]
ENTITIES = [
    "txn", "order", "quote", "trade", "position", "account", "customer", "payment", "alert",
    "event", "ledger", "limit", "price", "rate", "claim", "case", "session", "statement",
]
VERBS = [
    "enricher", "ingest", "aggregator", "router", "dedup", "joiner", "scorer", "archiver",
    "to-iceberg", "validator", "normalizer", "windower", "sink",
]
PEOPLE = [
    "alex.kim", "maria.santos", "omar.haddad", "julia.berg", "kenji.ito", "fatima.zahra",
    "lucas.moreau", "nina.patel", "tom.okafor", "chloe.martin", "diego.ruiz", "sara.lind",
]

# Failures a job can hit, as (exception class, message, root cause class, root cause message).
TM_LOST = (
    "org.apache.flink.runtime.jobmaster.JobMasterException",
    "TaskManager with id 10.44.{c}.{h}:6122-{x} is no longer reachable.",
    "java.util.concurrent.TimeoutException",
    "Heartbeat of TaskManager with id 10.44.{c}.{h}:6122-{x} timed out.",
)
STARTUP = (
    "org.apache.flink.runtime.JobException",
    "Recovery is suppressed by NoRestartBackoffTimeStrategy",
    "org.apache.kafka.common.errors.TimeoutException",
    "Timeout expired while fetching topic metadata",
)
FLAPS = [
    (
        "org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException",
        "Connection unexpectedly closed by remote task manager '10.44.{c}.{h}:41385'.",
        "org.apache.flink.shaded.netty4.io.netty.channel.unix.Errors$NativeIoException",
        "recvAddress(..) failed: Connection reset by peer",
    ),
    (
        "org.apache.kafka.common.errors.DisconnectException",
        "Cancelled fetchMetadata request with correlation id 4211 due to node 2 being disconnected",
        "org.apache.kafka.common.errors.DisconnectException",
        None,
    ),
    (
        "java.io.IOException",
        "Could not perform checkpoint 0 for operator Sink: Committer (1/1)#0.",
        "software.amazon.awssdk.services.s3.model.S3Exception",
        "Please reduce your request rate. (Service: S3, Status Code: 503)",
    ),
]
CRASHES = [
    (
        "java.lang.IllegalStateException",
        "Unknown currency code: XAU",
        "java.lang.IllegalStateException",
        "Unknown currency code: XAU",
    ),
    (
        "org.apache.flink.util.FlinkRuntimeException",
        "Error while processing record",
        "java.lang.ArrayIndexOutOfBoundsException",
        "Index 12 out of bounds for length 12",
    ),
    (
        "org.apache.flink.runtime.client.JobExecutionException",
        "Job execution failed.",
        "java.lang.OutOfMemoryError",
        "Java heap space",
    ),
]
CHATTER = [
    ("WARN", "org.apache.kafka.clients.NetworkClient", "Connection to node 3 could not be established. Node may not be available."),
    ("WARN", "org.apache.flink.runtime.checkpoint.CheckpointCoordinator", "Checkpoint was declined: task is not ready."),
    ("WARN", "org.apache.kafka.clients.consumer.internals.ConsumerCoordinator", "Offset commit failed on partition: the coordinator is not available."),
    ("WARN", "org.apache.iceberg.aws.s3.S3OutputStream", "Retrying upload of part 3 after a throttling error."),
]


def hex_id(rng, n):
    return "".join(rng.choice("0123456789abcdef") for _ in range(n))


def failure(template, cluster, rng):
    host, x = rng.randint(10, 250), hex_id(rng, 6)
    fill = lambda s: None if s is None else s.format(c=cluster, h=host, x=x)
    exc, msg, root, root_msg = template
    # The fleet-exporter builds the stack-trace head from these.
    return {
        "exception_class": exc,
        "message": fill(msg),
        "root_cause_class": root,
        "root_cause_message": fill(root_msg),
    }


def per_tick(hours):
    """The weight that makes a state happen about once every `hours`, given
    that a running job's weights total about 120 per 10 s tick."""
    return round(120 / (hours * 360), 3)


def weights(profile, rng):
    """Per-tick (10 s) transition weights out of the steady state. A job's
    weights total about 120, so a weight of 1 happens about every 20 minutes."""
    w = {
        "w_metrics": 100.0,
        "w_ckpt": 18.0,  # roughly a checkpoint a minute
        "w_ckpt_slow": per_tick(1),
        "w_ckpt_fail": per_tick(1.5),
        "w_deploy": per_tick(rng.uniform(0.5, 1.5)),
        "w_tm_lost": per_tick(rng.uniform(1, 3)),
        "w_chatter": 2.0,
        "w_peak": 0.0,
        "w_flap": 0.0,
        "w_crash": 0.0,
        "w_stall": 0.0,
        "w_grow": 0.0,
    }
    if profile == "peaky":
        w["w_peak"] = round(rng.uniform(1.4, 2.0), 2)  # a peak every 10-15 minutes
    elif profile == "flapper":
        w["w_flap"] = round(rng.uniform(5.0, 10.0), 2)  # a flap every 2-4 minutes
    elif profile in ("crash_loop", "stalled", "state_growth"):
        # Genuine problems start about 7 minutes in, then never recover.
        w[{"crash_loop": "w_crash", "stalled": "w_stall", "state_growth": "w_grow"}[profile]] = 3.0
        w["w_deploy"] = 0.0
    return w


def job_entry(rng, tenant, name, profile, cluster):
    kind = rng.choice(["java", "java", "sql"])
    rate = round(rng.lognormvariate(5.5, 1.0), 1)  # median ~250 records/s
    parallelism = rng.choice([1, 1, 2, 2, 4])
    first = NOW - timedelta(days=rng.randint(20, 900), minutes=rng.randint(0, 1439))
    entity = name.split("-")[-2] if name.count("-") >= 2 else "event"
    entry = {
        "job_name": name,
        "tenant": tenant,
        "owner": f"@{tenant}",
        "tier": rng.choices(["tier1", "tier2", "tier3"], [2, 5, 3])[0],
        "kind": kind,
        "max_parallelism": parallelism * 2,
        "parallelism": parallelism,
        "cluster": f"flink-{cluster:02d}",
        "input_topic": f"{tenant}.{entity}s",
        "sink_table": f"{tenant.replace('-', '_')}.{entity}s",
        "first_deployed_at": first.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "version": f"{rng.randint(1, 9)}.{rng.randint(0, 40)}.{rng.randint(0, 9)}",
        "base_rate": rate,
        "busy_ms": round(rng.uniform(80, 450), 0),
        "base_lag": round(rate * rng.uniform(0.2, 2.0), 0),
        "peak_lag": round(max(rate, 50) * rng.uniform(120, 400), 0),
        "heap_ratio": round(rng.uniform(0.25, 0.55), 2),
        "heap_max_bytes": rng.choice([1, 2, 2, 4]) * 1024 ** 3,
        "state_bytes": int(rng.lognormvariate(16, 1.5)),
        "ckpt_ms": round(rng.uniform(150, 2500), 0),
        "startup_failure": failure(STARTUP, cluster, rng),
        "tm_failure": failure(TM_LOST, cluster, rng),
        # Only the failures the job's profile uses (see `assign_profile`).
        "flap_failure": None,
        "crash_failure": None,
        "chatter": dict(zip(("level", "logger", "message"), rng.choice(CHATTER))),
        "author": rng.choice(PEOPLE),
    }
    assign_profile(entry, profile, rng, cluster)
    return entry


def assign_profile(entry, profile, rng, cluster):
    entry["profile"] = profile
    entry.update(weights(profile, rng))
    if profile == "flapper":
        entry["flap_failure"] = failure(rng.choice(FLAPS), cluster, rng)
    if profile == "crash_loop":
        entry["crash_failure"] = failure(rng.choice(CRASHES), cluster, rng)


def build():
    rng = random.Random(SEED)
    # Tenants differ in size, like a real multi-tenant platform.
    sizes = [rng.randint(8, 45) for _ in TENANTS]
    scale = CLOUD_SIZE / sum(sizes)
    sizes = [max(5, round(s * scale)) for s in sizes]
    sizes[-1] += CLOUD_SIZE - sum(sizes)

    jobs, names = [], set()
    for t, (tenant, size) in enumerate(zip(TENANTS, sizes)):
        cluster = t % 8 + 1
        for _ in range(size):
            while True:
                name = f"{tenant}-{rng.choice(ENTITIES)}-{rng.choice(VERBS)}"
                if name not in names:
                    break
                name = f"{name}-{rng.randint(2, 9)}"
                if name not in names:
                    break
            names.add(name)
            profile = rng.choices(["steady", "peaky", "flapper"], [78, 16, 6])[0]
            jobs.append(job_entry(rng, tenant, name, profile, cluster))

    # A few genuine problems hidden in the fleet (docs/design.md §7.2).
    genuine = {"crash_loop": 2, "stalled": 1, "state_growth": 1}
    candidates = [j for j in jobs if j["profile"] == "steady"]
    rng.shuffle(candidates)
    for profile, count in genuine.items():
        for job in candidates[:count]:
            assign_profile(job, profile, rng, int(job["cluster"].split("-")[1]))
        candidates = candidates[count:]
    return jobs


def local_subset(jobs):
    """Every genuine problem plus an even sample of the rest, so each tenant keeps some jobs."""
    genuine = [j for j in jobs if j["profile"] not in ("steady", "peaky", "flapper")]
    rest = [j for j in jobs if j not in genuine]
    step = len(rest) / (LOCAL_SIZE - len(genuine))
    return genuine + [rest[int(i * step)] for i in range(LOCAL_SIZE - len(genuine))]


# The ShadowTraffic config. Helpers build its function calls.

TICK_MS = 10_000
IDENTITY = [
    "job_name", "tenant", "owner", "tier", "kind", "max_parallelism", "parallelism", "cluster",
    "input_topic", "sink_table", "first_deployed_at", "version", "author", "base_rate", "heap_max_bytes",
]


def fk(*path):
    return {"_gen": "var", "var": "forkKey", "path": list(path)}


def scaled(field, low, high, decimals=1):
    """The roster field times a uniform random factor."""
    return {
        "_gen": "math",
        "expr": "x * f",
        "names": {"x": fk(field), "f": {"_gen": "uniformDistribution", "bounds": [low, high]}},
        "decimals": decimals,
    }


def uniform(low, high, decimals=0):
    return {"_gen": "uniformDistribution", "bounds": [low, high], "decimals": decimals}


def event(kind, fields, throttle_ms=None):
    state = {"value": {"_gen": "merge", "objects": [
        {"_gen": "var", "var": "forkKey", "selectKeys": IDENTITY},
        dict({"event": kind}, **fields),
    ]}}
    if throttle_ms:
        state["localConfigs"] = {"throttleMs": throttle_ms}
    return state


def metrics(records, busy, backpressure, pending, heap=None):
    return event("metrics", {
        "records_in_per_sec": records,
        "busy_ms_per_sec": busy,
        "backpressured_ms_per_sec": backpressure,
        "pending_records": pending,
        "heap_used_ratio": heap or scaled("heap_ratio", 0.92, 1.08, 3),
    })


def checkpoint(duration, size, state_size, failed=False, reason=None):
    return event("checkpoint", {
        "failed": failed, "duration_ms": duration, "size_bytes": size,
        "state_size_bytes": state_size, "failure_reason": reason,
    })


def previous(field):
    """A field of this job's previous event. (Null if that event lacks it, so
    only follow states whose events have the field.)"""
    return {"_gen": "previousEvent", "path": ["value", field]}


def grow(field, factor):
    return {"_gen": "math", "expr": "p * " + str(factor), "names": {"p": previous(field)}, "decimals": 0}


def weighted(*choices):
    return {"_gen": "weightedOneOf", "choices": [{"value": v, "weight": w} for v, w in choices]}


def config():
    states = {
        # Steady operation, and the noise patterns of §7.2.
        "running": metrics(scaled("base_rate", 0.9, 1.1), scaled("busy_ms", 0.85, 1.15),
                           uniform(0, 25), scaled("base_lag", 0.5, 1.5, 0)),
        "checkpoint": checkpoint(scaled("ckpt_ms", 0.8, 1.25, 0), scaled("state_bytes", 0.05, 0.2, 0),
                                 scaled("state_bytes", 0.98, 1.02, 0)),
        "checkpoint_slow": checkpoint(uniform(12_000, 26_000), scaled("state_bytes", 0.05, 0.2, 0),
                                      scaled("state_bytes", 0.98, 1.02, 0)),
        "checkpoint_expired": checkpoint(30_000, 0, scaled("state_bytes", 0.98, 1.02, 0), True,
                                         "Checkpoint expired before completing."),
        "chatter": event("log", {"level": fk("chatter", "level"), "logger": fk("chatter", "logger"),
                                 "message": fk("chatter", "message")}),
        "deploy": event("deploy", {
            "summary": {"_gen": "oneOf", "choices": [
                "Bump connector versions", "Tune watermark idleness", "Add field to output schema",
                "Refactor enrichment lookups", "Raise sink batch size", "Fix late-event handling",
                "Upgrade Jackson", "Add metrics for dropped records"]},
            "commit_sha": {"_gen": "gitSha"},
        }, 15_000),
        "post_deploy_failure": event("failure", {"failure": fk("startup_failure")}, 20_000),
        "tm_lost": event("failure", {"failure": fk("tm_failure")}, 3_000),
        # A TaskManager loss also fails the checkpoint in flight: one problem, several alerts.
        "tm_lost_checkpoint": checkpoint(uniform(2_000, 9_000), 0, scaled("state_bytes", 0.98, 1.02, 0), True,
                                         "Checkpoint was declined (task failure)."),
        "flap": event("failure", {"failure": fk("flap_failure")}, 8_000),
        "recovered": event("recovered", {}),
        "peak": metrics(scaled("base_rate", 1.8, 2.6), uniform(930, 1000), uniform(550, 900),
                        scaled("peak_lag", 0.5, 1.0, 0)),
        # Genuine problems: once entered, they never recover.
        # Down 40 s of every ~70 s: restarts, downtime, lag; an alert storm.
        "crash": event("failure", {"failure": fk("crash_failure")}, 40_000),
        "crash_restarted": event("recovered", {}, 20_000),
        "crash_running": metrics(scaled("base_rate", 1.2, 1.6), uniform(600, 900), uniform(100, 400),
                                 scaled("base_lag", 2, 4, 0)),
        # Entered from "running", whose metrics carry pending_records.
        "stalled": metrics(0, 0, 0, {"_gen": "math", "expr": "p + r * 10", "decimals": 0, "names": {
            "p": previous("pending_records"), "r": fk("base_rate")}}),
        # State and checkpoint durations grow ~5% a tick until checkpoints
        # exceed the 30 s timeout (F4's shape, in the fleet).
        "growing_start": checkpoint(scaled("ckpt_ms", 1.5, 2, 0), scaled("state_bytes", 0.1, 0.2, 0),
                                    scaled("state_bytes", 1.5, 2, 0)),
        "growing": checkpoint(grow("duration_ms", 1.06), grow("size_bytes", 1.03),
                              grow("state_size_bytes", 1.05)),
    }
    transitions = {
        "running": weighted(
            ("running", fk("w_metrics")), ("checkpoint", fk("w_ckpt")), ("checkpoint_slow", fk("w_ckpt_slow")),
            ("checkpoint_expired", fk("w_ckpt_fail")), ("chatter", fk("w_chatter")), ("deploy", fk("w_deploy")),
            ("tm_lost", fk("w_tm_lost")), ("peak", fk("w_peak")), ("flap", fk("w_flap")),
            ("crash", fk("w_crash")), ("stalled", fk("w_stall")), ("growing_start", fk("w_grow"))),
        "checkpoint": "running",
        "checkpoint_slow": "running",
        "checkpoint_expired": "running",
        "chatter": "running",
        # Half of deploys restart once while the new version starts up.
        "deploy": weighted(("post_deploy_failure", 1), ("running", 1)),
        "post_deploy_failure": "recovered",
        "tm_lost": "tm_lost_checkpoint",
        "tm_lost_checkpoint": "recovered",
        "flap": "recovered",
        "recovered": "running",
        "peak": weighted(("peak", 90), ("running", 10)),
        "crash": "crash_restarted",
        "crash_restarted": "crash_running",
        "crash_running": "crash",
        "stalled": "stalled",
        "growing_start": "growing",
        "growing": "growing",
    }
    return {
        "generators": [{
            "name": "fleet",
            "topic": "fleet.telemetry",
            "fork": {
                "key": {"_gen": "loadJsonFile", "file": "/fleet/roster.json"},
                "stagger": {"ms": 25},
            },
            "key": fk("job_name"),
            "value": {},
            "stateMachine": {"_gen": "stateMachine", "initial": "running", "transitions": transitions,
                             "states": states},
            "localConfigs": {"throttleMs": TICK_MS},
        }],
        "connections": {
            "kafka": {
                "kind": "kafka",
                "producerConfigs": {
                    "bootstrap.servers": "kafka:9092",
                    "key.serializer": "org.apache.kafka.common.serialization.StringSerializer",
                    "value.serializer": "io.shadowtraffic.kafka.serdes.JsonSerializer",
                },
            },
        },
    }


def main():
    (HERE.parent / "fleet.json").write_text(json.dumps(config(), indent=2) + "\n")
    print("fleet.json written")
    jobs = build()
    for name, roster in (("roster-cloud.json", jobs), ("roster-local.json", local_subset(jobs))):
        # One job per line: compact, and diffs show which jobs changed.
        lines = ",\n".join(json.dumps(job, separators=(",", ":")) for job in roster)
        (HERE / name).write_text(f"[\n{lines}\n]\n")
        profiles = {}
        for j in roster:
            profiles[j["profile"]] = profiles.get(j["profile"], 0) + 1
        print(f"{name}: {len(roster)} jobs, {len({j['tenant'] for j in roster})} tenants, {profiles}")


if __name__ == "__main__":
    main()
