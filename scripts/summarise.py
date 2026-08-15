#!/usr/bin/env python3
import glob
import json
import os
import statistics
import sys

ORDER = ["baseline", "burst", "permanent", "bulkhead", "breaker", "chaos"]


def load(directory):
    runs = {}
    for path in sorted(glob.glob(os.path.join(directory, "*.json"))):
        with open(path) as f:
            run = json.load(f)
        runs.setdefault((run["scenario"], run["profile"]), []).append(run)
    return runs


def spread(values, fmt="%.0f"):
    if not values:
        return "-"
    med = statistics.median(values)
    if min(values) == max(values):
        return fmt % med
    return "%s (%s-%s)" % (fmt % med, fmt % min(values), fmt % max(values))


def field(runs, *path):
    out = []
    for run in runs:
        node = run
        for key in path:
            node = node[key]
        out.append(node)
    return out


def main():
    directory = sys.argv[1] if len(sys.argv) > 1 else "bench"
    runs = load(directory)

    if not runs:
        print("no results in %s/" % directory)
        return 1

    first = next(iter(runs.values()))[0]
    print("%d hosts, %d keys, %d events per run\n" % (first["hosts"], first["keys"], first["events"]))

    head = "%-11s %-7s %3s %-15s %-15s %-15s %-9s %-6s %-4s" % (
        "scenario", "profile", "n", "accept/s", "p50 ms", "p95 ms", "arrived", "dlq", "inv")
    print(head)
    print("-" * len(head))

    keys = sorted(runs, key=lambda k: (ORDER.index(k[0]) if k[0] in ORDER else 99, k[1]))

    for key in keys:
        reps = runs[key]
        print("%-11s %-7s %3d %-15s %-15s %-15s %-9s %-6s %-4s" % (
            key[0],
            key[1],
            len(reps),
            spread(field(reps, "acceptRatePerSec")),
            spread(field(reps, "stats", "firstAttempt", "p50Ms"), "%.1f"),
            spread(field(reps, "stats", "firstAttempt", "p95Ms"), "%.1f"),
            spread(field(reps, "stats", "deliveries")),
            spread(field(reps, "deadLetterDelta")),
            spread(field(reps, "stats", "inversions")),
        ))

    print()
    for key, reps in runs.items():
        name = "%s/%s" % key
        for run in reps:
            outstanding = run["unaccounted"] - run["deadLetterDelta"]
            if outstanding > 0:
                if run.get("retriesDelta", 0) > 0:
                    print("OUTSTANDING: %s rep %d, %d still in retries when the wait gave up" % (
                        name, run["rep"], outstanding))
                else:
                    print("LOSS: %s rep %d, %d unaccounted with no durable home" % (
                        name, run["rep"], outstanding))
            if run["keyCollisions"] > 0:
                print("NOTE: %s rep %d, %d collisions — offered rate not held" % (
                    name, run["rep"], run["keyCollisions"]))

    return 0


if __name__ == "__main__":
    sys.exit(main())
