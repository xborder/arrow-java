#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Evaluation harness for the Jev investigation (see README.md in this directory).

Reads a JSONL file where each line is:

    {"id": "...",
     "state": "<text the model judges>",
     "questions": {"name": {"type": "choice"|"noul"|"score", ...}, ...},
     "gold": {"name": <expected answer>, ...}}

Gold values: for "choice" the option key; for "noul" 0 or 1; for "score" the
integer level index (0..n-1).

Calls the endpoint once per record with every question in one request, then
reports per-question accuracy, Brier score, MAE (score questions), latency
percentiles, token usage and cost at the published price.

Standard library only. Configuration via flags or environment:

    JEV_API_URL   full endpoint URL (the default below mirrors the open-source
                  Kev clone's path and is NOT verified against TypeSafe's docs)
    JEV_API_KEY   bearer token

Use --dry-run to print request shapes and rough token estimates with no network.
"""

import argparse
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.request

PRICE_PER_M_INPUT_TOKENS_USD = 0.042  # vendor-published; output tokens not billed


def estimate_tokens(text):
    # Crude: ~4 characters per token for English/log text. Good enough for budgeting.
    return max(1, len(text) // 4)


def call(url, key, state, questions, model, timeout):
    body = {"state": state, "questions": questions}
    if model:
        body["model"] = model
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    if key:
        req.add_header("Authorization", "Bearer " + key)
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return payload, (time.perf_counter() - t0) * 1000.0


def percentile(values, p):
    if not values:
        return float("nan")
    s = sorted(values)
    k = (len(s) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("dataset", help="JSONL file of records")
    ap.add_argument("--url", default=os.environ.get("JEV_API_URL", "https://api.typesafe.ai/v1/systemone"))
    ap.add_argument("--key", default=os.environ.get("JEV_API_KEY"))
    ap.add_argument("--model", default=None)
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--dry-run", action="store_true", help="print request shapes and token estimates only")
    ap.add_argument("--out", help="write raw responses as JSONL here")
    args = ap.parse_args()

    records = []
    with open(args.dataset, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    if not records:
        print("no records", file=sys.stderr)
        return 2

    if args.dry_run:
        total = 0
        for r in records:
            q_text = json.dumps(r["questions"])
            n = estimate_tokens(r["state"]) + estimate_tokens(q_text)
            total += n
            print(f"{r['id']}: ~{n} input tokens, {len(r['questions'])} questions")
        print(f"\n{len(records)} records, ~{total} input tokens, "
              f"~US${total / 1e6 * PRICE_PER_M_INPUT_TOKENS_USD:.6f} at the published price")
        print(json.dumps({"state": records[0]["state"][:200] + "...", "questions": records[0]["questions"]}, indent=2))
        return 0

    if not args.key:
        print("JEV_API_KEY (or --key) is required unless --dry-run", file=sys.stderr)
        return 2

    latencies, server_latencies, input_tokens = [], [], []
    per_q = {}  # name -> dict of lists
    out = open(args.out, "w", encoding="utf-8") if args.out else None
    failures = 0

    for r in records:
        try:
            payload, ms = call(args.url, args.key, r["state"], r["questions"], args.model, args.timeout)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            failures += 1
            print(f"{r['id']}: request failed: {e}", file=sys.stderr)
            continue
        latencies.append(ms)
        if isinstance(payload.get("latency_ms"), (int, float)):
            server_latencies.append(payload["latency_ms"])
        usage = payload.get("usage") or {}
        if isinstance(usage.get("input_tokens"), int):
            input_tokens.append(usage["input_tokens"])
        if out:
            out.write(json.dumps({"id": r["id"], "response": payload}) + "\n")

        answers = payload.get("answers") or {}
        for name, spec in r["questions"].items():
            gold = r.get("gold", {}).get(name)
            ans = answers.get(name)
            if ans is None or gold is None:
                continue
            stats = per_q.setdefault(name, {"type": spec["type"], "n": 0, "correct": 0, "brier": [], "abs_err": [],
                                            "conf_correct": [], "conf_wrong": []})
            stats["n"] += 1
            t = spec["type"]
            if t == "choice":
                probs = ans.get("probabilities") or {}
                pred = ans.get("choice")
                ok = pred == gold
                stats["correct"] += int(ok)
                # multi-class Brier: sum over options of (p - 1[gold])^2
                stats["brier"].append(sum((p - (1.0 if k == gold else 0.0)) ** 2 for k, p in probs.items()))
                (stats["conf_correct"] if ok else stats["conf_wrong"]).append(ans.get("confidence", float("nan")))
            elif t == "noul":
                p = float(ans.get("noul"))
                ok = (p >= 0.5) == bool(gold)
                stats["correct"] += int(ok)
                stats["brier"].append((p - float(gold)) ** 2)
            elif t == "score":
                probs = ans.get("probabilities") or {}
                pred_level = max(probs, key=probs.get) if probs else None
                ok = pred_level is not None and int(pred_level) == int(gold)
                stats["correct"] += int(ok)
                stats["abs_err"].append(abs(float(ans.get("score")) - float(gold)))
                stats["brier"].append(sum((p - (1.0 if int(k) == int(gold) else 0.0)) ** 2 for k, p in probs.items()))
                (stats["conf_correct"] if ok else stats["conf_wrong"]).append(ans.get("confidence", float("nan")))

    if out:
        out.close()

    print(f"\nrecords: {len(records)}  ok: {len(latencies)}  failed: {failures}")
    if latencies:
        print(f"wall-clock ms  p50 {percentile(latencies, .5):.0f}  p95 {percentile(latencies, .95):.0f}  "
              f"max {max(latencies):.0f}")
    if server_latencies:
        print(f"server ms      p50 {percentile(server_latencies, .5):.0f}  p95 {percentile(server_latencies, .95):.0f}")
    if input_tokens:
        tot = sum(input_tokens)
        print(f"input tokens   total {tot}  mean {tot / len(input_tokens):.0f}  "
              f"cost ~US${tot / 1e6 * PRICE_PER_M_INPUT_TOKENS_USD:.6f}")
    print()
    for name, s in per_q.items():
        line = f"{name:24s} [{s['type']:6s}] n={s['n']:3d}  acc={s['correct'] / s['n']:.3f}"
        if s["brier"]:
            line += f"  brier={statistics.mean(s['brier']):.3f}"
        if s["abs_err"]:
            line += f"  mae={statistics.mean(s['abs_err']):.3f}"
        cc = [c for c in s["conf_correct"] if c == c]
        cw = [c for c in s["conf_wrong"] if c == c]
        if cc or cw:
            line += (f"  conf(correct)={statistics.mean(cc) if cc else float('nan'):.2f}"
                     f"  conf(wrong)={statistics.mean(cw) if cw else float('nan'):.2f}")
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
