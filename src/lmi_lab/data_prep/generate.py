from __future__ import annotations

import argparse
import gzip
import json
import math
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class GenerateConfig:
    rows: int
    num_files: int
    outdir: Path
    seed: int
    mismatch_rate: float
    missing_rate: float
    optype_ratio: str
    avg_tags: int
    avg_levels: int
    avg_ts: int
    shuffle_rows: bool
    shuffle_list_elements: bool
    use_gzip: bool
    timestyle: str


def _bool(s: str) -> bool:
    return str(s).lower() in {"1", "true", "yes", "on"}


def _splitmix64(x: int) -> int:
    x = (x + 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> 30
    x = (x * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> 27
    x = (x * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> 31
    return x & 0xFFFFFFFFFFFFFFFF


def _rand_u64(seed: int, idx: int, stream: int) -> int:
    return _splitmix64(seed ^ (idx * 0x9E3779B185EBCA87) ^ (stream * 0xD1B54A32D192ED03))


def _rand01(seed: int, idx: int, stream: int) -> float:
    return _rand_u64(seed, idx, stream) / float(1 << 64)


def _sample_count(seed: int, idx: int, stream: int, avg: int) -> int:
    if avg <= 1:
        return 1
    lo = max(1, avg - 1)
    hi = avg + 1
    return lo + (_rand_u64(seed, idx, stream) % (hi - lo + 1))


def _parse_optype_ratio(spec: str) -> list[tuple[str, float]]:
    weights: list[tuple[str, float]] = []
    total = 0.0
    for item in spec.split(","):
        key, value = item.split(":", 1)
        k = key.strip().upper()
        if k not in {"U", "I", "D"}:
            raise ValueError(f"unknown op_type: {k}")
        w = float(value)
        total += w
        weights.append((k, w))
    if total <= 0:
        raise ValueError("optype ratio total must be > 0")
    acc = 0.0
    normalized: list[tuple[str, float]] = []
    for k, w in weights:
        acc += w / total
        normalized.append((k, acc))
    normalized[-1] = (normalized[-1][0], 1.0)
    return normalized


def _pick_op(seed: int, idx: int, ratios: list[tuple[str, float]]) -> str:
    r = _rand01(seed, idx, 4)
    for op, threshold in ratios:
        if r <= threshold:
            return op
    return "U"


def _format_ts(seed: int, idx: int, n: int, timestyle: str) -> list[str]:
    vals: list[str] = []
    base = 1 + (_rand_u64(seed, idx, 5) % 27)
    for i in range(n):
        day = ((base + i) % 28) + 1
        hh = int(_rand_u64(seed, idx, 100 + i) % 24)
        mm = int(_rand_u64(seed, idx, 200 + i) % 60)
        ss = int(_rand_u64(seed, idx, 300 + i) % 60)
        sep = " "
        if timestyle == "t":
            sep = "T"
        elif timestyle == "mixed":
            sep = "T" if (_rand_u64(seed, idx, 400 + i) % 2 == 0) else " "
        vals.append(f"2026-01-{day:02d}{sep}{hh:02d}:{mm:02d}:{ss:02d}")
    return vals


def _shuffle(vals: list[str], seed: int, idx: int, stream: int) -> list[str]:
    arr = vals[:]
    for i in range(len(arr) - 1, 0, -1):
        j = int(_rand_u64(seed, idx, stream + i) % (i + 1))
        arr[i], arr[j] = arr[j], arr[i]
    return arr


def _build_base_lists(cfg: GenerateConfig, idx: int) -> tuple[list[str], list[str], list[str]]:
    tags_n = _sample_count(cfg.seed, idx, 10, cfg.avg_tags)
    levels_n = _sample_count(cfg.seed, idx, 20, cfg.avg_levels)
    ts_n = _sample_count(cfg.seed, idx, 30, cfg.avg_ts)
    tags = [str(10_000_000 + (_rand_u64(cfg.seed, idx, 500 + i) % 90_000_000)) for i in range(tags_n)]
    levels = [str(1 + (_rand_u64(cfg.seed, idx, 600 + i) % 9)) for i in range(levels_n)]
    ts = _format_ts(cfg.seed, idx, ts_n, cfg.timestyle)
    return tags, levels, ts


def _mutate(vals: list[str], seed: int, idx: int, stream: int, fallback: str) -> list[str]:
    if not vals:
        return [fallback]
    out = vals[:]
    pos = int(_rand_u64(seed, idx, stream) % len(out))
    out[pos] = fallback
    return out


def _row_line(uid: str, tags: list[str], levels: list[str], ts: list[str], op: str) -> str:
    return "\t".join([uid, "|".join(tags), "|".join(levels), "|".join(ts), op])


HEADER = "user_id\ttags\tlevels\tts\top_type\n"


def _plan_presence(cfg: GenerateConfig, idx: int) -> tuple[bool, bool, bool]:
    missing_roll = _rand01(cfg.seed, idx, 1)
    before_present = True
    after_present = True
    if missing_roll < cfg.missing_rate:
        before_present = _rand01(cfg.seed, idx, 2) < 0.5
        after_present = not before_present
    mismatch = before_present and after_present and (_rand01(cfg.seed, idx, 3) < cfg.mismatch_rate)
    return before_present, after_present, mismatch


def _permute_index(cfg: GenerateConfig, k: int, side: str) -> int:
    if not cfg.shuffle_rows:
        return k
    n = cfg.rows
    if n <= 1:
        return 0
    a = int(_rand_u64(cfg.seed, n, 901 if side == "before" else 902) | 1)
    while math.gcd(a, n) != 1:
        a = (a + 2) % n
        if a == 0:
            a = 1
    b = int(_rand_u64(cfg.seed, n, 903 if side == "before" else 904) % n)
    return (a * k + b) % n


def _file_quotas(total: int, num_files: int) -> list[int]:
    q, r = divmod(total, num_files)
    return [q + (1 if i < r else 0) for i in range(num_files)]


def run_generate(cfg: GenerateConfig) -> int:
    cfg.outdir.mkdir(parents=True, exist_ok=True)
    before_dir = cfg.outdir / "before"
    after_dir = cfg.outdir / "after"
    before_dir.mkdir(parents=True, exist_ok=True)
    after_dir.mkdir(parents=True, exist_ok=True)

    ratios = _parse_optype_ratio(cfg.optype_ratio)

    before_total = 0
    after_total = 0
    mismatch_total = 0
    for idx in range(cfg.rows):
        bp, ap, mm = _plan_presence(cfg, idx)
        before_total += 1 if bp else 0
        after_total += 1 if ap else 0
        mismatch_total += 1 if mm else 0

    before_quota = _file_quotas(before_total, cfg.num_files)
    after_quota = _file_quotas(after_total, cfg.num_files)

    suffix = ".tsv.gz" if cfg.use_gzip else ".tsv"
    opener = (lambda p: gzip.open(p, "wt", encoding="utf-8", newline="")) if cfg.use_gzip else (lambda p: p.open("w", encoding="utf-8", newline=""))

    before_paths = [before_dir / f"part-{i:05d}{suffix}" for i in range(cfg.num_files)]
    after_paths = [after_dir / f"part-{i:05d}{suffix}" for i in range(cfg.num_files)]

    before_writers = [opener(p) for p in before_paths]
    after_writers = [opener(p) for p in after_paths]
    header_bytes = len(HEADER.encode("utf-8"))
    before_bytes = [header_bytes] * cfg.num_files
    after_bytes = [header_bytes] * cfg.num_files

    try:
        for w in before_writers:
            w.write(HEADER)
        for w in after_writers:
            w.write(HEADER)

        bi = 0
        bj = 0
        for k in range(cfg.rows):
            idx = _permute_index(cfg, k, "before")
            bp, _, _ = _plan_presence(cfg, idx)
            if not bp:
                continue
            uid = f"a.{idx:09d}"
            tags, levels, ts = _build_base_lists(cfg, idx)
            op = _pick_op(cfg.seed, idx, ratios)
            if cfg.shuffle_list_elements:
                tags = _shuffle(tags, cfg.seed, idx, 700)
                levels = _shuffle(levels, cfg.seed, idx, 710)
                ts = _shuffle(ts, cfg.seed, idx, 720)
            while bi < cfg.num_files and bj >= before_quota[bi]:
                bi += 1
                bj = 0
            line = _row_line(uid, tags, levels, ts, op)
            before_writers[bi].write(line + "\n")
            before_bytes[bi] += len((line + "\n").encode("utf-8"))
            bj += 1

        ai = 0
        aj = 0
        for k in range(cfg.rows):
            idx = _permute_index(cfg, k, "after")
            _, ap, mm = _plan_presence(cfg, idx)
            if not ap:
                continue
            uid = f"a.{idx:09d}"
            tags, levels, ts = _build_base_lists(cfg, idx)
            op = _pick_op(cfg.seed, idx, ratios)
            if cfg.shuffle_list_elements:
                tags = _shuffle(tags, cfg.seed, idx, 730)
                levels = _shuffle(levels, cfg.seed, idx, 740)
                ts = _shuffle(ts, cfg.seed, idx, 750)
            if mm:
                selector = int(_rand_u64(cfg.seed, idx, 760) % 3)
                if selector == 0:
                    tags = _mutate(tags, cfg.seed, idx, 761, "99999999")
                elif selector == 1:
                    levels = _mutate(levels, cfg.seed, idx, 762, "99")
                else:
                    ts = _mutate(ts, cfg.seed, idx, 763, "2026-02-01 00:00:00")
            while ai < cfg.num_files and aj >= after_quota[ai]:
                ai += 1
                aj = 0
            line = _row_line(uid, tags, levels, ts, op)
            after_writers[ai].write(line + "\n")
            after_bytes[ai] += len((line + "\n").encode("utf-8"))
            aj += 1
    finally:
        for f in before_writers + after_writers:
            f.close()

    for i, p in enumerate(before_paths):
        gz_size = p.stat().st_size
        print(f"before/{p.name}: gzip={gz_size}B, uncompressed_est={before_bytes[i]}B")
    for i, p in enumerate(after_paths):
        gz_size = p.stat().st_size
        print(f"after/{p.name}: gzip={gz_size}B, uncompressed_est={after_bytes[i]}B")

    manifest = {
        "rows_requested": cfg.rows,
        "rows_before": before_total,
        "rows_after": after_total,
        "num_files": cfg.num_files,
        "seed": cfg.seed,
        "mismatch_rate": cfg.mismatch_rate,
        "missing_rate": cfg.missing_rate,
        "mismatch_rows": mismatch_total,
        "optype_ratio": cfg.optype_ratio,
        "gzip": cfg.use_gzip,
        "timestyle": cfg.timestyle,
        "before_files": [str(p.relative_to(cfg.outdir)) for p in before_paths],
        "after_files": [str(p.relative_to(cfg.outdir)) for p in after_paths],
        "before_uncompressed_est_bytes": sum(before_bytes),
        "after_uncompressed_est_bytes": sum(after_bytes),
    }
    with (cfg.outdir / "manifest.json").open("w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"generated dataset under: {cfg.outdir}")
    return 0


def add_generate_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    p = subparsers.add_parser("generate", help="before/after データセットを生成")
    p.add_argument("--rows", type=int, default=15_000_000)
    p.add_argument("--num-files", type=int, default=100)
    p.add_argument("--outdir", type=Path, required=True)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--mismatch-rate", type=float, default=0.01)
    p.add_argument("--missing-rate", type=float, default=0.01)
    p.add_argument("--optype-ratio", default="U:0.9,I:0.08,D:0.02")
    p.add_argument("--avg-tags", type=int, default=3)
    p.add_argument("--avg-levels", type=int, default=3)
    p.add_argument("--avg-ts", type=int, default=2)
    p.add_argument("--shuffle-rows", type=_bool, default=True)
    p.add_argument("--shuffle-list-elements", type=_bool, default=True)
    p.add_argument("--gzip", dest="use_gzip", type=_bool, default=False)
    p.add_argument("--timestyle", choices=["mixed", "space", "t"], default="mixed")

    def _cmd(args: argparse.Namespace) -> int:
        cfg = GenerateConfig(
            rows=args.rows,
            num_files=args.num_files,
            outdir=args.outdir,
            seed=args.seed,
            mismatch_rate=args.mismatch_rate,
            missing_rate=args.missing_rate,
            optype_ratio=args.optype_ratio,
            avg_tags=args.avg_tags,
            avg_levels=args.avg_levels,
            avg_ts=args.avg_ts,
            shuffle_rows=args.shuffle_rows,
            shuffle_list_elements=args.shuffle_list_elements,
            use_gzip=args.use_gzip,
            timestyle=args.timestyle,
        )
        return run_generate(cfg)

    p.set_defaults(func=_cmd)
