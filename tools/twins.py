#!/usr/bin/env python3
"""Find copy-paste twins in the Rust sources.

WHY THIS EXISTS. Merges leave duplicated logic behind, and the duplicates are
worse than untidy: while two copies of a guard both exist, neither can be
pinned by a test, because deleting either one leaves the other covering for it.
That is not hypothetical. On 2026-08-14 `input_timing` carried two `if is_live`
early returns, the fork's and upstream's. They were not equivalent (one clamped
`transcoded_until` into the item, the other read it raw), the second was
unreachable, and a test written for the invariant passed with either one
deleted.

TWO PASSES, because the twins found in this repo have different shapes.

  exact      Contiguous windows of N normalised lines that hash identically.
             Catches the classic case: a block pasted verbatim. Deliberately
             window based rather than function based, because the twin found
             before this tool existed was a 28 line block inside two large,
             otherwise different functions, and a function level detector
             reports nothing for it.

  similar    Brace balanced blocks opening with a control flow keyword, scored
             by CONTAINMENT of the smaller block in the larger. Both choices
             were forced by the pair described above, which the first version
             of this tool missed. Fixed windows cut a guard at an arbitrary
             point, and the longer copy spent its whole 8 line window computing
             a local before it reached the `return` the two shared. Jaccard
             asks how alike two blocks are, which is the wrong question once
             one copy has grown lines the other lacks: that pair scores 0.14 by
             Jaccard and 0.70 by containment.

KNOWN NOISE, both deliberate duplication rather than merge damage:

  crates/ffpipeline/tests/      one integration suite per hardware backend
  crates/ffpipeline/src/accel/  one HwAccel implementation per backend

Test modules and tests/ directories are skipped by default for that reason;
including them took the 0.6 count from 356 to 789 when measured. accel/ is
left in, because a real twin could hide there, but expect most of its hits to
be by design.

Neither pass is authoritative. Both report candidates for a human to judge.

  tools/twins.py                          both passes over crates/
  tools/twins.py --pass exact             verbatim blocks only
  tools/twins.py --threshold 0.8          tighter containment
  tools/twins.py --include-tests          scan test code too, noisy
  tools/twins.py crates/ersatztv-channel  one crate
"""

import argparse
import hashlib
import re
import sys
from collections import defaultdict
from pathlib import Path

# a block opening with one of these is a candidate guard, which is the shape a
# duplicated early return has
CONTROL_FLOW = ("if ", "match ", "while ", "for ", "return ", "} else if ")

COMMENT = re.compile(r"^\s*(//|/\*|\*)")
TRAILING_COMMENT = re.compile(r"\s+//.*$")
WHITESPACE = re.compile(r"\s+")


def normalise(path):
    """Return [(line_number, normalised_text)] for lines that carry logic.

    Comments and blank lines go, because a twin that has been re-commented is
    still a twin, and after a merge that is the usual case. Indentation goes,
    because the same block at a different nesting depth is still the same
    block. Identifiers are NOT stripped: two blocks doing the same thing to
    different variables are usually a refactoring opportunity rather than a
    duplicate, and stripping names makes every getter in the tree look alike.
    """
    out = []
    for number, raw in enumerate(path.read_text(errors="replace").splitlines(), 1):
        # test modules sit at the end of a file by convention, and their
        # duplication is usually deliberate: the same case per hardware
        # backend or per codec. Scanning them buries the real findings
        if raw.startswith("#[cfg(test)]"):
            break
        if COMMENT.match(raw):
            continue
        text = TRAILING_COMMENT.sub("", raw).strip()
        if not text:
            continue
        out.append((number, WHITESPACE.sub(" ", text)))
    return out


def is_noise(window):
    """Blocks that are structurally repetitive rather than duplicated logic."""
    texts = [t for _, t in window]
    if len(set(texts)) <= max(2, len(texts) // 3):
        return True
    # mostly punctuation: struct literals and match arms line up like this all
    # over a codebase, and matching on them says nothing
    if sum(1 for t in texts if len(t) <= 3) > len(texts) // 2:
        return True
    return False


def windows(lines, size):
    for i in range(len(lines) - size + 1):
        yield lines[i : i + size]


def blocks(lines, minimum, maximum):
    """Brace balanced regions opening with a control flow keyword."""
    for i, (_, text) in enumerate(lines):
        if not text.startswith(CONTROL_FLOW) or not text.endswith("{"):
            continue
        depth = 0
        for j in range(i, min(i + maximum, len(lines))):
            depth += lines[j][1].count("{") - lines[j][1].count("}")
            if depth <= 0 and j > i:
                if j - i + 1 >= minimum:
                    yield lines[i : j + 1]
                break


def exact_pass(files, size):
    buckets = defaultdict(list)
    for path, lines in files.items():
        for window in windows(lines, size):
            if is_noise(window):
                continue
            digest = hashlib.sha1("\n".join(t for _, t in window).encode()).hexdigest()
            buckets[digest].append((path, window[0][0], window[-1][0]))

    findings = []
    for sites in buckets.values():
        # a window overlaps its own neighbours; require starts far enough apart
        # to be genuinely separate blocks
        distinct = []
        for site in sites:
            if not any(s[0] == site[0] and abs(s[1] - site[1]) < size for s in distinct):
                distinct.append(site)
        if len(distinct) > 1:
            findings.append(distinct)
    return findings


def similar_pass(files, threshold, minimum=5, maximum=40):
    candidates = []
    for path, lines in files.items():
        for block in blocks(lines, minimum, maximum):
            if is_noise(block):
                continue
            candidates.append((path, block[0][0], block[-1][0], {t for _, t in block}))

    findings = []
    for i, (path_a, start_a, end_a, set_a) in enumerate(candidates):
        for path_b, start_b, end_b, set_b in candidates[i + 1 :]:
            # a block contains its own nested blocks; those are not twins
            if path_a == path_b and not (end_a < start_b or end_b < start_a):
                continue
            smaller = min(len(set_a), len(set_b))
            if not smaller:
                continue
            score = len(set_a & set_b) / smaller
            if score >= threshold:
                findings.append(
                    (score, (path_a, start_a, end_a), (path_b, start_b, end_b))
                )
    findings.sort(key=lambda f: -f[0])
    return findings


def collect(roots, include_tests=False):
    files = {}
    for root in roots:
        root = Path(root)
        paths = [root] if root.is_file() else sorted(root.rglob("*.rs"))
        for path in paths:
            if "target" in path.parts:
                continue
            if "tests" in path.parts and not include_tests:
                continue
            lines = normalise(path)
            if lines:
                files[path] = lines
    return files


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("roots", nargs="*", default=["crates"])
    parser.add_argument(
        "--pass", dest="which", choices=["exact", "similar", "both"], default="both"
    )
    parser.add_argument("--exact-window", type=int, default=12)
    parser.add_argument("--threshold", type=float, default=0.6)
    parser.add_argument(
        "--include-tests",
        action="store_true",
        help="scan test modules and tests/ as well; noisy by design",
    )
    args = parser.parse_args()

    files = collect(args.roots or ["crates"], args.include_tests)
    if not files:
        print("no rust sources found", file=sys.stderr)
        return 1

    print(f"scanned {len(files)} files")
    total = 0

    if args.which in ("exact", "both"):
        findings = exact_pass(files, args.exact_window)
        total += len(findings)
        print(f"\n=== exact, {args.exact_window} line windows: {len(findings)} ===")
        for sites in findings:
            print("  duplicated block:")
            for path, start, end in sites:
                print(f"    {path}:{start}-{end}")

    if args.which in ("similar", "both"):
        findings = similar_pass(files, args.threshold)
        total += len(findings)
        print(
            f"\n=== similar blocks at >= {args.threshold:.2f} containment: "
            f"{len(findings)} ==="
        )
        for score, a, b in findings:
            print(f"  {score:.2f}  {a[0]}:{a[1]}-{a[2]}")
            print(f"        {b[0]}:{b[1]}-{b[2]}")

    if not total:
        print("\nno candidates")
    return 0


if __name__ == "__main__":
    sys.exit(main())
