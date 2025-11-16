## Why It Skips
- Skips occur inside name resolution when a name is unknown or maps to multiple part numbers. Current behavior skips even in non-strict mode (see resolve logic in snowmobile_importer.py:281–306).

## Resolution Strategy
1. Stronger name normalization
- Build a case-insensitive index: store `name_to_pn_lower[name.lower()] → [pns]` in addition to exact.
- Normalize names by trimming whitespace and collapsing multiple spaces.

2. Unknown names handling
- If a name is unknown, check if the name string is actually a part number; resolve via parts keys.
- If still unknown and non-strict, log and skip; in strict mode, fail.

3. Ambiguous names handling (no skip in non-strict)
- In non-strict mode, deterministically pick a single part number using tie-breakers:
  - Prefer latest `Revision` (if available)
  - Prefer `View='Design'` (configurable via `--prefer-view`)
  - Prefer a `--prefer-container`
  - Otherwise choose the lowest/lexicographically first part number for stability
- In strict mode, still fail on ambiguity.

4. Config flags
- `--name-resolution first|strict` (default `first`) to control behavior
- `--prefer-view`, `--prefer-container`, `--prefer-latest-revision` to tune selection

5. Implementation points
- Extend index builder to produce `name_to_pn_lower` and collect metadata (revision/view/container) per name (we already collect `name_sources` in build_cross_index).
- Update `resolve_edges_by_name` to:
  - Try exact → lower-case match → numeric fallback
  - Apply tie-breakers when multiple pns exist (non-strict)
  - Only skip when unknown after all attempts; strict mode raises

6. Tests
- Ambiguous names resolve to a single PN in non-strict mode using tie-breakers
- Unknown names resolve when name is actually a PN string
- Case-insensitive matching works
- Strict mode still raises on ambiguity/unknown

7. Logging and Metrics
- Replace “Skipped N name-based edges” with a breakdown: `resolved`, `ambiguous_resolved`, `unknown_skipped`
- Add optional `--resolution-report path` with rows of name, candidates, chosen PN, and reason

## Outcome
- The 2073 skipped edges should drop to near zero under non-strict policy with deterministic tie-breakers.

## Proceeding
On approval, I will implement the normalization, tie-breakers, policy flags, update the resolver, add tests and a small report output, and verify a near-zero skip count in import logs.