# Qualification evidence chronology

- `initial-failed/` preserves the original missed snapshot-growth injection:
  19 passing tests and one failure, with 18 of the then-required 19 observations.
- `qualified-v1/` preserves the pre-review passing report: 22 tests, 20 connected
  observations and 14 model cases. This report did **not** catch the model type or
  optimized-assertion defects. Its initial review clearance is superseded.
- `model-before-review.py` is the exact model at reviewed head
  `e0eff0bc74b2ed35420a98d43cab7edb13258e56`.
- `model-review-failed.json` reproduces eight false acceptances for UNKNOWN and
  integer proof flags. `model-optimized-failed.json` reproduces 13 falsely accepted
  negative controls when assertions were disabled and the model always accepted.
- `qualified-v3/` is the corrected local run: 22 tests, 20 connected observations,
  32 model cases and two optimized-process oracle fault probes. Its report binds
  the qualification files and all artifact hashes. New independent review and CI
  are separate gates; see the PR for those exact-head results.

`review-history.json` retains the findings and corrections. No earlier report is
rewritten to claim it caught a later finding. Whole-root capacity remains UNKNOWN,
quota enforcement false, the copied Waypoint fifth failure unchanged, and live
Jetstream2 blocked.
