"""
file_costar_downloads.py — move validated CoStar exports from downloads/ into
data/costar/, under exactly the filenames the existing loaders already parse.

Written 2026-09-20 as prep for the CoStar automation build. This is the step
scripts/fetch_costar_portal.py's docstring refers to when it says "a separate
step (not this script) is responsible for moving these into data/costar/".

WHY THIS EXISTS AS ITS OWN STEP
  fetch_costar_portal.py deliberately writes to downloads/ so that a partial
  or corrupt fetch never lands in data/costar/, which the live Railway app
  reads on every push to main. Filing is therefore a separate, explicit act:
  nothing moves unless it validates here too.

NAMING IS NOT NEGOTIABLE
  data/costar/ is FLAT. Do not introduce subfolders; load_costar_market_daily.py
  and load_costar_segmentation.py both glob a flat directory. The target names
  below are the ones those loaders already match:

      daily_MM_DD_YY.xlsx        -> load_costar_market_daily.py
      monthly_MM_DD_YY.xlsx      -> load_costar_market_daily.py
      daily_seg_MM_DD_YY.xlsx    -> load_costar_segmentation.py
      monthly_seg_MM_DD_YY.xlsx  -> load_costar_segmentation.py
      particp_MM_DD_YY.xlsx      -> load_costar_segmentation.py
      <CoStar's own name>.pdf    -> load_costar_reports.py

  CoStar's own spelling of the participation export varies ("Particpation",
  their typo, and "particp"). Both already exist in data/costar/. We normalise
  outbound to particp_MM_DD_YY.xlsx and leave the historical files alone.

SAFETY
  - Never overwrites an existing file in data/costar/. A same-named file is
    reported and skipped, not clobbered.
  - Re-validates every file before moving (20,000-byte floor plus a real
    open test), mirroring fetch_costar_portal.py's _validate_export. The
    24-byte-corrupt failure mode has recurred repeatedly in this project
    across both data/str/ and data/costar/; it does not get a second door.
  - PDFs get their own check (size floor plus a PyMuPDF open), since the
    Excel check does not apply to them.

Exit codes: 0 if everything present was filed or correctly skipped, 1 if any
file failed validation. A run that finds nothing to file is a success, not a
failure, so this stays safe to call unconditionally from the pipeline.
"""

from __future__ import annotations

import os
import re
import shutil
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DOWNLOADS_DIR = Path(os.getenv("COSTAR_OUTPUT_DIR", PROJECT_ROOT / "downloads"))
COSTAR_DIR = PROJECT_ROOT / "data" / "costar"

MIN_VALID_XLSX_BYTES = 20_000
MIN_VALID_PDF_BYTES = 100_000

# Source stem (lowercased, without the MM_DD_YY tag) -> target stem.
# Order matters: more specific patterns first, because a plain "daily" would
# otherwise also match "daily_seg". This is the same first-match trap that
# already bit load_datafy_reports.py's handler list.
STEM_MAP: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^daily[_-]?seg(ment(ation)?)?$"), "daily_seg"),
    (re.compile(r"^monthly[_-]?seg(ment(ation)?)?$"), "monthly_seg"),
    (re.compile(r"^partic(ip(ation)?|pation)$"), "particp"),
    (re.compile(r"^daily$"), "daily"),
    (re.compile(r"^monthly$"), "monthly"),
]

DATE_TAG_RE = re.compile(r"(\d{2}_\d{2}_\d{2})$")


def _validate_xlsx(path: Path) -> None:
    size = path.stat().st_size
    if size < MIN_VALID_XLSX_BYTES:
        raise RuntimeError(
            f"{path.name} is only {size} bytes (floor {MIN_VALID_XLSX_BYTES}). "
            f"Same failure mode as the 24-byte corrupt downloads already in "
            f"_to_delete/. Treating as a failed fetch."
        )
    import pandas as pd

    df = pd.read_excel(path, sheet_name=0, nrows=5)
    if df.empty:
        raise RuntimeError(f"{path.name} opened as Excel but has no rows.")


def _validate_pdf(path: Path) -> None:
    size = path.stat().st_size
    if size < MIN_VALID_PDF_BYTES:
        raise RuntimeError(
            f"{path.name} is only {size} bytes (floor {MIN_VALID_PDF_BYTES}). "
            f"Real CoStar market report PDFs run 2-5 MB."
        )
    import fitz  # PyMuPDF, already a project dependency

    with fitz.open(path) as doc:
        if doc.page_count < 1:
            raise RuntimeError(f"{path.name} opened as PDF but has no pages.")


def _target_name(src: Path) -> str | None:
    """Map a downloaded filename to its canonical data/costar/ name.

    Returns None if the file is not something we know how to file, so an
    unrelated file sitting in downloads/ is left alone rather than guessed at.
    """
    if src.suffix.lower() == ".pdf":
        return src.name  # CoStar's own report names are already canonical

    if src.suffix.lower() not in (".xlsx", ".xls"):
        return None

    stem = src.stem.lower()
    m = DATE_TAG_RE.search(stem)
    if not m:
        print(f"  SKIP {src.name}: no MM_DD_YY tag in the filename. "
              f"Period stamping must come from the name, never a shared default.")
        return None

    date_tag = m.group(1)
    base = stem[: m.start()].rstrip("_-")

    for pattern, target_stem in STEM_MAP:
        if pattern.match(base):
            return f"{target_stem}_{date_tag}.xlsx"

    print(f"  SKIP {src.name}: stem '{base}' matches no known CoStar export type.")
    return None


def main() -> int:
    if not DOWNLOADS_DIR.exists():
        print(f"OK: {DOWNLOADS_DIR} does not exist, nothing to file.")
        return 0

    COSTAR_DIR.mkdir(parents=True, exist_ok=True)

    candidates = sorted(
        p for p in DOWNLOADS_DIR.iterdir()
        if p.is_file() and p.suffix.lower() in (".xlsx", ".xls", ".pdf")
    )
    if not candidates:
        print(f"OK: no CoStar exports waiting in {DOWNLOADS_DIR}.")
        return 0

    filed = skipped = failed = 0

    for src in candidates:
        target_name = _target_name(src)
        if target_name is None:
            skipped += 1
            continue

        dest = COSTAR_DIR / target_name

        if dest.exists():
            print(f"  SKIP {src.name}: {dest.name} already exists in data/costar/. "
                  f"Not overwriting.")
            skipped += 1
            continue

        try:
            if src.suffix.lower() == ".pdf":
                _validate_pdf(src)
            else:
                _validate_xlsx(src)
        except Exception as exc:
            print(f"  FAIL {src.name}: {exc}")
            failed += 1
            continue

        shutil.move(str(src), str(dest))
        print(f"  OK   {src.name} -> data/costar/{dest.name}")
        filed += 1

    print(f"\nOK: {filed} filed, {skipped} skipped, {failed} failed.")
    if failed:
        print("FAIL: at least one download did not validate. "
              "It was left in downloads/ and NOT filed.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
