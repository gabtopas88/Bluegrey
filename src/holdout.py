"""
src/holdout.py
Terminal Holdout Vault — the last line of defense against researcher overfitting.

The problem this solves: CPCV destroys *parameter* overfitting, but nothing in
the stack protects against *researcher* overfitting — the slow leak that happens
when every notebook, every grid search and every "quick look" can see the full
history. With ML strategies the degrees of freedom (features x labels x
hyperparameters) explode, and the only defense that survives contact with a
P5_Sharpe you want to believe is a fence enforced in code, not discipline.

Mechanics (mirrors the S3 study charter, §7.2):
  1. register() ONCE per study, at pre-registration: writes a small JSON
     manifest pinning the cutoff date. Registration is write-once — a second
     register() raises instead of silently moving the fence.
  2. enforce() on every research DataFrame: returns only rows strictly BEFORE
     the cutoff. CPCVOptimizer calls this automatically when constructed with
     a vault, so the optimizer can never see, slice, or score the holdout.
  3. open_holdout() EXACTLY ONCE: returns the fenced slice and permanently
     marks the manifest as opened. A second call raises. Re-opening requires
     manually editing the manifest on disk — and that edit is, by definition,
     an amendment to the study's pre-registration.

The manifest lives on disk (DATA_DIR/holdouts/) rather than in memory so the
fence survives kernel restarts, new notebooks and months of research sessions —
exactly the situations where discipline alone fails.
"""
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.config import DATA_DIR

logger = logging.getLogger("HoldoutVault")

HOLDOUT_DIR = Path(DATA_DIR) / "holdouts"

# Same identifier discipline as telemetry run_ids: filesystem-safe, no surprises.
_STUDY_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")


class HoldoutVault:
    """
    Write-once registry for a study's terminal holdout boundary.

    One vault = one study. The study_id keys the on-disk manifest, so two
    notebooks (or two months of research sessions) referencing the same
    study_id share the same fence automatically.
    """

    def __init__(self, study_id: str):
        if not study_id or not _STUDY_ID_PATTERN.match(study_id):
            raise ValueError(
                f"Invalid study_id '{study_id}'. "
                f"Allowed: ASCII letters, digits, dot, underscore, hyphen."
            )
        self.study_id = study_id
        self.manifest_path = HOLDOUT_DIR / f"{study_id}.json"

    # ==========================================
    # 📄 MANIFEST I/O
    # ==========================================
    def _load(self) -> dict:
        if not self.manifest_path.exists():
            raise FileNotFoundError(
                f"❌ No holdout registered for study '{self.study_id}'. "
                f"Call HoldoutVault('{self.study_id}').register(cutoff=...) ONCE, "
                f"at study pre-registration, before any research runs."
            )
        with open(self.manifest_path, "r") as f:
            return json.load(f)

    def _save(self, manifest: dict):
        HOLDOUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(self.manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

    # ==========================================
    # 🔒 1. REGISTRATION (write-once)
    # ==========================================
    def register(self, cutoff, note: str = "") -> dict:
        """
        Pin the holdout boundary. Everything at or after `cutoff` is fenced.

        Write-once by design: if a manifest already exists this raises rather
        than moving the fence — moving the fence after seeing results is
        precisely the failure mode this module exists to prevent.

        :param cutoff: anything pd.Timestamp can parse (e.g. '2025-08-01').
        :param note: free-text provenance (which charter, which data end date).
        """
        if self.manifest_path.exists():
            existing = self._load()
            raise FileExistsError(
                f"❌ Holdout for study '{self.study_id}' is already registered "
                f"(cutoff={existing['cutoff']}, created={existing['created_utc']}). "
                f"The boundary is write-once. If it is genuinely wrong, delete the "
                f"manifest manually and log the change in the study's Amendments "
                f"section — that deletion IS an amendment."
            )

        cutoff_ts = pd.Timestamp(cutoff)
        if pd.isna(cutoff_ts):
            raise ValueError(f"Unparseable cutoff: {cutoff!r}")

        manifest = {
            "study_id": self.study_id,
            "cutoff": cutoff_ts.isoformat(),
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "opened": False,
            "opened_utc": None,
            "note": note,
        }
        self._save(manifest)
        logger.warning(
            f"🔒 Holdout REGISTERED for study '{self.study_id}': "
            f"all data >= {cutoff_ts} is fenced until open_holdout()."
        )
        return manifest

    @property
    def cutoff(self) -> pd.Timestamp:
        return pd.Timestamp(self._load()["cutoff"])

    @property
    def is_opened(self) -> bool:
        return bool(self._load().get("opened", False))

    def describe(self) -> dict:
        """Manifest contents, for run metadata / provenance logging."""
        return self._load()

    # ==========================================
    # 🕰️ TZ ALIGNMENT
    # ==========================================
    # The ETF daily library will be tz-naive while the FX minute libraries are
    # UTC-aware. The fence must work against both without the caller caring.
    @staticmethod
    def _align_cutoff(index: pd.DatetimeIndex, cutoff: pd.Timestamp) -> pd.Timestamp:
        if index.tz is not None and cutoff.tz is None:
            return cutoff.tz_localize(index.tz)
        if index.tz is None and cutoff.tz is not None:
            return cutoff.tz_convert("UTC").tz_localize(None)
        return cutoff

    # ==========================================
    # 🛡️ 2. ENFORCEMENT (every research DataFrame)
    # ==========================================
    def enforce(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Return only the rows strictly BEFORE the registered cutoff.

        Deliberately a pure function of (df, manifest): it can be called on
        price matrices, feature matrices and label frames alike, and calling
        it twice is harmless. Idempotence matters — the fence must be safe to
        apply at every layer without coordination.
        """
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("HoldoutVault.enforce requires a DatetimeIndex.")

        cutoff = self._align_cutoff(df.index, self.cutoff)
        fenced = df[df.index < cutoff]
        removed = len(df) - len(fenced)

        if fenced.empty:
            raise ValueError(
                f"❌ Holdout cutoff {cutoff} precedes the entire dataset — "
                f"nothing left to research on. Check the registered date."
            )

        if removed == 0:
            logger.info(
                f"🔒 Holdout '{self.study_id}': 0 rows removed "
                f"(data already ends before {cutoff})."
            )
        else:
            logger.warning(
                f"🔒 Holdout '{self.study_id}': fenced {removed} rows "
                f">= {cutoff} ({len(fenced)} rows remain for research)."
            )
        return fenced

    # ==========================================
    # 🔓 3. THE ONE-TIME OPEN
    # ==========================================
    def open_holdout(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Return the fenced slice (rows >= cutoff) and permanently mark the
        holdout as opened.

        Second calls raise. The K5 verdict is computed on what this returns,
        once, on the final production candidate — pass or fail, the result is
        recorded and there is no second attempt with a "fixed" model.
        """
        manifest = self._load()
        if manifest.get("opened"):
            raise RuntimeError(
                f"❌ Holdout for study '{self.study_id}' was already opened at "
                f"{manifest['opened_utc']}. There is no second open. If this is "
                f"genuinely a new, independently pre-registered study, register "
                f"it under a NEW study_id."
            )

        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("HoldoutVault.open_holdout requires a DatetimeIndex.")

        cutoff = self._align_cutoff(df.index, pd.Timestamp(manifest["cutoff"]))
        holdout_slice = df[df.index >= cutoff]

        # Mark opened BEFORE returning — if the caller crashes mid-evaluation,
        # the open still counts. Fail-closed, never fail-open.
        manifest["opened"] = True
        manifest["opened_utc"] = datetime.now(timezone.utc).isoformat()
        self._save(manifest)

        logger.critical(
            f"🔓 HOLDOUT OPENED for study '{self.study_id}' "
            f"({len(holdout_slice)} rows >= {cutoff}). This was the one open. "
            f"The K5 verdict on this slice is final."
        )
        if holdout_slice.empty:
            logger.error(
                "⚠️ Holdout slice is EMPTY — the loaded data does not reach "
                "the cutoff. The open has still been consumed."
            )
        return holdout_slice