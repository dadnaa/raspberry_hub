"""
failure_guard.py — Sprint 6: False Positive Protection Layer

Accumulates consecutive FAILURE results and only triggers intervention
when the configured threshold is reached AND confidence is sufficient.

Also enforces a cooldown period after each intervention so the system
doesn't spam pause requests.

This is a pure decision engine — it never calls anything directly.
It returns a VisionDecision that the VisionMonitor acts upon.
"""

import logging
import time
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional

from config.settings import (
    VISION_CONFIDENCE_MIN,
    VISION_COOLDOWN_SEC,
    VISION_FAILURE_THRESHOLD,
)
from src.vision.ai_client import AIInferenceResult

logger = logging.getLogger(__name__)


class Action(Enum):
    NONE   = auto()   # keep monitoring
    PAUSE  = auto()   # call job_manager.pause()


@dataclass
class VisionDecision:
    action:          Action
    classification:  str
    confidence:      float
    consecutive_failures: int
    reason:          Optional[str] = None


class FailureGuard:
    """
    Stateful decision engine that filters AI results before triggering intervention.

    Args:
        failure_threshold  – number of consecutive FAILUREs required to act
        confidence_min     – minimum confidence on FAILURE to count it
        cooldown_sec       – seconds to wait before allowing another intervention
    """

    def __init__(
        self,
        failure_threshold: int   = VISION_FAILURE_THRESHOLD,
        confidence_min:    float = VISION_CONFIDENCE_MIN,
        cooldown_sec:      float = VISION_COOLDOWN_SEC,
    ) -> None:
        self._threshold      = failure_threshold
        self._confidence_min = confidence_min
        self._cooldown_sec   = cooldown_sec

        self._consecutive_failures: int   = 0
        self._last_intervention_ts: float = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate(self, result: AIInferenceResult) -> VisionDecision:
        """
        Feed one AI result through the guard and get a decision back.

        Returns VisionDecision(action=PAUSE) only when threshold crossed
        and cooldown elapsed.
        """
        # ── OK resets the counter ─────────────────────────────────────
        if result.is_ok:
            self._consecutive_failures = 0
            return VisionDecision(
                action=Action.NONE,
                classification="OK",
                confidence=result.confidence,
                consecutive_failures=0,
            )

        # ── UNCERTAIN counts as noise — reset counter, no action ──────
        if result.is_uncertain:
            self._consecutive_failures = 0
            return VisionDecision(
                action=Action.NONE,
                classification="UNCERTAIN",
                confidence=result.confidence,
                consecutive_failures=0,
            )

        # ── FAILURE ───────────────────────────────────────────────────
        if result.confidence >= self._confidence_min:
            self._consecutive_failures += 1
        else:
            # Low-confidence failure — treat as noise
            logger.debug(
                f"[Guard] Low-confidence FAILURE ({result.confidence:.2f}) — ignored."
            )
            return VisionDecision(
                action=Action.NONE,
                classification="FAILURE",
                confidence=result.confidence,
                consecutive_failures=self._consecutive_failures,
            )

        logger.info(
            f"[Guard] Consecutive failures: {self._consecutive_failures}/{self._threshold} "
            f"(conf={result.confidence:.2f})"
        )

        if self._consecutive_failures < self._threshold:
            return VisionDecision(
                action=Action.NONE,
                classification="FAILURE",
                confidence=result.confidence,
                consecutive_failures=self._consecutive_failures,
            )

        # ── Threshold crossed — check cooldown ────────────────────────
        now = time.monotonic()
        if now - self._last_intervention_ts < self._cooldown_sec:
            remaining = self._cooldown_sec - (now - self._last_intervention_ts)
            logger.info(f"[Guard] Threshold crossed but cooldown active ({remaining:.0f}s left).")
            return VisionDecision(
                action=Action.NONE,
                classification="FAILURE",
                confidence=result.confidence,
                consecutive_failures=self._consecutive_failures,
                reason="cooldown_active",
            )

        # ── Intervention authorised ───────────────────────────────────
        self._last_intervention_ts = now
        self._consecutive_failures = 0   # reset after acting
        logger.warning(
            f"[Guard] INTERVENTION AUTHORISED — "
            f"{self._threshold} consecutive failures at conf={result.confidence:.2f}"
        )
        return VisionDecision(
            action=Action.PAUSE,
            classification="FAILURE",
            confidence=result.confidence,
            consecutive_failures=self._threshold,
            reason=f"{self._threshold}_consecutive_failures",
        )

    def reset(self) -> None:
        """Reset state when job changes (new job, restart)."""
        self._consecutive_failures = 0
        self._last_intervention_ts = 0.0
