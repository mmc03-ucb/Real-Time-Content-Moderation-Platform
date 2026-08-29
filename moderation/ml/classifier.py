"""
The toxicity model.

Rules catch the obvious cases cheaply. Whatever survives them gets a score from
0 (fine) to 1 (clearly abusive), and the pipeline turns that score into one of
three outcomes: allow, send to a human, or delete.

Two backends ship here:

* Detoxify, a real pretrained toxicity model, used automatically when the
  optional `detoxify` package is installed.
* A word-list scorer that needs no downloads, so the project runs anywhere.

Both take a list of messages rather than one at a time, because scoring a batch
is far cheaper per message than scoring each on its own.
"""

import logging
import re
from typing import List, Optional

log = logging.getLogger(__name__)

# Words that carry most of the signal in the sample traffic.
INSULTS = {
    "idiot", "moron", "loser", "worthless", "trash", "stupid", "clown",
    "shut up", "uninstall", "nobody likes you", "go away",
}
SCAM = {"free skins", "dm me", "click now", "link in bio", "make $"}

SHOUTING = re.compile(r"[A-Z]{4,}")


class LexiconClassifier:
    """A small, explainable scorer. No model download, no GPU, always available."""

    name = "lexicon"

    def score_batch(self, texts: List[str]) -> List[float]:
        return [self._score(t) for t in texts]

    def _score(self, text: str) -> float:
        lowered = text.lower()
        score = 0.0
        score += 0.55 * sum(word in lowered for word in INSULTS)
        score += 0.45 * sum(phrase in lowered for phrase in SCAM)
        if SHOUTING.search(text):
            score += 0.1
        if len(text) > 12 and text == text.upper():
            score += 0.1
        return min(score, 1.0)


class DetoxifyClassifier:
    """Wraps the pretrained Detoxify model when it is installed."""

    name = "detoxify"

    def __init__(self):
        from detoxify import Detoxify  # imported here so it stays optional
        self._model = Detoxify("original")

    def score_batch(self, texts: List[str]) -> List[float]:
        if not texts:
            return []
        return [float(s) for s in self._model.predict(texts)["toxicity"]]


class SafeClassifier:
    """
    Keeps the pipeline alive when the model does not answer.

    If scoring fails we return no score instead of dropping messages, and the
    pipeline falls back to rules only. Losing the model should slow moderation
    down, not stop the stream.
    """

    def __init__(self, inner):
        self.inner = inner
        self.name = inner.name
        self.healthy = True
        self.failures = 0

    def score_batch(self, texts: List[str]) -> List[Optional[float]]:
        try:
            scores = self.inner.score_batch(texts)
            if not self.healthy:
                log.warning("toxicity model recovered")
            self.healthy = True
            return list(scores)
        except Exception:
            self.failures += 1
            if self.healthy:
                log.exception("toxicity model failed, falling back to rules only")
            self.healthy = False
            return [None] * len(texts)


def load_classifier(prefer_detoxify: bool = True):
    """Use the real model if it is installed, otherwise the built-in word list."""
    if prefer_detoxify:
        try:
            return SafeClassifier(DetoxifyClassifier())
        except Exception:
            log.info("detoxify not installed, using the built-in lexicon scorer")
    return SafeClassifier(LexiconClassifier())
