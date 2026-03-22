"""Single-pass keyword matcher with Aho-Corasick fallback."""

from __future__ import annotations

import re
from collections import Counter
from typing import Dict, Iterable, List

try:
    import ahocorasick  # type: ignore
except ImportError:  # pragma: no cover
    ahocorasick = None


class KeywordMatcher:
    def __init__(self, terms: Iterable[str], ignore_case: bool = True):
        unique_terms: List[str] = []
        seen = set()
        for raw in terms:
            term = str(raw).strip()
            if not term:
                continue
            key = term.lower() if ignore_case else term
            if key in seen:
                continue
            seen.add(key)
            unique_terms.append(term)

        self.ignore_case = ignore_case
        self.terms = unique_terms
        self._term_map = {(term.lower() if ignore_case else term): term for term in self.terms}
        self._automaton = None
        self._regex = None

        if not self.terms:
            return

        if ahocorasick is not None:
            automaton = ahocorasick.Automaton()
            for term in self.terms:
                key = term.lower() if ignore_case else term
                automaton.add_word(key, self._term_map[key])
            automaton.make_automaton()
            self._automaton = automaton
            return

        escaped = [re.escape(term) for term in sorted(self.terms, key=len, reverse=True)]
        pattern = "|".join(escaped)
        flags = re.IGNORECASE if ignore_case else 0
        self._regex = re.compile(pattern, flags)

    def count(self, text: str) -> Dict[str, int]:
        if not text or not self.terms:
            return {}

        counts = Counter()
        if self._automaton is not None:
            haystack = text.lower() if self.ignore_case else text
            for _, canonical in self._automaton.iter(haystack):
                counts[canonical] += 1
            return dict(counts)

        if self._regex is None:
            return {}

        for match in self._regex.finditer(text):
            value = match.group(0)
            key = value.lower() if self.ignore_case else value
            canonical = self._term_map.get(key)
            if canonical:
                counts[canonical] += 1
        return dict(counts)
