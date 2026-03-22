#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Compatibility launcher for tools that avoid dots in script names."""

from __future__ import annotations

from pathlib import Path
import runpy


if __name__ == "__main__":
    target = Path(__file__).with_name("geo_keyword_analyzer_v8.0.py")
    runpy.run_path(str(target), run_name="__main__")
