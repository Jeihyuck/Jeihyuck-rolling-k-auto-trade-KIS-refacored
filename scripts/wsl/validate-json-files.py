#!/usr/bin/env python3
import json, sys
from pathlib import Path
for path in Path(sys.argv[1]).rglob("*.json"):
    with path.open(encoding="utf-8") as fh: json.load(fh)
