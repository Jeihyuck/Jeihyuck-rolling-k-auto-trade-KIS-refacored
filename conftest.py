# conftest.py — 루트 conftest: sys.path에 workspace root 추가
import sys
import os

# 프로젝트 루트를 sys.path에 추가하여 'trader' 패키지를 import 가능하게 한다
sys.path.insert(0, os.path.dirname(__file__))
