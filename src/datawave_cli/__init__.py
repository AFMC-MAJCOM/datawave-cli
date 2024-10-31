import os
import sys
from pathlib import Path

if os.getenv('DWV_ENV') == 'development':
    sys.path.insert(0, Path(__file__).resolve().parents[1])