#!/usr/bin/env python3
"""The tool's first name, kept so existing scripts keep working.

The tool is now `run_validation.py`, and its `certify` command is now
`validate`. Both old names still work.
"""

import sys

from run_validation import main

if __name__ == "__main__":
    sys.exit(main())
