#!/usr/bin/env python
import sys
sys.path.append(".")

from recon.meld import MeldReader

for file in sys.argv[1:]:
    with open(file, "rb") as fp:
        meld = MeldReader(fp, verbose=False)
        meld.asJSON(sys.stdout)
