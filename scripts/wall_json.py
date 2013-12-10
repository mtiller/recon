#!/usr/bin/env python
import sys
sys.path.append(".")

from recon.wall import WallReader

for file in sys.argv[1:]:
    with open(file, "rb") as fp:
        wall = WallReader(fp, verbose=False)
        wall.asJSON(sys.stdout)
