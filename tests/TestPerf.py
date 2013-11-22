# These tests are here just to give me something to profile

from recon.trans import dsres2meld
from recon.meld import MeldReader
from nose.tools import *
import os

def testDsres2Meld_Robot():
    with open(os.path.join("test_output","dsres_robot.mld"), "w+") as fp:
        dsres2meld("tests/fullRobot.mat", fp, verbose=False, compression=False)

    with open(os.path.join("test_output","dsres.mld"), "rb") as fp:
        meld = MeldReader(fp, verbose=False)
        print str(meld.report())
