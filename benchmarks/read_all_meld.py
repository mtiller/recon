import sys
import time

sys.path.append(".")

from recon.meld import MeldReader

def extract_time():
    ret = {}
    with open("test_output/dsres_robot.mld", "rb") as fp:
        meld = MeldReader(fp)

        dt = meld.read_table("T2")
        for signal in dt.signals():
            ret[signal] = dt.data(signal)
    return ret

start = time.time()
x = extract_time()
# print str(x)
end = time.time()
print "Time: "+str(end-start)
