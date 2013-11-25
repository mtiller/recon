import sys
import time

sys.path.append(".")

from recon.meld import MeldReader

def extract_time():
    with open("test_output/dsres_robot.mld", "rb") as fp:
        meld = MeldReader(fp)

        dt = meld.read_table("T2")
        time = dt.data("Time")
        return time

start = time.time()
extract_time()
end = time.time()
print "Time: "+str(end-start)
