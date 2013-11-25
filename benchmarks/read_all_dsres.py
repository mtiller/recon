import time
import dymat

def extract_time():
    ret = {}
    mf = dymat.DyMatFile("tests/fullRobot.mat")
    for signal in mf.names(2):
        ret[signal] = mf.data(signal)
    ret["Time"] = mf.abscissa(2)
    return ret

start = time.time()
x = extract_time()
#print str(x.keys())
end = time.time()
print "Time: "+str(end-start)
