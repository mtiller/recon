import time
from scipy.io import loadmat

def extract_time():
    mat = loadmat("tests/fullRobot.mat")
    T2 = mat["data_2"]
    time = T2[0,:]
    return time

start = time.time()
extract_time()
end = time.time()
print "Time: "+str(end-start)
