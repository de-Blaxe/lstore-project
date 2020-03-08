"""
from collections import defaultdict

threadID_tid_map = defaultdict(list) # returns dictionary object
print("init map:", threadID_tid_map)

cur_threadID = 9
#try:
if len(threadID_tid_map[cur_threadID]) == 0: # no key error if DNE bc of defaultdict behavior
    print("nothing for cur threadID")
else:
    print("something stored!")

#except:
#    print("new entry")
#    isEmptyFlag = -1
#    threadID_tid_map[cur_threadID].append(isEmptyFlag)
"""

mydict = dict()


mydict[1] = 100
print("before " ,mydict)

mydict[1] -= 1
print("after", mydict)
