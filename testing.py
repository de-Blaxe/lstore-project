#lock_tut.py
from threading import Lock, Thread
lock = Lock()

g = dict()
g[94] = 0 # RID=94 initialized with zero readers/writers

def add_sub():
   global g
   lock.acquire()
   g[94] += 1
   lock.release()

   print("other stuff")

   lock.acquire()
   print("before: ", g)
   g[94] -= 1
   print("after: ", g)
   lock.release()

# At end of three threads, sum should be 0 (+1 -1 +1 -1 = 0)

threads = []

for _ in range(3):
    threads.append(Thread(target=add_sub))

for thread in threads:
    thread.start()

for thread in threads:
   """
   Waits for threads to complete before moving on with the main
   script.
   """
   thread.join()

# should be 0
print("Main thread now sees: ", g)
