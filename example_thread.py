# NOTE: Just experimenting with thread behavior and R Locks
import threading

# Check if RLock is already acquired by another thread

shared_lock = threading.RLock()

def get_lock():
    if not shared_lock.acquire(blocking=False):
        print("Thread {} does not own the lock.".format(threading.get_ident()))
        try:
            shared_lock.release()
        except RuntimeError:
            print("Thread {} cannot released unacquired lock.".format(threading.get_ident()))
    else:
        print("Thread {} owns the lock. Releasing it now.".format(threading.get_ident()))
        shared_lock.release() # Owner can release it


threads = []
for _ in range(10): # Make 10 threads
    thread = threading.Thread(target=get_lock, args=())
    threads.append(thread)

for i in range(10):
    threads[i].start()

for i in range(10):
    threads[i].join()


