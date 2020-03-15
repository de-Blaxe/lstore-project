# RECORD: rid, key value, and columns (user data, an array)

# Inside Table.Insert_Tail_Record(self, record, schema_encoding):

1. Given Record, get its key
    a. Determine its baseID --> Indexer.locate(record.key)
2. With baseID...
    a. Check if Record with baseID has ZERO outstanding readers
    ---> self.lock_manager.shared_locks[mapped baseID] = # of Readers 
    ---> Abort if # Readers > 0
        # Rollback:
        invalidate the tids made --> self.invalid_rids += tids_made by current Thread// lazily
        restore base records indirection
            * copy get_shared_lock() logic
        return False
    ---> Else: Readers == 0
            --> Grab exclusive Lock for Record: 
            # Check if we need to init the exclusive lock for mapped baseID:
            try:
                self.lock_manager.exclusive_locks[mapped baseID]
                pass
                # Exists so acquire it
            except KeyError:
                # must init dictionary entry with lock
                self.lock_manager.exclusive_locks[mapped baseID] = threading.RLock()

            self.lock_manager.exclusive_locks[mapped baseID].acquire()

            # Get Lock Managers Latch
            latch.acquire()

            self.lock_manager.shared_locks[mapped baseID] = EXCLUSIVE_LOCK = -1
            # Get current thread's ID
            curr_threadID = threading.get_ident()
            try:
                self.lock_manager.threadID_to_tids[curr_threadID][mapped baseID]
                pass # no need to init for mapped baseID
            except KeyError:
                # INIT to a dict(defaultdict(list))
                self.lock_manager.threadID_to_tids[curr_threadID] = defaultdict(list)
            # by default, insert Record.rid to dictionary
            self.lock_manager.threadID_to_tids[curr_threadID][mapped baseID].append(Record.rid)

            latch.release()

            # Find Tail Page Set Name
            self.memory_manager.exclusive_locks[Tail_PageSetName].acquire()
            # Critical Section: Write TAIL RECORD info as usual
            # self.unpinPages(Tail_PageSetName)
            # self.setDirty(Tail...)
            self.memory_manager.exclusive_locks[Tail_PageSetName].release()
    
            # Find Base Page Set Name
            self.memorymanager.exclusive_locks[Base_PageSetName].acquire()
            # CS: MODIFY BASE RECORDs indirection & schema
            # self.unpinPages(Base_PageSetName)
            # self.setDirty(Base...)
            self.memorymanager.exclusive_locks[Base_PageSetName].release()
            
            # self.unpinPages(Tail_PageSetName + Base_PageSetName)
            self.lock_manager.exclusive_locks[mapped baseID].release()
