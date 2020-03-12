#LOG_LEVEL = 0

#BaseRID | Committed Indir RID | Pending Indir RID | Commited / Status
from template.table import Table
from template.config import *

import threading

LATEST_TID = 1

# assuming that Table init calls Logger init
class Logger():

    def __init__(self, name, table):
        self.table_name = name
        self.logged_updates = defaultdict(defaultdict(list)) 

        # by default status should be 0?
        { baseID=13: 
            {Thread A : [CommitedTID, mostRecentTID, Status], Thread B: [CommitedTID, UpdatedTID, Status]}, # defaultdict(list)

          baseID=28:
            {......}, etc for all updated baseIDs

        } # defaultdict(defaultdict(list))

    # Table will populate dictionary
    def update_log_entry(self, baseID, commitedTID, pendingTID):
        # Assuming that baseID exists (try,except..)
        curr_threadID = threading.get_ident()
        try:
            self.logged_updates[baseID][curr_threadID][0] # if list is already initialized, pass
        except KeyError: # init list
            self.logged_updates[baseID][curr_threadID] = []

        self.logged_updates[baseID][curr_threadID][LATEST_TID] = pendingTID




Main Thread inserts baseID=1 indirection=0

Thread B updates baseID=1 indirection=999999999 -> base indirection unchanged, but we create a Tail record in database, but info gets logged ->ABORTs
Thread A reads baseID=1 indirection=0 -> reads from commmited base indirection
Thread C reads baseID=1 indirection=0 -> reads from commmited base indirection
// Thread A and C unaffected by Thread B?
