"""
NOTES 
Concurrent Reads + Writes
Actual code due later, but still need to figure out logic (for Presentation)
"""
 
# Task 1
- Updating commmitted Base Records Indirection (ptr to latest TID)
- Need to account for TIDs made by an ABORTED Thread/Transaction
- So far, we are marking TIDs made as invalid (storing them into Tables invalid_rids list)
- Problem: Corresponding (& committed) Base Records Indirection may point to an invalid TID!
- Idea (?):
  * Have Log per Table
  * Log 3 Entries: BaseRID | Speculative Indirection | Committed Indirection
  * OR... Add metadata column for Speculative Indirection
    -> All queries read from Committed Indirection
    -> Once Transaction commits (i.e., all queries succeed), 'update' Log somehow?
    -> But... Transaction cannot access a Table instance?

# Other Considerations (but I don't think we have to actually do, given tester)
- Reverting changes made in Indexer Class / dictionary
    * Also, user / given tester does not call create_index() explicitly
    * So, Indexer only manages dictionary for primary keys, which are never updated in tester
        --> since Tester calls query.Increment() only at col index=1 (non primary col)
    * Probably don't have to consider non primary indexing
