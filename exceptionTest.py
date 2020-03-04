# Testing exception handling 
# (TODO: test with threads)

"""
# Pretend tableMethod is read_records or insert_tail_record
"""
def tableMethod():
    must_abort = True # True: Exclusive Lock in use. Else, False.
    if must_abort:
        raise Exception # Automatically returns
    print("This will not get printed if must_abort is True")
    return

"""
# Pretend this is a Transaction thread
"""
def transactionThread():
    try:
        tableMethod()
    except Exception:
        abort()
    else:
        commit()
    return

"""
# Abort and commit are defined in Transaction Class only
"""
def abort():
    print("Aborting!")
    return

def commit():
    print("Successful Transaction!")
    return

transactionThread()
