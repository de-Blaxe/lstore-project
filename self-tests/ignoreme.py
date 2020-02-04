# Testing behavior for query sum
prev = [1, 2, 3, 4, 5]
new = [1, 2, 3, 6, 7] # last two records were updated

check_equality = lambda prev, new: prev == new
bools = list(map(check_equality, prev, new)) # returns list of booleans

total = 0 # dummy test

for i in range(len(new)):
    rid = new[i]
    if bools[i]: # No updates made to this record
       print("I am a LID (base) \n")
    else:
       print("I am a TID (tail) \n")
    total += rid

print("SUM OF ALL RIDS: ", total, "should be 19 [1+2+3+6+7]\n")
