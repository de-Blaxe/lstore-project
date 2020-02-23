from collections import defaultdict

# TODO: Account for case when primary_index != 0
num_cols = 5
primary_index = 3 # Or any randint(1, num_cols), not 0.



### EDITED VERSION ###
new_indices = []
for col in range(0, num_cols):
    if (col != primary_index):
        new_indices.append(defaultdict(list))
new_indices.insert(primary_index, dict())

# testing ...
print("len of dictionary:", len(new_indices[1]))

"""
### CURRENT VERSION ###
cur_indices = []
cur_indices.insert(primary_index, dict())
for i in range(0, num_cols):
    cur_indices.append(defaultdict(list))



# Display comparison
#print("new_indices: Edited version & cur_indices: Current version")
#print("Primary index is set to ", primary_index)
for i in range(0, num_cols):
    if i == primary_index:
        alert = "THIS ENTRY IS FOR THE PRIMARY INDEX"
    else:
        alert = " non primary"
    print("new_indices[", i, "] ", new_indices[i], "\t\t\t vs. cur_indices[", i, "] ", cur_indices[i], alert)
"""
