from collections import defaultdict

num_cols = 5
primary_index = 0

# init indices
indices = []
for col in range(0, num_cols):
    if (col != primary_index):
        indices.append(defaultdict(list))
indices.insert(primary_index, dict())

# want to update a dictionary at col_num
col_num = 1
indices.update({
