# Checking enum behavior

queries = []

for i in range(3): # i = 0 ... 2
    for j in range(6): # j = 0 ... 5
        queries.append((i, j))

# Let i = query function
# Let j = arguments for that query

# queries should look like
# [(0, 0), ... (0, 5), (1,0), ... (1,5) ... (2, 0), .. (2,5)]

for count, request in enumerate(queries):
    [query, args] = request
    print("count {}, query {}, args {}".format(count, query, args), "\n")
