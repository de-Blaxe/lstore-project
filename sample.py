mylist = [] # base set
sublist = [] # base row

"""
for _ in range(10):
    sublist.append([])


print("sublist: ", sublist)
"""
for _ in range(4):
    mylist.append(sublist)

print("list: ", mylist)
print("len of a empty entry: ", len(mylist[3][0]))
