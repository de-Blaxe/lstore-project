import copy

mylist = [1,2,3]
dup1 = mylist.copy()
dup2 = dup1
#dup2.append(5)

dup1.append(4)

print("dup2 ", dup2)
print("dup1 ", dup1)


