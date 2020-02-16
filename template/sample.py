"""
mapping = []
try:
	print(mapping[4])
except:
	print("ERROR EXITTING")
else:
	print("Success!")
print("will this run?")
print("why you print")
"""
#[1,2,3,4] for 2 times

#[[1,2,3,4],[1,2,3,4]]

"""
mapping = []
for _ in range(2):
    mapping.append(list(range(5))) # list of 5 consecutive numbers

print(mapping)
"""
# Testing again

"""
md = dict()
md[1] = [2,3]
md[2] = [4,5]
md[3] = [5,6]

print("dictionary entries:", md)
[first, second] = md[1]
print(first)
print(second)
"""

s = "hello"
s[0] = 'y'
print(s)
