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

mapping = []
for i in range(2):
    mapping.append(list(range(5))) # list of 5 consecutive numbers

print(mapping)
