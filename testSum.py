from random import randint, choice, sample, seed

keys = [1,2,3,4,5]
r = sorted(sample(range(0, len(keys)), 2))
print("r: ", r)


records = {} # dictionary with Key values with colvals

for i in range(1,6):
	records[i] = [i for _ in range(1,6)]

print("records: ", records)

for c in range(0, 5):
	for i in range(0, 20):
		r = sorted(sample(range(0, len(keys)), 2))
		colSum = sum(map(lambda key: records[key][c], keys[r[1] + 1]))
		print("colSum = ", colSum)
