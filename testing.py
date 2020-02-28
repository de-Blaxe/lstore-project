import pickle

intArray = [i for i in range(1,100)]
output = open('data.pkl', 'wb')
pickle.dump(intArray, output)
output.close()
