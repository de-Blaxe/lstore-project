mylist = [1,2,3,4]

def myfunct(alist):
    mylist[0] = 100000
    print("myfunction:", alist)

myfunct(mylist)

print("original pointer: " , mylist)
