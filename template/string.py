pagesetname = "123321/Grades"
for i, val in enumerate(pagesetname):
    if val == '/':
        print(pagesetname[i+1:])

result = pagesetname.split('/')
print("numbers:", result[0])
print("table name", result[1])

