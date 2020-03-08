class Table:
    def __init__(self):
        self.table_member = 100


class Query:
    def __init__(self, table):
        self.table = table
    
    def QueryMethod_A(self):
        print("query method A!")
        print("query Table member:", self.table.table_member)
        a_boolean = True # or False
        return a_boolean # Every query method returns a boolean

    def QueryMethod_B(self):
        print("this is from query method B!")
        a_boolean = False 
        return a_boolean # Every query method returns a boolean


class Txn:
    def __init__(self):
        pass
    
    def add_query(self, query_function):
        query_function() # definition of a query function 
        #print("Q's Table's tablemember:", query_function.table.table_member) 
        # above FAILS, not possible to get Table from QueryMethod Definition
        return


grades_table = Table()
q = Query(grades_table)

txn = Txn()
txn.add_query("QueryMethod_A", q) # Test to see if we can access table, if we just pass Query Method?
