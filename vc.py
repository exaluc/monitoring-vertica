import vertica_python

class vc(object):
    def __init__(self, cur='dict'):
        self.co = vertica_python.connect(**self.ci)
        self.cur = self.co.cursor(cur)
        print('vertica: connection ok')
    
    def fetchall(self):
        return self.cur.fetchall()

    def query(self, query):
        self.cur.execute(query)

    def close(self):
        self.co.close()
        print('vertica: connection closed')