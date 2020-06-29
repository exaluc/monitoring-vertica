from fastapi import FastAPI
from vc import vc
import json

with open('config.json') as jf:
        d = json.load(jf)
        vh = d['vertica']['host']
        vpo = d['vertica']['port']
        vu = d['vertica']['user']
        vp = d['vertica']['password']
        vd = d['vertica']['database']

class connection(vc):
    ci = {'host': vh,
             'port': vpo,
             'user': vu,
             'password': vp,
             'database': vd,
             'read_timeout': 100}
    def go(self, query):
        q = f'{query}'
        self.query(q)
        r = self.fetchall()
        self.close()
        return r
        

app = FastAPI(title="Monitoring Vertica")

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/query/{content}")
def custom_query(content: str):
    v = connection()
    try:
        r = v.go(content)
    except Exception as e:
        return {"error": e}
    return {"data": r}