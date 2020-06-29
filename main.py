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

@app.get("/", tags=["index"])
def read_root():
    return {"Hello": "World"}

@app.get("/query/{content}", tags=["query"])
def custom_query(content: str):
    v = connection()
    try:
        r = v.go(content)
    except Exception as e:
        return {"error": e}
    return {"data": r}

@app.get("/node/status", tags=["System Health"])
def monitor_the_node_status():
    v = connection()
    try:
        r = v.go("SELECT node_name, node_state FROM nodes ORDER BY 1;")
    except Exception as e:
        return {"error": e}
    return {"data": r}

@app.get("/epoch/status", tags=["System Health"])
def monitor_the_epoch_status():
    v = connection()
    try:
        r = v.go("SELECT current_epoch, ahm_epoch, last_good_epoch, designed_fault_tolerance, current_fault_tolerance, wos_used_bytes, ros_used_bytes FROM system;")
    except Exception as e:
        return {"error": e}
    return {"data": r}