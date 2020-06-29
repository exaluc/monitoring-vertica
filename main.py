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
def node_status():
    v = connection()
    try:
        r = v.go("""SELECT node_name, 
                 node_state 
                 FROM nodes 
                 ORDER BY 1;""")
    except Exception as e:
        return {"error": e}
    return {"data": r}

@app.get("/epoch/status", tags=["System Health"])
def epoch_status():
    v = connection()
    try:
        r = v.go("""SELECT current_epoch, 
                 ahm_epoch, 
                 last_good_epoch, 
                 designed_fault_tolerance, 
                 current_fault_tolerance, 
                 wos_used_bytes, 
                 ros_used_bytes FROM system;""")
    except Exception as e:
        return {"error": e}
    return {"data": r}

@app.get("/delete/vector/count", tags=["System Health"])
def delete_vector():
    v = connection()
    try:
        r = v.go("SELECT COUNT(*) FROM v_monitor.delete_vectors;")
    except Exception as e:
        return {"error": e}
    return {"data": r}

@app.get("/delete/vector", tags=["System Health"])
def delete_vector():
    v = connection()
    try:
        r = v.go("""SELECT node_name, 
                 schema_name, 
                 projection_name, 
                 total_row_count, 
                 deleted_row_count, 
                 delete_vector_count 
                 FROM storage_containers 
                 WHERE deleted_row_count > total_row_count*.05::float 
                 ORDER BY deleted_row_count desc;""")
    except Exception as e:
        return {"error": e}
    return {"data": r}

@app.get("/delete/vector/ros/containers", tags=["System Health"])
def delete_vector():
    v = connection()
    try:
        r = v.go("""SELECT node_name, 
                 projection_schema, 
                 projection_name, 
                 SUM(ros_count) AS ros_count 
                 FROM v_monitor.projection_storage 
                 GROUP BY node_name, 
                 projection_schema, 
                 projection_name 
                 ORDER BY ros_count DESC;""")
    except Exception as e:
        return {"error": e}
    return {"data": r}

@app.get("/resource/pools", tags=["Resource Usage"])
def resource_pools():
    v = connection()
    try:
        r = v.go("""SELECT sysdate AS current_time, 
                 node_name, 
                 pool_name, 
                 memory_inuse_kb, 
                 general_memory_borrowed_kb, 
                 running_query_count 
                 FROM resource_pool_status
                 WHERE pool_name IN ('general') 
                 ORDER BY 1,2,3;""")
    except Exception as e:
        return {"error": e}
    return {"data": r}

@app.get("/resource/pools/queue/status", tags=["Resource Usage"])
def resource_pool_queue_status():
    v = connection()
    try:
        r = v.go("SELECT * FROM v_monitor.resource_queues;")
    except Exception as e:
        return {"error": e}
    return {"data": r}

@app.get("/resource/request/rejections", tags=["Resource Usage"])
def resource_request_rejections():
    v = connection()
    try:
        r = v.go("SELECT * FROM v_monitor.resource_rejections;")
    except Exception as e:
        return {"error": e}
    return {"data": r}

@app.get("/resource/bottleneck", tags=["Resource Usage"])
def resource_bottleneck():
    v = connection()
    try:
        r = v.go("SELECT * FROM v_monitor.system_resource_usage ORDER BY end_time DESC;")
    except Exception as e:
        return {"error": e}
    return {"data": r}

@app.get("/storage/space", tags=["Resource Usage"])
def storage_space_availability():
    v = connection()
    try:
        r = v.go("SELECT * FROM v_monitor.storage_usage ORDER BY poll_timestamp DESC;")
    except Exception as e:
        return {"error": e}
    return {"data": r}

@app.get("/active/sessions", tags=["Active Sessions"])
def active_sessions():
    v = connection()
    try:
        r = v.go("SELECT user_name, session_id, current_statement, statement_start FROM v_monitor.sessions;")
    except Exception as e:
        return {"error": e}
    return {"data": r}

@app.get("/active/sessions/close/{session_id}", tags=["Active Sessions"])
def close_the_active_sessions(session_id: str):
    v = connection()
    try:
        r = v.go(f"SELECT close_session ('{session_id}');")
    except Exception as e:
        return {"error": e}
    return {"data": r}