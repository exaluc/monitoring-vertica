from fastapi import FastAPI
from vc import vc
import json
from fastapi.openapi.utils import get_openapi
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)

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

    def custom(self, query, commit):
        q = f'{query}'
        self.query(q)
        r = self.fetchall()
        if commit:
            self.commit()
        self.close()
        return r


app = FastAPI(title="Monitoring Vertica", docs_url=None, redoc_url=None)


@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + " - Swagger UI",
        oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
        swagger_js_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@3/swagger-ui-bundle.js",
        swagger_css_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@3/swagger-ui.css",
        swagger_favicon_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@3/favicon-32x32.png",
    )


@app.get(app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
async def swagger_ui_redirect():
    return get_swagger_ui_oauth2_redirect_html()


@app.get("/redoc", include_in_schema=False)
async def redoc_html():
    return get_redoc_html(
        openapi_url=app.openapi_url,
        title=app.title + " - ReDoc",
        redoc_js_url="https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js",
        redoc_favicon_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@3/favicon-32x32.png",
    )


@app.get("/", tags=["index"])
def read_root():
    return {"Hello": "World"}


@app.get("/query/{content}", tags=["query"])
def custom_query(content: str, commit: bool = False):
    v = connection()
    try:
        r = v.custom(content, commit)
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
def gather_the_total_count_of_delete_vectors_for_the_system():
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
def view_the_number_of_ROS_containers_per_projection_per_node():
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


@app.get("/query/excessive/{memory}", tags=["Resource Usage"])
def monitor_if_a_query_is_taking_excessive_memory_resource_and_causing_the_cluster_to_slow_down(memory: str):
    v = connection()
    try:
        r = v.go(
            f"SELECT * FROM resource_acquisitions ORDER BY memory_inuse_kb desc limit {memory};")
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
        r = v.go(
            "SELECT * FROM v_monitor.system_resource_usage ORDER BY end_time DESC;")
    except Exception as e:
        return {"error": e}
    return {"data": r}


@app.get("/storage/space", tags=["Resource Usage"])
def storage_space_availability():
    v = connection()
    try:
        r = v.go(
            "SELECT * FROM v_monitor.storage_usage ORDER BY poll_timestamp DESC;")
    except Exception as e:
        return {"error": e}
    return {"data": r}


@app.get("/active/sessions", tags=["Active Sessions"])
def active_sessions():
    v = connection()
    try:
        r = v.go(
            "SELECT user_name, session_id, current_statement, statement_start FROM v_monitor.sessions;")
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


@app.get("/running/queries", tags=["Active Queries"])
def get_a_list_of_queries_executing_at_the_moment():
    v = connection()
    try:
        r = v.go("""SELECT node_name, 
                 query, 
                 query_start, 
                 user_name, 
                 is_executing 
                 FROM v_monitor.query_profiles 
                 WHERE is_executing = 't';""")
    except Exception as e:
        return {"error": e}
    return {"data": r}


@app.get("/load/status", tags=["Active Queries"])
def check_the_loading_progress_of_active_and_historical_queries():
    v = connection()
    try:
        r = v.go("""SELECT table_name, 
                 read_bytes, 
                 input_file_size_bytes, 
                 accepted_row_count, 
                 rejected_row_count, 
                 parse_complete_percent, 
                 sort_complete_percent 
                 FROM load_streams 
                 WHERE is_executing = 't' 
                 ORDER BY table_name;""")
    except Exception as e:
        return {"error": e}
    return {"data": r}


@app.get("/lock/status", tags=["Active Queries"])
def a_query_with_no_results_indicates_that_no_locks_are_in_use():
    v = connection()
    try:
        r = v.go("""SELECT locks.lock_mode, 
                 locks.lock_scope, 
                 substr(locks.transaction_description, 1, 100) AS "left", 
                 locks.request_timestamp, 
                 locks.grant_timestamp 
                 FROM v_monitor.locks;""")
    except Exception as e:
        return {"error": e}
    return {"data": r}


@app.get("/recovery/status", tags=["Recovery"])
def node_recovery_status():
    v = connection()
    try:
        r = v.go("""SELECT node_name, 
                 recover_epoch, 
                 recovery_phase, 
                 current_completed, 
                 current_total, 
                 is_running 
                 FROM v_monitor.recovery_status 
                 ORDER BY 1;""")
    except Exception as e:
        return {"error": e}
    return {"data": r}


@app.get("/rebalance/status", tags=["Rebalance"])
def rebalance_status():
    v = connection()
    try:
        r = v.go("SELECT GET_NODE_DEPENDENCIES();")
    except Exception as e:
        return {"error": e}
    return {"data": r}


@app.get("/overall/progress/rebalance/operation", tags=["Rebalance"])
def progress_of_each_currently_executing_rebalance_operation():
    v = connection()
    try:
        r = v.go("""SELECT rebalance_method 
                 Rebalance_method, 
                 Status, 
                 COUNT(*) AS Count
                 FROM 
                 ( SELECT rebalance_method, 
                 CASE WHEN (separated_percent = 100 AND transferred_percent = 100) 
                 THEN 'Completed' 
                 WHEN ( separated_percent <>  0 and separated_percent <> 100) 
                 OR (transferred_percent <> 0 AND transferred_percent <> 100) 
                 THEN 'In Progress' 
                 ELSE 'Queued' 
                 END AS  Status 
                 FROM v_monitor.rebalance_projection_status 
                 WHERE is_latest)
                 AS tab 
                 GROUP BY 1, 2 
                 ORDER BY 1, 2;""")
    except Exception as e:
        return {"error": e}
    return {"data": r}


@app.get("/execution/time/{limit}", tags=["Historical Activities"])
def queries_based_on_execution_time(limit: int):
    v = connection()
    try:
        r = v.go(f"""SELECT user_name, 
                 start_timestamp, 
                 request_duration_ms, 
                 transaction_id, 
                 statement_id, 
                 substr(request, 0, 1000) as request 
                 FROM v_monitor.query_requests 
                 WHERE transaction_id > 0 
                 ORDER BY request_duration_ms 
                 DESC limit {limit};""")
    except Exception as e:
        return {"error": e}
    return {"data": r}


@app.get("/memory/usage", tags=["Historical Activities"])
def memory_usage_for_a_particular_query():
    v = connection()
    try:
        r = v.go("""SELECT node_name, 
                 transaction_id, 
                 statement_id, 
                 user_name, 
                 start_timestamp, 
                 request_duration_ms, 
                 memory_acquired_mb, 
                 substr(request, 1, 100) AS request 
                 FROM v_monitor.query_requests 
                 WHERE transaction_id = transaction_id 
                 AND statement_id = statement_id;""")
    except Exception as e:
        return {"error": e}
    return {"data": r}


@app.get("/partitions", tags=["Object Statistics"])
def view_the_partition_count_per_node_per_projection():
    v = connection()
    try:
        r = v.go("""SELECT node_name, 
                 projection_name, 
                 count(partition_key) 
                 FROM v_monitor.partitions 
                 GROUP BY node_name, 
                 projection_name 
                 ORDER BY node_name, 
                 projection_name;""")
    except Exception as e:
        return {"error": e}
    return {"data": r}


@app.get("/segmentation/data/skew", tags=["Object Statistics"])
def view_the_row_count_per_segmented_projection_per_node():
    v = connection()
    try:
        r = v.go("""SELECT ps.node_name, 
                 ps.projection_schema, 
                 ps.projection_name, 
                 ps.row_count 
                 FROM v_monitor.projection_storage ps
                 INNER JOIN v_catalog.projections p 
                 ON ps.projection_schema = p.projection_schema 
                 AND ps.projection_name = p.projection_name 
                 WHERE p.is_segmented 
                 ORDER BY ps.projection_schema, 
                 ps.projection_name, 
                 ps.node_name;""")
    except Exception as e:
        return {"error": e}
    return {"data": r}


@app.get("/load/streams", tags=["Performance"])
def view_the_performance_of_load_streams():
    v = connection()
    try:
        r = v.go("""SELECT schema_name, 
                 table_name, 
                 load_start, 
                 load_duration_ms, 
                 is_executing, 
                 parse_complete_percent, 
                 sort_complete_percent, 
                 accepted_row_count, 
                 rejected_row_count 
                 FROM v_monitor.load_streams;""")
    except Exception as e:
        return {"error": e}
    return {"data": r}


def custom_openapi(openapi_prefix: str):
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Monitoring Vertica",
        version="0.0.1",
        description="Vertica api <br><br> Project launched for test the <a href='https://fastapi.tiangolo.com/' target='_blank'>FastAPI</a> <br><br> Based on: <a href='https://www.vertica.com/kb/Best-Practices-for-Monitoring-Vertica/Content/BestPractices/BestPracticesforMonitoringVertica.htm' target='_blank'>Best Practices for Monitoring Vertica</a>",
        routes=app.routes,
        openapi_prefix=openapi_prefix,
    )
    openapi_schema["info"]["x-logo"] = {
        "url": "https://upload.wikimedia.org/wikipedia/commons/thumb/0/03/Vertica_pos_blk_rgb.svg/300px-Vertica_pos_blk_rgb.svg.png"
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi
