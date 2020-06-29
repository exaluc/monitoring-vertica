from fastapi import FastAPI
import json

with open('config.json') as jf:
        d = json.load(jf)
        vh = d['vertica']['host']
        vpo = d['vertica']['port']
        vu = d['vertica']['user']
        vp = d['vertica']['password']
        vd = d['vertica']['database']

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}

print(vh, vpo, vu, vp, vd)