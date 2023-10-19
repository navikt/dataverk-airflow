import requests
import json
import time

print("python script test")

with open("/workspace/dags/notebooks/myfile.json") as f:
    data = f.read()

print("fil:", data)
