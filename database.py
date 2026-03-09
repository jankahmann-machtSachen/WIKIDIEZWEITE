"""
Datenbank-Operationen für den Edit-War Scanner
Mit Turso (Cloud SQLite) via HTTP API
"""

import requests
from datetime import datetime
from config import TURSO_DATABASE_URL, TURSO_AUTH_TOKEN


def get_http_url():
    url = TURSO_DATABASE_URL.replace("libsql://", "https://")
    return url


def execute_sql(sql, params=None):
    url = get_http_url()
    headers = {
        "Authorization": f"Bearer {TURSO_AUTH_TOKEN}",
        "Content-Type": "application/json"
    }
    
    body = {
        "requests": [
            {"type": "execute", "stmt": {"sql": sql, "args": params or []}}
        ]
    }
    
    response = requests.post(f"{url}/v2/pipeline", headers=headers, json=body)
    
    if response.status_code != 200:
        raise Exception(f"Turso API Error: {response.status_code} - {response.text}")
    
    r
