"""
Datenbank-Operationen für den Edit-War Scanner
Mit Turso (Cloud SQLite) via HTTP API
"""

import requests
from datetime import datetime
from config import TURSO_DATABASE_URL, TURSO_AUTH_TOKEN

# Konvertiere libsql:// URL zu HTTPS URL
def get_http_url():
    url = TURSO_DATABASE_URL.replace("libsql://", "https://")
    return url

def execute_sql(sql, params=None):
    """Führt eine SQL-Abfrage über die Turso HTTP API aus."""
    url = get_http_url()
    headers = {
        "Authorization": f"Bearer {TURSO_AUTH_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Turso HTTP API Format
    body = {
        "requests": [
            {"type": "execute", "stmt": {"sql": sql, "args": params or []}}
        ]
    }
    
    response = requests.post(f"{url}/v2/pipeline", headers=headers, json=body)
    
    if response.status_code != 200:
        raise Exception(f"Turso API Error: {response.status_code} - {response.text}")
    
    return response.json()


def init_database():
    """Erstellt die Tabellen, falls nicht vorhanden."""
    execute_sql('''
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            wiki_lang TEXT NOT NULL,
            url TEXT NOT NULL,
            topic TEXT,
            revision_count INTEGER,
            revert_count INTEGER,
            editor_count INTEGER,
            conflict_score INTEGER,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(title, wiki_lang)
        )
    ''')
    
    execute_sql('''
        CREATE TABLE IF NOT EXISTS scan_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_type TEXT NOT NULL,
            wiki_lang TEXT,
            articles_scanned INTEGER,
            articles_added INTEGER,
            articles_updated INTEGER DEFAULT 0,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')


def get_article(title, wiki_lang):
    """Holt einen Artikel aus der Datenbank, falls vorhanden."""
    result = execute_sql(
        'SELECT * FROM articles WHERE title = ? AND wiki_lang = ?',
        [{"type": "text", "value": title}, {"type": "text", "value": wiki_lang}]
    )
    
    try:
        rows = result["results"][0]["response"]["result"]["rows"]
        if rows:
            cols = result["results"][0]["response"]["result"]["cols"]
            col_names = [c["name"] for c in cols]
            row_values = [cell["value"] if cell["type"] != "null" else None for cell in rows[0]]
            return dict(zip(col_names, row_values))
    except (KeyError, IndexError):
        pass
    return None


def article_needs_update(existing, new_data):
    """Prüft, ob ein Artikel aktualisiert werden muss."""
    if existing is None:
        return True
    
    return (
        int(existing['revision_count'] or 0) != new_data['revision_count'] or
        int(existing['revert_count'] or 0) != new_data['revert_count'] or
        int(existing['editor_count'] or 0) != new_data['editor_count']
    )


def add_or_update_article(article_data):
    """Fügt einen Artikel hinzu oder aktualisiert ihn."""
    existing = get_article(article_data['title'], article_data['wiki_lang'])
    
    if not article_needs_update(existing, article_data):
        return 'unchanged'
    
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    
    if existing is None:
        execute_sql('''
            INSERT INTO articles 
            (title, wiki_lang, url, topic, revision_count, revert_count, 
             editor_count, conflict_score, first_seen, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', [
            {"type": "text", "value": article_data['title']},
            {"type": "text", "value": article_data['wiki_lang']},
            {"type": "text", "value": article_data['url']},
            {"type": "text", "value": article_data.get('topic', '')},
            {"type": "integer", "value": str(article_data['revision_count'])},
            {"type": "integer", "value": str(article_data['revert_count'])},
            {"type": "integer", "value": str(article_data['editor_count'])},
            {"type": "integer", "value": str(article_data['conflict_score'])},
            {"type": "text", "value": now},
            {"type": "text", "value": now}
        ])
        return 'added'
    else:
        execute_sql('''
            UPDATE articles 
            SET url = ?,
                topic = ?,
                revision_count = ?,
                revert_count = ?,
                editor_count = ?,
                conflict_score = ?,
                last_updated = ?
            WHERE title = ? AND wiki_lang = ?
        ''', [
            {"type": "text", "value": article_data['url']},
            {"type": "text", "value": article_data.get('topic', '')},
            {"type": "integer", "value": str(article_data['revision_count'])},
            {"type": "integer", "value": str(article_data['revert_count'])},
            {"type": "integer", "value": str(article_data['editor_count'])},
            {"type": "integer", "value": str(article_data['conflict_score'])},
            {"type": "text", "value": now},
            {"type": "text", "value": article_data['title']},
            {"type": "text", "value": article_data['wiki_lang']}
        ])
        return 'updated'


def get_all_articles():
    """Gibt alle Artikel aus der Datenbank zurück."""
    result = execute_sql('''
        SELECT * FROM articles 
        ORDER BY conflict_score DESC, last_updated DESC
    ''')
    
    try:
        response = result["results"][0]["response"]["result"]
        cols = [c["name"] for c in response["cols"]]
        articles = []
        for row in response["rows"]:
            values = [cell["value"] if cell["type"] != "null" else None for cell in row]
            articles.append(dict(zip(cols, values)))
        return articles
    except (KeyError, IndexError):
        return []


def log_scan(scan_type, wiki_lang, articles_scanned, articles_added, articles_updated=0):
    """Protokolliert einen Scan-Durchlauf."""
    execute_sql('''
        INSERT INTO scan_log (scan_type, wiki_lang, articles_scanned, articles_added, articles_updated)
        VALUES (?, ?, ?, ?, ?)
    ''', [
        {"type": "text", "value": scan_type},
        {"type": "text", "value": wiki_lang},
        {"type": "integer", "value": str(articles_scanned)},
        {"type": "integer", "value": str(articles_added)},
        {"type": "integer", "value": str(articles_updated)}
    ])


def get_scan_history(limit=20):
    """Gibt die letzten Scan-Durchläufe zurück."""
    result = execute_sql(f'''
        SELECT * FROM scan_log 
        ORDER BY timestamp DESC 
        LIMIT {limit}
    ''')
    
    try:
        response = result["results"][0]["response"]["result"]
        cols = [c["name"] for c in response["cols"]]
        history = []
        for row in response["rows"]:
            values = [cell["value"] if cell["type"] != "null" else None for cell in row]
            history.append(dict(zip(cols, values)))
        return history
    except (KeyError, IndexError):
        return []


def delete_article(article_id):
    """Löscht einen Artikel aus der Datenbank."""
    execute_sql('DELETE FROM articles WHERE id = ?', 
                [{"type": "integer", "value": str(article_id)}])
```

---

**Und ändere `requirements.txt`** – wir brauchen `libsql-client` nicht mehr:
```
flask==3.0.0
requests==2.31.0
gunicorn==21.2.0
