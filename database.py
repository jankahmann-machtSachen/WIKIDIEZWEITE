"""
Datenbank-Operationen für den Edit-War Scanner
Mit Turso (Cloud SQLite) und Update-Logik
"""

import libsql_client
from datetime import datetime
from config import TURSO_DATABASE_URL, TURSO_AUTH_TOKEN


def get_connection():
    """Erstellt eine Verbindung zur Turso-Datenbank."""
    return libsql_client.create_client_sync(
        url=TURSO_DATABASE_URL,
        auth_token=TURSO_AUTH_TOKEN
    )


def init_database():
    """Erstellt die Tabellen, falls nicht vorhanden."""
    client = get_connection()
    
    client.execute('''
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
    
    client.execute('''
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
    
    client.close()


def get_article(title, wiki_lang):
    """Holt einen Artikel aus der Datenbank, falls vorhanden."""
    client = get_connection()
    
    result = client.execute(
        'SELECT * FROM articles WHERE title = ? AND wiki_lang = ?',
        [title, wiki_lang]
    )
    
    client.close()
    
    if result.rows:
        columns = ['id', 'title', 'wiki_lang', 'url', 'topic', 
                   'revision_count', 'revert_count', 'editor_count',
                   'conflict_score', 'first_seen', 'last_updated']
        return dict(zip(columns, result.rows[0]))
    return None


def article_needs_update(existing, new_data):
    """
    Prüft, ob ein Artikel aktualisiert werden muss.
    Update wenn: Edits ODER Reverts ODER Editoren sich geändert haben.
    """
    if existing is None:
        return True
    
    return (
        existing['revision_count'] != new_data['revision_count'] or
        existing['revert_count'] != new_data['revert_count'] or
        existing['editor_count'] != new_data['editor_count']
    )


def add_or_update_article(article_data):
    """
    Fügt einen Artikel hinzu oder aktualisiert ihn.
    
    Returns:
        'added' - Neuer Artikel wurde hinzugefügt
        'updated' - Bestehender Artikel wurde aktualisiert
        'unchanged' - Keine Änderung nötig
    """
    existing = get_article(article_data['title'], article_data['wiki_lang'])
    
    if not article_needs_update(existing, article_data):
        return 'unchanged'
    
    client = get_connection()
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    
    if existing is None:
        # Neuer Artikel
        client.execute('''
            INSERT INTO articles 
            (title, wiki_lang, url, topic, revision_count, revert_count, 
             editor_count, conflict_score, first_seen, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', [
            article_data['title'],
            article_data['wiki_lang'],
            article_data['url'],
            article_data.get('topic', ''),
            article_data['revision_count'],
            article_data['revert_count'],
            article_data['editor_count'],
            article_data['conflict_score'],
            now,
            now
        ])
        client.close()
        return 'added'
    else:
        # Artikel aktualisieren
        client.execute('''
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
            article_data['url'],
            article_data.get('topic', ''),
            article_data['revision_count'],
            article_data['revert_count'],
            article_data['editor_count'],
            article_data['conflict_score'],
            now,
            article_data['title'],
            article_data['wiki_lang']
        ])
        client.close()
        return 'updated'


def get_all_articles():
    """Gibt alle Artikel aus der Datenbank zurück."""
    client = get_connection()
    
    result = client.execute('''
        SELECT * FROM articles 
        ORDER BY conflict_score DESC, last_updated DESC
    ''')
    
    client.close()
    
    columns = ['id', 'title', 'wiki_lang', 'url', 'topic', 
               'revision_count', 'revert_count', 'editor_count',
               'conflict_score', 'first_seen', 'last_updated']
    
    return [dict(zip(columns, row)) for row in result.rows]


def log_scan(scan_type, wiki_lang, articles_scanned, articles_added, articles_updated=0):
    """Protokolliert einen Scan-Durchlauf."""
    client = get_connection()
    
    client.execute('''
        INSERT INTO scan_log (scan_type, wiki_lang, articles_scanned, articles_added, articles_updated)
        VALUES (?, ?, ?, ?, ?)
    ''', [scan_type, wiki_lang, articles_scanned, articles_added, articles_updated])
    
    client.close()


def get_scan_history(limit=20):
    """Gibt die letzten Scan-Durchläufe zurück."""
    client = get_connection()
    
    result = client.execute('''
        SELECT * FROM scan_log 
        ORDER BY timestamp DESC 
        LIMIT ?
    ''', [limit])
    
    client.close()
    
    columns = ['id', 'scan_type', 'wiki_lang', 'articles_scanned', 
               'articles_added', 'articles_updated', 'timestamp']
    
    return [dict(zip(columns, row)) for row in result.rows]


def delete_article(article_id):
    """Löscht einen Artikel aus der Datenbank."""
    client = get_connection()
    
    client.execute('DELETE FROM articles WHERE id = ?', [article_id])
    
    client.close()
