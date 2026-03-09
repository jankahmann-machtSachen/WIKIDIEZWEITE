"""
Datenbank-Operationen für den Edit-War Scanner
Mit Turso (Cloud SQLite) und Update-Logik
"""

import libsql_experimental as libsql
from datetime import datetime
from config import TURSO_DATABASE_URL, TURSO_AUTH_TOKEN


def get_connection():
    """Erstellt eine Verbindung zur Turso-Datenbank."""
    return libsql.connect(
        database=TURSO_DATABASE_URL,
        auth_token=TURSO_AUTH_TOKEN
    )


def init_database():
    """Erstellt die Tabellen, falls nicht vorhanden."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
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
    
    cursor.execute('''
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
    
    conn.commit()
    conn.close()


def get_article(title, wiki_lang):
    """Holt einen Artikel aus der Datenbank, falls vorhanden."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute(
        'SELECT * FROM articles WHERE title = ? AND wiki_lang = ?',
        (title, wiki_lang)
    )
    row = cursor.fetchone()
    conn.close()
    
    if row:
        columns = ['id', 'title', 'wiki_lang', 'url', 'topic', 
                   'revision_count', 'revert_count', 'editor_count',
                   'conflict_score', 'first_seen', 'last_updated']
        return dict(zip(columns, row))
    return None


def article_needs_update(existing, new_data):
    """
    Prüft, ob ein Artikel aktualisiert werden muss.
    Update wenn: Edits ODER Reverts ODER Editoren sich geändert haben.
    """
    if existing is None:
        return True  # Neuer Artikel
    
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
    
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    
    if existing is None:
        # Neuer Artikel
        cursor.execute('''
            INSERT INTO articles 
            (title, wiki_lang, url, topic, revision_count, revert_count, 
             editor_count, conflict_score, first_seen, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
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
        ))
        conn.commit()
        conn.close()
        return 'added'
    else:
        # Artikel aktualisieren
        cursor.execute('''
            UPDATE articles 
            SET url = ?,
                topic = ?,
                revision_count = ?,
                revert_count = ?,
                editor_count = ?,
                conflict_score = ?,
                last_updated = ?
            WHERE title = ? AND wiki_lang = ?
        ''', (
            article_data['url'],
            article_data.get('topic', ''),
            article_data['revision_count'],
            article_data['revert_count'],
            article_data['editor_count'],
            article_data['conflict_score'],
            now,
            article_data['title'],
            article_data['wiki_lang']
        ))
        conn.commit()
        conn.close()
        return 'updated'


def get_all_articles():
    """Gibt alle Artikel aus der Datenbank zurück."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM articles 
        ORDER BY conflict_score DESC, last_updated DESC
    ''')
    
    rows = cursor.fetchall()
    conn.close()
    
    columns = ['id', 'title', 'wiki_lang', 'url', 'topic', 
               'revision_count', 'revert_count', 'editor_count',
               'conflict_score', 'first_seen', 'last_updated']
    
    return [dict(zip(columns, row)) for row in rows]


def log_scan(scan_type, wiki_lang, articles_scanned, articles_added, articles_updated=0):
    """Protokolliert einen Scan-Durchlauf."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO scan_log (scan_type, wiki_lang, articles_scanned, articles_added, articles_updated)
        VALUES (?, ?, ?, ?, ?)
    ''', (scan_type, wiki_lang, articles_scanned, articles_added, articles_updated))
    
    conn.commit()
    conn.close()


def get_scan_history(limit=20):
    """Gibt die letzten Scan-Durchläufe zurück."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM scan_log 
        ORDER BY timestamp DESC 
        LIMIT ?
    ''', (limit,))
    
    rows = cursor.fetchall()
    conn.close()
    
    columns = ['id', 'scan_type', 'wiki_lang', 'articles_scanned', 
               'articles_added', 'articles_updated', 'timestamp']
    
    return [dict(zip(columns, row)) for row in rows]


def delete_article(article_id):
    """Löscht einen Artikel aus der Datenbank."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('DELETE FROM articles WHERE id = ?', (article_id,))
    
    conn.commit()
    conn.close()
