"""
Datenbank-Operationen für den Edit-War Scanner
"""

import sqlite3
import os
from datetime import datetime
from config import DATABASE_PATH


def get_db_path():
    """Gibt den Pfad zur Datenbank zurück."""
    return DATABASE_PATH


def init_database():
    """Erstellt die Datenbank und Tabellen, falls nicht vorhanden."""
    db_path = get_db_path()
    
    # Ordner erstellen falls nötig
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    
    conn = sqlite3.connect(db_path)
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
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()


def article_exists(title, wiki_lang):
    """Prüft, ob ein Artikel bereits in der Datenbank ist."""
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    
    cursor.execute(
        'SELECT id FROM articles WHERE title = ? AND wiki_lang = ?',
        (title, wiki_lang)
    )
    result = cursor.fetchone()
    conn.close()
    
    return result is not None


def add_article(article_data):
    """Fügt einen Artikel zur Datenbank hinzu."""
    if article_exists(article_data['title'], article_data['wiki_lang']):
        return False
    
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO articles 
        (title, wiki_lang, url, topic, revision_count, revert_count, 
         editor_count, conflict_score)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        article_data['title'],
        article_data['wiki_lang'],
        article_data['url'],
        article_data.get('topic', ''),
        article_data['revision_count'],
        article_data['revert_count'],
        article_data['editor_count'],
        article_data['conflict_score']
    ))
    
    conn.commit()
    conn.close()
    return True


def get_all_articles():
    """Gibt alle Artikel aus der Datenbank zurück."""
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM articles 
        ORDER BY conflict_score DESC, last_updated DESC
    ''')
    
    articles = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return articles


def log_scan(scan_type, wiki_lang, articles_scanned, articles_added):
    """Protokolliert einen Scan-Durchlauf."""
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO scan_log (scan_type, wiki_lang, articles_scanned, articles_added)
        VALUES (?, ?, ?, ?)
    ''', (scan_type, wiki_lang, articles_scanned, articles_added))
    
    conn.commit()
    conn.close()


def get_scan_history(limit=20):
    """Gibt die letzten Scan-Durchläufe zurück."""
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM scan_log 
        ORDER BY timestamp DESC 
        LIMIT ?
    ''', (limit,))
    
    history = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return history


def delete_article(article_id):
    """Löscht einen Artikel aus der Datenbank."""
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    
    cursor.execute('DELETE FROM articles WHERE id = ?', (article_id,))
    
    conn.commit()
    conn.close()
