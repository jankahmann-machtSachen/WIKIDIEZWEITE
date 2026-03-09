"""
Konfiguration für den Wikipedia Edit-War Scanner
"""
import os

# Anzahl der Artikel pro automatischem Scan
SCAN_LIMIT = 80

# Zeitraum für Recent Changes (in Stunden)
SCAN_HOURS = 48

# Wikipedia API Endpoints
WIKIS = {
    'de': 'https://de.wikipedia.org/w/api.php',
    'en': 'https://en.wikipedia.org/w/api.php'
}

# Gewichtung für Konflikt-Score (muss 1.0 ergeben)
SCORE_WEIGHTS = {
    'revert_ratio': 0.35,
    'editor_conflict': 0.25,
    'edit_frequency': 0.20,
    'protection_level': 0.20
}

# Schwellenwerte
MIN_EDITS_FOR_ANALYSIS = 5
REVERT_KEYWORDS = ['revert', 'rv', 'undid', 'undo', 'zurückgesetzt', 'rückgängig']

# Turso Datenbank
TURSO_DATABASE_URL = os.environ.get('TURSO_DATABASE_URL', '')
TURSO_AUTH_TOKEN = os.environ.get('TURSO_AUTH_TOKEN', '')
