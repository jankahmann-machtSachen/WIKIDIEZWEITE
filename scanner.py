"""
Wikipedia API Scanner - Findet Artikel mit Edit-War-Aktivität
"""

import requests
import time
from datetime import datetime, timedelta
from config import WIKIS, SCAN_LIMIT, SCAN_HOURS, MIN_EDITS_FOR_ANALYSIS, REVERT_KEYWORDS

# User-Agent ist wichtig für Wikipedia API
HEADERS = {
    'User-Agent': 'EditWarScanner/1.0 (Educational Project; Contact: your@email.com)'
}


def get_recent_changes(wiki_lang, hours=SCAN_HOURS, limit=SCAN_LIMIT):
    """
    Holt die letzten Änderungen aus Wikipedia.
    """
    api_url = WIKIS[wiki_lang]
    
    start_time = datetime.utcnow() - timedelta(hours=hours)
    start_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    params = {
        'action': 'query',
        'list': 'recentchanges',
        'rcstart': 'now',
        'rcend': start_str,
        'rctype': 'edit',
        'rcnamespace': 0,
        'rclimit': 500,
        'rcprop': 'title|timestamp|user|comment',
        'format': 'json'
    }
    
    all_changes = []
    continue_token = None
    
    for _ in range(3):
        if continue_token:
            params['rccontinue'] = continue_token
        
        try:
            response = requests.get(api_url, params=params, headers=HEADERS, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            changes = data.get('query', {}).get('recentchanges', [])
            all_changes.extend(changes)
            
            if 'continue' in data:
                continue_token = data['continue'].get('rccontinue')
            else:
                break
                
            time.sleep(0.5)
            
        except requests.RequestException as e:
            print(f"API-Fehler bei {wiki_lang}: {e}")
            break
    
    # Zähle Edits pro Artikel
    edit_counts = {}
    for change in all_changes:
        title = change['title']
        if title not in edit_counts:
            edit_counts[title] = {
                'count': 0,
                'editors': set(),
                'comments': []
            }
        edit_counts[title]['count'] += 1
        edit_counts[title]['editors'].add(change.get('user', 'anonymous'))
        edit_counts[title]['comments'].append(change.get('comment', ''))
    
    sorted_articles = sorted(
        edit_counts.items(),
        key=lambda x: x[1]['count'],
        reverse=True
    )
    
    filtered = [
        (title, data) for title, data in sorted_articles
        if data['count'] >= MIN_EDITS_FOR_ANALYSIS
    ]
    
    return filtered[:limit]


def get_article_details(wiki_lang, title):
    """Holt detaillierte Informationen zu einem Artikel."""
    api_url = WIKIS[wiki_lang]
    
    params = {
        'action': 'query',
        'titles': title,
        'prop': 'revisions|info|categories',
        'rvprop': 'user|comment|timestamp',
        'rvlimit': 500,
        'inprop': 'protection',
        'format': 'json'
    }
    
    try:
        response = requests.get(api_url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        pages = data.get('query', {}).get('pages', {})
        page_data = list(pages.values())[0]
        
        if 'missing' in page_data:
            return None
        
        revisions = page_data.get('revisions', [])
        protection = page_data.get('protection', [])
        categories = page_data.get('categories', [])
        
        return {
            'revisions': revisions,
            'is_protected': len(protection) > 0,
            'categories': [cat['title'].replace('Category:', '').replace('Kategorie:', '') 
                          for cat in categories[:5]]
        }
        
    except requests.RequestException as e:
        print(f"Fehler bei Artikeldetails {title}: {e}")
        return None


def count_reverts(comments):
    """Zählt die Anzahl der Reverts basierend auf Kommentaren."""
    revert_count = 0
    for comment in comments:
        comment_lower = comment.lower()
        if any(keyword in comment_lower for keyword in REVERT_KEYWORDS):
            revert_count += 1
    return revert_count


def build_article_url(wiki_lang, title):
    """Erstellt die URL zum Wikipedia-Artikel."""
    base_urls = {
        'de': 'https://de.wikipedia.org/wiki/',
        'en': 'https://en.wikipedia.org/wiki/'
    }
    url_title = title.replace(' ', '_')
    return base_urls[wiki_lang] + url_title


def search_article(wiki_lang, search_term):
    """Sucht nach Artikeln basierend auf einem Suchbegriff."""
    api_url = WIKIS[wiki_lang]
    
    params = {
        'action': 'query',
        'list': 'search',
        'srsearch': search_term,
        'srlimit': 10,
        'format': 'json'
    }
    
    try:
        response = requests.get(api_url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        results = data.get('query', {}).get('search', [])
        return [r['title'] for r in results]
        
    except requests.RequestException as e:
        print(f"Suchfehler: {e}")
        return []
