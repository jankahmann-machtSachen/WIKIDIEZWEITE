"""
Konflikt-Analyse und Scoring für Wikipedia-Artikel
"""

from config import SCORE_WEIGHTS, MIN_EDITS_FOR_ANALYSIS
from scanner import get_article_details, count_reverts, build_article_url


def calculate_conflict_score(edit_count, revert_count, editor_count, is_protected):
    """Berechnet den Konflikt-Score (1-10)."""
    if edit_count < MIN_EDITS_FOR_ANALYSIS:
        return 1
    
    revert_ratio = min(revert_count / edit_count, 1.0) if edit_count > 0 else 0
    
    if editor_count <= 2 and edit_count > 10:
        editor_conflict = 1.0
    elif editor_count <= 3 and edit_count > 20:
        editor_conflict = 0.8
    elif editor_count < edit_count / 5:
        editor_conflict = 0.6
    else:
        editor_conflict = edit_count / (editor_count * 10) if editor_count > 0 else 0
    editor_conflict = min(editor_conflict, 1.0)
    
    edit_frequency = min(edit_count / 50, 1.0)
    protection_score = 1.0 if is_protected else 0.0
    
    raw_score = (
        revert_ratio * SCORE_WEIGHTS['revert_ratio'] +
        editor_conflict * SCORE_WEIGHTS['editor_conflict'] +
        edit_frequency * SCORE_WEIGHTS['edit_frequency'] +
        protection_score * SCORE_WEIGHTS['protection_level']
    )
    
    final_score = max(1, min(10, round(raw_score * 10)))
    return final_score


def analyze_article(wiki_lang, title, edit_data=None):
    """Führt eine vollständige Analyse eines Artikels durch."""
    details = get_article_details(wiki_lang, title)
    
    if details is None:
        return None
    
    if edit_data:
        edit_count = edit_data['count']
        editors = edit_data['editors']
        comments = edit_data['comments']
    else:
        revisions = details['revisions']
        edit_count = len(revisions)
        editors = set(r.get('user', 'anonymous') for r in revisions)
        comments = [r.get('comment', '') for r in revisions]
    
    editor_count = len(editors)
    revert_count = count_reverts(comments)
    is_protected = details['is_protected']
    
    conflict_score = calculate_conflict_score(
        edit_count, revert_count, editor_count, is_protected
    )
    
    topic = ', '.join(details['categories'][:3]) if details['categories'] else 'Unbekannt'
    
    return {
        'title': title,
        'wiki_lang': wiki_lang,
        'url': build_article_url(wiki_lang, title),
        'topic': topic,
        'revision_count': edit_count,
        'revert_count': revert_count,
        'editor_count': editor_count,
        'conflict_score': conflict_score
    }
