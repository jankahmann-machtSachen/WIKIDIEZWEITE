"""
Export-Funktionen für die Datenbank
"""

import csv
import io
from database import get_all_articles


def export_to_csv():
    """Exportiert alle Artikel als CSV."""
    articles = get_all_articles()
    
    if not articles:
        return None
    
    output = io.StringIO()
    
    fieldnames = [
        'id', 'title', 'wiki_lang', 'url', 'topic',
        'revision_count', 'revert_count', 'editor_count',
        'conflict_score', 'first_seen', 'last_updated'
    ]
    
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    
    for article in articles:
        writer.writerow(article)
    
    return output.getvalue()


def export_to_excel_compatible_csv():
    """Exportiert als CSV mit UTF-8 BOM für Excel-Kompatibilität."""
    csv_content = export_to_csv()
    if csv_content:
        return '\ufeff' + csv_content
    return None
