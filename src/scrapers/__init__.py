"""
🔥 SCRAPERS APIFY - ARQV30 Enhanced v3.0
Scrapers para Facebook e Instagram usando Apify
"""

try:
    from .apify_facebook_scraper import apify_facebook_scraper, FacebookScrapingConfig
    from .apify_instagram_scraper import apify_instagram_scraper, InstagramScrapingConfig
    
    __all__ = [
        'apify_facebook_scraper',
        'FacebookScrapingConfig', 
        'apify_instagram_scraper',
        'InstagramScrapingConfig'
    ]
except ImportError as e:
    # Fallback se houver problemas de importação
    print(f"⚠️ Erro ao importar scrapers: {e}")
    __all__ = []