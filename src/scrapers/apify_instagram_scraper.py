"""
🔥 APIFY INSTAGRAM SCRAPER
Implementação do scraper: https://apify.com/apify/instagram-scraper
"""

import os
import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import json

# Configuração de logging
logger = logging.getLogger(__name__)

class ApifyInstagramScraper:
    """Scraper do Instagram usando Apify apify/instagram-scraper"""
    
    def __init__(self):
        self.api_keys = self._load_api_keys()
        self.current_key_index = 0
        self.actor_id = "apify/instagram-scraper"
        
    def _load_api_keys(self) -> List[str]:
        """Carrega chaves da API Apify do ambiente"""
        keys = []
        
        # Carrega chaves individuais
        for i in range(1, 11):  # Suporta até 10 chaves
            key = os.getenv(f'APIFY_API_KEY_{i}')
            if key:
                keys.append(key)
        
        # Fallback para chave única
        if not keys:
            main_key = os.getenv('APIFY_API_KEY')
            if main_key:
                keys.append(main_key)
        
        if not keys:
            logger.warning("⚠️ Nenhuma chave da API Apify encontrada")
        else:
            logger.info(f"✅ {len(keys)} chaves da API Apify carregadas")
            
        return keys
    
    def _get_next_api_key(self) -> Optional[str]:
        """Rotaciona entre as chaves disponíveis"""
        if not self.api_keys:
            return None
            
        key = self.api_keys[self.current_key_index]
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        return key
    
    async def scrape_instagram_posts(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Scraping de posts do Instagram
        
        Args:
            config: Configuração do scraping com:
                - search_query: Termo de busca ou hashtag
                - max_posts: Máximo de posts
                - include_comments: Se deve incluir comentários
                - include_stories: Se deve incluir stories
                - proxy_config: Configuração de proxy (opcional)
        """
        
        api_key = self._get_next_api_key()
        if not api_key:
            raise Exception("Nenhuma chave da API Apify disponível")
        
        logger.info(f"📸 Iniciando scraping Instagram: {config.get('search_query', 'N/A')}")
        
        try:
            # Simula chamada para a API Apify
            await asyncio.sleep(2)  # Simula tempo de processamento
            
            # Configuração do actor
            actor_input = {
                "hashtags": [config.get('search_query', '').replace('#', '')],
                "resultsLimit": config.get('max_posts', 10),
                "includeComments": config.get('include_comments', True),
                "includeStories": config.get('include_stories', False),
                "proxyConfiguration": config.get('proxy_config', {
                    "useApifyProxy": True,
                    "apifyProxyGroups": ["RESIDENTIAL"]
                })
            }
            
            # Simula resposta da API (substituir por chamada real)
            posts = self._generate_sample_instagram_data(
                config.get('search_query', ''),
                config.get('max_posts', 10)
            )
            
            result = {
                "success": True,
                "posts": posts,
                "total_posts": len(posts),
                "actor_id": self.actor_id,
                "api_key_used": f"***{api_key[-4:]}",
                "scraped_at": datetime.now().isoformat(),
                "config": actor_input
            }
            
            logger.info(f"✅ Instagram scraping concluído: {len(posts)} posts extraídos")
            return result
            
        except Exception as e:
            logger.error(f"❌ Erro no scraping Instagram: {e}")
            return {
                "success": False,
                "error": str(e),
                "actor_id": self.actor_id
            }
    
    def _generate_sample_instagram_data(self, query: str, max_posts: int) -> List[Dict[str, Any]]:
        """Gera dados de exemplo (substituir por dados reais da API)"""
        posts = []
        
        for i in range(min(max_posts, 20)):
            post = {
                "id": f"ig_post_{hash(query + str(i)) % 1000000}",
                "shortCode": f"ABC{i:03d}XYZ",
                "caption": f"📸 Post incrível sobre {query}! Compartilhando conteúdo de qualidade sobre este tema. #{query.replace(' ', '')} #instagram #content #viral",
                "hashtags": [
                    f"#{query.replace(' ', '')}",
                    "#instagram",
                    "#content",
                    "#viral",
                    "#trending"
                ],
                "url": f"https://www.instagram.com/p/ABC{i:03d}XYZ/",
                "displayUrl": f"https://picsum.photos/1080/1080?random={i}",
                "owner": {
                    "username": f"creator_{query.replace(' ', '_').lower()}_{i+1}",
                    "fullName": f"Creator {query.title()} {i+1}",
                    "id": f"user_{i+1}",
                    "isVerified": i % 4 == 0,
                    "followersCount": 50000 + (i * 10000),
                    "followingCount": 1000 + (i * 100)
                },
                "timestamp": "2024-08-27T12:00:00.000Z",
                "likesCount": 500 + (i * 50),
                "commentsCount": 25 + (i * 3),
                "viewsCount": 2000 + (i * 200),
                "isVideo": i % 3 == 0,
                "videoDuration": 30 + (i * 5) if i % 3 == 0 else None,
                "location": {
                    "name": f"Local {query.title()} {i+1}",
                    "id": f"location_{i+1}"
                } if i % 2 == 0 else None,
                "engagement_rate": round((500 + i * 50) / (50000 + i * 10000) * 100, 2),
                "virality_score": 60 + (i * 3),
                "sentiment": ["positive", "neutral", "negative"][i % 3],
                "comments": [
                    {
                        "id": f"comment_{j}",
                        "text": f"Comentário sobre {query} - muito interessante!",
                        "owner": {
                            "username": f"user_{j}",
                            "isVerified": False
                        },
                        "likesCount": 2 + j,
                        "timestamp": "2024-08-27T12:30:00.000Z"
                    }
                    for j in range(min(5, 25 + (i * 3)))
                ]
            }
            posts.append(post)
        
        return posts

# Instância global
apify_instagram_scraper = ApifyInstagramScraper()

# Configuração para compatibilidade
class InstagramScrapingConfig:
    """Configuração para scraping do Instagram"""
    
    def __init__(self, search_query: str, max_posts: int = 10, 
                 include_comments: bool = True, include_stories: bool = False,
                 proxy_config: Optional[Dict] = None):
        self.search_query = search_query
        self.max_posts = max_posts
        self.include_comments = include_comments
        self.include_stories = include_stories
        self.proxy_config = proxy_config or {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"]
        }
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "search_query": self.search_query,
            "max_posts": self.max_posts,
            "include_comments": self.include_comments,
            "include_stories": self.include_stories,
            "proxy_config": self.proxy_config
        }

if __name__ == "__main__":
    # Teste do scraper
    async def test_scraper():
        config = InstagramScrapingConfig(
            search_query="marketing digital",
            max_posts=5,
            include_comments=True
        )
        
        result = await apify_instagram_scraper.scrape_instagram_posts(config.to_dict())
        print(json.dumps(result, indent=2, ensure_ascii=False))
    
    asyncio.run(test_scraper())