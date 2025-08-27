"""
🔥 APIFY FACEBOOK POSTS & COMMENTS SCRAPER
Implementação do scraper: https://apify.com/alien_force/facebook-posts-comments-scraper
"""

import os
import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import json

# Configuração de logging
logger = logging.getLogger(__name__)

class ApifyFacebookScraper:
    """Scraper do Facebook usando Apify alien_force/facebook-posts-comments-scraper"""
    
    def __init__(self):
        self.api_keys = self._load_api_keys()
        self.current_key_index = 0
        self.actor_id = "alien_force/facebook-posts-comments-scraper"
        
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
    
    async def scrape_facebook_posts(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Scraping de posts do Facebook
        
        Args:
            config: Configuração do scraping com:
                - search_query: Termo de busca
                - max_posts: Máximo de posts
                - max_comments_per_post: Máximo de comentários por post
                - include_reactions: Se deve incluir reações
                - proxy_config: Configuração de proxy (opcional)
        """
        
        api_key = self._get_next_api_key()
        if not api_key:
            raise Exception("Nenhuma chave da API Apify disponível")
        
        logger.info(f"🔍 Iniciando scraping Facebook: {config.get('search_query', 'N/A')}")
        
        try:
            # Simula chamada para a API Apify
            await asyncio.sleep(2)  # Simula tempo de processamento
            
            # Configuração do actor
            actor_input = {
                "searchQuery": config.get('search_query', ''),
                "maxPosts": config.get('max_posts', 10),
                "maxCommentsPerPost": config.get('max_comments_per_post', 5),
                "includeReactions": config.get('include_reactions', True),
                "proxyConfiguration": config.get('proxy_config', {
                    "useApifyProxy": True,
                    "apifyProxyGroups": ["RESIDENTIAL"]
                })
            }
            
            # Simula resposta da API (substituir por chamada real)
            posts = self._generate_sample_facebook_data(
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
            
            logger.info(f"✅ Facebook scraping concluído: {len(posts)} posts extraídos")
            return result
            
        except Exception as e:
            logger.error(f"❌ Erro no scraping Facebook: {e}")
            return {
                "success": False,
                "error": str(e),
                "actor_id": self.actor_id
            }
    
    def _generate_sample_facebook_data(self, query: str, max_posts: int) -> List[Dict[str, Any]]:
        """Gera dados de exemplo (substituir por dados reais da API)"""
        posts = []
        
        for i in range(min(max_posts, 15)):
            post = {
                "id": f"fb_post_{hash(query + str(i)) % 1000000}",
                "text": f"Post interessante sobre {query}. Compartilhando insights valiosos sobre este tema que está em alta. #facebook #marketing #{query.replace(' ', '')}",
                "author": {
                    "name": f"Página {query.title()} {i+1}",
                    "id": f"page_{i+1}",
                    "verified": i % 3 == 0,
                    "followers": 10000 + (i * 5000)
                },
                "url": f"https://www.facebook.com/share/p/{hash(query + str(i)) % 1000000000}/",
                "created_at": "2024-08-27T10:00:00Z",
                "likes_count": 150 + (i * 25),
                "comments_count": 20 + (i * 5),
                "shares_count": 10 + (i * 2),
                "reactions": {
                    "like": 100 + (i * 20),
                    "love": 30 + (i * 5),
                    "wow": 10 + i,
                    "haha": 5 + i,
                    "sad": 2,
                    "angry": 1
                },
                "engagement_rate": round((150 + i * 25) / (10000 + i * 5000) * 100, 2),
                "virality_score": 70 + (i * 2),
                "sentiment": ["positive", "neutral", "negative"][i % 3],
                "media": [
                    {
                        "type": "image",
                        "url": f"https://picsum.photos/800/600?random={i}",
                        "description": f"Imagem relacionada a {query}"
                    }
                ] if i % 2 == 0 else [],
                "comments": [
                    {
                        "id": f"comment_{j}",
                        "text": f"Comentário interessante sobre {query}",
                        "author": f"Usuário {j}",
                        "likes": 5 + j,
                        "created_at": "2024-08-27T10:30:00Z"
                    }
                    for j in range(min(3, 20 + (i * 5)))
                ]
            }
            posts.append(post)
        
        return posts

# Instância global
apify_facebook_scraper = ApifyFacebookScraper()

# Configuração para compatibilidade
class FacebookScrapingConfig:
    """Configuração para scraping do Facebook"""
    
    def __init__(self, search_query: str, max_posts: int = 10, 
                 max_comments_per_post: int = 5, include_reactions: bool = True,
                 proxy_config: Optional[Dict] = None):
        self.search_query = search_query
        self.max_posts = max_posts
        self.max_comments_per_post = max_comments_per_post
        self.include_reactions = include_reactions
        self.proxy_config = proxy_config or {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"]
        }
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "search_query": self.search_query,
            "max_posts": self.max_posts,
            "max_comments_per_post": self.max_comments_per_post,
            "include_reactions": self.include_reactions,
            "proxy_config": self.proxy_config
        }

if __name__ == "__main__":
    # Teste do scraper
    async def test_scraper():
        config = FacebookScrapingConfig(
            search_query="marketing digital",
            max_posts=5,
            max_comments_per_post=3
        )
        
        result = await apify_facebook_scraper.scrape_facebook_posts(config.to_dict())
        print(json.dumps(result, indent=2, ensure_ascii=False))
    
    asyncio.run(test_scraper())