#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v3.0 - Apify Instagram Scraper
Scraper robusto para Instagram usando Apify API com rotação de chaves
"""

import os
import logging
import asyncio
import time
import json
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import requests
from dataclasses import dataclass

# Importa o gerenciador de rotação de APIs
try:
    from .api_rotation_manager import api_rotation_manager
except ImportError:
    api_rotation_manager = None

logger = logging.getLogger(__name__)

@dataclass
class InstagramScrapingConfig:
    """Configuração para scraping do Instagram"""
    search_query: str = ""
    hashtags: List[str] = None
    usernames: List[str] = None
    max_posts: int = 50
    include_comments: bool = True
    max_comments_per_post: int = 20
    include_stories: bool = False
    timeout_minutes: int = 10
    results_type: str = "posts"  # posts, hashtag, profile

class ApifyInstagramScraper:
    """Scraper do Instagram usando Apify API com rotação de chaves"""
    
    def __init__(self):
        """Inicializa o scraper do Instagram"""
        self.actor_id = "apify/instagram-scraper"
        self.base_url = "https://api.apify.com/v2"
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'ARQV30-InstagramScraper/3.0'
        })
        
        logger.info(f"📸 Apify Instagram Scraper inicializado - Actor: {self.actor_id}")
    
    def get_api_key(self) -> Optional[str]:
        """Obtém chave da API com rotação automática"""
        if api_rotation_manager:
            return api_rotation_manager.get_api_key('APIFY', rotate=True)
        else:
            # Fallback manual
            return os.getenv('APIFY_API_KEY') or os.getenv('APIFY_API_KEY_1')
    
    async def scrape_instagram_posts(self, config: InstagramScrapingConfig) -> Dict[str, Any]:
        """Scraping principal do Instagram"""
        logger.info(f"🔍 Iniciando scraping do Instagram para: {config.search_query or config.hashtags or config.usernames}")
        
        try:
            # Prepara input para o actor
            actor_input = self._prepare_actor_input(config)
            
            # Executa o actor
            run_result = await self._run_actor(actor_input)
            
            if not run_result.get('success'):
                logger.error(f"❌ Falha na execução do actor: {run_result.get('error')}")
                return self._create_error_response(config, run_result.get('error'))
            
            # Obtém os dados do dataset
            dataset_data = await self._get_dataset_data(run_result['dataset_id'])
            
            # Processa e estrutura os dados
            processed_data = self._process_instagram_data(dataset_data, config)
            
            logger.info(f"✅ Scraping concluído: {len(processed_data.get('posts', []))} posts coletados")
            
            return {
                "success": True,
                "platform": "instagram",
                "query": config.search_query or str(config.hashtags) or str(config.usernames),
                "posts": processed_data.get('posts', []),
                "total_posts": len(processed_data.get('posts', [])),
                "total_comments": processed_data.get('total_comments', 0),
                "total_likes": processed_data.get('total_likes', 0),
                "scraping_config": {
                    "max_posts": config.max_posts,
                    "include_comments": config.include_comments,
                    "max_comments_per_post": config.max_comments_per_post,
                    "results_type": config.results_type
                },
                "metadata": {
                    "scraped_at": datetime.now().isoformat(),
                    "actor_id": self.actor_id,
                    "execution_time_seconds": processed_data.get('execution_time', 0)
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Erro no scraping do Instagram: {e}")
            if api_rotation_manager:
                api_rotation_manager.report_failure('APIFY', str(e))
            return self._create_error_response(config, str(e))
    
    def _prepare_actor_input(self, config: InstagramScrapingConfig) -> Dict[str, Any]:
        """Prepara input para o actor do Instagram"""
        input_data = {
            "resultsLimit": config.max_posts,
            "addParentData": config.include_comments,
            "enhanceUserSearchWithFacebookPage": False,
            "isUserTaggedFeedURL": False,
            "onlyPostsWithLocation": False,
            "likedByInfluencer": False,
            "searchLimit": config.max_posts,
            "searchType": config.results_type,
            "timeout": config.timeout_minutes * 60,  # Converte para segundos
            "proxyConfiguration": {
                "useApifyProxy": True,
                "apifyProxyGroups": ["RESIDENTIAL"]
            }
        }
        
        # Configura baseado no tipo de busca
        if config.hashtags:
            input_data["hashtags"] = config.hashtags
            input_data["searchType"] = "hashtag"
        elif config.usernames:
            input_data["usernames"] = config.usernames
            input_data["searchType"] = "user"
        elif config.search_query:
            # Para busca geral, usa hashtags derivadas da query
            hashtags = self._extract_hashtags_from_query(config.search_query)
            input_data["hashtags"] = hashtags
            input_data["searchType"] = "hashtag"
        
        return input_data
    
    def _extract_hashtags_from_query(self, query: str) -> List[str]:
        """Extrai hashtags relevantes de uma query"""
        # Remove caracteres especiais e divide em palavras
        words = query.lower().replace(',', ' ').replace('.', ' ').split()
        
        # Filtra palavras muito curtas e adiciona # 
        hashtags = []
        for word in words:
            if len(word) >= 3:
                hashtags.append(f"#{word}")
        
        # Adiciona hashtag da query completa se não for muito longa
        if len(query.replace(' ', '')) <= 30:
            hashtags.append(f"#{query.replace(' ', '')}")
        
        return hashtags[:5]  # Limita a 5 hashtags
    
    async def _run_actor(self, actor_input: Dict[str, Any]) -> Dict[str, Any]:
        """Executa o actor da Apify"""
        api_key = self.get_api_key()
        if not api_key:
            raise Exception("Nenhuma chave da API Apify disponível")
        
        url = f"{self.base_url}/acts/{self.actor_id}/runs"
        headers = {"Authorization": f"Bearer {api_key}"}
        
        try:
            # Inicia a execução
            response = self.session.post(url, json=actor_input, headers=headers, timeout=30)
            response.raise_for_status()
            
            run_data = response.json()
            run_id = run_data['data']['id']
            dataset_id = run_data['data']['defaultDatasetId']
            
            logger.info(f"🚀 Actor iniciado - Run ID: {run_id}")
            
            # Aguarda conclusão
            await self._wait_for_completion(run_id, api_key)
            
            if api_rotation_manager:
                api_rotation_manager.report_success('APIFY')
            
            return {
                "success": True,
                "run_id": run_id,
                "dataset_id": dataset_id
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro na requisição para Apify: {e}")
            if api_rotation_manager:
                api_rotation_manager.report_failure('APIFY', str(e))
            raise
    
    async def _wait_for_completion(self, run_id: str, api_key: str, max_wait_minutes: int = 15):
        """Aguarda conclusão da execução do actor"""
        url = f"{self.base_url}/actor-runs/{run_id}"
        headers = {"Authorization": f"Bearer {api_key}"}
        
        start_time = time.time()
        max_wait_seconds = max_wait_minutes * 60
        
        while time.time() - start_time < max_wait_seconds:
            try:
                response = self.session.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                
                run_info = response.json()
                status = run_info['data']['status']
                
                if status == 'SUCCEEDED':
                    logger.info(f"✅ Actor concluído com sucesso - Run ID: {run_id}")
                    return
                elif status == 'FAILED':
                    error_msg = run_info['data'].get('statusMessage', 'Execução falhou')
                    raise Exception(f"Actor falhou: {error_msg}")
                elif status in ['ABORTED', 'TIMED-OUT']:
                    raise Exception(f"Actor {status.lower()}")
                
                # Status ainda em execução
                logger.debug(f"⏳ Actor em execução - Status: {status}")
                await asyncio.sleep(10)  # Aguarda 10 segundos
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"⚠️ Erro ao verificar status: {e}")
                await asyncio.sleep(5)
        
        raise Exception(f"Timeout aguardando conclusão do actor após {max_wait_minutes} minutos")
    
    async def _get_dataset_data(self, dataset_id: str) -> List[Dict[str, Any]]:
        """Obtém dados do dataset"""
        api_key = self.get_api_key()
        if not api_key:
            raise Exception("Nenhuma chave da API Apify disponível")
        
        url = f"{self.base_url}/datasets/{dataset_id}/items"
        headers = {"Authorization": f"Bearer {api_key}"}
        
        try:
            response = self.session.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"📊 Dataset obtido: {len(data)} itens")
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro ao obter dataset: {e}")
            raise
    
    def _process_instagram_data(self, raw_data: List[Dict[str, Any]], config: InstagramScrapingConfig) -> Dict[str, Any]:
        """Processa dados brutos do Instagram"""
        processed_posts = []
        total_comments = 0
        total_likes = 0
        
        for item in raw_data:
            try:
                # Processa post principal
                post = self._process_single_post(item)
                if post:
                    processed_posts.append(post)
                    total_comments += len(post.get('comments', []))
                    total_likes += post.get('likes_count', 0)
                    
            except Exception as e:
                logger.warning(f"⚠️ Erro ao processar post: {e}")
                continue
        
        return {
            "posts": processed_posts,
            "total_comments": total_comments,
            "total_likes": total_likes,
            "execution_time": 0  # Será calculado se necessário
        }
    
    def _process_single_post(self, raw_post: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Processa um post individual do Instagram"""
        try:
            # Extrai informações básicas do post
            post = {
                "id": raw_post.get('id', ''),
                "shortcode": raw_post.get('shortCode', ''),
                "caption": raw_post.get('caption', ''),
                "text": raw_post.get('caption', ''),  # Alias para compatibilidade
                "author": {
                    "username": raw_post.get('ownerUsername', ''),
                    "full_name": raw_post.get('ownerFullName', ''),
                    "profile_pic": raw_post.get('ownerProfilePicUrl', ''),
                    "verified": raw_post.get('isOwnerVerified', False),
                    "followers_count": raw_post.get('followersCount', 0)
                },
                "url": f"https://instagram.com/p/{raw_post.get('shortCode', '')}",
                "created_at": raw_post.get('timestamp', ''),
                "platform": "instagram",
                
                # Métricas de engajamento
                "likes_count": raw_post.get('likesCount', 0),
                "comments_count": raw_post.get('commentsCount', 0),
                "video_views": raw_post.get('videoViewCount', 0),
                
                # Tipo de mídia
                "media_type": self._determine_media_type(raw_post),
                "is_video": raw_post.get('isVideo', False),
                
                # Mídia
                "media": self._extract_instagram_media(raw_post),
                
                # Hashtags e menções
                "hashtags": self._extract_hashtags(raw_post.get('caption', '')),
                "mentions": self._extract_mentions(raw_post.get('caption', '')),
                
                # Localização
                "location": {
                    "name": raw_post.get('locationName', ''),
                    "id": raw_post.get('locationId', '')
                },
                
                # Comentários
                "comments": self._extract_instagram_comments(raw_post),
                
                # Análise de sentimento
                "sentiment": self._analyze_sentiment(raw_post.get('caption', '')),
                
                # Métricas calculadas
                "engagement_rate": self._calculate_instagram_engagement_rate(raw_post),
                "virality_score": self._calculate_instagram_virality_score(raw_post)
            }
            
            return post
            
        except Exception as e:
            logger.warning(f"⚠️ Erro ao processar post individual do Instagram: {e}")
            return None
    
    def _determine_media_type(self, raw_post: Dict[str, Any]) -> str:
        """Determina o tipo de mídia do post"""
        if raw_post.get('isVideo'):
            return "VIDEO"
        elif raw_post.get('childPosts'):
            return "CAROUSEL"
        else:
            return "IMAGE"
    
    def _extract_instagram_media(self, raw_post: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extrai informações de mídia do post do Instagram"""
        media = []
        
        # Mídia principal
        if raw_post.get('displayUrl'):
            media.append({
                "type": "video" if raw_post.get('isVideo') else "image",
                "url": raw_post.get('displayUrl'),
                "thumbnail": raw_post.get('displayUrl'),
                "width": raw_post.get('dimensions', {}).get('width', 0),
                "height": raw_post.get('dimensions', {}).get('height', 0)
            })
        
        # Posts filhos (carrossel)
        if raw_post.get('childPosts'):
            for child in raw_post['childPosts']:
                media.append({
                    "type": "video" if child.get('isVideo') else "image",
                    "url": child.get('displayUrl', ''),
                    "thumbnail": child.get('displayUrl', ''),
                    "width": child.get('dimensions', {}).get('width', 0),
                    "height": child.get('dimensions', {}).get('height', 0)
                })
        
        return media
    
    def _extract_hashtags(self, text: str) -> List[str]:
        """Extrai hashtags do texto"""
        import re
        if not text:
            return []
        
        hashtags = re.findall(r'#\w+', text)
        return [tag.lower() for tag in hashtags]
    
    def _extract_mentions(self, text: str) -> List[str]:
        """Extrai menções do texto"""
        import re
        if not text:
            return []
        
        mentions = re.findall(r'@\w+', text)
        return [mention.lower() for mention in mentions]
    
    def _extract_instagram_comments(self, raw_post: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extrai comentários do post do Instagram"""
        comments = []
        
        if raw_post.get('latestComments'):
            for comment in raw_post['latestComments']:
                comments.append({
                    "id": comment.get('id', ''),
                    "text": comment.get('text', ''),
                    "author": comment.get('ownerUsername', ''),
                    "created_at": comment.get('timestamp', ''),
                    "likes_count": comment.get('likesCount', 0),
                    "sentiment": self._analyze_sentiment(comment.get('text', ''))
                })
        
        return comments
    
    def _analyze_sentiment(self, text: str) -> str:
        """Análise básica de sentimento"""
        if not text:
            return "neutral"
        
        text_lower = text.lower()
        
        # Palavras positivas em português e inglês
        positive_words = [
            'bom', 'ótimo', 'excelente', 'maravilhoso', 'incrível', 'perfeito', 'amor', 'feliz',
            'good', 'great', 'excellent', 'amazing', 'incredible', 'perfect', 'love', 'happy',
            '❤️', '😍', '🥰', '😊', '👏', '🔥', '💯'
        ]
        
        # Palavras negativas em português e inglês
        negative_words = [
            'ruim', 'péssimo', 'terrível', 'horrível', 'ódio', 'triste', 'raiva',
            'bad', 'terrible', 'horrible', 'hate', 'sad', 'angry',
            '😢', '😭', '😡', '👎', '💔'
        ]
        
        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)
        
        if positive_count > negative_count:
            return "positive"
        elif negative_count > positive_count:
            return "negative"
        else:
            return "neutral"
    
    def _calculate_instagram_engagement_rate(self, raw_post: Dict[str, Any]) -> float:
        """Calcula taxa de engajamento do Instagram"""
        try:
            likes = raw_post.get('likesCount', 0)
            comments = raw_post.get('commentsCount', 0)
            followers = raw_post.get('followersCount', 1000)  # Estimativa mínima
            
            total_engagement = likes + comments
            
            return round((total_engagement / followers) * 100, 2)
        except:
            return 0.0
    
    def _calculate_instagram_virality_score(self, raw_post: Dict[str, Any]) -> float:
        """Calcula score de viralidade do Instagram"""
        try:
            likes = raw_post.get('likesCount', 0)
            comments = raw_post.get('commentsCount', 0)
            video_views = raw_post.get('videoViewCount', 0)
            
            # Fórmula ponderada para viralidade
            virality = (likes * 1) + (comments * 5) + (video_views * 0.1)
            
            # Normaliza para escala 0-100
            return min(round(virality / 1000, 2), 100.0)
        except:
            return 0.0
    
    def _create_error_response(self, config: InstagramScrapingConfig, error: str) -> Dict[str, Any]:
        """Cria resposta de erro padronizada"""
        return {
            "success": False,
            "platform": "instagram",
            "query": config.search_query or str(config.hashtags) or str(config.usernames),
            "error": error,
            "posts": [],
            "total_posts": 0,
            "total_comments": 0,
            "total_likes": 0,
            "scraped_at": datetime.now().isoformat()
        }

# Instância global
apify_instagram_scraper = ApifyInstagramScraper()

# Função para compatibilidade
def get_instagram_scraper():
    """Retorna a instância global do Instagram scraper"""
    return apify_instagram_scraper