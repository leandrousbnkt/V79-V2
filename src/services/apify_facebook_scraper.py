#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v3.0 - Apify Facebook Scraper
Scraper robusto para Facebook usando Apify API com rotação de chaves
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
class FacebookScrapingConfig:
    """Configuração para scraping do Facebook"""
    search_query: str
    max_posts: int = 50
    max_comments_per_post: int = 20
    include_reactions: bool = True
    include_shares: bool = True
    timeout_minutes: int = 10
    language: str = "pt"

class ApifyFacebookScraper:
    """Scraper do Facebook usando Apify API com rotação de chaves"""
    
    def __init__(self):
        """Inicializa o scraper do Facebook"""
        self.actor_id = "alien_force/facebook-posts-comments-scraper"
        self.base_url = "https://api.apify.com/v2"
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'ARQV30-FacebookScraper/3.0'
        })
        
        logger.info(f"🔵 Apify Facebook Scraper inicializado - Actor: {self.actor_id}")
    
    def get_api_key(self) -> Optional[str]:
        """Obtém chave da API com rotação automática"""
        if api_rotation_manager:
            return api_rotation_manager.get_api_key('APIFY', rotate=True)
        else:
            # Fallback manual
            return os.getenv('APIFY_API_KEY') or os.getenv('APIFY_API_KEY_1')
    
    async def scrape_facebook_posts(self, config: FacebookScrapingConfig) -> Dict[str, Any]:
        """Scraping principal do Facebook"""
        logger.info(f"🔍 Iniciando scraping do Facebook para: {config.search_query}")
        
        try:
            # Prepara input para o actor
            actor_input = self._prepare_actor_input(config)
            
            # Executa o actor
            run_result = await self._run_actor(actor_input)
            
            if not run_result.get('success'):
                logger.error(f"❌ Falha na execução do actor: {run_result.get('error')}")
                return self._create_error_response(config.search_query, run_result.get('error'))
            
            # Obtém os dados do dataset
            dataset_data = await self._get_dataset_data(run_result['dataset_id'])
            
            # Processa e estrutura os dados
            processed_data = self._process_facebook_data(dataset_data, config)
            
            logger.info(f"✅ Scraping concluído: {len(processed_data.get('posts', []))} posts coletados")
            
            return {
                "success": True,
                "platform": "facebook",
                "query": config.search_query,
                "posts": processed_data.get('posts', []),
                "total_posts": len(processed_data.get('posts', [])),
                "total_comments": processed_data.get('total_comments', 0),
                "total_reactions": processed_data.get('total_reactions', 0),
                "scraping_config": {
                    "max_posts": config.max_posts,
                    "max_comments_per_post": config.max_comments_per_post,
                    "include_reactions": config.include_reactions,
                    "include_shares": config.include_shares
                },
                "metadata": {
                    "scraped_at": datetime.now().isoformat(),
                    "actor_id": self.actor_id,
                    "execution_time_seconds": processed_data.get('execution_time', 0)
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Erro no scraping do Facebook: {e}")
            if api_rotation_manager:
                api_rotation_manager.report_failure('APIFY', str(e))
            return self._create_error_response(config.search_query, str(e))
    
    def _prepare_actor_input(self, config: FacebookScrapingConfig) -> Dict[str, Any]:
        """Prepara input para o actor do Facebook"""
        return {
            "searchQuery": config.search_query,
            "maxPosts": config.max_posts,
            "maxCommentsPerPost": config.max_comments_per_post,
            "includeReactions": config.include_reactions,
            "includeShares": config.include_shares,
            "language": config.language,
            "timeout": config.timeout_minutes * 60,  # Converte para segundos
            "proxyConfiguration": {
                "useApifyProxy": True,
                "apifyProxyGroups": ["RESIDENTIAL"]
            },
            "outputFormat": "json"
        }
    
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
    
    def _process_facebook_data(self, raw_data: List[Dict[str, Any]], config: FacebookScrapingConfig) -> Dict[str, Any]:
        """Processa dados brutos do Facebook"""
        processed_posts = []
        total_comments = 0
        total_reactions = 0
        
        for item in raw_data:
            try:
                # Processa post principal
                post = self._process_single_post(item)
                if post:
                    processed_posts.append(post)
                    total_comments += len(post.get('comments', []))
                    total_reactions += post.get('reactions_count', 0)
                    
            except Exception as e:
                logger.warning(f"⚠️ Erro ao processar post: {e}")
                continue
        
        return {
            "posts": processed_posts,
            "total_comments": total_comments,
            "total_reactions": total_reactions,
            "execution_time": 0  # Será calculado se necessário
        }
    
    def _process_single_post(self, raw_post: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Processa um post individual"""
        try:
            # Extrai informações básicas do post
            post = {
                "id": raw_post.get('postId', ''),
                "text": raw_post.get('text', ''),
                "author": {
                    "name": raw_post.get('authorName', ''),
                    "url": raw_post.get('authorUrl', ''),
                    "verified": raw_post.get('authorVerified', False)
                },
                "url": raw_post.get('postUrl', ''),
                "created_at": raw_post.get('publishedTime', ''),
                "platform": "facebook",
                
                # Métricas de engajamento
                "likes_count": raw_post.get('likesCount', 0),
                "comments_count": raw_post.get('commentsCount', 0),
                "shares_count": raw_post.get('sharesCount', 0),
                "reactions_count": raw_post.get('reactionsCount', 0),
                
                # Reações detalhadas
                "reactions": {
                    "like": raw_post.get('reactions', {}).get('like', 0),
                    "love": raw_post.get('reactions', {}).get('love', 0),
                    "wow": raw_post.get('reactions', {}).get('wow', 0),
                    "haha": raw_post.get('reactions', {}).get('haha', 0),
                    "sad": raw_post.get('reactions', {}).get('sad', 0),
                    "angry": raw_post.get('reactions', {}).get('angry', 0)
                },
                
                # Mídia
                "media": self._extract_media(raw_post),
                
                # Comentários
                "comments": self._extract_comments(raw_post),
                
                # Análise de sentimento básica
                "sentiment": self._analyze_sentiment(raw_post.get('text', '')),
                
                # Métricas calculadas
                "engagement_rate": self._calculate_engagement_rate(raw_post),
                "virality_score": self._calculate_virality_score(raw_post)
            }
            
            return post
            
        except Exception as e:
            logger.warning(f"⚠️ Erro ao processar post individual: {e}")
            return None
    
    def _extract_media(self, raw_post: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extrai informações de mídia do post"""
        media = []
        
        # Imagens
        if raw_post.get('images'):
            for img in raw_post['images']:
                media.append({
                    "type": "image",
                    "url": img.get('url', ''),
                    "width": img.get('width', 0),
                    "height": img.get('height', 0)
                })
        
        # Vídeos
        if raw_post.get('videos'):
            for video in raw_post['videos']:
                media.append({
                    "type": "video",
                    "url": video.get('url', ''),
                    "thumbnail": video.get('thumbnail', ''),
                    "duration": video.get('duration', 0)
                })
        
        return media
    
    def _extract_comments(self, raw_post: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extrai comentários do post"""
        comments = []
        
        if raw_post.get('comments'):
            for comment in raw_post['comments']:
                comments.append({
                    "id": comment.get('commentId', ''),
                    "text": comment.get('text', ''),
                    "author": comment.get('authorName', ''),
                    "created_at": comment.get('publishedTime', ''),
                    "likes_count": comment.get('likesCount', 0),
                    "replies_count": comment.get('repliesCount', 0),
                    "sentiment": self._analyze_sentiment(comment.get('text', ''))
                })
        
        return comments
    
    def _analyze_sentiment(self, text: str) -> str:
        """Análise básica de sentimento"""
        if not text:
            return "neutral"
        
        text_lower = text.lower()
        
        # Palavras positivas em português
        positive_words = ['bom', 'ótimo', 'excelente', 'maravilhoso', 'incrível', 'perfeito', 'amor', 'feliz']
        # Palavras negativas em português
        negative_words = ['ruim', 'péssimo', 'terrível', 'horrível', 'ódio', 'triste', 'raiva']
        
        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)
        
        if positive_count > negative_count:
            return "positive"
        elif negative_count > positive_count:
            return "negative"
        else:
            return "neutral"
    
    def _calculate_engagement_rate(self, raw_post: Dict[str, Any]) -> float:
        """Calcula taxa de engajamento"""
        try:
            total_engagement = (
                raw_post.get('likesCount', 0) +
                raw_post.get('commentsCount', 0) +
                raw_post.get('sharesCount', 0)
            )
            
            # Estimativa de alcance baseada em engajamento
            estimated_reach = max(total_engagement * 10, 1000)
            
            return round((total_engagement / estimated_reach) * 100, 2)
        except:
            return 0.0
    
    def _calculate_virality_score(self, raw_post: Dict[str, Any]) -> float:
        """Calcula score de viralidade"""
        try:
            shares = raw_post.get('sharesCount', 0)
            comments = raw_post.get('commentsCount', 0)
            reactions = raw_post.get('reactionsCount', 0)
            
            # Fórmula ponderada para viralidade
            virality = (shares * 3) + (comments * 2) + (reactions * 1)
            
            # Normaliza para escala 0-100
            return min(round(virality / 100, 2), 100.0)
        except:
            return 0.0
    
    def _create_error_response(self, query: str, error: str) -> Dict[str, Any]:
        """Cria resposta de erro padronizada"""
        return {
            "success": False,
            "platform": "facebook",
            "query": query,
            "error": error,
            "posts": [],
            "total_posts": 0,
            "total_comments": 0,
            "total_reactions": 0,
            "scraped_at": datetime.now().isoformat()
        }

# Instância global
apify_facebook_scraper = ApifyFacebookScraper()

# Função para compatibilidade
def get_facebook_scraper():
    """Retorna a instância global do Facebook scraper"""
    return apify_facebook_scraper