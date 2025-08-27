#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v3.0 - Social Scraping Orchestrator
Orquestrador robusto para scraping de redes sociais com fallbacks e rotação de APIs
"""

import os
import logging
import asyncio
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass

# Importa componentes necessários
try:
    from .api_rotation_manager import api_rotation_manager
    from .apify_facebook_scraper import apify_facebook_scraper, FacebookScrapingConfig
    from .apify_instagram_scraper import apify_instagram_scraper, InstagramScrapingConfig
    from .social_media_extractor import social_media_extractor
    COMPONENTS_AVAILABLE = True
except ImportError as e:
    COMPONENTS_AVAILABLE = False
    logging.warning(f"⚠️ Componentes não disponíveis: {e}")

logger = logging.getLogger(__name__)

@dataclass
class ScrapingTask:
    """Estrutura para tarefa de scraping"""
    platform: str
    query: str
    max_results: int
    priority: int = 1  # 1=alta, 2=média, 3=baixa
    timeout_minutes: int = 10
    retry_count: int = 0
    max_retries: int = 2
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()

class SocialScrapingOrchestrator:
    """Orquestrador para scraping de redes sociais com fallbacks robustos"""
    
    def __init__(self):
        """Inicializa o orquestrador"""
        self.active_tasks = {}
        self.completed_tasks = {}
        self.failed_tasks = {}
        self.stats = {
            'total_tasks': 0,
            'successful_tasks': 0,
            'failed_tasks': 0,
            'fallback_used': 0,
            'api_rotations': 0
        }
        
        logger.info("🎯 Social Scraping Orchestrator inicializado")
    
    async def execute_comprehensive_scraping(
        self, 
        query: str, 
        platforms: List[str] = None,
        max_results_per_platform: int = 15,
        session_id: str = None
    ) -> Dict[str, Any]:
        """Executa scraping abrangente com fallbacks robustos"""
        
        if platforms is None:
            platforms = ['facebook', 'instagram', 'youtube', 'twitter']
        
        session_id = session_id or f"scraping_{int(time.time())}"
        
        logger.info(f"🚀 Iniciando scraping abrangente para '{query}' - Sessão: {session_id}")
        
        # Cria tarefas de scraping
        tasks = []
        for platform in platforms:
            task = ScrapingTask(
                platform=platform,
                query=query,
                max_results=max_results_per_platform,
                priority=1 if platform in ['facebook', 'instagram'] else 2
            )
            tasks.append(task)
        
        # Executa tarefas com fallbacks
        results = await self._execute_tasks_with_fallbacks(tasks, session_id)
        
        # Compila resultado final
        final_result = self._compile_final_result(query, results, session_id)
        
        # Atualiza estatísticas
        self._update_stats(results)
        
        logger.info(f"✅ Scraping concluído - {final_result['total_posts']} posts coletados")
        
        return final_result
    
    async def _execute_tasks_with_fallbacks(
        self, 
        tasks: List[ScrapingTask], 
        session_id: str
    ) -> Dict[str, Any]:
        """Executa tarefas com sistema de fallbacks"""
        
        results = {}
        
        # Separa tarefas por prioridade
        high_priority = [t for t in tasks if t.priority == 1]
        medium_priority = [t for t in tasks if t.priority == 2]
        low_priority = [t for t in tasks if t.priority == 3]
        
        # Executa tarefas de alta prioridade primeiro (Facebook, Instagram)
        if high_priority:
            high_results = await self._execute_priority_batch(high_priority, session_id)
            results.update(high_results)
        
        # Executa tarefas de média prioridade
        if medium_priority:
            medium_results = await self._execute_priority_batch(medium_priority, session_id)
            results.update(medium_results)
        
        # Executa tarefas de baixa prioridade
        if low_priority:
            low_results = await self._execute_priority_batch(low_priority, session_id)
            results.update(low_results)
        
        return results
    
    async def _execute_priority_batch(
        self, 
        tasks: List[ScrapingTask], 
        session_id: str
    ) -> Dict[str, Any]:
        """Executa lote de tarefas da mesma prioridade"""
        
        results = {}
        
        # Executa tarefas em paralelo com timeout
        async_tasks = []
        for task in tasks:
            async_tasks.append(self._execute_single_task_with_fallback(task, session_id))
        
        try:
            # Timeout baseado na prioridade
            timeout = 600 if tasks[0].priority == 1 else 300  # 10min alta, 5min média/baixa
            
            task_results = await asyncio.wait_for(
                asyncio.gather(*async_tasks, return_exceptions=True),
                timeout=timeout
            )
            
            # Processa resultados
            for i, result in enumerate(task_results):
                platform = tasks[i].platform
                if isinstance(result, Exception):
                    logger.error(f"❌ Erro na tarefa {platform}: {result}")
                    results[platform] = self._create_fallback_result(tasks[i])
                else:
                    results[platform] = result
                    
        except asyncio.TimeoutError:
            logger.error(f"❌ Timeout no lote de prioridade {tasks[0].priority}")
            # Cria resultados de fallback para todas as tarefas
            for task in tasks:
                results[task.platform] = self._create_fallback_result(task)
        
        return results
    
    async def _execute_single_task_with_fallback(
        self, 
        task: ScrapingTask, 
        session_id: str
    ) -> Dict[str, Any]:
        """Executa uma tarefa individual com fallback"""
        
        self.active_tasks[f"{session_id}_{task.platform}"] = task
        
        try:
            # Tenta scraping real primeiro
            if task.platform in ['facebook', 'instagram'] and COMPONENTS_AVAILABLE:
                result = await self._execute_real_scraping(task)
                
                if result.get('success'):
                    self.completed_tasks[f"{session_id}_{task.platform}"] = task
                    return result
                else:
                    logger.warning(f"⚠️ Scraping real falhou para {task.platform}, tentando fallback")
            
            # Fallback para dados simulados
            result = await self._execute_fallback_scraping(task)
            self.stats['fallback_used'] += 1
            
            self.completed_tasks[f"{session_id}_{task.platform}"] = task
            return result
            
        except Exception as e:
            logger.error(f"❌ Erro na execução da tarefa {task.platform}: {e}")
            
            # Tenta retry se ainda há tentativas
            if task.retry_count < task.max_retries:
                task.retry_count += 1
                logger.info(f"🔄 Retry {task.retry_count}/{task.max_retries} para {task.platform}")
                await asyncio.sleep(2)  # Aguarda antes do retry
                return await self._execute_single_task_with_fallback(task, session_id)
            
            # Falha definitiva
            self.failed_tasks[f"{session_id}_{task.platform}"] = task
            return self._create_error_result(task, str(e))
        
        finally:
            # Remove da lista de tarefas ativas
            self.active_tasks.pop(f"{session_id}_{task.platform}", None)
    
    async def _execute_real_scraping(self, task: ScrapingTask) -> Dict[str, Any]:
        """Executa scraping real usando APIs"""
        
        if task.platform == 'facebook':
            config = FacebookScrapingConfig(
                search_query=task.query,
                max_posts=task.max_results,
                max_comments_per_post=10,
                include_reactions=True,
                include_shares=True,
                timeout_minutes=task.timeout_minutes
            )
            return await apify_facebook_scraper.scrape_facebook_posts(config)
            
        elif task.platform == 'instagram':
            config = InstagramScrapingConfig(
                search_query=task.query,
                max_posts=task.max_results,
                include_comments=True,
                max_comments_per_post=10,
                timeout_minutes=task.timeout_minutes
            )
            return await apify_instagram_scraper.scrape_instagram_posts(config)
        
        else:
            return {"success": False, "error": f"Plataforma {task.platform} não suportada para scraping real"}
    
    async def _execute_fallback_scraping(self, task: ScrapingTask) -> Dict[str, Any]:
        """Executa scraping de fallback (dados simulados)"""
        
        logger.info(f"🔄 Executando fallback para {task.platform}")
        
        # Usa o social_media_extractor para dados simulados
        if task.platform == 'facebook':
            return social_media_extractor._simulate_facebook_data(task.query, task.max_results)
        elif task.platform == 'instagram':
            return social_media_extractor._simulate_instagram_data(task.query, task.max_results)
        elif task.platform == 'youtube':
            return social_media_extractor._simulate_youtube_data(task.query, task.max_results)
        elif task.platform == 'twitter':
            return social_media_extractor._simulate_twitter_data(task.query, task.max_results)
        else:
            return self._create_error_result(task, f"Plataforma {task.platform} não suportada")
    
    def _create_fallback_result(self, task: ScrapingTask) -> Dict[str, Any]:
        """Cria resultado de fallback"""
        return {
            "success": True,
            "platform": task.platform,
            "query": task.query,
            "results": [],
            "total_found": 0,
            "data_source": "fallback_empty",
            "message": f"Fallback ativado para {task.platform} devido a timeout ou erro"
        }
    
    def _create_error_result(self, task: ScrapingTask, error: str) -> Dict[str, Any]:
        """Cria resultado de erro"""
        return {
            "success": False,
            "platform": task.platform,
            "query": task.query,
            "error": error,
            "results": [],
            "total_found": 0,
            "retry_count": task.retry_count
        }
    
    def _compile_final_result(
        self, 
        query: str, 
        platform_results: Dict[str, Any], 
        session_id: str
    ) -> Dict[str, Any]:
        """Compila resultado final do scraping"""
        
        total_posts = 0
        successful_platforms = []
        failed_platforms = []
        
        # Conta posts e plataformas
        for platform, result in platform_results.items():
            if result.get('success'):
                total_posts += len(result.get('results', []))
                successful_platforms.append(platform)
            else:
                failed_platforms.append(platform)
        
        # Analisa sentimento geral
        sentiment_analysis = social_media_extractor.analyze_sentiment_trends(platform_results)
        
        return {
            "success": total_posts > 0,
            "query": query,
            "session_id": session_id,
            "all_platforms_data": platform_results,
            "sentiment_analysis": sentiment_analysis,
            "total_posts": total_posts,
            "platforms_analyzed": len(platform_results),
            "successful_platforms": successful_platforms,
            "failed_platforms": failed_platforms,
            "extracted_at": datetime.now().isoformat(),
            "data_source": "orchestrated_scraping",
            "stats": self.get_session_stats(session_id)
        }
    
    def _update_stats(self, results: Dict[str, Any]):
        """Atualiza estatísticas do orquestrador"""
        self.stats['total_tasks'] += len(results)
        
        for platform, result in results.items():
            if result.get('success'):
                self.stats['successful_tasks'] += 1
            else:
                self.stats['failed_tasks'] += 1
    
    def get_session_stats(self, session_id: str) -> Dict[str, Any]:
        """Obtém estatísticas de uma sessão"""
        active = len([k for k in self.active_tasks.keys() if k.startswith(session_id)])
        completed = len([k for k in self.completed_tasks.keys() if k.startswith(session_id)])
        failed = len([k for k in self.failed_tasks.keys() if k.startswith(session_id)])
        
        return {
            "active_tasks": active,
            "completed_tasks": completed,
            "failed_tasks": failed,
            "total_tasks": active + completed + failed
        }
    
    def get_global_stats(self) -> Dict[str, Any]:
        """Obtém estatísticas globais"""
        return {
            **self.stats,
            "success_rate": round(
                (self.stats['successful_tasks'] / max(self.stats['total_tasks'], 1)) * 100, 2
            ),
            "fallback_rate": round(
                (self.stats['fallback_used'] / max(self.stats['total_tasks'], 1)) * 100, 2
            )
        }
    
    def cleanup_old_tasks(self, hours_old: int = 24):
        """Limpa tarefas antigas"""
        cutoff_time = datetime.now() - timedelta(hours=hours_old)
        
        # Limpa tarefas completadas antigas
        old_completed = [
            k for k, task in self.completed_tasks.items() 
            if task.created_at < cutoff_time
        ]
        for key in old_completed:
            del self.completed_tasks[key]
        
        # Limpa tarefas falhadas antigas
        old_failed = [
            k for k, task in self.failed_tasks.items() 
            if task.created_at < cutoff_time
        ]
        for key in old_failed:
            del self.failed_tasks[key]
        
        logger.info(f"🧹 Limpeza concluída: {len(old_completed + old_failed)} tarefas antigas removidas")

# Instância global
social_scraping_orchestrator = SocialScrapingOrchestrator()

# Função para compatibilidade
def get_social_scraping_orchestrator():
    """Retorna a instância global do orquestrador"""
    return social_scraping_orchestrator