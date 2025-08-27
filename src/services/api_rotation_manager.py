#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v3.0 - API Rotation Manager
Sistema centralizado de rotação de APIs e fallbacks
"""

import os
import logging
import asyncio
import time
from typing import Dict, List, Any, Optional, Callable, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from pathlib import Path

# Carrega variáveis de ambiente
try:
    from .environment_loader import EnvironmentLoader
    env_loader = EnvironmentLoader()
    env_loader.load_environment()
except ImportError:
    # Fallback manual se environment_loader não estiver disponível
    env_file = Path(__file__).parent.parent.parent / '.env'
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value

logger = logging.getLogger(__name__)

@dataclass
class APIProvider:
    """Estrutura para provedor de API"""
    name: str
    keys: List[str]
    current_index: int = 0
    failures: int = 0
    last_failure: Optional[datetime] = None
    is_healthy: bool = True
    timeout: int = 30
    max_failures: int = 3
    cooldown_minutes: int = 5

@dataclass
class ServiceConfig:
    """Configuração de serviço com fallbacks"""
    name: str
    primary_providers: List[str]
    fallback_providers: List[str]
    current_provider_index: int = 0
    last_rotation: Optional[datetime] = None

class APIRotationManager:
    """Gerenciador centralizado de rotação de APIs e fallbacks"""
    
    def __init__(self):
        """Inicializa o gerenciador"""
        self.providers: Dict[str, APIProvider] = {}
        self.services: Dict[str, ServiceConfig] = {}
        self.stats_file = Path("api_rotation_stats.json")
        
        # Carrega configurações
        self._load_api_providers()
        self._setup_service_configs()
        self._load_stats()
        
        logger.info(f"🔄 API Rotation Manager inicializado com {len(self.providers)} provedores")
    
    def _load_api_providers(self):
        """Carrega todos os provedores de API do ambiente"""
        
        # Mapeamento de provedores e suas configurações
        provider_configs = {
            'JINA': {'timeout': 20, 'max_failures': 2},
            'EXA': {'timeout': 30, 'max_failures': 3},
            'SERPER': {'timeout': 25, 'max_failures': 3},
            'FIRECRAWL': {'timeout': 45, 'max_failures': 2},
            'OPENROUTER': {'timeout': 60, 'max_failures': 2},
            'GEMINI': {'timeout': 45, 'max_failures': 2},
            'GROQ': {'timeout': 30, 'max_failures': 3},
            'SUPADATA': {'timeout': 35, 'max_failures': 3},
            'GOOGLE': {'timeout': 20, 'max_failures': 3},
            'YOUTUBE': {'timeout': 25, 'max_failures': 3},
            'TAVILY': {'timeout': 30, 'max_failures': 3},
            'APIFY': {'timeout': 900, 'max_failures': 2, 'cooldown_minutes': 10}  # 15 min timeout para scraping
        }
        
        for provider_name, config in provider_configs.items():
            keys = self._get_provider_keys(provider_name)
            if keys:
                self.providers[provider_name] = APIProvider(
                    name=provider_name,
                    keys=keys,
                    timeout=config['timeout'],
                    max_failures=config['max_failures'],
                    cooldown_minutes=config.get('cooldown_minutes', 5)
                )
                logger.info(f"✅ {provider_name}: {len(keys)} chaves carregadas")
    
    def _get_provider_keys(self, provider: str) -> List[str]:
        """Obtém todas as chaves de um provedor"""
        keys = []
        
        # Chave principal
        main_key = os.getenv(f"{provider}_API_KEY")
        if main_key:
            keys.append(main_key)
        
        # Chaves numeradas
        counter = 1
        while True:
            numbered_key = os.getenv(f"{provider}_API_KEY_{counter}")
            if numbered_key:
                keys.append(numbered_key)
                counter += 1
            else:
                break
        
        return keys
    
    def _setup_service_configs(self):
        """Configura os serviços com seus fallbacks"""
        
        # Configurações de serviços
        service_configs = {
            'web_scraping': {
                'primary_providers': ['JINA', 'FIRECRAWL'],
                'fallback_providers': ['EXA', 'SERPER', 'GOOGLE']
            },
            'search_engines': {
                'primary_providers': ['JINA', 'EXA'],
                'fallback_providers': ['SERPER', 'GOOGLE', 'TAVILY']
            },
            'ai_models': {
                'primary_providers': ['OPENROUTER'],
                'fallback_providers': ['GEMINI', 'GROQ']
            },
            'social_data': {
                'primary_providers': ['SUPADATA'],
                'fallback_providers': ['YOUTUBE', 'GOOGLE']
            },
            'social_scraping': {
                'primary_providers': ['APIFY'],
                'fallback_providers': ['SUPADATA', 'YOUTUBE']
            }
        }
        
        for service_name, config in service_configs.items():
            # Filtra apenas provedores que têm chaves disponíveis
            available_primary = [p for p in config['primary_providers'] if p in self.providers]
            available_fallback = [p for p in config['fallback_providers'] if p in self.providers]
            
            if available_primary or available_fallback:
                self.services[service_name] = ServiceConfig(
                    name=service_name,
                    primary_providers=available_primary,
                    fallback_providers=available_fallback
                )
                logger.info(f"🔧 Serviço {service_name}: {len(available_primary)} primários, {len(available_fallback)} fallbacks")
    
    def get_api_key(self, provider: str, rotate: bool = True) -> Optional[str]:
        """Obtém chave de API com rotação automática"""
        if provider not in self.providers:
            logger.warning(f"⚠️ Provedor {provider} não encontrado")
            return None
        
        provider_obj = self.providers[provider]
        
        # Verifica se o provedor está em cooldown
        if not provider_obj.is_healthy and provider_obj.last_failure:
            cooldown_end = provider_obj.last_failure + timedelta(minutes=provider_obj.cooldown_minutes)
            if datetime.now() < cooldown_end:
                logger.warning(f"⚠️ {provider} em cooldown até {cooldown_end.strftime('%H:%M:%S')}")
                return None
            else:
                # Sai do cooldown
                provider_obj.is_healthy = True
                provider_obj.failures = 0
                logger.info(f"✅ {provider} saiu do cooldown")
        
        # Obtém chave atual
        current_key = provider_obj.keys[provider_obj.current_index]
        
        # Rotaciona para próxima chave se solicitado
        if rotate and len(provider_obj.keys) > 1:
            provider_obj.current_index = (provider_obj.current_index + 1) % len(provider_obj.keys)
            logger.debug(f"🔄 {provider}: Rotacionado para chave {provider_obj.current_index + 1}/{len(provider_obj.keys)}")
        
        return current_key
    
    def report_failure(self, provider: str, error: str = ""):
        """Reporta falha de um provedor"""
        if provider not in self.providers:
            return
        
        provider_obj = self.providers[provider]
        provider_obj.failures += 1
        provider_obj.last_failure = datetime.now()
        
        logger.warning(f"⚠️ Falha reportada para {provider}: {error} (Total: {provider_obj.failures})")
        
        # Se excedeu o limite de falhas, marca como não saudável
        if provider_obj.failures >= provider_obj.max_failures:
            provider_obj.is_healthy = False
            logger.error(f"❌ {provider} marcado como não saudável após {provider_obj.failures} falhas")
        
        self._save_stats()
    
    def report_success(self, provider: str):
        """Reporta sucesso de um provedor"""
        if provider not in self.providers:
            return
        
        provider_obj = self.providers[provider]
        if provider_obj.failures > 0:
            provider_obj.failures = max(0, provider_obj.failures - 1)
            logger.debug(f"✅ Sucesso para {provider}, falhas reduzidas para {provider_obj.failures}")
        
        if not provider_obj.is_healthy and provider_obj.failures == 0:
            provider_obj.is_healthy = True
            logger.info(f"✅ {provider} restaurado para saudável")
    
    def get_service_provider(self, service: str, attempt: int = 0) -> Optional[Tuple[str, str]]:
        """Obtém provedor para um serviço com fallback automático"""
        if service not in self.services:
            logger.warning(f"⚠️ Serviço {service} não configurado")
            return None
        
        service_config = self.services[service]
        all_providers = service_config.primary_providers + service_config.fallback_providers
        
        if attempt >= len(all_providers):
            logger.error(f"❌ Todos os provedores falharam para {service}")
            return None
        
        provider_name = all_providers[attempt]
        api_key = self.get_api_key(provider_name)
        
        if api_key:
            provider_type = "primary" if provider_name in service_config.primary_providers else "fallback"
            logger.info(f"🔧 {service}: Usando {provider_name} ({provider_type}) - tentativa {attempt + 1}")
            return provider_name, api_key
        else:
            # Tenta próximo provedor
            return self.get_service_provider(service, attempt + 1)
    
    async def execute_with_fallback(
        self, 
        service: str, 
        operation: Callable,
        *args, 
        **kwargs
    ) -> Any:
        """Executa operação com fallback automático entre provedores"""
        
        attempt = 0
        last_error = None
        
        while True:
            provider_info = self.get_service_provider(service, attempt)
            
            if not provider_info:
                logger.error(f"❌ Nenhum provedor disponível para {service}")
                if last_error:
                    raise last_error
                else:
                    raise Exception(f"Nenhum provedor disponível para {service}")
            
            provider_name, api_key = provider_info
            
            try:
                # Adiciona informações do provedor aos kwargs
                kwargs['provider_name'] = provider_name
                kwargs['api_key'] = api_key
                kwargs['timeout'] = self.providers[provider_name].timeout
                
                # Executa operação
                if asyncio.iscoroutinefunction(operation):
                    result = await operation(*args, **kwargs)
                else:
                    result = operation(*args, **kwargs)
                
                # Reporta sucesso
                self.report_success(provider_name)
                return result
                
            except Exception as e:
                last_error = e
                logger.error(f"❌ Falha em {provider_name} para {service}: {e}")
                
                # Reporta falha
                self.report_failure(provider_name, str(e))
                
                # Tenta próximo provedor
                attempt += 1
                
                # Aguarda um pouco antes da próxima tentativa
                if attempt < 3:  # Só aguarda se não for a última tentativa
                    await asyncio.sleep(1)
    
    def get_provider_status(self) -> Dict[str, Any]:
        """Obtém status de todos os provedores"""
        status = {}
        
        for name, provider in self.providers.items():
            status[name] = {
                'healthy': provider.is_healthy,
                'failures': provider.failures,
                'total_keys': len(provider.keys),
                'current_key_index': provider.current_index,
                'last_failure': provider.last_failure.isoformat() if provider.last_failure else None,
                'timeout': provider.timeout
            }
        
        return status
    
    def _load_stats(self):
        """Carrega estatísticas salvas"""
        try:
            if self.stats_file.exists():
                with open(self.stats_file, 'r') as f:
                    stats = json.load(f)
                
                for provider_name, provider_stats in stats.items():
                    if provider_name in self.providers:
                        provider = self.providers[provider_name]
                        provider.failures = provider_stats.get('failures', 0)
                        provider.is_healthy = provider_stats.get('is_healthy', True)
                        
                        if provider_stats.get('last_failure'):
                            provider.last_failure = datetime.fromisoformat(provider_stats['last_failure'])
                
                logger.info("📊 Estatísticas de API carregadas")
        except Exception as e:
            logger.warning(f"⚠️ Erro ao carregar estatísticas: {e}")
    
    def _save_stats(self):
        """Salva estatísticas"""
        try:
            stats = {}
            for name, provider in self.providers.items():
                stats[name] = {
                    'failures': provider.failures,
                    'is_healthy': provider.is_healthy,
                    'last_failure': provider.last_failure.isoformat() if provider.last_failure else None
                }
            
            with open(self.stats_file, 'w') as f:
                json.dump(stats, f, indent=2)
                
        except Exception as e:
            logger.warning(f"⚠️ Erro ao salvar estatísticas: {e}")

# Instância global
api_rotation_manager = APIRotationManager()