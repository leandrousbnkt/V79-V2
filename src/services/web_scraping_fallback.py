#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v3.0 - Web Scraping Fallback System
Sistema de fallback para web scraping: Jina → EXA → Serper → BeautifulSoup
"""

import os
import logging
import asyncio
import aiohttp
import requests
from typing import Dict, List, Any, Optional
from urllib.parse import quote_plus
import json
from bs4 import BeautifulSoup

from .api_rotation_manager import api_rotation_manager

logger = logging.getLogger(__name__)

class WebScrapingFallback:
    """Sistema de fallback para web scraping"""
    
    def __init__(self):
        """Inicializa o sistema de fallback"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        logger.info("🔄 Web Scraping Fallback System inicializado")
    
    async def extract_content(self, url: str) -> Dict[str, Any]:
        """Extrai conteúdo com sistema de fallback completo"""
        logger.info(f"🔍 Iniciando extração com fallback para: {url}")
        
        # Tenta cada método em ordem de prioridade
        methods = [
            ('jina', self._extract_with_jina),
            ('exa', self._extract_with_exa),
            ('serper', self._extract_with_serper),
            ('beautifulsoup', self._extract_with_beautifulsoup)
        ]
        
        for method_name, method_func in methods:
            try:
                logger.info(f"🔧 Tentando {method_name.upper()} para {url}")
                result = await method_func(url)
                
                if result and result.get('success') and result.get('content'):
                    logger.info(f"✅ {method_name.upper()} extraiu {len(result['content'])} caracteres")
                    result['extraction_method'] = method_name
                    return result
                else:
                    logger.warning(f"⚠️ {method_name.upper()} não retornou conteúdo suficiente")
                    
            except Exception as e:
                logger.error(f"❌ Erro em {method_name.upper()}: {e}")
                continue
        
        # Se todos falharam
        logger.error(f"❌ Todos os métodos falharam para {url}")
        return {
            'success': False,
            'content': '',
            'error': 'Todos os métodos de extração falharam',
            'extraction_method': 'none'
        }
    
    async def _extract_with_jina(self, url: str) -> Dict[str, Any]:
        """Extrai conteúdo usando Jina Reader com rotação de chaves"""
        api_key = api_rotation_manager.get_api_key('JINA')
        if not api_key:
            raise Exception("Nenhuma chave Jina disponível")
        
        headers = {
            'Authorization': f'Bearer {api_key}',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        jina_url = f"https://r.jina.ai/{url}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(jina_url, headers=headers, timeout=20) as response:
                if response.status == 200:
                    content = await response.text()
                    
                    if len(content) > 500:  # Conteúdo mínimo
                        api_rotation_manager.report_success('JINA')
                        return {
                            'success': True,
                            'content': content,
                            'url': url,
                            'method': 'jina_reader'
                        }
                    else:
                        raise Exception("Conteúdo insuficiente do Jina")
                else:
                    raise Exception(f"Jina retornou status {response.status}")
    
    async def _extract_with_exa(self, url: str) -> Dict[str, Any]:
        """Extrai conteúdo usando EXA Neural Search"""
        api_key = api_rotation_manager.get_api_key('EXA')
        if not api_key:
            raise Exception("Nenhuma chave EXA disponível")
        
        headers = {
            'x-api-key': api_key,
            'Content-Type': 'application/json'
        }
        
        # EXA funciona melhor com busca por domínio
        from urllib.parse import urlparse
        domain = urlparse(url).netloc
        
        payload = {
            'query': f'site:{domain}',
            'type': 'neural',
            'useAutoprompt': True,
            'numResults': 1,
            'contents': {
                'text': True
            }
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                'https://api.exa.ai/search',
                headers=headers,
                json=payload,
                timeout=30
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get('results') and len(data['results']) > 0:
                        result = data['results'][0]
                        content = result.get('text', '')
                        
                        if len(content) > 300:
                            api_rotation_manager.report_success('EXA')
                            return {
                                'success': True,
                                'content': content,
                                'url': url,
                                'method': 'exa_neural'
                            }
                        else:
                            raise Exception("Conteúdo insuficiente do EXA")
                    else:
                        raise Exception("EXA não retornou resultados")
                else:
                    raise Exception(f"EXA retornou status {response.status}")
    
    async def _extract_with_serper(self, url: str) -> Dict[str, Any]:
        """Extrai conteúdo usando Serper Google Search"""
        api_key = api_rotation_manager.get_api_key('SERPER')
        if not api_key:
            raise Exception("Nenhuma chave Serper disponível")
        
        headers = {
            'X-API-KEY': api_key,
            'Content-Type': 'application/json'
        }
        
        # Busca pela URL específica
        payload = {
            'q': f'site:{url}',
            'num': 1
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                'https://google.serper.dev/search',
                headers=headers,
                json=payload,
                timeout=25
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get('organic') and len(data['organic']) > 0:
                        result = data['organic'][0]
                        snippet = result.get('snippet', '')
                        title = result.get('title', '')
                        
                        content = f"{title}\n\n{snippet}"
                        
                        if len(content) > 100:
                            api_rotation_manager.report_success('SERPER')
                            return {
                                'success': True,
                                'content': content,
                                'url': url,
                                'method': 'serper_search'
                            }
                        else:
                            raise Exception("Conteúdo insuficiente do Serper")
                    else:
                        raise Exception("Serper não retornou resultados")
                else:
                    raise Exception(f"Serper retornou status {response.status}")
    
    async def _extract_with_beautifulsoup(self, url: str) -> Dict[str, Any]:
        """Extrai conteúdo usando BeautifulSoup (fallback final)"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Remove scripts e styles
                    for script in soup(["script", "style"]):
                        script.decompose()
                    
                    # Extrai texto
                    text = soup.get_text()
                    
                    # Limpa texto
                    lines = (line.strip() for line in text.splitlines())
                    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                    content = ' '.join(chunk for chunk in chunks if chunk)
                    
                    if len(content) > 200:
                        return {
                            'success': True,
                            'content': content,
                            'url': url,
                            'method': 'beautifulsoup'
                        }
                    else:
                        raise Exception("Conteúdo insuficiente do BeautifulSoup")
                else:
                    raise Exception(f"BeautifulSoup retornou status {response.status}")
    
    async def batch_extract(self, urls: List[str], max_concurrent: int = 5) -> List[Dict[str, Any]]:
        """Extrai conteúdo de múltiplas URLs com fallback"""
        logger.info(f"🔍 Iniciando extração em lote de {len(urls)} URLs")
        
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def extract_single(url):
            async with semaphore:
                return await self.extract_content(url)
        
        tasks = [extract_single(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Processa resultados
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"❌ Erro na URL {urls[i]}: {result}")
                processed_results.append({
                    'success': False,
                    'url': urls[i],
                    'error': str(result),
                    'content': ''
                })
            else:
                processed_results.append(result)
        
        successful = sum(1 for r in processed_results if r.get('success'))
        logger.info(f"✅ Extração em lote concluída: {successful}/{len(urls)} sucessos")
        
        return processed_results

# Instância global
web_scraping_fallback = WebScrapingFallback()