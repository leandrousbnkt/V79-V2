#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v2.0 - Unified Search Manager
Gerenciador unificado de busca com Exa, Google, Serper e outros provedores
"""

import os
import logging
import time
import requests
from typing import Dict, List, Optional, Any
from urllib.parse import quote_plus
from bs4 import BeautifulSoup
import json
import random
from datetime import datetime
from services.exa_client import exa_client
from services.auto_save_manager import salvar_etapa, salvar_erro

logger = logging.getLogger(__name__)

class UnifiedSearchManager:
    """Gerenciador unificado de busca com múltiplos provedores"""
    
    def __init__(self):
        """Inicializa o gerenciador unificado"""
        self.providers = {
            'exa': {
                'enabled': exa_client.is_available(),
                'priority': 1,  # Prioridade máxima
                'error_count': 0,
                'max_errors': 3,
                'client': exa_client
            },
            'google': {
                'enabled': bool(os.getenv('GOOGLE_SEARCH_KEY') and os.getenv('GOOGLE_CSE_ID')),
                'priority': 2,
                'error_count': 0,
                'max_errors': 3,
                'api_key': os.getenv('GOOGLE_SEARCH_KEY'),
                'cse_id': os.getenv('GOOGLE_CSE_ID'),
                'base_url': 'https://www.googleapis.com/customsearch/v1'
            },
            'serper': {
                'enabled': bool(os.getenv('SERPER_API_KEY')),
                'priority': 3,
                'error_count': 0,
                'max_errors': 3,
                'api_key': os.getenv('SERPER_API_KEY'),
                'base_url': 'https://google.serper.dev/search'
            },
            'bing': {
                'enabled': True,  # Sempre disponível via scraping
                'priority': 4,
                'error_count': 0,
                'max_errors': 5,
                'base_url': 'https://www.bing.com/search'
            },
            'duckduckgo': {
                'enabled': True,  # Sempre disponível via scraping
                'priority': 5,
                'error_count': 0,
                'max_errors': 5,
                'base_url': 'https://html.duckduckgo.com/html/'
            }
        }
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive'
        }
        
        # Domínios brasileiros preferenciais
        self.preferred_domains = [
            "g1.globo.com", "exame.com", "valor.globo.com", "estadao.com.br",
            "folha.uol.com.br", "canaltech.com.br", "tecmundo.com.br",
            "olhardigital.com.br", "infomoney.com.br", "startse.com",
            "revistapegn.globo.com", "epocanegocios.globo.com", "istoedinheiro.com.br"
        ]
        
        enabled_count = sum(1 for p in self.providers.values() if p['enabled'])
        logger.info(f"🔍 Unified Search Manager inicializado com {enabled_count} provedores")
    
    def unified_search(
        self, 
        query: str, 
        max_results: int = 20,
        context: Dict[str, Any] = None,
        session_id: str = None
    ) -> Dict[str, Any]:
        """Realiza busca unificada com todos os provedores disponíveis"""
        
        logger.info(f"🔍 Iniciando busca unificada para: {query}")
        start_time = time.time()
        
        # Salva início da busca
        salvar_etapa("busca_unificada_iniciada", {
            "query": query,
            "max_results": max_results,
            "context": context,
            "providers_enabled": [name for name, p in self.providers.items() if p['enabled']]
        }, categoria="pesquisa_web")
        
        all_results = []
        provider_results = {}
        
        # Executa buscas em paralelo para Exa e Google
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        parallel_searches = []
        
        # 1. Busca com Exa (prioridade máxima)
        if self.providers['exa']['enabled']:
            parallel_searches.append(('exa', self._search_with_exa, query, max_results // 2, context))
        
        # 2. Busca com Google Custom Search
        if self.providers['google']['enabled']:
            parallel_searches.append(('google', self._search_google, query, max_results // 3, None))
        
        # Executa buscas paralelas
        if parallel_searches:
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_to_provider = {}
                
                for provider_name, search_func, search_query, max_res, ctx in parallel_searches:
                    if provider_name == 'exa':
                        future = executor.submit(search_func, search_query, max_res, ctx)
                    else:
                        future = executor.submit(search_func, search_query, max_res)
                    future_to_provider[future] = provider_name
                
                for future in as_completed(future_to_provider, timeout=30):
                    provider_name = future_to_provider[future]
                    try:
                        results = future.result()
                        if results:
                            all_results.extend(results)
                            provider_results[provider_name] = results
                            logger.info(f"✅ {provider_name}: {len(results)} resultados")
                    except Exception as e:
                        logger.error(f"❌ Erro no {provider_name}: {e}")
                        self._record_provider_error(provider_name)
        
        # 3. Busca sequencial com outros provedores
        sequential_providers = ['serper', 'bing', 'duckduckgo']
        
        for provider_name in sequential_providers:
            if not self.providers[provider_name]['enabled']:
                continue
                
            try:
                logger.info(f"🔍 Executando busca com {provider_name}...")
                
                if provider_name == 'serper':
                    results = self._search_serper(query, max_results // 3)
                elif provider_name == 'bing':
                    results = self._search_bing(query, max_results // 4)
                elif provider_name == 'duckduckgo':
                    results = self._search_duckduckgo(query, max_results // 4)
                else:
                    continue
                
                if results:
                    all_results.extend(results)
                    provider_results[provider_name] = results
                    logger.info(f"✅ {provider_name}: {len(results)} resultados")
                
            except Exception as e:
                logger.error(f"❌ Erro no {provider_name}: {e}")
                self._record_provider_error(provider_name)
                continue
        
        # Remove duplicatas por URL
        unique_results = self._remove_duplicates(all_results)
        
        # Prioriza domínios brasileiros
        prioritized_results = self._prioritize_brazilian_sources(unique_results)
        
        # Calcula métricas
        search_time = time.time() - start_time
        
        unified_result = {
            'query': query,
            'context': context,
            'results': prioritized_results,
            'provider_results': provider_results,
            'statistics': {
                'total_results': len(prioritized_results),
                'providers_used': len(provider_results),
                'search_time': search_time,
                'brazilian_sources': sum(1 for r in prioritized_results if r.get('is_brazilian')),
                'preferred_sources': sum(1 for r in prioritized_results if r.get('is_preferred'))
            },
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'session_id': session_id,
                'search_type': 'unified_multi_provider'
            }
        }
        
        # Salva resultado da busca
        salvar_etapa("busca_unificada_resultado", unified_result, categoria="pesquisa_web")
        
        logger.info(f"✅ Busca unificada concluída: {len(prioritized_results)} resultados únicos em {search_time:.2f}s")
        
        return unified_result
    
    def _search_with_exa(self, query: str, max_results: int, context: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Busca usando Exa API"""
        
        try:
            # Melhora query para mercado brasileiro
            enhanced_query = self._enhance_query_for_brazil(query)
            
            # Configura domínios preferenciais
            include_domains = self.preferred_domains if context else None
            
            # Busca com Exa
            exa_response = exa_client.search(
                query=enhanced_query,
                num_results=max_results,
                include_domains=include_domains,
                start_published_date="2023-01-01",  # Últimos 2 anos
                use_autoprompt=True,
                type="neural"
            )
            
            if not exa_response or 'results' not in exa_response:
                return []
            
            # Converte resultados para formato padrão
            results = []
            for item in exa_response['results']:
                results.append({
                    'title': item.get('title', ''),
                    'url': item.get('url', ''),
                    'snippet': item.get('text', '')[:300],  # Limita snippet
                    'source': 'exa',
                    'score': item.get('score', 0),
                    'published_date': item.get('publishedDate', ''),
                    'author': item.get('author', ''),
                    'exa_id': item.get('id', '')
                })
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Erro na busca Exa: {e}")
            return []
    
    def _search_google(self, query: str, max_results: int) -> List[Dict[str, Any]]:
        """Busca usando Google Custom Search API"""
        
        provider = self.providers['google']
        
        try:
            enhanced_query = self._enhance_query_for_brazil(query)
            
            params = {
                'key': provider['api_key'],
                'cx': provider['cse_id'],
                'q': enhanced_query,
                'num': min(max_results, 10),
                'lr': 'lang_pt',
                'gl': 'br',
                'safe': 'off',
                'dateRestrict': 'm12'  # Últimos 12 meses
            }
            
            response = requests.get(
                provider['base_url'],
                params=params,
                headers=self.headers,
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                results = []
                
                for item in data.get('items', []):
                    results.append({
                        'title': item.get('title', ''),
                        'url': item.get('link', ''),
                        'snippet': item.get('snippet', ''),
                        'source': 'google'
                    })
                
                return results
            else:
                raise Exception(f"Google API retornou status {response.status_code}")
                
        except Exception as e:
            raise e
    
    def _search_serper(self, query: str, max_results: int) -> List[Dict[str, Any]]:
        """Busca usando Serper API"""
        
        provider = self.providers['serper']
        
        try:
            enhanced_query = self._enhance_query_for_brazil(query)
            
            headers = {
                **self.headers,
                'X-API-KEY': provider['api_key'],
                'Content-Type': 'application/json'
            }
            
            payload = {
                'q': enhanced_query,
                'gl': 'br',
                'hl': 'pt',
                'num': max_results
            }
            
            response = requests.post(
                provider['base_url'],
                json=payload,
                headers=headers,
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                results = []
                
                for item in data.get('organic', []):
                    results.append({
                        'title': item.get('title', ''),
                        'url': item.get('link', ''),
                        'snippet': item.get('snippet', ''),
                        'source': 'serper'
                    })
                
                return results
            else:
                raise Exception(f"Serper API retornou status {response.status_code}")
                
        except Exception as e:
            raise e
    
    def _search_bing(self, query: str, max_results: int) -> List[Dict[str, Any]]:
        """Busca usando Bing (scraping)"""
        
        try:
            enhanced_query = self._enhance_query_for_brazil(query)
            search_url = f"{self.providers['bing']['base_url']}?q={quote_plus(enhanced_query)}&cc=br&setlang=pt-br&count={max_results}"
            
            response = requests.get(search_url, headers=self.headers, timeout=15)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                results = []
                
                result_items = soup.find_all('li', class_='b_algo')
                
                for item in result_items[:max_results]:
                    title_elem = item.find('h2')
                    if title_elem:
                        link_elem = title_elem.find('a')
                        if link_elem:
                            title = title_elem.get_text(strip=True)
                            url = link_elem.get('href', '')
                            
                            snippet_elem = item.find('p')
                            snippet = snippet_elem.get_text(strip=True) if snippet_elem else ""
                            
                            if url and title and url.startswith('http'):
                                results.append({
                                    'title': title,
                                    'url': url,
                                    'snippet': snippet,
                                    'source': 'bing'
                                })
                
                return results
            else:
                raise Exception(f"Bing retornou status {response.status_code}")
                
        except Exception as e:
            raise e
    
    def _search_duckduckgo(self, query: str, max_results: int) -> List[Dict[str, Any]]:
        """Busca usando DuckDuckGo (scraping)"""
        
        try:
            enhanced_query = self._enhance_query_for_brazil(query)
            search_url = f"{self.providers['duckduckgo']['base_url']}?q={quote_plus(enhanced_query)}"
            
            response = requests.get(search_url, headers=self.headers, timeout=15)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                results = []
                
                result_divs = soup.find_all('div', class_='result')
                
                for div in result_divs[:max_results]:
                    title_elem = div.find('a', class_='result__a')
                    snippet_elem = div.find('a', class_='result__snippet')
                    
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                        url = title_elem.get('href', '')
                        snippet = snippet_elem.get_text(strip=True) if snippet_elem else ""
                        
                        if url and title and url.startswith('http'):
                            results.append({
                                'title': title,
                                'url': url,
                                'snippet': snippet,
                                'source': 'duckduckgo'
                            })
                
                return results
            else:
                raise Exception(f"DuckDuckGo retornou status {response.status_code}")
                
        except Exception as e:
            raise e
    
    def _enhance_query_for_brazil(self, query: str) -> str:
        """Melhora query para pesquisa no Brasil"""
        
        enhanced_query = query
        query_lower = query.lower()
        
        # Adiciona termos brasileiros se não estiverem presentes
        if not any(term in query_lower for term in ["brasil", "brasileiro", "br"]):
            enhanced_query += " Brasil"
        
        # Adiciona ano atual se não estiver presente
        if not any(year in query for year in ["2024", "2025"]):
            enhanced_query += " 2024"
        
        return enhanced_query.strip()
    
    def _remove_duplicates(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicatas baseado na URL"""
        
        seen_urls = set()
        unique_results = []
        
        for result in results:
            url = result.get('url', '')
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_results.append(result)
        
        return unique_results
    
    def _prioritize_brazilian_sources(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prioriza fontes brasileiras"""
        
        for result in results:
            url = result.get('url', '')
            domain = url.split('/')[2] if len(url.split('/')) > 2 else ''
            
            # Marca se é fonte brasileira
            result['is_brazilian'] = (
                domain.endswith('.br') or 
                'brasil' in domain.lower() or
                any(pref in domain for pref in self.preferred_domains)
            )
            
            # Marca se é fonte preferencial
            result['is_preferred'] = any(pref in domain for pref in self.preferred_domains)
            
            # Calcula score de prioridade
            priority_score = 1.0
            
            if result['is_preferred']:
                priority_score += 3.0
            elif result['is_brazilian']:
                priority_score += 2.0
            
            # Bonus por fonte Exa
            if result.get('source') == 'exa':
                priority_score += 1.5
            
            result['priority_score'] = priority_score
        
        # Ordena por prioridade
        results.sort(key=lambda x: x.get('priority_score', 0), reverse=True)
        
        return results
    
    def _record_provider_error(self, provider_name: str):
        """Registra erro do provedor"""
        if provider_name in self.providers:
            self.providers[provider_name]['error_count'] += 1
            
            if self.providers[provider_name]['error_count'] >= self.providers[provider_name]['max_errors']:
                logger.warning(f"⚠️ Provedor {provider_name} desabilitado temporariamente")
                self.providers[provider_name]['enabled'] = False
    
    def get_provider_status(self) -> Dict[str, Any]:
        """Retorna status de todos os provedores"""
        status = {}
        
        for name, provider in self.providers.items():
            status[name] = {
                'enabled': provider['enabled'],
                'priority': provider['priority'],
                'error_count': provider['error_count'],
                'max_errors': provider['max_errors'],
                'available': provider['enabled'] and provider['error_count'] < provider['max_errors']
            }
        
        return status
    
    def reset_provider_errors(self, provider_name: str = None):
        """Reset contadores de erro dos provedores"""
        if provider_name:
            if provider_name in self.providers:
                self.providers[provider_name]['error_count'] = 0
                self.providers[provider_name]['enabled'] = True
                logger.info(f"🔄 Reset erros do provedor: {provider_name}")
        else:
            for provider in self.providers.values():
                provider['error_count'] = 0
                provider['enabled'] = True
            logger.info("🔄 Reset erros de todos os provedores")

# Instância global
unified_search_manager = UnifiedSearchManager()