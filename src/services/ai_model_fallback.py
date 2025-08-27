#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v3.0 - AI Model Fallback System
Sistema de fallback para modelos de AI: OpenRouter (Qwen) → Gemini → Groq
"""

import os
import logging
import asyncio
from typing import Dict, List, Any, Optional, Union
import json

from .api_rotation_manager import api_rotation_manager

logger = logging.getLogger(__name__)

class AIModelFallback:
    """Sistema de fallback para modelos de AI"""
    
    def __init__(self):
        """Inicializa o sistema de fallback"""
        self.model_configs = {
            'OPENROUTER': {
                'models': ['qwen-2.5-72b-instruct', 'qwen-2.5-32b-instruct'],
                'base_url': 'https://openrouter.ai/api/v1',
                'timeout': 60
            },
            'GEMINI': {
                'models': ['gemini-2.0-flash-exp', 'gemini-1.5-pro'],
                'base_url': 'https://generativelanguage.googleapis.com/v1beta',
                'timeout': 45
            },
            'GROQ': {
                'models': ['llama-3.1-70b-versatile', 'mixtral-8x7b-32768'],
                'base_url': 'https://api.groq.com/openai/v1',
                'timeout': 30
            }
        }
        
        logger.info("🤖 AI Model Fallback System inicializado")
    
    async def generate_completion(
        self,
        prompt: str,
        max_tokens: int = 4000,
        temperature: float = 0.7,
        system_prompt: str = None
    ) -> Dict[str, Any]:
        """Gera completion com fallback automático entre modelos"""
        
        # Ordem de prioridade dos provedores
        providers = ['OPENROUTER', 'GEMINI', 'GROQ']
        
        for provider in providers:
            try:
                logger.info(f"🤖 Tentando {provider} para geração de texto")
                
                result = await self._generate_with_provider(
                    provider, prompt, max_tokens, temperature, system_prompt
                )
                
                if result.get('success'):
                    logger.info(f"✅ {provider} gerou {len(result.get('content', ''))} caracteres")
                    api_rotation_manager.report_success(provider)
                    return result
                else:
                    logger.warning(f"⚠️ {provider} falhou: {result.get('error', 'Erro desconhecido')}")
                    api_rotation_manager.report_failure(provider, result.get('error', ''))
                    
            except Exception as e:
                logger.error(f"❌ Erro em {provider}: {e}")
                api_rotation_manager.report_failure(provider, str(e))
                continue
        
        # Se todos falharam
        return {
            'success': False,
            'error': 'Todos os modelos de AI falharam',
            'content': '',
            'provider_used': 'none'
        }
    
    async def _generate_with_provider(
        self,
        provider: str,
        prompt: str,
        max_tokens: int,
        temperature: float,
        system_prompt: str = None
    ) -> Dict[str, Any]:
        """Gera texto com um provedor específico"""
        
        if provider == 'OPENROUTER':
            return await self._generate_with_openrouter(prompt, max_tokens, temperature, system_prompt)
        elif provider == 'GEMINI':
            return await self._generate_with_gemini(prompt, max_tokens, temperature, system_prompt)
        elif provider == 'GROQ':
            return await self._generate_with_groq(prompt, max_tokens, temperature, system_prompt)
        else:
            raise Exception(f"Provedor {provider} não suportado")
    
    async def _generate_with_openrouter(
        self,
        prompt: str,
        max_tokens: int,
        temperature: float,
        system_prompt: str = None
    ) -> Dict[str, Any]:
        """Gera texto usando OpenRouter"""
        import aiohttp
        
        api_key = api_rotation_manager.get_api_key('OPENROUTER')
        if not api_key:
            raise Exception("Nenhuma chave OpenRouter disponível")
        
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'HTTP-Referer': 'https://arqv30.com',
            'X-Title': 'ARQV30 Enhanced'
        }
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        payload = {
            'model': 'qwen/qwen-2.5-72b-instruct',
            'messages': messages,
            'max_tokens': max_tokens,
            'temperature': temperature,
            'stream': False
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                'https://openrouter.ai/api/v1/chat/completions',
                headers=headers,
                json=payload,
                timeout=60
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get('choices') and len(data['choices']) > 0:
                        content = data['choices'][0]['message']['content']
                        
                        return {
                            'success': True,
                            'content': content,
                            'provider_used': 'openrouter',
                            'model_used': 'qwen-2.5-72b-instruct',
                            'tokens_used': data.get('usage', {}).get('total_tokens', 0)
                        }
                    else:
                        raise Exception("OpenRouter não retornou choices válidas")
                else:
                    error_text = await response.text()
                    raise Exception(f"OpenRouter retornou status {response.status}: {error_text}")
    
    async def _generate_with_gemini(
        self,
        prompt: str,
        max_tokens: int,
        temperature: float,
        system_prompt: str = None
    ) -> Dict[str, Any]:
        """Gera texto usando Gemini"""
        import aiohttp
        
        api_key = api_rotation_manager.get_api_key('GEMINI')
        if not api_key:
            raise Exception("Nenhuma chave Gemini disponível")
        
        # Combina system prompt com user prompt se necessário
        full_prompt = prompt
        if system_prompt:
            full_prompt = f"{system_prompt}\n\n{prompt}"
        
        payload = {
            'contents': [{
                'parts': [{'text': full_prompt}]
            }],
            'generationConfig': {
                'maxOutputTokens': max_tokens,
                'temperature': temperature
            }
        }
        
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent?key={api_key}"
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json=payload,
                timeout=45
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get('candidates') and len(data['candidates']) > 0:
                        candidate = data['candidates'][0]
                        if candidate.get('content') and candidate['content'].get('parts'):
                            content = candidate['content']['parts'][0]['text']
                            
                            return {
                                'success': True,
                                'content': content,
                                'provider_used': 'gemini',
                                'model_used': 'gemini-2.0-flash-exp',
                                'tokens_used': data.get('usageMetadata', {}).get('totalTokenCount', 0)
                            }
                        else:
                            raise Exception("Gemini não retornou conteúdo válido")
                    else:
                        raise Exception("Gemini não retornou candidates válidos")
                else:
                    error_text = await response.text()
                    raise Exception(f"Gemini retornou status {response.status}: {error_text}")
    
    async def _generate_with_groq(
        self,
        prompt: str,
        max_tokens: int,
        temperature: float,
        system_prompt: str = None
    ) -> Dict[str, Any]:
        """Gera texto usando Groq"""
        import aiohttp
        
        api_key = api_rotation_manager.get_api_key('GROQ')
        if not api_key:
            raise Exception("Nenhuma chave Groq disponível")
        
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        payload = {
            'model': 'llama-3.1-70b-versatile',
            'messages': messages,
            'max_tokens': max_tokens,
            'temperature': temperature,
            'stream': False
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                'https://api.groq.com/openai/v1/chat/completions',
                headers=headers,
                json=payload,
                timeout=30
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get('choices') and len(data['choices']) > 0:
                        content = data['choices'][0]['message']['content']
                        
                        return {
                            'success': True,
                            'content': content,
                            'provider_used': 'groq',
                            'model_used': 'llama-3.1-70b-versatile',
                            'tokens_used': data.get('usage', {}).get('total_tokens', 0)
                        }
                    else:
                        raise Exception("Groq não retornou choices válidas")
                else:
                    error_text = await response.text()
                    raise Exception(f"Groq retornou status {response.status}: {error_text}")
    
    async def analyze_content(
        self,
        content: str,
        analysis_type: str = "general",
        context: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Analisa conteúdo com fallback automático"""
        
        # Prompts específicos por tipo de análise
        analysis_prompts = {
            "general": "Analise o seguinte conteúdo e forneça insights detalhados:",
            "viral": "Analise este conteúdo do ponto de vista de viralidade e engajamento:",
            "market": "Faça uma análise de mercado baseada no seguinte conteúdo:",
            "competitor": "Analise este conteúdo como inteligência competitiva:",
            "trend": "Identifique tendências e padrões no seguinte conteúdo:"
        }
        
        system_prompt = f"""Você é um analista especializado em {analysis_type}. 
        Forneça uma análise detalhada, estruturada e acionável.
        Foque em insights práticos e recomendações específicas."""
        
        user_prompt = f"{analysis_prompts.get(analysis_type, analysis_prompts['general'])}\n\n{content}"
        
        if context:
            user_prompt += f"\n\nContexto adicional: {json.dumps(context, ensure_ascii=False)}"
        
        return await self.generate_completion(
            prompt=user_prompt,
            system_prompt=system_prompt,
            max_tokens=3000,
            temperature=0.7
        )
    
    async def summarize_content(
        self,
        content: str,
        max_length: int = 500,
        focus: str = "key_points"
    ) -> Dict[str, Any]:
        """Sumariza conteúdo com fallback automático"""
        
        focus_prompts = {
            "key_points": "Extraia os pontos-chave mais importantes",
            "actionable": "Foque em insights acionáveis e recomendações",
            "trends": "Destaque tendências e padrões identificados",
            "competitive": "Foque em inteligência competitiva e oportunidades"
        }
        
        system_prompt = f"""Você é um especialista em síntese de informações.
        Crie um resumo conciso e estruturado em até {max_length} caracteres.
        {focus_prompts.get(focus, focus_prompts['key_points'])}."""
        
        user_prompt = f"Resuma o seguinte conteúdo:\n\n{content}"
        
        return await self.generate_completion(
            prompt=user_prompt,
            system_prompt=system_prompt,
            max_tokens=max_length // 2,  # Aproximadamente metade dos tokens para caracteres
            temperature=0.5
        )
    
    def get_status(self) -> Dict[str, Any]:
        """Obtém status dos modelos de AI"""
        status = {}
        
        for provider in ['OPENROUTER', 'GEMINI', 'GROQ']:
            provider_status = api_rotation_manager.get_provider_status().get(provider, {})
            status[provider] = {
                'available': provider in api_rotation_manager.providers,
                'healthy': provider_status.get('healthy', False),
                'models': self.model_configs.get(provider, {}).get('models', []),
                'failures': provider_status.get('failures', 0)
            }
        
        return status

# Instância global
ai_model_fallback = AIModelFallback()