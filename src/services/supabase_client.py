#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v2.0 - Supabase Client
Cliente configurado para integração com Supabase
"""

import os
import logging
import time
from typing import Dict, List, Optional, Any
from supabase import create_client, Client
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class SupabaseClient:
    """Cliente Supabase para ARQV30 Enhanced"""
    
    def __init__(self):
        """Inicializa cliente Supabase"""
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_ANON_KEY')
        self.service_role_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if not self.supabase_url or not self.supabase_key:
            logger.info("ℹ️ Supabase não configurado - usando modo local")
            self.client = None
            self.admin_client = None
            return
        
        try:
            # Cliente principal (anon key)
            self.client: Client = create_client(self.supabase_url, self.supabase_key)
            
            # Cliente admin (service role) se disponível
            if self.service_role_key:
                self.admin_client: Client = create_client(self.supabase_url, self.service_role_key)
            else:
                self.admin_client = self.client
            
            logger.info("✅ Supabase client inicializado com sucesso")
            
        except Exception as e:
            logger.error(f"❌ Erro ao inicializar Supabase: {str(e)}")
            self.client = None
            self.admin_client = None
    
    def is_connected(self) -> bool:
        """Verifica se está conectado ao Supabase"""
        return self.client is not None
    
    def test_connection(self) -> bool:
        """Testa conexão com Supabase"""
        if not self.client:
            return False
        
        try:
            # Tenta fazer uma query simples
            result = self.client.table('analyses').select('id').limit(1).execute()
            return True
        except Exception as e:
            logger.error(f"❌ Erro ao testar conexão Supabase: {str(e)}")
            return False
    
    def create_analysis(self, analysis_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Cria nova análise no Supabase"""
        if not self.client:
            logger.warning("⚠️ Supabase não conectado")
            return None
        
        # Valida chave de API antes de tentar
        if not self._validate_api_key():
            logger.error("❌ Chave de API do Supabase inválida")
            return None
        
        try:
            # Prepara dados para inserção
            insert_data = {
                'segmento': analysis_data.get('segmento', ''),
                'produto': analysis_data.get('produto', ''),
                'publico': analysis_data.get('publico', ''),
                'preco': float(analysis_data.get('preco', 0)) if analysis_data.get('preco') else None,
                'objetivo_receita': float(analysis_data.get('objetivo_receita', 0)) if analysis_data.get('objetivo_receita') else None,
                'orcamento_marketing': float(analysis_data.get('orcamento_marketing', 0)) if analysis_data.get('orcamento_marketing') else None,
                'prazo_lancamento': analysis_data.get('prazo_lancamento', ''),
                'concorrentes': analysis_data.get('concorrentes', ''),
                'dados_adicionais': analysis_data.get('dados_adicionais', ''),
                'query': analysis_data.get('query', ''),
                'status': analysis_data.get('status', 'completed'),
                'avatar_data': analysis_data.get('avatar_ultra_detalhado'),
                'positioning_data': analysis_data.get('escopo'),
                'competition_data': analysis_data.get('analise_concorrencia_detalhada'),
                'marketing_data': analysis_data.get('estrategia_palavras_chave'),
                'metrics_data': analysis_data.get('metricas_performance_detalhadas'),
                'funnel_data': analysis_data.get('funil_vendas_detalhado'),
                'action_plan_data': analysis_data.get('plano_acao_detalhado'),
                'insights_data': analysis_data.get('insights_exclusivos'),
                'drivers_mentais_data': analysis_data.get('drivers_mentais_customizados'),
                'provas_visuais_data': analysis_data.get('provas_visuais_instantaneas'),
                'anti_objecao_data': analysis_data.get('sistema_anti_objecao'),
                'pre_pitch_data': analysis_data.get('pre_pitch_invisivel'),
                'predicoes_futuro_data': analysis_data.get('predicoes_futuro_completas'),
                'pesquisa_web_data': analysis_data.get('pesquisa_web_massiva'),
                'comprehensive_analysis': analysis_data,
                'local_files_path': analysis_data.get('local_files_path'),
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            
            # Remove campos None
            insert_data = {k: v for k, v in insert_data.items() if v is not None}
            
            # Insere no banco com retry
            result = self._insert_with_retry(insert_data)
            
            if result.data:
                logger.info(f"✅ Análise criada no Supabase com ID: {result.data[0]['id']}")
                return result.data[0]
            else:
                logger.error("❌ Erro ao criar análise no Supabase: resultado vazio")
                return None
                
        except Exception as e:
            logger.error(f"❌ Erro ao criar análise no Supabase: {str(e)}")
            return None
    
    def _validate_api_key(self) -> bool:
        """Valida se a chave de API está funcionando"""
        try:
            # Tenta uma operação simples
            result = self.client.table('analyses').select('id').limit(1).execute()
            return True
        except Exception as e:
            error_str = str(e).lower()
            if 'invalid api key' in error_str or 'unauthorized' in error_str:
                logger.error("❌ Chave de API do Supabase inválida ou expirada")
                return False
            # Outros erros podem ser temporários
            return True
    
    def _insert_with_retry(self, insert_data: Dict[str, Any], max_retries: int = 3) -> Any:
        """Insere dados com retry e backoff exponencial"""
        for attempt in range(max_retries):
            try:
                result = self.client.table('analyses').insert(insert_data).execute()
                return result
            except Exception as e:
                error_str = str(e).lower()
                
                if 'invalid api key' in error_str or 'unauthorized' in error_str:
                    # Erro de autenticação - não tenta novamente
                    raise e
                
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + 1  # Backoff exponencial
                    logger.warning(f"⚠️ Tentativa {attempt + 1} falhou, tentando novamente em {wait_time}s: {str(e)}")
                    time.sleep(wait_time)
                else:
                    raise e
    
    def get_analysis(self, analysis_id: str) -> Optional[Dict[str, Any]]:
        """Busca análise por ID"""
        if not self.client:
            return None
        
        try:
            result = self.client.table('analyses').select('*').eq('id', analysis_id).execute()
            
            if result.data:
                return result.data[0]
            else:
                return None
                
        except Exception as e:
            logger.error(f"❌ Erro ao buscar análise {analysis_id}: {str(e)}")
            return None
    
    def list_analyses(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Lista análises com paginação"""
        if not self.client:
            return []
        
        try:
            result = self.client.table('analyses')\
                .select('id, segmento, produto, status, created_at, updated_at, local_files_path')\
                .order('created_at', desc=True)\
                .range(offset, offset + limit - 1)\
                .execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"❌ Erro ao listar análises: {str(e)}")
            return []
    
    def update_analysis(self, analysis_id: str, update_data: Dict[str, Any]) -> bool:
        """Atualiza análise existente"""
        if not self.client:
            return False
        
        try:
            # Adiciona timestamp de atualização
            update_data['updated_at'] = datetime.now().isoformat()
            
            # Atualiza no banco
            result = self.client.table('analyses').update(update_data).eq('id', analysis_id).execute()
            
            if result.data:
                logger.info(f"✅ Análise {analysis_id} atualizada no Supabase")
                return True
            else:
                logger.error(f"❌ Erro ao atualizar análise {analysis_id}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Erro ao atualizar análise {analysis_id}: {str(e)}")
            return False
    
    def delete_analysis(self, analysis_id: str) -> bool:
        """Remove análise do banco"""
        if not self.client:
            return False
        
        try:
            result = self.client.table('analyses').delete().eq('id', analysis_id).execute()
            
            if result.data:
                logger.info(f"✅ Análise {analysis_id} removida do Supabase")
                return True
            else:
                logger.error(f"❌ Erro ao remover análise {analysis_id}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Erro ao remover análise {analysis_id}: {str(e)}")
            return False
    
    def save_analysis_file(self, analysis_id: str, file_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Salva informações de arquivo de análise"""
        if not self.client:
            return None
        
        try:
            insert_data = {
                'analysis_id': analysis_id,
                'file_type': file_data.get('file_type'),
                'file_name': file_data.get('file_name'),
                'file_path': file_data.get('file_path'),
                'file_size': file_data.get('file_size', 0),
                'content_preview': file_data.get('content_preview', ''),
                'created_at': datetime.now().isoformat()
            }
            
            result = self.client.table('analysis_files').insert(insert_data).execute()
            
            if result.data:
                logger.info(f"✅ Arquivo de análise salvo: {file_data.get('file_name')}")
                return result.data[0]
            else:
                logger.error("❌ Erro ao salvar arquivo de análise")
                return None
                
        except Exception as e:
            logger.error(f"❌ Erro ao salvar arquivo de análise: {str(e)}")
            return None
    
    def get_analysis_files(self, analysis_id: str) -> List[Dict[str, Any]]:
        """Busca arquivos de uma análise"""
        if not self.client:
            return []
        
        try:
            result = self.client.table('analysis_files')\
                .select('*')\
                .eq('analysis_id', analysis_id)\
                .order('created_at', desc=True)\
                .execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"❌ Erro ao buscar arquivos da análise {analysis_id}: {str(e)}")
            return []
    
    def get_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas do banco"""
        if not self.client:
            return {
                'total_analyses': 0,
                'status_counts': {},
                'recent_analyses': 0,
                'error': 'Supabase não conectado'
            }
        
        try:
            # Total de análises
            total_result = self.client.table('analyses').select('id', count='exact').execute()
            total_analyses = total_result.count if total_result.count else 0
            
            # Análises por status
            status_result = self.client.table('analyses')\
                .select('status', count='exact')\
                .execute()
            
            status_counts = {}
            if status_result.data:
                for item in status_result.data:
                    status = item.get('status', 'unknown')
                    status_counts[status] = status_counts.get(status, 0) + 1
            
            # Análises recentes (últimos 7 dias)
            from datetime import timedelta
            week_ago = (datetime.now() - timedelta(days=7)).isoformat()
            
            recent_result = self.client.table('analyses')\
                .select('id', count='exact')\
                .gte('created_at', week_ago)\
                .execute()
            
            recent_count = recent_result.count if recent_result.count else 0
            
            return {
                'total_analyses': total_analyses,
                'status_counts': status_counts,
                'recent_analyses': recent_count,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Erro ao obter estatísticas: {str(e)}")
            return {
                'total_analyses': 0,
                'status_counts': {},
                'recent_analyses': 0,
                'error': str(e)
            }

# Instância global
supabase_client = SupabaseClient()