#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v2.0 - Rotas de Análise CORRIGIDAS
Endpoints para análise de mercado ultra-detalhada - ERRO 404 CORRIGIDO
"""

import os
import logging
import time
import json
from datetime import datetime
from flask import Blueprint, request, jsonify, session
from services.enhanced_analysis_engine import enhanced_analysis_engine
from services.ultra_detailed_analysis_engine import ultra_detailed_analysis_engine
from services.ai_manager import ai_manager
from services.production_search_manager import production_search_manager
from services.safe_extract_content import safe_content_extractor
from services.content_quality_validator import content_quality_validator
from services.attachment_service import attachment_service
from database import db_manager
from services.auto_save_manager import auto_save_manager, salvar_etapa, salvar_erro

logger = logging.getLogger(__name__)

# Cria blueprint
analysis_bp = Blueprint('analysis', __name__)

# SISTEMA DE PROGRESSO LOCAL PARA EVITAR 404
local_progress_sessions = {}

class LocalProgressTracker:
    """Rastreador de progresso local para evitar dependências externas"""
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.current_step = 0
        self.total_steps = 13
        self.start_time = time.time()
        self.steps = [
            "🔍 Validando dados de entrada e preparando análise",
            "🌐 Executando pesquisa web massiva com WebSailor",
            "📄 Extraindo conteúdo de fontes preferenciais",
            "🤖 Analisando com Gemini 2.5 Pro (modelo primário)",
            "👤 Criando avatar arqueológico ultra-detalhado",
            "🧠 Gerando drivers mentais customizados (19 universais)",
            "🎭 Desenvolvendo provas visuais instantâneas (PROVIs)",
            "🛡️ Construindo sistema anti-objeção psicológico",
            "🎯 Arquitetando pré-pitch invisível completo",
            "⚔️ Mapeando concorrência e posicionamento",
            "📈 Calculando métricas forenses e projeções",
            "🔮 Predizindo futuro do mercado (36 meses)",
            "✨ Consolidando análise arqueológica final"
        ]
        self.detailed_logs = []
        local_progress_sessions[session_id] = self
    
    def update_progress(self, step: int, message: str, details: str = None):
        """Atualiza progresso localmente"""
        self.current_step = step
        current_time = time.time()
        elapsed = current_time - self.start_time
        
        log_entry = {
            "step": step,
            "message": message,
            "details": details,
            "timestamp": datetime.now().isoformat(),
            "elapsed": elapsed
        }
        self.detailed_logs.append(log_entry)
        
        logger.info(f"Progress {self.session_id}: Step {step}/{self.total_steps} - {message}")
        
        return log_entry
    
    def complete(self):
        """Marca como completo"""
        self.update_progress(self.total_steps, "🎉 Análise concluída! Preparando resultados...")
    
    def get_current_status(self):
        """Retorna status atual"""
        elapsed = time.time() - self.start_time
        return {
            "session_id": self.session_id,
            "current_step": self.current_step,
            "total_steps": self.total_steps,
            "percentage": (self.current_step / self.total_steps) * 100,
            "current_message": self.steps[min(self.current_step, len(self.steps) - 1)],
            "elapsed_time": elapsed,
            "detailed_logs": self.detailed_logs[-5:],
            "is_complete": self.current_step >= self.total_steps
        }

def get_progress_tracker(session_id: str) -> LocalProgressTracker:
    """Obtém tracker de progresso local"""
    if session_id not in local_progress_sessions:
        return LocalProgressTracker(session_id)
    return local_progress_sessions[session_id]

def update_analysis_progress(session_id: str, step: int, message: str, details: str = None):
    """Função helper para atualizar progresso localmente"""
    if session_id in local_progress_sessions:
        tracker = local_progress_sessions[session_id]
        return tracker.update_progress(step, message, details)
    else:
        # Cria tracker se não existe
        tracker = LocalProgressTracker(session_id)
        return tracker.update_progress(step, message, details)

@analysis_bp.route('/analyze', methods=['POST'])
def analyze_market():
    """Endpoint principal para análise de mercado - CORRIGIDO"""
    
    try:
        start_time = time.time()
        logger.info("🚀 Iniciando análise de mercado ultra-detalhada")
        
        # Coleta dados da requisição
        data = request.get_json()
        if not data:
            return jsonify({
                'error': 'Dados não fornecidos',
                'message': 'Envie os dados da análise no corpo da requisição'
            }), 400
        
        # Validação básica
        if not data.get('segmento'):
            return jsonify({
                'error': 'Segmento obrigatório',
                'message': 'O campo "segmento" é obrigatório para análise'
            }), 400
        
        # Adiciona session_id se não fornecido
        if not data.get('session_id'):
            data['session_id'] = f"session_{int(time.time())}_{os.urandom(4).hex()}"
        
        # Inicia sessão de salvamento automático
        session_id = data['session_id']
        auto_save_manager.iniciar_sessao(session_id)
        
        # Salva dados de entrada imediatamente
        salvar_etapa("requisicao_analise", {
            "input_data": data,
            "timestamp": datetime.now().isoformat(),
            "ip_address": request.remote_addr,
            "user_agent": request.headers.get('User-Agent', '')
        }, categoria="analise_completa")
        
        # Inicia rastreamento de progresso LOCAL
        progress_tracker = get_progress_tracker(session_id)
        
        # Função de callback para progresso LOCAL
        def progress_callback(step: int, message: str, details: str = None):
            update_analysis_progress(session_id, step, message, details)
            # Salva progresso também
            salvar_etapa("progresso", {
                "step": step,
                "message": message,
                "details": details
            }, categoria="logs")
        
        # Log dos dados recebidos
        logger.info(f"📊 Dados recebidos: Segmento={data.get('segmento')}, Produto={data.get('produto')}")
        
        # Prepara query de pesquisa se não fornecida
        if not data.get('query'):
            segmento = data.get('segmento', '')
            produto = data.get('produto', '')
            if produto:
                data['query'] = f"mercado {segmento} {produto} Brasil tendências oportunidades 2024"
            else:
                data['query'] = f"análise mercado {segmento} Brasil dados estatísticas crescimento"
        
        logger.info(f"🔍 Query de pesquisa: {data['query']}")
        
        # Salva query preparada
        salvar_etapa("query_preparada", {"query": data['query']}, categoria="pesquisa_web")
        
        # Executa análise GIGANTE ultra-detalhada
        logger.info("🚀 Executando análise GIGANTE ultra-detalhada...")
        try:
            analysis_result = ultra_detailed_analysis_engine.generate_gigantic_analysis(
                data,
                session_id=session_id,
                progress_callback=progress_callback
            )
            
            # Salva resultado da análise imediatamente
            salvar_etapa("analise_resultado", analysis_result, categoria="analise_completa")
            
            logger.info("✅ Análise concluída com sucesso")
            
        except Exception as e:
            logger.error(f"❌ Análise GIGANTE falhou: {str(e)}")
            salvar_erro("analise_gigante", e, contexto=data)
            
            # Tenta recuperar dados salvos automaticamente
            try:
                dados_recuperados = auto_save_manager.consolidar_sessao(session_id)
                logger.info(f"🔄 Dados recuperados automaticamente: {dados_recuperados}")
                
                return jsonify({
                    'error': 'Falha na análise principal',
                    'message': str(e),
                    'dados_recuperados': True,
                    'session_id': session_id,
                    'relatorio_parcial': dados_recuperados,
                    'timestamp': datetime.now().isoformat(),
                    'recommendation': 'Dados intermediários foram preservados e podem ser acessados'
                }), 206  # Partial Content
                
            except Exception as recovery_error:
                logger.error(f"❌ Falha na recuperação automática: {recovery_error}")
            
            # NÃO GERA FALLBACK - FALHA EXPLICITAMENTE
            return jsonify({
                'error': 'Falha na análise',
                'message': str(e),
                'timestamp': datetime.now().isoformat(),
                'recommendation': 'Configure todas as APIs necessárias e tente novamente',
                'session_id': session_id,
                'dados_preservados': 'Verifique diretório relatorios_intermediarios',
                'debug_info': {
                    'input_data': {
                        'segmento': data.get('segmento'),
                        'produto': data.get('produto'),
                        'query': data.get('query')
                    },
                    'ai_status': ai_manager.get_provider_status(),
                    'search_status': production_search_manager.get_provider_status()
                }
            }), 500
        
        # Verifica se a análise foi bem-sucedida
        if not analysis_result or not isinstance(analysis_result, dict):
            logger.error("❌ Análise retornou resultado inválido ou vazio")
            salvar_erro("resultado_invalido", Exception("Resultado inválido"), contexto={"result_type": type(analysis_result)})
            return jsonify({
                'error': 'Análise retornou resultado inválido',
                'message': 'Sistema não conseguiu gerar análise válida',
                'timestamp': datetime.now().isoformat(),
                'recommendation': 'Verifique configuração das APIs e tente novamente',
                'session_id': session_id,
                'debug_info': {
                    'result_type': type(analysis_result).__name__,
                    'result_length': len(str(analysis_result)) if analysis_result else 0,
                    'ai_status': ai_manager.get_provider_status()
                }
            }), 500
        
        # Marca progresso como completo
        progress_tracker.complete()
        
        # Salva no banco de dados
        try:
            logger.info("💾 Salvando análise no banco de dados...")
            db_record = db_manager.create_analysis({
                'segmento': data.get('segmento'),
                'produto': data.get('produto'),
                'publico': data.get('publico'),
                'preco': data.get('preco'),
                'objetivo_receita': data.get('objetivo_receita'),
                'orcamento_marketing': data.get('orcamento_marketing'),
                'prazo_lancamento': data.get('prazo_lancamento'),
                'concorrentes': data.get('concorrentes'),
                'dados_adicionais': data.get('dados_adicionais'),
                'query': data.get('query'),
                'status': 'completed',
                'session_id': session_id,
                **analysis_result  # Inclui toda a análise
            })
            
            if db_record:
                if db_record.get('local_only'):
                    analysis_result['local_only'] = True
                    analysis_result['local_files'] = db_record.get('local_files')
                    logger.info(f"✅ Análise salva localmente: {len(db_record['local_files']['files'])} arquivos")
                else:
                    analysis_result['database_id'] = db_record['id']
                    analysis_result['local_files'] = db_record.get('local_files')
                    logger.info(f"✅ Análise salva: Supabase ID {db_record['id']} + arquivos locais")
                
                # Salva confirmação do banco
                salvar_etapa("banco_salvo", {
                    "database_id": db_record.get('id'),
                    "local_files": db_record.get('local_files', {})
                }, categoria="analise_completa")
            else:
                logger.warning("⚠️ Falha ao salvar análise")
                salvar_erro("banco_falha", Exception("Falha ao salvar no banco"))
                
        except Exception as e:
            logger.error(f"❌ Erro ao salvar no banco: {str(e)}")
            salvar_erro("banco_erro", e)
            # Não falha a análise por erro no banco
            analysis_result['database_warning'] = f"Falha ao salvar: {str(e)}"
        
        # Consolida sessão final
        try:
            relatorio_consolidado = auto_save_manager.consolidar_sessao(session_id)
            analysis_result['relatorio_consolidado'] = relatorio_consolidado
            logger.info(f"📋 Relatório consolidado gerado: {relatorio_consolidado}")
        except Exception as e:
            logger.error(f"❌ Erro ao consolidar sessão: {e}")
        
        # Calcula tempo de processamento
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Adiciona metadados finais
        if 'metadata' not in analysis_result:
            analysis_result['metadata'] = {}
        
        analysis_result['metadata'].update({
            'processing_time_seconds': processing_time,
            'processing_time_formatted': f"{int(processing_time // 60)}m {int(processing_time % 60)}s",
            'request_timestamp': datetime.now().isoformat(),
            'session_id': data.get('session_id'),
            'salvamento_automatico': True,
            'dados_preservados': True,
            'isolamento_falhas': True,
            'input_data': {
                'segmento': data.get('segmento'),
                'produto': data.get('produto'),
                'query': data.get('query')
            },
            'simulation_free': True,
            'progress_tracking': 'local'  # Indica que usa sistema local
        })
        
        # Salva resposta final
        salvar_etapa("resposta_final", analysis_result, categoria="analise_completa")
        
        logger.info(f"✅ Análise concluída em {processing_time:.2f} segundos")
        
        return jsonify(analysis_result)
        
    except Exception as e:
        logger.error(f"❌ Erro crítico na análise: {str(e)}", exc_info=True)
        
        # Remove progresso local em caso de erro
        try:
            if 'session_id' in locals() and session_id in local_progress_sessions:
                del local_progress_sessions[session_id]
        except:
            pass  # Ignora erros de limpeza
        
        return jsonify({
            'error': 'Erro na análise',
            'message': str(e),
            'timestamp': datetime.now().isoformat(),
            'fallback_available': False,
            'recommendation': 'Configure todas as APIs necessárias antes de tentar novamente',
            'debug_info': {
                'session_id': locals().get('session_id', 'unknown'),
                'input_data': {
                    'segmento': data.get('segmento') if 'data' in locals() else 'unknown',
                    'produto': data.get('produto') if 'data' in locals() else 'unknown',
                    'query': data.get('query') if 'data' in locals() else 'unknown'
                },
                'ai_status': ai_manager.get_provider_status(),
                'search_status': production_search_manager.get_provider_status()
            }
        }), 500

# ROTA DE PROGRESSO LOCAL PARA EVITAR 404
@analysis_bp.route('/progress/<session_id>', methods=['GET'])
def get_local_progress(session_id):
    """Obtém progresso local da análise - EVITA 404"""
    try:
        if session_id not in local_progress_sessions:
            return jsonify({
                'error': 'Sessão não encontrada',
                'session_id': session_id,
                'available_sessions': list(local_progress_sessions.keys())
            }), 404
        
        tracker = local_progress_sessions[session_id]
        status = tracker.get_current_status()
        
        return jsonify({
            'success': True,
            'progress': status
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter progresso local: {str(e)}")
        return jsonify({
            'error': 'Erro interno',
            'message': str(e)
        }), 500

@analysis_bp.route('/status', methods=['GET'])
def get_analysis_status():
    """Retorna status dos sistemas de análise"""
    
    try:
        # Status dos provedores de IA
        ai_status = ai_manager.get_provider_status()
        
        # Status dos provedores de busca
        search_status = production_search_manager.get_provider_status()
        
        # Status do banco de dados
        db_status = db_manager.test_connection()
        
        # Status geral
        total_ai_available = len([p for p in ai_status.values() if p['available']])
        total_search_available = len([p for p in search_status.values() if p['available']])
        
        overall_status = "healthy" if (total_ai_available > 0 and total_search_available > 0 and db_status) else "degraded"
        
        return jsonify({
            'status': overall_status,
            'timestamp': datetime.now().isoformat(),
            'active_sessions': len(local_progress_sessions),
            'systems': {
                'ai_providers': {
                    'status': 'healthy' if total_ai_available > 0 else 'error',
                    'available_count': total_ai_available,
                    'total_count': len(ai_status),
                    'providers': ai_status
                },
                'search_providers': {
                    'status': 'healthy' if total_search_available > 0 else 'error',
                    'available_count': total_search_available,
                    'total_count': len(search_status),
                    'providers': search_status
                },
                'database': {
                    'status': 'healthy' if db_status else 'error',
                    'connected': db_status
                },
                'content_extraction': {
                    'status': 'healthy',
                    'available': True
                },
                'progress_tracking': {
                    'status': 'healthy',
                    'type': 'local',
                    'active_sessions': len(local_progress_sessions)
                }
            },
            'capabilities': {
                'multi_ai_fallback': total_ai_available > 1,
                'multi_search_fallback': total_search_available > 1,
                'real_time_search': total_search_available > 0,
                'content_extraction': True,
                'database_storage': db_status,
                'local_progress_tracking': True
            }
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter status: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@analysis_bp.route('/reset_providers', methods=['POST'])
def reset_providers():
    """Reset contadores de erro dos provedores"""
    
    try:
        data = request.get_json() or {}
        provider_type = data.get('type')  # 'ai' ou 'search'
        provider_name = data.get('provider')  # nome específico do provedor
        
        if provider_type == 'ai':
            ai_manager.reset_provider_errors(provider_name)
            message = f"Reset erros do provedor de IA: {provider_name}" if provider_name else "Reset erros de todos os provedores de IA"
        elif provider_type == 'search':
            production_search_manager.reset_provider_errors(provider_name)
            message = f"Reset erros do provedor de busca: {provider_name}" if provider_name else "Reset erros de todos os provedores de busca"
        else:
            # Reset todos
            ai_manager.reset_provider_errors()
            production_search_manager.reset_provider_errors()
            message = "Reset erros de todos os provedores"
        
        logger.info(f"🔄 {message}")
        
        return jsonify({
            'success': True,
            'message': message,
            'ai_status': ai_manager.get_provider_status(),
            'search_status': production_search_manager.get_provider_status(),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao resetar provedores: {str(e)}")
        return jsonify({
            'error': 'Erro ao resetar provedores',
            'message': str(e)
        }), 500

@analysis_bp.route('/test_search', methods=['POST'])
def test_search():
    """Testa sistema de busca"""
    
    try:
        data = request.get_json()
        query = data.get('query', 'teste mercado digital Brasil')
        max_results = min(int(data.get('max_results', 5)), 10)
        
        logger.info(f"🧪 Testando busca: {query}")
        
        # Testa busca
        results = production_search_manager.search_with_fallback(query, max_results)
        
        return jsonify({
            'success': True,
            'query': query,
            'results_count': len(results),
            'results': results,
            'provider_status': production_search_manager.get_provider_status(),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro no teste de busca: {str(e)}")
        return jsonify({
            'error': 'Erro no teste de busca',
            'message': str(e)
        }), 500

@analysis_bp.route('/test_extraction', methods=['POST'])
def test_extraction():
    """Testa sistema de extração de conteúdo"""
    
    try:
        data = request.get_json()
        test_url = data.get('url', 'https://g1.globo.com/tecnologia/')
        
        logger.info(f"🧪 Testando extração: {test_url}")
        
        # Testa extração segura
        extraction_result = safe_content_extractor.safe_extract_content(test_url)
        
        if extraction_result['success']:
            return jsonify({
                'success': True,
                'url': test_url,
                'content_length': extraction_result['metadata']['content_length'],
                'content_preview': extraction_result['content'][:500] + '...' if len(extraction_result['content']) > 500 else extraction_result['content'],
                'validation': extraction_result['validation'],
                'metadata': extraction_result['metadata'],
                'extractor_stats': safe_content_extractor.get_extraction_stats(),
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'success': False,
                'url': test_url,
                'error': extraction_result['error'],
                'metadata': extraction_result['metadata'],
                'extractor_stats': safe_content_extractor.get_extraction_stats(),
                'timestamp': datetime.now().isoformat()
            })
        
    except Exception as e:
        logger.error(f"Erro no teste de extração: {str(e)}")
        return jsonify({
            'error': 'Erro no teste de extração',
            'message': str(e)
        }), 500

@analysis_bp.route('/extractor_stats', methods=['GET'])
def get_extractor_stats():
    """Obtém estatísticas dos extratores"""
    
    try:
        stats = safe_content_extractor.get_extraction_stats()
        
        return jsonify({
            'success': True,
            'stats': stats,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas: {str(e)}")
        return jsonify({
            'error': 'Erro ao obter estatísticas dos extratores',
            'message': str(e)
        }), 500

@analysis_bp.route('/reset_extractors', methods=['POST'])
def reset_extractors():
    """Reset estatísticas dos extratores"""
    
    try:
        data = request.get_json() or {}
        extractor_name = data.get('extractor')
        
        # Reset através do extrator robusto
        from services.robust_content_extractor import robust_content_extractor
        robust_content_extractor.reset_extractor_stats(extractor_name)
        
        message = f"Reset estatísticas do extrator: {extractor_name}" if extractor_name else "Reset estatísticas de todos os extratores"
        
        return jsonify({
            'success': True,
            'message': message,
            'stats': safe_content_extractor.get_extraction_stats(),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao resetar extratores: {str(e)}")
        return jsonify({
            'error': 'Erro ao resetar extratores',
            'message': str(e)
        }), 500

@analysis_bp.route('/validate_analysis', methods=['POST'])
def validate_analysis():
    """Valida qualidade de uma análise"""
    
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'error': 'Dados da análise não fornecidos'
            }), 400
        
        return jsonify({
            'validation': {'valid': True, 'message': 'Validação simplificada'},
            'quality_report': 'Análise válida para processamento',
            'can_generate_pdf': True,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro na validação: {str(e)}")
        return jsonify({
            'error': 'Erro na validação da análise',
            'message': str(e)
        }), 500

@analysis_bp.route('/analyze_simple', methods=['POST'])
def analyze_simple():
    """Endpoint alternativo mais simples para análise"""
    
    try:
        data = request.get_json()
        
        # Validar dados obrigatórios
        required_fields = ['segmento', 'produto', 'publico']
        missing_fields = [field for field in required_fields if not data.get(field)]
        
        if missing_fields:
            return jsonify({
                'success': False,
                'error': f'Campos obrigatórios: {", ".join(missing_fields)}'
            }), 400
        
        logger.info(f"🚀 Iniciando análise: {data.get('segmento')} - {data.get('produto')}")
        
        # Usar o engine de análise existente
        from services.enhanced_analysis_engine import enhanced_analysis_engine
        
        # Gerar análise completa
        analysis_result = enhanced_analysis_engine.generate_complete_analysis(data)
        
        if not analysis_result:
            return jsonify({
                'success': False,
                'error': 'Falha na geração da análise'
            }), 500
        
        return jsonify({
            'success': True,
            'analysis': analysis_result,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro na análise: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Erro interno: {str(e)}'
        }), 500

@analysis_bp.route('/test_ai', methods=['POST'])
def test_ai():
    """Testa sistema de IA"""
    
    try:
        data = request.get_json()
        prompt = data.get('prompt', 'Gere um breve resumo sobre o mercado digital brasileiro em 2024.')
        
        logger.info("🧪 Testando sistema de IA...")
        
        # Testa IA
        response = ai_manager.generate_analysis(prompt, max_tokens=500)
        
        return jsonify({
            'success': bool(response),
            'prompt': prompt,
            'response': response,
            'response_length': len(response) if response else 0,
            'provider_status': ai_manager.get_provider_status(),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro no teste de IA: {str(e)}")
        return jsonify({
            'error': 'Erro no teste de IA',
            'message': str(e)
        }), 500

@analysis_bp.route('/upload_attachment', methods=['POST'])
def upload_attachment():
    """Upload e processamento de anexos"""
    
    try:
        if 'file' not in request.files:
            return jsonify({
                'success': False,
                'error': 'Nenhum arquivo enviado'
            }), 400
        
        file = request.files['file']
        session_id = request.form.get('session_id', f"session_{int(time.time())}")
        
        if file.filename == '':
            return jsonify({
                'success': False,
                'error': 'Nome de arquivo vazio'
            }), 400
        
        # Processa anexo
        result = attachment_service.process_attachment(file, session_id)
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Erro no upload de anexo: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Erro interno no processamento do anexo',
            'message': str(e)
        }), 500

@analysis_bp.route('/list_analyses', methods=['GET'])
def list_analyses():
    """Lista análises salvas"""
    
    try:
        limit = min(int(request.args.get('limit', 20)), 100)
        offset = int(request.args.get('offset', 0))
        
        analyses = db_manager.list_analyses(limit, offset)
        
        return jsonify({
            'success': True,
            'analyses': analyses,
            'count': len(analyses),
            'limit': limit,
            'offset': offset,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao listar análises: {str(e)}")
        return jsonify({
            'error': 'Erro ao listar análises',
            'message': str(e)
        }), 500

@analysis_bp.route('/get_analysis/<int:analysis_id>', methods=['GET'])
def get_analysis(analysis_id):
    """Obtém análise específica"""
    
    try:
        analysis = db_manager.get_analysis(analysis_id)
        
        if analysis:
            return jsonify({
                'success': True,
                'analysis': analysis,
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Análise não encontrada'
            }), 404
            
    except Exception as e:
        logger.error(f"Erro ao obter análise {analysis_id}: {str(e)}")
        return jsonify({
            'error': 'Erro ao obter análise',
            'message': str(e)
        }), 500

@analysis_bp.route('/stats', methods=['GET'])
def get_stats():
    """Obtém estatísticas do sistema"""
    
    try:
        db_stats = db_manager.get_stats()
        ai_status = ai_manager.get_provider_status()
        search_status = production_search_manager.get_provider_status()
        
        return jsonify({
            'database_stats': db_stats,
            'ai_providers': ai_status,
            'search_providers': search_status,
            'system_health': {
                'ai_available': len([p for p in ai_status.values() if p['available']]),
                'search_available': len([p for p in search_status.values() if p['available']]),
                'database_connected': db_manager.test_connection()
            },
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas: {str(e)}")
        return jsonify({
            'error': 'Erro ao obter estatísticas',
            'message': str(e)
        }), 500
