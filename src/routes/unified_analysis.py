#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v2.0 - Unified Analysis Routes
Rotas unificadas que combinam todas as capacidades de análise
"""

import os
import logging
import time
import json
from datetime import datetime
from flask import Blueprint, request, jsonify, render_template
from services.unified_analysis_engine import unified_analysis_engine
from services.unified_search_manager import unified_search_manager
from services.exa_client import exa_client
from services.pymupdf_client import pymupdf_client
from services.attachment_service import attachment_service
from database import db_manager
from routes.progress import get_progress_tracker, update_analysis_progress
from services.auto_save_manager import auto_save_manager, salvar_etapa, salvar_erro

logger = logging.getLogger(__name__)

# Cria blueprint unificado
unified_bp = Blueprint('unified', __name__)

@unified_bp.route('/analyze_unified', methods=['POST'])
def analyze_unified():
    """Endpoint unificado para todas as análises"""
    
    try:
        start_time = time.time()
        logger.info("🚀 Iniciando análise unificada")
        
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
        
        # Determina tipo de análise
        analysis_type = data.get('analysis_type', 'complete')
        
        # Adiciona session_id se não fornecido
        if not data.get('session_id'):
            data['session_id'] = f"unified_{int(time.time())}_{os.urandom(4).hex()}"
        
        session_id = data['session_id']
        auto_save_manager.iniciar_sessao(session_id)
        
        # Salva dados de entrada
        salvar_etapa("requisicao_unificada", {
            "input_data": data,
            "analysis_type": analysis_type,
            "timestamp": datetime.now().isoformat(),
            "ip_address": request.remote_addr
        }, categoria="analise_completa")
        
        # Inicia rastreamento de progresso
        progress_tracker = get_progress_tracker(session_id)
        
        def progress_callback(step: int, message: str, details: str = None):
            update_analysis_progress(session_id, step, message, details)
            salvar_etapa("progresso_unificado", {
                "step": step,
                "message": message,
                "details": details
            }, categoria="logs")
        
        # Executa análise unificada
        logger.info(f"🔬 Executando análise unificada tipo: {analysis_type}")
        
        unified_result = unified_analysis_engine.execute_unified_analysis(
            data,
            analysis_type=analysis_type,
            session_id=session_id,
            progress_callback=progress_callback
        )
        
        # Salva resultado unificado
        salvar_etapa("resultado_unificado", unified_result, categoria="analise_completa")
        
        # Marca progresso como completo
        progress_tracker.complete()
        
        # Salva no banco de dados
        try:
            db_record = db_manager.create_analysis({
                **data,
                **unified_result,
                'analysis_type': f'unified_{analysis_type}',
                'session_id': session_id,
                'status': 'completed'
            })
            
            if db_record:
                unified_result['database_id'] = db_record.get('id')
                unified_result['local_files'] = db_record.get('local_files')
                logger.info(f"✅ Análise unificada salva: ID {db_record.get('id')}")
        except Exception as e:
            logger.error(f"❌ Erro ao salvar no banco: {e}")
            unified_result['database_warning'] = f"Falha ao salvar: {str(e)}"
        
        # Calcula tempo de processamento
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Adiciona metadados finais
        unified_result['metadata_final'] = {
            'processing_time_seconds': processing_time,
            'processing_time_formatted': f"{int(processing_time // 60)}m {int(processing_time % 60)}s",
            'request_timestamp': datetime.now().isoformat(),
            'session_id': session_id,
            'analysis_type': f'unified_{analysis_type}',
            'unified_system': True,
            'exa_enhanced': exa_client.is_available(),
            'pymupdf_pro': pymupdf_client.is_available(),
            'providers_used': len(unified_result.get('pesquisa_unificada', {}).get('provider_results', {})),
            'total_sources': unified_result.get('pesquisa_unificada', {}).get('statistics', {}).get('total_results', 0)
        }
        
        # Salva resposta final
        salvar_etapa("resposta_unificada_final", unified_result, categoria="analise_completa")
        
        logger.info(f"✅ Análise unificada concluída em {processing_time:.2f} segundos")
        
        return jsonify(unified_result)
        
    except Exception as e:
        logger.error(f"❌ Erro crítico na análise unificada: {str(e)}", exc_info=True)
        
        return jsonify({
            'error': 'Erro na análise unificada',
            'message': str(e),
            'timestamp': datetime.now().isoformat(),
            'recommendation': 'Configure todas as APIs necessárias e tente novamente',
            'session_id': locals().get('session_id', 'unknown'),
            'analysis_type': locals().get('analysis_type', 'unknown'),
            'capabilities': unified_analysis_engine.get_analysis_capabilities()
        }), 500

@unified_bp.route('/capabilities', methods=['GET'])
def get_capabilities():
    """Retorna capacidades do sistema unificado"""
    
    try:
        capabilities = unified_analysis_engine.get_analysis_capabilities()
        
        return jsonify({
            'success': True,
            'capabilities': capabilities,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter capacidades: {e}")
        return jsonify({
            'error': 'Erro ao obter capacidades',
            'message': str(e)
        }), 500

@unified_bp.route('/test_exa', methods=['POST'])
def test_exa():
    """Testa integração com Exa"""
    
    try:
        data = request.get_json()
        query = data.get('query', 'mercado digital Brasil 2024')
        
        if not exa_client.is_available():
            return jsonify({
                'success': False,
                'error': 'Exa não está disponível',
                'message': 'Configure EXA_API_KEY'
            }), 400
        
        # Testa busca
        results = exa_client.search(query, num_results=5)
        
        return jsonify({
            'success': True,
            'query': query,
            'results': results,
            'exa_available': True,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro no teste Exa: {e}")
        return jsonify({
            'error': 'Erro no teste Exa',
            'message': str(e)
        }), 500

@unified_bp.route('/test_pymupdf', methods=['POST'])
def test_pymupdf():
    """Testa integração com PyMuPDF Pro"""
    
    try:
        data = request.get_json()
        test_url = data.get('url', 'https://www.example.com/sample.pdf')
        
        if not pymupdf_client.is_available():
            return jsonify({
                'success': False,
                'error': 'PyMuPDF não está disponível',
                'message': 'Execute: pip install PyMuPDF'
            }), 400
        
        # Testa extração de PDF
        if test_url.endswith('.pdf'):
            result = pymupdf_client.extract_from_url(test_url)
        else:
            result = {
                'success': False,
                'error': 'URL não é um PDF',
                'message': 'Forneça uma URL de PDF para teste'
            }
        
        return jsonify({
            'success': result['success'],
            'result': result,
            'pymupdf_available': True,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro no teste PyMuPDF: {e}")
        return jsonify({
            'error': 'Erro no teste PyMuPDF',
            'message': str(e)
        }), 500

@unified_bp.route('/search_unified', methods=['POST'])
def search_unified():
    """Endpoint para busca unificada"""
    
    try:
        data = request.get_json()
        query = data.get('query')
        max_results = min(int(data.get('max_results', 20)), 50)
        context = data.get('context', {})
        
        if not query:
            return jsonify({
                'error': 'Query obrigatória',
                'message': 'Forneça uma query para busca'
            }), 400
        
        # Executa busca unificada
        search_results = unified_search_manager.unified_search(
            query, 
            max_results=max_results, 
            context=context
        )
        
        return jsonify({
            'success': True,
            'search_results': search_results,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro na busca unificada: {e}")
        return jsonify({
            'error': 'Erro na busca unificada',
            'message': str(e)
        }), 500

@unified_bp.route('/upload_unified', methods=['POST'])
def upload_unified():
    """Upload unificado com processamento avançado"""
    
    try:
        if 'file' not in request.files:
            return jsonify({
                'success': False,
                'error': 'Nenhum arquivo enviado'
            }), 400
        
        file = request.files['file']
        session_id = request.form.get('session_id', f"upload_{int(time.time())}")
        
        if file.filename == '':
            return jsonify({
                'success': False,
                'error': 'Nome de arquivo vazio'
            }), 400
        
        # Processa arquivo
        if file.filename.lower().endswith('.pdf'):
            # Usa PyMuPDF Pro para PDFs
            result = self._process_pdf_upload(file, session_id)
        else:
            # Usa processador padrão
            result = attachment_service.process_attachment(file, session_id)
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Erro no upload unificado: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno no upload',
            'message': str(e)
        }), 500

def _process_pdf_upload(file, session_id: str) -> Dict[str, Any]:
    """Processa upload de PDF com PyMuPDF Pro"""
    
    try:
        import tempfile
        
        # Salva arquivo temporariamente
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            file.save(temp_file.name)
            temp_path = temp_file.name
        
        try:
            # Extrai com PyMuPDF Pro
            if pymupdf_client.is_available():
                extraction_result = pymupdf_client.extract_text_advanced(
                    temp_path,
                    include_tables=True,
                    include_annotations=True
                )
                
                if extraction_result['success']:
                    return {
                        'success': True,
                        'message': 'PDF processado com PyMuPDF Pro',
                        'session_id': session_id,
                        'filename': file.filename,
                        'content_type': 'pdf_advanced',
                        'extraction_result': extraction_result,
                        'metadata': {
                            'extractor': 'PyMuPDF_Pro',
                            'pages': extraction_result['metadata']['pages'],
                            'total_characters': extraction_result['statistics']['total_characters'],
                            'total_words': extraction_result['statistics']['total_words'],
                            'tables_found': extraction_result['statistics']['total_tables'],
                            'processed_at': datetime.now().isoformat()
                        }
                    }
                else:
                    raise Exception(extraction_result['error'])
            else:
                # Fallback para processador padrão
                return attachment_service.process_attachment(file, session_id)
                
        finally:
            # Remove arquivo temporário
            try:
                os.unlink(temp_path)
            except:
                pass
                
    except Exception as e:
        logger.error(f"Erro ao processar PDF: {e}")
        return {
            'success': False,
            'error': f'Erro no processamento de PDF: {str(e)}'
        }

@unified_bp.route('/system_status', methods=['GET'])
def get_system_status():
    """Retorna status completo do sistema unificado"""
    
    try:
        # Status dos provedores de busca
        search_status = unified_search_manager.get_provider_status()
        
        # Status das capacidades
        capabilities = unified_analysis_engine.get_analysis_capabilities()
        
        # Status dos extratores
        from services.robust_content_extractor import robust_content_extractor
        extractor_stats = robust_content_extractor.get_extractor_stats()
        
        # Status do banco
        db_status = db_manager.test_connection()
        
        # Calcula status geral
        total_search_available = sum(1 for p in search_status.values() if p['available'])
        total_ai_available = len([p for p in capabilities['ai_providers'].values() if p['available']])
        
        overall_status = "healthy" if (total_search_available > 0 and total_ai_available > 0 and db_status) else "degraded"
        
        return jsonify({
            'status': overall_status,
            'timestamp': datetime.now().isoformat(),
            'version': '2.0.0 - Unified',
            'systems': {
                'search_providers': {
                    'status': 'healthy' if total_search_available > 0 else 'error',
                    'available_count': total_search_available,
                    'providers': search_status
                },
                'ai_providers': {
                    'status': 'healthy' if total_ai_available > 0 else 'error',
                    'available_count': total_ai_available,
                    'providers': capabilities['ai_providers']
                },
                'extraction_capabilities': capabilities['extraction_capabilities'],
                'database': {
                    'status': 'healthy' if db_status else 'error',
                    'connected': db_status
                },
                'extractor_stats': extractor_stats
            },
            'enhanced_features': {
                'exa_neural_search': exa_client.is_available(),
                'pymupdf_pro': pymupdf_client.is_available(),
                'unified_interface': True,
                'multi_agent_analysis': True,
                'auto_save_system': True,
                'resilient_execution': True
            }
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter status: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@unified_bp.route('/reset_system', methods=['POST'])
def reset_system():
    """Reset completo do sistema"""
    
    try:
        data = request.get_json() or {}
        reset_type = data.get('type', 'all')  # all, search, extractors, ai
        
        if reset_type in ['all', 'search']:
            unified_search_manager.reset_provider_errors()
        
        if reset_type in ['all', 'extractors']:
            robust_content_extractor.reset_extractor_stats()
        
        if reset_type in ['all', 'ai']:
            from services.ai_manager import ai_manager
            ai_manager.reset_provider_errors()
        
        message = f"Reset do sistema: {reset_type}"
        logger.info(f"🔄 {message}")
        
        return jsonify({
            'success': True,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'system_status': unified_search_manager.get_provider_status()
        })
        
    except Exception as e:
        logger.error(f"Erro no reset: {e}")
        return jsonify({
            'error': 'Erro no reset do sistema',
            'message': str(e)
        }), 500

@unified_bp.route('/analysis_types', methods=['GET'])
def get_analysis_types():
    """Retorna tipos de análise disponíveis"""
    
    try:
        return jsonify({
            'success': True,
            'analysis_types': unified_analysis_engine.analysis_types,
            'available_agents': list(unified_analysis_engine.available_agents.keys()),
            'capabilities': unified_analysis_engine.get_analysis_capabilities()
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter tipos de análise: {e}")
        return jsonify({
            'error': 'Erro ao obter tipos de análise',
            'message': str(e)
        }), 500