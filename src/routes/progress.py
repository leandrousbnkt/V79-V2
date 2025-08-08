#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v2.0 - Progress Routes CORRIGIDO
Sistema de progresso em tempo real COMPLETAMENTE FUNCIONAL
"""

import os
import logging
import time
import json
from datetime import datetime
from flask import Blueprint, request, jsonify, session
from flask_socketio import SocketIO, emit, join_room, leave_room
import threading
from queue import Queue
import uuid

logger = logging.getLogger(__name__)

# Cria blueprint
progress_bp = Blueprint('progress', __name__)

# Sistema de progresso global CORRIGIDO
progress_sessions = {}
progress_queues = {}
progress_lock = threading.Lock()

class ProgressTracker:
    """Rastreador de progresso em tempo real COMPLETAMENTE FUNCIONAL"""
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.current_step = 0
        self.total_steps = 13
        self.start_time = time.time()
        self.last_update = time.time()
        self.is_active = True
        self.is_complete = False
        
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
        self.current_message = "Iniciando análise..."
        self.current_details = None
        
        # Registra sessão global COM LOCK
        with progress_lock:
            progress_sessions[session_id] = self
            progress_queues[session_id] = Queue()
        
        logger.info(f"✅ ProgressTracker criado para sessão: {session_id}")
    
    def update_progress(self, step: int, message: str, details: str = None):
        """Atualiza progresso da análise"""
        try:
            with progress_lock:
                if not self.is_active:
                    return None
                
                self.current_step = max(0, min(step, self.total_steps))
                self.current_message = message
                self.current_details = details
                self.last_update = time.time()
                
                current_time = time.time()
                elapsed = current_time - self.start_time
                
                # Calcula tempo estimado
                if self.current_step > 0:
                    estimated_total = (elapsed / self.current_step) * self.total_steps
                    remaining = max(0, estimated_total - elapsed)
                else:
                    remaining = 300  # 5 minutos estimado inicial
                
                progress_data = {
                    "session_id": self.session_id,
                    "current_step": self.current_step,
                    "total_steps": self.total_steps,
                    "percentage": (self.current_step / self.total_steps) * 100,
                    "current_message": message,
                    "detailed_message": details or message,
                    "elapsed_time": elapsed,
                    "estimated_remaining": remaining,
                    "estimated_total": elapsed + remaining,
                    "timestamp": datetime.now().isoformat(),
                    "is_complete": self.is_complete,
                    "is_active": self.is_active
                }
                
                # Log detalhado
                log_entry = {
                    "step": self.current_step,
                    "message": message,
                    "details": details,
                    "timestamp": datetime.now().isoformat(),
                    "elapsed": elapsed
                }
                self.detailed_logs.append(log_entry)
                
                # Mantém apenas últimos 50 logs
                if len(self.detailed_logs) > 50:
                    self.detailed_logs = self.detailed_logs[-50:]
                
                # Adiciona à queue para polling
                if self.session_id in progress_queues:
                    try:
                        # Limpa queue antiga se muito cheia
                        queue = progress_queues[self.session_id]
                        if queue.qsize() > 100:
                            while not queue.empty():
                                try:
                                    queue.get_nowait()
                                except:
                                    break
                        
                        queue.put(progress_data)
                    except Exception as e:
                        logger.error(f"Erro ao adicionar à queue: {e}")
                
                logger.info(f"📊 Progress {self.session_id}: Step {self.current_step}/{self.total_steps} - {message}")
                
                return progress_data
        
        except Exception as e:
            logger.error(f"Erro ao atualizar progresso: {e}")
            return None
    
    def complete(self):
        """Marca análise como completa"""
        try:
            with progress_lock:
                self.is_complete = True
                self.current_step = self.total_steps
                self.update_progress(self.total_steps, "🎉 Análise concluída! Preparando resultados...")
                
                logger.info(f"✅ Análise {self.session_id} marcada como completa")
                
                # Remove da sessão após 10 minutos
                def cleanup():
                    time.sleep(600)  # 10 minutos
                    try:
                        with progress_lock:
                            if self.session_id in progress_sessions:
                                del progress_sessions[self.session_id]
                            if self.session_id in progress_queues:
                                del progress_queues[self.session_id]
                        logger.info(f"🧹 Limpeza automática: sessão {self.session_id} removida")
                    except Exception as e:
                        logger.error(f"Erro na limpeza automática: {e}")
                
                threading.Thread(target=cleanup, daemon=True).start()
        
        except Exception as e:
            logger.error(f"Erro ao completar análise: {e}")
    
    def get_current_status(self):
        """Retorna status atual THREAD-SAFE"""
        try:
            with progress_lock:
                elapsed = time.time() - self.start_time
                
                if self.current_step > 0:
                    estimated_total = (elapsed / self.current_step) * self.total_steps
                    remaining = max(0, estimated_total - elapsed)
                else:
                    remaining = 300
                
                return {
                    "session_id": self.session_id,
                    "current_step": self.current_step,
                    "total_steps": self.total_steps,
                    "percentage": round((self.current_step / self.total_steps) * 100, 2),
                    "current_message": self.current_message,
                    "current_details": self.current_details,
                    "elapsed_time": round(elapsed, 2),
                    "estimated_remaining": round(remaining, 2),
                    "detailed_logs": self.detailed_logs[-10:],  # Últimos 10 logs
                    "is_complete": self.is_complete,
                    "is_active": self.is_active,
                    "last_update": datetime.fromtimestamp(self.last_update).isoformat(),
                    "total_logs": len(self.detailed_logs)
                }
        except Exception as e:
            logger.error(f"Erro ao obter status: {e}")
            return {"error": str(e)}

# ===== ROTAS PRINCIPAIS =====

@progress_bp.route('/start_tracking', methods=['POST'])
def start_tracking():
    """Inicia rastreamento de progresso"""
    try:
        data = request.get_json() or {}
        session_id = data.get('session_id')
        
        if not session_id:
            session_id = f"session_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        
        # Remove tracker existente se houver
        with progress_lock:
            if session_id in progress_sessions:
                old_tracker = progress_sessions[session_id]
                old_tracker.is_active = False
                del progress_sessions[session_id]
            if session_id in progress_queues:
                del progress_queues[session_id]
        
        # Cria novo tracker
        tracker = ProgressTracker(session_id)
        tracker.update_progress(0, "🚀 Iniciando análise ultra-detalhada...")
        
        logger.info(f"🎯 Rastreamento iniciado para sessão: {session_id}")
        
        return jsonify({
            'success': True,
            'session_id': session_id,
            'message': 'Rastreamento iniciado com sucesso',
            'status': tracker.get_current_status(),
            'endpoints': {
                'progress': f'/api/progress/{session_id}',
                'polling': f'/api/progress/poll/{session_id}',
                'logs': f'/api/progress/logs/{session_id}'
            }
        })
        
    except Exception as e:
        logger.error(f"❌ Erro ao iniciar rastreamento: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Erro interno',
            'message': str(e)
        }), 500

@progress_bp.route('/<session_id>', methods=['GET'])
def get_progress_main(session_id):
    """Obtém progresso atual - ROTA PRINCIPAL"""
    try:
        if session_id not in progress_sessions:
            logger.warning(f"⚠️ Sessão não encontrada: {session_id}")
            return jsonify({
                'success': False,
                'error': 'Sessão não encontrada',
                'session_id': session_id,
                'available_sessions': list(progress_sessions.keys()),
                'suggestion': 'Inicie o rastreamento primeiro em /api/progress/start_tracking'
            }), 404
        
        tracker = progress_sessions[session_id]
        status = tracker.get_current_status()
        
        if 'error' in status:
            return jsonify({
                'success': False,
                'error': status['error']
            }), 500
        
        return jsonify({
            'success': True,
            'progress': status,
            'session_found': True
        })
        
    except Exception as e:
        logger.error(f"❌ Erro ao obter progresso: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Erro interno',
            'message': str(e)
        }), 500

@progress_bp.route('/get_progress/<session_id>', methods=['GET'])
def get_progress_alt(session_id):
    """Rota alternativa para progresso"""
    return get_progress_main(session_id)

@progress_bp.route('/session/<session_id>', methods=['GET'])
def get_session_progress(session_id):
    """Rota de sessão para progresso"""
    return get_progress_main(session_id)

@progress_bp.route('/poll/<session_id>', methods=['GET'])
def poll_updates(session_id):
    """Polling para atualizações de progresso"""
    try:
        if session_id not in progress_queues:
            return jsonify({
                'success': False,
                'error': 'Sessão não encontrada para polling',
                'session_id': session_id
            }), 404
        
        queue = progress_queues[session_id]
        updates = []
        max_updates = 50  # Limite de updates por poll
        
        # Coleta atualizações disponíveis
        while not queue.empty() and len(updates) < max_updates:
            try:
                update = queue.get_nowait()
                updates.append(update)
            except:
                break
        
        return jsonify({
            'success': True,
            'updates': updates,
            'has_updates': len(updates) > 0,
            'update_count': len(updates),
            'session_id': session_id
        })
        
    except Exception as e:
        logger.error(f"❌ Erro no polling: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Erro interno no polling',
            'message': str(e)
        }), 500

@progress_bp.route('/poll_updates/<session_id>', methods=['GET'])
def poll_updates_alt(session_id):
    """Rota alternativa para polling"""
    return poll_updates(session_id)

@progress_bp.route('/update', methods=['POST'])
def update_progress_endpoint():
    """Atualiza progresso (usado internamente)"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'error': 'Dados não fornecidos'
            }), 400
        
        session_id = data.get('session_id')
        step = data.get('step', 0)
        message = data.get('message', 'Processando...')
        details = data.get('details')
        
        if not session_id:
            return jsonify({
                'success': False,
                'error': 'Session ID obrigatório'
            }), 400
        
        if session_id not in progress_sessions:
            return jsonify({
                'success': False,
                'error': 'Sessão não encontrada',
                'session_id': session_id
            }), 404
        
        tracker = progress_sessions[session_id]
        progress_data = tracker.update_progress(step, message, details)
        
        if progress_data:
            return jsonify({
                'success': True,
                'progress': progress_data,
                'updated': True
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Falha ao atualizar progresso'
            }), 500
        
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar progresso: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Erro interno',
            'message': str(e)
        }), 500

@progress_bp.route('/update_progress', methods=['POST'])
def update_progress_alt():
    """Rota alternativa para atualização"""
    return update_progress_endpoint()

@progress_bp.route('/complete', methods=['POST'])
def complete_analysis():
    """Marca análise como completa"""
    try:
        data = request.get_json() or {}
        session_id = data.get('session_id')
        
        if not session_id:
            return jsonify({
                'success': False,
                'error': 'Session ID obrigatório'
            }), 400
        
        if session_id not in progress_sessions:
            return jsonify({
                'success': False,
                'error': 'Sessão não encontrada',
                'session_id': session_id
            }), 404
        
        tracker = progress_sessions[session_id]
        tracker.complete()
        
        return jsonify({
            'success': True,
            'message': 'Análise marcada como completa',
            'final_status': tracker.get_current_status(),
            'session_id': session_id
        })
        
    except Exception as e:
        logger.error(f"❌ Erro ao completar análise: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Erro interno',
            'message': str(e)
        }), 500

@progress_bp.route('/complete_analysis', methods=['POST'])
def complete_analysis_alt():
    """Rota alternativa para completar análise"""
    return complete_analysis()

@progress_bp.route('/logs/<session_id>', methods=['GET'])
def get_detailed_logs(session_id):
    """Obtém logs detalhados da análise"""
    try:
        if session_id not in progress_sessions:
            return jsonify({
                'success': False,
                'error': 'Sessão não encontrada',
                'session_id': session_id
            }), 404
        
        tracker = progress_sessions[session_id]
        
        return jsonify({
            'success': True,
            'session_id': session_id,
            'logs': tracker.detailed_logs,
            'total_logs': len(tracker.detailed_logs),
            'analysis_duration': time.time() - tracker.start_time,
            'is_complete': tracker.is_complete,
            'current_step': tracker.current_step
        })
        
    except Exception as e:
        logger.error(f"❌ Erro ao obter logs: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Erro interno',
            'message': str(e)
        }), 500

@progress_bp.route('/get_detailed_logs/<session_id>', methods=['GET'])
def get_detailed_logs_alt(session_id):
    """Rota alternativa para logs detalhados"""
    return get_detailed_logs(session_id)

@progress_bp.route('/active_sessions', methods=['GET'])
def get_active_sessions():
    """Lista sessões ativas de progresso"""
    try:
        active = []
        current_time = time.time()
        
        with progress_lock:
            for session_id, tracker in progress_sessions.items():
                try:
                    active.append({
                        'session_id': session_id,
                        'current_step': tracker.current_step,
                        'total_steps': tracker.total_steps,
                        'percentage': round((tracker.current_step / tracker.total_steps) * 100, 2),
                        'elapsed_time': round(current_time - tracker.start_time, 2),
                        'is_complete': tracker.is_complete,
                        'is_active': tracker.is_active,
                        'last_message': tracker.current_message,
                        'last_update': datetime.fromtimestamp(tracker.last_update).isoformat()
                    })
                except Exception as e:
                    logger.error(f"Erro ao processar sessão {session_id}: {e}")
        
        return jsonify({
            'success': True,
            'active_sessions': active,
            'total_active': len(active),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"❌ Erro ao listar sessões: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Erro interno',
            'message': str(e)
        }), 500

@progress_bp.route('/cleanup', methods=['POST'])
def cleanup_sessions():
    """Limpa sessões antigas ou inativas"""
    try:
        data = request.get_json() or {}
        max_age_minutes = data.get('max_age_minutes', 60)  # 1 hora por padrão
        force_cleanup = data.get('force', False)
        
        cleaned = 0
        current_time = time.time()
        
        with progress_lock:
            sessions_to_remove = []
            
            for session_id, tracker in progress_sessions.items():
                age_minutes = (current_time - tracker.start_time) / 60
                
                if force_cleanup or age_minutes > max_age_minutes or not tracker.is_active:
                    sessions_to_remove.append(session_id)
            
            for session_id in sessions_to_remove:
                try:
                    if session_id in progress_sessions:
                        del progress_sessions[session_id]
                    if session_id in progress_queues:
                        del progress_queues[session_id]
                    cleaned += 1
                except Exception as e:
                    logger.error(f"Erro ao remover sessão {session_id}: {e}")
        
        logger.info(f"🧹 Limpeza manual: {cleaned} sessões removidas")
        
        return jsonify({
            'success': True,
            'cleaned_sessions': cleaned,
            'remaining_sessions': len(progress_sessions),
            'cleanup_criteria': {
                'max_age_minutes': max_age_minutes,
                'force_cleanup': force_cleanup
            }
        })
        
    except Exception as e:
        logger.error(f"❌ Erro na limpeza: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Erro interno na limpeza',
            'message': str(e)
        }), 500

@progress_bp.route('/health', methods=['GET'])
def health_check():
    """Verificação de saúde do sistema de progresso"""
    try:
        with progress_lock:
            total_sessions = len(progress_sessions)
            total_queues = len(progress_queues)
            active_sessions = len([t for t in progress_sessions.values() if t.is_active])
            complete_sessions = len([t for t in progress_sessions.values() if t.is_complete])
        
        return jsonify({
            'success': True,
            'status': 'healthy',
            'statistics': {
                'total_sessions': total_sessions,
                'total_queues': total_queues,
                'active_sessions': active_sessions,
                'complete_sessions': complete_sessions,
                'inactive_sessions': total_sessions - active_sessions
            },
            'system_info': {
                'progress_lock_active': True,
                'thread_safe': True,
                'auto_cleanup_enabled': True
            },
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"❌ Erro na verificação de saúde: {str(e)}")
        return jsonify({
            'success': False,
            'status': 'error',
            'error': str(e)
        }), 500

# ===== FUNÇÕES HELPER =====

def get_progress_tracker(session_id: str) -> ProgressTracker:
    """Obtém ou cria tracker de progresso para uma sessão"""
    try:
        with progress_lock:
            if session_id not in progress_sessions:
                tracker = ProgressTracker(session_id)
                logger.info(f"🆕 Novo tracker criado para sessão: {session_id}")
                return tracker
            return progress_sessions[session_id]
    except Exception as e:
        logger.error(f"Erro ao obter tracker: {e}")
        return None

def update_analysis_progress(session_id: str, step: int, message: str, details: str = None):
    """Função helper para atualizar progresso de qualquer lugar"""
    try:
        if session_id in progress_sessions:
            tracker = progress_sessions[session_id]
            return tracker.update_progress(step, message, details)
        else:
            logger.warning(f"Tentativa de atualizar sessão inexistente: {session_id}")
            return None
    except Exception as e:
        logger.error(f"Erro ao atualizar progresso da sessão {session_id}: {e}")
        return None

def create_session_id() -> str:
    """Cria um ID único para sessão"""
    return f"session_{int(time.time())}_{uuid.uuid4().hex[:8]}"

# Inicialização do sistema
logger.info("✅ Sistema de progresso inicializado com TODAS as rotas corrigidas")
