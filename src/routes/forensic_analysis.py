#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v2.0 - Forensic Analysis Routes
Rotas para os novos módulos de análise forense
"""

import os
import logging
import time
import json
from datetime import datetime
from flask import Blueprint, request, jsonify, render_template
from werkzeug.utils import secure_filename
from services.forensic_cpl_analyzer import forensic_cpl_analyzer
from services.visceral_leads_engineer import visceral_leads_engineer
from services.pre_pitch_architect_advanced import pre_pitch_architect_advanced
from services.attachment_service import attachment_service
from database import db_manager
from services.auto_save_manager import salvar_etapa, salvar_erro

logger = logging.getLogger(__name__)

# Cria blueprint
forensic_bp = Blueprint('forensic', __name__)

@forensic_bp.route('/forensic_interface')
def forensic_interface():
    """Interface principal dos módulos forenses"""
    return render_template('forensic_interface.html')

@forensic_bp.route('/analyze_cpl_forensic', methods=['POST'])
def analyze_cpl_forensic():
    """Endpoint para análise forense de CPL"""
    
    try:
        logger.info("🔬 Iniciando análise forense de CPL")
        
        # Coleta dados da requisição
        data = request.get_json()
        if not data:
            return jsonify({
                'error': 'Dados não fornecidos',
                'message': 'Envie os dados da análise forense no corpo da requisição'
            }), 400
        
        # Validação básica
        transcription = data.get('transcription', '').strip()
        if not transcription:
            return jsonify({
                'error': 'Transcrição obrigatória',
                'message': 'A transcrição do CPL é obrigatória para análise forense'
            }), 400
        
        if len(transcription) < 500:
            return jsonify({
                'error': 'Transcrição muito curta',
                'message': 'A transcrição deve ter pelo menos 500 caracteres para análise forense'
            }), 400
        
        # Adiciona session_id se não fornecido
        session_id = data.get('session_id', f"cpl_forensic_{int(time.time())}")
        
        # Contexto da análise
        context_data = {
            'contexto_estrategico': data.get('contexto_estrategico', ''),
            'objetivo_cpl': data.get('objetivo_cpl', ''),
            'sequencia': data.get('sequencia', ''),
            'formato': data.get('formato', ''),
            'temperatura_audiencia': data.get('temperatura_audiencia', ''),
            'tamanho_audiencia': data.get('tamanho_audiencia', ''),
            'origem_audiencia': data.get('origem_audiencia', ''),
            'nivel_consciencia': data.get('nivel_consciencia', ''),
            'produto_preco': data.get('produto_preco', ''),
            'novidade_produto': data.get('novidade_produto', '')
        }
        
        # Executa análise forense
        forensic_result = forensic_cpl_analyzer.analyze_cpl_forensically(
            transcription, context_data, session_id
        )
        
        # Salva no banco de dados
        try:
            db_record = db_manager.create_analysis({
                'segmento': 'Análise Forense CPL',
                'produto': context_data.get('produto_preco', 'CPL'),
                'status': 'completed',
                'session_id': session_id,
                'analysis_type': 'forensic_cpl',
                **forensic_result
            })
            
            if db_record:
                forensic_result['database_id'] = db_record.get('id')
                logger.info(f"✅ Análise forense salva: ID {db_record.get('id')}")
        except Exception as e:
            logger.error(f"❌ Erro ao salvar análise forense: {e}")
            forensic_result['database_warning'] = f"Falha ao salvar: {str(e)}"
        
        # Adiciona metadados finais
        forensic_result['metadata_final'] = {
            'session_id': session_id,
            'analysis_type': 'forensic_cpl_analysis',
            'transcription_length': len(transcription),
            'context_provided': bool(any(context_data.values())),
            'generated_at': datetime.now().isoformat(),
            'agent': 'ARQUEÓLOGO MESTRE DA PERSUASÃO'
        }
        
        logger.info("✅ Análise forense de CPL concluída")
        return jsonify(forensic_result)
        
    except Exception as e:
        logger.error(f"❌ Erro na análise forense de CPL: {str(e)}")
        return jsonify({
            'error': 'Erro na análise forense',
            'message': str(e),
            'timestamp': datetime.now().isoformat(),
            'recommendation': 'Verifique a transcrição e tente novamente'
        }), 500

@forensic_bp.route('/reverse_engineer_leads', methods=['POST'])
def reverse_engineer_leads():
    """Endpoint para engenharia reversa de leads"""
    
    try:
        logger.info("🧠 Iniciando engenharia reversa de leads")
        
        # Coleta dados da requisição
        data = request.get_json()
        if not data:
            return jsonify({
                'error': 'Dados não fornecidos',
                'message': 'Envie os dados da engenharia reversa no corpo da requisição'
            }), 400
        
        # Validação básica
        leads_data = data.get('leads_data', '').strip()
        if not leads_data:
            return jsonify({
                'error': 'Dados de leads obrigatórios',
                'message': 'Os dados dos leads são obrigatórios para engenharia reversa'
            }), 400
        
        if len(leads_data) < 200:
            return jsonify({
                'error': 'Dados de leads insuficientes',
                'message': 'Os dados devem ter pelo menos 200 caracteres para análise'
            }), 400
        
        # Adiciona session_id se não fornecido
        session_id = data.get('session_id', f"leads_visceral_{int(time.time())}")
        
        # Contexto da análise
        context_data = {
            'produto_servico': data.get('produto_servico', ''),
            'principais_perguntas': data.get('principais_perguntas', ''),
            'numero_respostas': data.get('numero_respostas', 0),
            'informacoes_demograficas': data.get('informacoes_demograficas', '')
        }
        
        # Executa engenharia reversa
        visceral_result = visceral_leads_engineer.reverse_engineer_leads(
            leads_data, context_data, session_id
        )
        
        # Salva no banco de dados
        try:
            db_record = db_manager.create_analysis({
                'segmento': 'Engenharia Reversa Leads',
                'produto': context_data.get('produto_servico', 'Leads'),
                'status': 'completed',
                'session_id': session_id,
                'analysis_type': 'visceral_leads',
                **visceral_result
            })
            
            if db_record:
                visceral_result['database_id'] = db_record.get('id')
                logger.info(f"✅ Engenharia reversa salva: ID {db_record.get('id')}")
        except Exception as e:
            logger.error(f"❌ Erro ao salvar engenharia reversa: {e}")
            visceral_result['database_warning'] = f"Falha ao salvar: {str(e)}"
        
        # Adiciona metadados finais
        visceral_result['metadata_final'] = {
            'session_id': session_id,
            'analysis_type': 'visceral_leads_engineering',
            'leads_data_length': len(leads_data),
            'context_provided': bool(any(context_data.values())),
            'generated_at': datetime.now().isoformat(),
            'agent': 'MESTRE DA PERSUASÃO VISCERAL'
        }
        
        logger.info("✅ Engenharia reversa de leads concluída")
        return jsonify(visceral_result)
        
    except Exception as e:
        logger.error(f"❌ Erro na engenharia reversa: {str(e)}")
        return jsonify({
            'error': 'Erro na engenharia reversa',
            'message': str(e),
            'timestamp': datetime.now().isoformat(),
            'recommendation': 'Verifique os dados dos leads e tente novamente'
        }), 500

@forensic_bp.route('/orchestrate_pre_pitch', methods=['POST'])
def orchestrate_pre_pitch():
    """Endpoint para orquestração de pré-pitch"""
    
    try:
        logger.info("🎯 Iniciando orquestração de pré-pitch")
        
        # Coleta dados da requisição
        data = request.get_json()
        if not data:
            return jsonify({
                'error': 'Dados não fornecidos',
                'message': 'Envie os dados da orquestração no corpo da requisição'
            }), 400
        
        # Validação básica
        selected_drivers = data.get('selected_drivers', [])
        if not selected_drivers:
            return jsonify({
                'error': 'Drivers mentais obrigatórios',
                'message': 'Selecione pelo menos um driver mental para orquestração'
            }), 400
        
        avatar_data = data.get('avatar_data', {})
        if not avatar_data:
            return jsonify({
                'error': 'Avatar obrigatório',
                'message': 'Dados do avatar são obrigatórios para orquestração'
            }), 400
        
        event_structure = data.get('event_structure', '').strip()
        product_offer = data.get('product_offer', '').strip()
        
        if not event_structure:
            return jsonify({
                'error': 'Estrutura do evento obrigatória',
                'message': 'Descreva a estrutura do evento/lançamento'
            }), 400
        
        if not product_offer:
            return jsonify({
                'error': 'Produto e oferta obrigatórios',
                'message': 'Detalhe o produto e a oferta'
            }), 400
        
        # Adiciona session_id se não fornecido
        session_id = data.get('session_id', f"pre_pitch_{int(time.time())}")
        
        # Executa orquestração
        orchestration_result = pre_pitch_architect_advanced.orchestrate_psychological_symphony(
            selected_drivers, avatar_data, event_structure, product_offer, session_id
        )
        
        # Salva no banco de dados
        try:
            db_record = db_manager.create_analysis({
                'segmento': 'Orquestração Pré-Pitch',
                'produto': 'Pré-Pitch Invisível',
                'status': 'completed',
                'session_id': session_id,
                'analysis_type': 'pre_pitch_orchestration',
                **orchestration_result
            })
            
            if db_record:
                orchestration_result['database_id'] = db_record.get('id')
                logger.info(f"✅ Orquestração salva: ID {db_record.get('id')}")
        except Exception as e:
            logger.error(f"❌ Erro ao salvar orquestração: {e}")
            orchestration_result['database_warning'] = f"Falha ao salvar: {str(e)}"
        
        # Adiciona metadados finais
        orchestration_result['metadata_final'] = {
            'session_id': session_id,
            'analysis_type': 'pre_pitch_orchestration',
            'drivers_count': len(selected_drivers),
            'event_structure_length': len(event_structure),
            'product_offer_length': len(product_offer),
            'generated_at': datetime.now().isoformat(),
            'agent': 'MESTRE DO PRÉ-PITCH INVISÍVEL'
        }
        
        logger.info("✅ Orquestração de pré-pitch concluída")
        return jsonify(orchestration_result)
        
    except Exception as e:
        logger.error(f"❌ Erro na orquestração: {str(e)}")
        return jsonify({
            'error': 'Erro na orquestração',
            'message': str(e),
            'timestamp': datetime.now().isoformat(),
            'recommendation': 'Verifique os dados fornecidos e tente novamente'
        }), 500

@forensic_bp.route('/upload_cpl_content', methods=['POST'])
def upload_cpl_content():
    """Upload de conteúdo de CPL (vídeo/áudio)"""
    
    try:
        if 'file' not in request.files:
            return jsonify({
                'success': False,
                'error': 'Nenhum arquivo enviado'
            }), 400
        
        file = request.files['file']
        session_id = request.form.get('session_id', f"cpl_upload_{int(time.time())}")
        
        if file.filename == '':
            return jsonify({
                'success': False,
                'error': 'Nome de arquivo vazio'
            }), 400
        
        # Verifica tipo de arquivo
        allowed_extensions = {'.mp4', '.mp3', '.wav', '.m4a', '.avi', '.mov', '.txt', '.pdf'}
        file_ext = os.path.splitext(file.filename)[1].lower()
        
        if file_ext not in allowed_extensions:
            return jsonify({
                'success': False,
                'error': f'Tipo de arquivo não suportado: {file_ext}'
            }), 400
        
        # Processa arquivo
        result = attachment_service.process_attachment(file, session_id)
        
        if result['success']:
            # Adiciona informações específicas para CPL
            result['cpl_info'] = {
                'file_type': 'cpl_content',
                'requires_transcription': file_ext in {'.mp4', '.mp3', '.wav', '.m4a', '.avi', '.mov'},
                'ready_for_analysis': file_ext in {'.txt', '.pdf'}
            }
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"❌ Erro no upload de CPL: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Erro interno no upload',
            'message': str(e)
        }), 500

@forensic_bp.route('/upload_leads_data', methods=['POST'])
def upload_leads_data():
    """Upload de dados de leads"""
    
    try:
        if 'file' not in request.files:
            return jsonify({
                'success': False,
                'error': 'Nenhum arquivo enviado'
            }), 400
        
        file = request.files['file']
        session_id = request.form.get('session_id', f"leads_upload_{int(time.time())}")
        
        if file.filename == '':
            return jsonify({
                'success': False,
                'error': 'Nome de arquivo vazio'
            }), 400
        
        # Verifica tipo de arquivo
        allowed_extensions = {'.csv', '.xlsx', '.xls', '.txt', '.json'}
        file_ext = os.path.splitext(file.filename)[1].lower()
        
        if file_ext not in allowed_extensions:
            return jsonify({
                'success': False,
                'error': f'Tipo de arquivo não suportado para leads: {file_ext}'
            }), 400
        
        # Processa arquivo
        result = attachment_service.process_attachment(file, session_id)
        
        if result['success']:
            # Adiciona informações específicas para leads
            result['leads_info'] = {
                'file_type': 'leads_data',
                'data_format': file_ext,
                'ready_for_analysis': True
            }
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"❌ Erro no upload de leads: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Erro interno no upload',
            'message': str(e)
        }), 500

@forensic_bp.route('/get_available_drivers', methods=['GET'])
def get_available_drivers():
    """Obtém drivers mentais disponíveis"""
    
    try:
        # Lista drivers mentais padrão disponíveis
        available_drivers = [
            {
                'id': 'ferida_exposta',
                'nome': 'Driver da Ferida Exposta',
                'categoria': 'Confrontação',
                'intensidade': 'Alta',
                'descricao': 'Expõe feridas emocionais para criar urgência'
            },
            {
                'id': 'trofeu_secreto',
                'nome': 'Driver do Troféu Secreto',
                'categoria': 'Aspiração',
                'intensidade': 'Média',
                'descricao': 'Ativa desejos secretos de reconhecimento'
            },
            {
                'id': 'inveja_produtiva',
                'nome': 'Driver da Inveja Produtiva',
                'categoria': 'Comparação',
                'intensidade': 'Alta',
                'descricao': 'Usa comparação social para motivar ação'
            },
            {
                'id': 'relogio_psicologico',
                'nome': 'Driver do Relógio Psicológico',
                'categoria': 'Urgência',
                'intensidade': 'Máxima',
                'descricao': 'Cria pressão temporal psicológica'
            },
            {
                'id': 'identidade_aprisionada',
                'nome': 'Driver da Identidade Aprisionada',
                'categoria': 'Transformação',
                'intensidade': 'Alta',
                'descricao': 'Questiona identidade atual limitante'
            },
            {
                'id': 'custo_invisivel',
                'nome': 'Driver do Custo Invisível',
                'categoria': 'Conscientização',
                'intensidade': 'Média',
                'descricao': 'Revela custos ocultos da inação'
            },
            {
                'id': 'ambicao_expandida',
                'nome': 'Driver da Ambição Expandida',
                'categoria': 'Inspiração',
                'intensidade': 'Alta',
                'descricao': 'Expande visão de possibilidades'
            },
            {
                'id': 'diagnostico_brutal',
                'nome': 'Driver do Diagnóstico Brutal',
                'categoria': 'Realidade',
                'intensidade': 'Máxima',
                'descricao': 'Diagnóstico direto e confrontador'
            },
            {
                'id': 'ambiente_vampiro',
                'nome': 'Driver do Ambiente Vampiro',
                'categoria': 'Conscientização',
                'intensidade': 'Alta',
                'descricao': 'Identifica influências tóxicas'
            },
            {
                'id': 'mentor_salvador',
                'nome': 'Driver do Mentor Salvador',
                'categoria': 'Autoridade',
                'intensidade': 'Média',
                'descricao': 'Posiciona como mentor necessário'
            },
            {
                'id': 'coragem_necessaria',
                'nome': 'Driver da Coragem Necessária',
                'categoria': 'Empoderamento',
                'intensidade': 'Alta',
                'descricao': 'Instala coragem para mudança'
            },
            {
                'id': 'mecanismo_revelado',
                'nome': 'Driver do Mecanismo Revelado',
                'categoria': 'Educação',
                'intensidade': 'Média',
                'descricao': 'Revela como funciona o sistema'
            },
            {
                'id': 'prova_matematica',
                'nome': 'Driver da Prova Matemática',
                'categoria': 'Lógica',
                'intensidade': 'Média',
                'descricao': 'Usa dados e números para convencer'
            },
            {
                'id': 'padrao_oculto',
                'nome': 'Driver do Padrão Oculto',
                'categoria': 'Revelação',
                'intensidade': 'Alta',
                'descricao': 'Revela padrões não percebidos'
            },
            {
                'id': 'excecao_possivel',
                'nome': 'Driver da Exceção Possível',
                'categoria': 'Esperança',
                'intensidade': 'Média',
                'descricao': 'Mostra que exceções são possíveis'
            },
            {
                'id': 'atalho_etico',
                'nome': 'Driver do Atalho Ético',
                'categoria': 'Eficiência',
                'intensidade': 'Média',
                'descricao': 'Apresenta atalho moral e eficaz'
            },
            {
                'id': 'decisao_binaria',
                'nome': 'Driver da Decisão Binária',
                'categoria': 'Decisão',
                'intensidade': 'Máxima',
                'descricao': 'Força escolha entre duas opções'
            },
            {
                'id': 'oportunidade_oculta',
                'nome': 'Driver da Oportunidade Oculta',
                'categoria': 'Revelação',
                'intensidade': 'Alta',
                'descricao': 'Revela oportunidade não percebida'
            },
            {
                'id': 'metodo_vs_sorte',
                'nome': 'Driver do Método vs Sorte',
                'categoria': 'Diferenciação',
                'intensidade': 'Alta',
                'descricao': 'Contrasta método com tentativa'
            }
        ]
        
        return jsonify({
            'success': True,
            'drivers': available_drivers,
            'total_drivers': len(available_drivers),
            'categories': list(set(d['categoria'] for d in available_drivers))
        })
        
    except Exception as e:
        logger.error(f"❌ Erro ao obter drivers: {str(e)}")
        return jsonify({
            'error': 'Erro ao obter drivers',
            'message': str(e)
        }), 500

@forensic_bp.route('/get_available_avatars', methods=['GET'])
def get_available_avatars():
    """Obtém avatares disponíveis"""
    
    try:
        # Lista análises que contêm avatares
        analyses = db_manager.list_analyses(limit=50)
        
        available_avatars = []
        
        for analysis in analyses:
            if analysis.get('avatar_data') or 'avatar' in str(analysis).lower():
                available_avatars.append({
                    'id': analysis['id'],
                    'nome': f"Avatar - {analysis.get('segmento', 'Análise')}",
                    'segmento': analysis.get('segmento', 'N/A'),
                    'produto': analysis.get('produto', 'N/A'),
                    'created_at': analysis.get('created_at', ''),
                    'has_avatar_data': bool(analysis.get('avatar_data'))
                })
        
        # Adiciona avatares padrão se não houver análises
        if not available_avatars:
            available_avatars = [
                {
                    'id': 'default_entrepreneur',
                    'nome': 'Empreendedor Digital Padrão',
                    'segmento': 'Produtos Digitais',
                    'produto': 'Infoprodutos',
                    'created_at': datetime.now().isoformat(),
                    'has_avatar_data': True,
                    'is_default': True
                },
                {
                    'id': 'default_consultant',
                    'nome': 'Consultor Profissional Padrão',
                    'segmento': 'Consultoria',
                    'produto': 'Serviços de Consultoria',
                    'created_at': datetime.now().isoformat(),
                    'has_avatar_data': True,
                    'is_default': True
                }
            ]
        
        return jsonify({
            'success': True,
            'avatars': available_avatars,
            'total_avatars': len(available_avatars)
        })
        
    except Exception as e:
        logger.error(f"❌ Erro ao obter avatares: {str(e)}")
        return jsonify({
            'error': 'Erro ao obter avatares',
            'message': str(e)
        }), 500

@forensic_bp.route('/get_avatar_data/<avatar_id>', methods=['GET'])
def get_avatar_data(avatar_id):
    """Obtém dados detalhados de um avatar"""
    
    try:
        if avatar_id.startswith('default_'):
            # Avatar padrão
            default_avatars = {
                'default_entrepreneur': {
                    'nome_ficticio': 'Empreendedor Digital Brasileiro',
                    'dores_viscerais': [
                        'Trabalhar 12+ horas sem crescimento proporcional',
                        'Ver concorrentes menores crescendo mais rápido',
                        'Sentir-se preso no operacional',
                        'Não conseguir se desconectar do trabalho',
                        'Viver com medo de que tudo desmorone'
                    ],
                    'desejos_secretos': [
                        'Ser reconhecido como autoridade no mercado',
                        'Ter um negócio que funcione sem presença',
                        'Ganhar dinheiro de forma passiva',
                        'Ter liberdade total de horários',
                        'Deixar um legado significativo'
                    ]
                },
                'default_consultant': {
                    'nome_ficticio': 'Consultor Profissional Brasileiro',
                    'dores_viscerais': [
                        'Trocar tempo por dinheiro constantemente',
                        'Não conseguir escalar sem trabalhar mais',
                        'Competir apenas por preço',
                        'Depender de indicações para crescer',
                        'Não ter previsibilidade de receita'
                    ],
                    'desejos_secretos': [
                        'Ser procurado como especialista premium',
                        'Ter metodologia própria reconhecida',
                        'Cobrar valores premium sem resistência',
                        'Ter agenda lotada com clientes ideais',
                        'Ser referência no mercado'
                    ]
                }
            }
            
            avatar_data = default_avatars.get(avatar_id, {})
        else:
            # Avatar de análise existente
            analysis = db_manager.get_analysis(avatar_id)
            if analysis:
                avatar_data = analysis.get('avatar_data') or analysis.get('comprehensive_analysis', {}).get('avatar_ultra_detalhado', {})
            else:
                return jsonify({
                    'error': 'Avatar não encontrado'
                }), 404
        
        return jsonify({
            'success': True,
            'avatar_data': avatar_data,
            'avatar_id': avatar_id
        })
        
    except Exception as e:
        logger.error(f"❌ Erro ao obter dados do avatar: {str(e)}")
        return jsonify({
            'error': 'Erro ao obter dados do avatar',
            'message': str(e)
        }), 500

@forensic_bp.route('/generate_forensic_pdf', methods=['POST'])
def generate_forensic_pdf():
    """Gera PDF da análise forense"""
    
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'error': 'Dados não fornecidos'
            }), 400
        
        analysis_type = data.get('analysis_type', 'forensic')
        analysis_data = data.get('analysis_data', {})
        
        # Gera PDF usando o gerador existente
        from routes.pdf_generator import pdf_generator
        
        pdf_buffer = pdf_generator.generate_analysis_report(analysis_data)
        
        # Salva arquivo temporário
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
            tmp_file.write(pdf_buffer.getvalue())
            tmp_file_path = tmp_file.name
        
        # Retorna arquivo
        from flask import send_file
        return send_file(
            tmp_file_path,
            as_attachment=True,
            download_name=f"analise_forense_{analysis_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
            mimetype='application/pdf'
        )
        
    except Exception as e:
        logger.error(f"❌ Erro ao gerar PDF forense: {str(e)}")
        return jsonify({
            'error': 'Erro ao gerar PDF',
            'message': str(e)
        }), 500

@forensic_bp.route('/test_forensic_system', methods=['POST'])
def test_forensic_system():
    """Testa sistema forense com dados de exemplo"""
    
    try:
        test_type = request.json.get('test_type', 'cpl') if request.json else 'cpl'
        
        if test_type == 'cpl':
            # Teste de análise forense de CPL
            test_transcription = """
            Olá pessoal, bem-vindos ao nosso treinamento sobre marketing digital. 
            Eu sou João Silva, e nos últimos 10 anos ajudei mais de 500 empresas a triplicarem suas vendas online.
            Hoje vou compartilhar com vocês o método exato que uso para isso.
            Mas antes, deixa eu te fazer uma pergunta: você já se sentiu frustrado vendo seus concorrentes crescerem mais rápido que você?
            Se a resposta é sim, você não está sozinho. 90% dos empreendedores passam por isso.
            O problema não é falta de esforço, é falta de método. E é exatamente isso que vou te ensinar hoje.
            """
            
            context_data = {
                'contexto_estrategico': 'Primeiro contato',
                'objetivo_cpl': 'Educar',
                'formato': 'Gravado',
                'temperatura_audiencia': 'Fria'
            }
            
            result = forensic_cpl_analyzer.analyze_cpl_forensically(
                test_transcription, context_data, f"test_cpl_{int(time.time())}"
            )
            
        elif test_type == 'leads':
            # Teste de engenharia reversa de leads
            test_leads_data = """
            Pergunta 1: Qual seu maior desafio no negócio?
            Resposta 1: Não consigo escalar sem trabalhar mais horas
            Resposta 2: Tenho dificuldade para precificar meus serviços
            Resposta 3: Não sei como me posicionar no mercado
            
            Pergunta 2: O que mais te frustra atualmente?
            Resposta 1: Ver concorrentes menores crescendo mais rápido
            Resposta 2: Trabalhar muito e ganhar pouco
            Resposta 3: Não ter tempo para família
            """
            
            context_data = {
                'produto_servico': 'Consultoria em Marketing',
                'numero_respostas': 3,
                'principais_perguntas': 'Desafios e frustrações no negócio'
            }
            
            result = visceral_leads_engineer.reverse_engineer_leads(
                test_leads_data, context_data, f"test_leads_{int(time.time())}"
            )
            
        else:
            return jsonify({
                'error': 'Tipo de teste inválido',
                'valid_types': ['cpl', 'leads']
            }), 400
        
        return jsonify({
            'success': True,
            'test_type': test_type,
            'result': result,
            'message': f'Teste de {test_type} executado com sucesso'
        })
        
    except Exception as e:
        logger.error(f"❌ Erro no teste forense: {str(e)}")
        return jsonify({
            'error': 'Erro no teste do sistema forense',
            'message': str(e)
        }), 500