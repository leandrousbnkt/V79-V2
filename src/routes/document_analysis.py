#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v3.0 - Módulo de Análise de Documentos
Rota para upload e análise inteligente de documentos
"""

import os
import json
import uuid
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from flask import Blueprint, request, jsonify, current_app
from werkzeug.utils import secure_filename
import mimetypes

# Imports para processamento de documentos
import pandas as pd
from PIL import Image
import PyPDF2
import docx
import markdown

logger = logging.getLogger(__name__)

document_analysis_bp = Blueprint('document_analysis', __name__)

# Configurações de upload
UPLOAD_FOLDER = 'uploads/documents'
ALLOWED_EXTENSIONS = {
    'json', 'md', 'txt', 'doc', 'docx', 'xls', 'xlsx', 'csv', 'png', 'jpg', 'jpeg', 'pdf'
}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB

def allowed_file(filename):
    """Verifica se o arquivo é permitido"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def ensure_upload_dir():
    """Garante que o diretório de upload existe"""
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@document_analysis_bp.route('/upload', methods=['POST'])
def upload_documents():
    """Upload de múltiplos documentos para análise"""
    try:
        ensure_upload_dir()
        
        if 'files' not in request.files:
            return jsonify({'error': 'Nenhum arquivo enviado'}), 400
        
        files = request.files.getlist('files')
        if not files or all(f.filename == '' for f in files):
            return jsonify({'error': 'Nenhum arquivo selecionado'}), 400
        
        session_id = str(uuid.uuid4())
        session_dir = os.path.join(UPLOAD_FOLDER, session_id)
        os.makedirs(session_dir, exist_ok=True)
        
        uploaded_files = []
        
        for file in files:
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                file_path = os.path.join(session_dir, filename)
                
                # Verifica tamanho do arquivo
                file.seek(0, os.SEEK_END)
                file_size = file.tell()
                file.seek(0)
                
                if file_size > MAX_FILE_SIZE:
                    continue
                
                file.save(file_path)
                
                uploaded_files.append({
                    'filename': filename,
                    'path': file_path,
                    'size': file_size,
                    'type': mimetypes.guess_type(filename)[0] or 'unknown'
                })
        
        if not uploaded_files:
            return jsonify({'error': 'Nenhum arquivo válido foi enviado'}), 400
        
        # Salva metadados da sessão
        session_metadata = {
            'session_id': session_id,
            'created_at': datetime.now().isoformat(),
            'files': uploaded_files,
            'status': 'uploaded',
            'analysis_status': 'pending'
        }
        
        metadata_path = os.path.join(session_dir, 'session_metadata.json')
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(session_metadata, f, indent=2, ensure_ascii=False)
        
        return jsonify({
            'success': True,
            'session_id': session_id,
            'files_uploaded': len(uploaded_files),
            'files': uploaded_files
        })
        
    except Exception as e:
        logger.error(f"Erro no upload: {e}")
        return jsonify({'error': f'Erro no upload: {str(e)}'}), 500

@document_analysis_bp.route('/analyze/<session_id>', methods=['POST'])
def analyze_documents(session_id):
    """Inicia análise inteligente dos documentos"""
    try:
        session_dir = os.path.join(UPLOAD_FOLDER, session_id)
        metadata_path = os.path.join(session_dir, 'session_metadata.json')
        
        if not os.path.exists(metadata_path):
            return jsonify({'error': 'Sessão não encontrada'}), 404
        
        with open(metadata_path, 'r', encoding='utf-8') as f:
            session_metadata = json.load(f)
        
        # Extrai conteúdo dos documentos
        extracted_content = extract_documents_content(session_metadata['files'])
        
        # Atualiza metadados
        session_metadata['analysis_status'] = 'processing'
        session_metadata['extracted_content'] = extracted_content
        session_metadata['analysis_started_at'] = datetime.now().isoformat()
        
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(session_metadata, f, indent=2, ensure_ascii=False)
        
        # Inicia análise com IA (assíncrono)
        from services.document_ai_analyzer import DocumentAIAnalyzer
        analyzer = DocumentAIAnalyzer()
        analysis_result = analyzer.analyze_documents(session_id, extracted_content)
        
        return jsonify({
            'success': True,
            'session_id': session_id,
            'status': 'analysis_started',
            'documents_processed': len(extracted_content),
            'analysis_id': analysis_result.get('analysis_id')
        })
        
    except Exception as e:
        logger.error(f"Erro na análise: {e}")
        return jsonify({'error': f'Erro na análise: {str(e)}'}), 500

@document_analysis_bp.route('/status/<session_id>', methods=['GET'])
def get_analysis_status(session_id):
    """Obtém status da análise"""
    try:
        session_dir = os.path.join(UPLOAD_FOLDER, session_id)
        metadata_path = os.path.join(session_dir, 'session_metadata.json')
        
        if not os.path.exists(metadata_path):
            return jsonify({'error': 'Sessão não encontrada'}), 404
        
        with open(metadata_path, 'r', encoding='utf-8') as f:
            session_metadata = json.load(f)
        
        return jsonify({
            'success': True,
            'session_id': session_id,
            'status': session_metadata.get('analysis_status', 'unknown'),
            'files_count': len(session_metadata.get('files', [])),
            'created_at': session_metadata.get('created_at'),
            'analysis_started_at': session_metadata.get('analysis_started_at'),
            'analysis_completed_at': session_metadata.get('analysis_completed_at'),
            'progress': session_metadata.get('progress', 0)
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter status: {e}")
        return jsonify({'error': f'Erro ao obter status: {str(e)}'}), 500

@document_analysis_bp.route('/results/<session_id>', methods=['GET'])
def get_analysis_results(session_id):
    """Obtém resultados da análise"""
    try:
        session_dir = os.path.join(UPLOAD_FOLDER, session_id)
        results_path = os.path.join(session_dir, 'analysis_results.json')
        
        if not os.path.exists(results_path):
            return jsonify({'error': 'Resultados não encontrados'}), 404
        
        with open(results_path, 'r', encoding='utf-8') as f:
            results = json.load(f)
        
        return jsonify({
            'success': True,
            'session_id': session_id,
            'results': results
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter resultados: {e}")
        return jsonify({'error': f'Erro ao obter resultados: {str(e)}'}), 500

def extract_documents_content(files_info):
    """Extrai conteúdo dos documentos baseado no tipo"""
    extracted_content = []
    
    for file_info in files_info:
        file_path = file_info['path']
        filename = file_info['filename']
        file_type = file_info['type']
        
        try:
            content = ""
            metadata = {}
            
            # Extração baseada no tipo de arquivo
            if filename.endswith('.txt'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
            elif filename.endswith('.md'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    metadata['format'] = 'markdown'
                    
            elif filename.endswith('.json'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                    content = json.dumps(json_data, indent=2, ensure_ascii=False)
                    metadata['format'] = 'json'
                    metadata['structure'] = analyze_json_structure(json_data)
                    
            elif filename.endswith(('.csv')):
                df = pd.read_csv(file_path)
                content = df.to_string()
                metadata['format'] = 'csv'
                metadata['rows'] = len(df)
                metadata['columns'] = list(df.columns)
                
            elif filename.endswith(('.xls', '.xlsx')):
                df = pd.read_excel(file_path)
                content = df.to_string()
                metadata['format'] = 'excel'
                metadata['rows'] = len(df)
                metadata['columns'] = list(df.columns)
                
            elif filename.endswith('.pdf'):
                content = extract_pdf_content(file_path)
                metadata['format'] = 'pdf'
                
            elif filename.endswith('.docx'):
                doc = docx.Document(file_path)
                content = '\n'.join([paragraph.text for paragraph in doc.paragraphs])
                metadata['format'] = 'docx'
                
            elif filename.endswith(('.png', '.jpg', '.jpeg')):
                # Para imagens, extrair metadados básicos
                with Image.open(file_path) as img:
                    metadata['format'] = 'image'
                    metadata['size'] = img.size
                    metadata['mode'] = img.mode
                    content = f"Imagem: {filename} - Dimensões: {img.size}"
            
            extracted_content.append({
                'filename': filename,
                'content': content,
                'metadata': metadata,
                'file_type': file_type,
                'extracted_at': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"Erro ao extrair conteúdo de {filename}: {e}")
            extracted_content.append({
                'filename': filename,
                'content': "",
                'metadata': {'error': str(e)},
                'file_type': file_type,
                'extracted_at': datetime.now().isoformat()
            })
    
    return extracted_content

def extract_pdf_content(file_path):
    """Extrai texto de arquivo PDF"""
    try:
        with open(file_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text() + "\n"
            return text
    except Exception as e:
        logger.error(f"Erro ao extrair PDF: {e}")
        return f"Erro ao extrair conteúdo do PDF: {str(e)}"

def analyze_json_structure(json_data):
    """Analisa estrutura de dados JSON"""
    if isinstance(json_data, dict):
        return {
            'type': 'object',
            'keys': list(json_data.keys()),
            'key_count': len(json_data)
        }
    elif isinstance(json_data, list):
        return {
            'type': 'array',
            'length': len(json_data),
            'item_types': list(set(type(item).__name__ for item in json_data[:10]))
        }
    else:
        return {
            'type': type(json_data).__name__
        }

