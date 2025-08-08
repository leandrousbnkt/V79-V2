#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v2.0 - Comprehensive Analysis Validator
Validador que garante análises completas e robustas sem conteúdo simulado
"""

import logging
import json
import re
import os
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

class ComprehensiveAnalysisValidator:
    """Validador que garante análises completas e robustas"""
    
    def __init__(self):
        """Inicializa o validador"""
        self.required_sections = {
            'avatar_ultra_detalhado': {
                'min_content_length': 3000,
                'required_fields': ['nome_ficticio', 'perfil_demografico', 'dores_viscerais', 'desejos_secretos'],
                'min_items': {'dores_viscerais': 10, 'desejos_secretos': 10},
                'pdf_pages_estimate': 2.5
            },
            'drivers_mentais_customizados': {
                'min_content_length': 2500,
                'required_fields': ['drivers_customizados'],
                'min_items': {'drivers_customizados': 8},
                'pdf_pages_estimate': 3.0
            },
            'provas_visuais_sugeridas': {
                'min_content_length': 2000,
                'min_items': {'provas_visuais': 6},
                'pdf_pages_estimate': 2.5
            },
            'sistema_anti_objecao': {
                'min_content_length': 2000,
                'required_fields': ['objecoes_universais', 'arsenal_emergencia'],
                'min_items': {'arsenal_emergencia': 8},
                'pdf_pages_estimate': 2.0
            },
            'pesquisa_web_massiva': {
                'min_content_length': 5000,
                'required_fields': ['estatisticas', 'fontes'],
                'min_items': {'fontes': 8},
                'pdf_pages_estimate': 3.0
            },
            'insights_exclusivos': {
                'min_content_length': 2500,
                'min_items': {'insights': 20},
                'pdf_pages_estimate': 2.0
            },
            'analise_concorrencia_detalhada': {
                'min_content_length': 2000,
                'min_items': {'concorrentes': 3},
                'pdf_pages_estimate': 2.0
            },
            'estrategia_palavras_chave': {
                'min_content_length': 1500,
                'required_fields': ['palavras_primarias', 'palavras_secundarias'],
                'min_items': {'palavras_primarias': 15, 'palavras_secundarias': 30},
                'pdf_pages_estimate': 1.5
            },
            'metricas_performance_detalhadas': {
                'min_content_length': 1800,
                'required_fields': ['kpis_principais', 'projecoes_financeiras'],
                'pdf_pages_estimate': 2.0
            },
            'plano_acao_detalhado': {
                'min_content_length': 2500,
                'required_fields': ['fase_1_preparacao', 'fase_2_lancamento', 'fase_3_crescimento'],
                'pdf_pages_estimate': 2.5
            },
            'funil_vendas_detalhado': {
                'min_content_length': 1800,
                'required_fields': ['etapas_funil', 'metricas_conversao'],
                'pdf_pages_estimate': 2.0
            },
            'predicoes_futuro_completas': {
                'min_content_length': 2200,
                'required_fields': ['cenarios_futuros', 'tendencias_atuais'],
                'pdf_pages_estimate': 2.5
            },
            'pre_pitch_invisivel': {
                'min_content_length': 2000,
                'required_fields': ['orquestracao_emocional', 'roteiro_completo'],
                'pdf_pages_estimate': 2.0
            }
        }
        
        # Indicadores de conteúdo simulado/genérico (mais rigoroso)
        self.simulation_indicators = [
            'n/a', 'não informado', 'customizado para', 'baseado em',
            'específico para', 'exemplo de', 'placeholder', 'mock',
            'simulado', 'fictício', 'genérico', 'padrão',
            'lorem ipsum', 'teste', 'sample', 'demo', 'análise em andamento',
            'sistema em recuperação', 'dados limitados', 'configure apis',
            'modo emergência', 'fallback', 'backup'
        ]
        
        # Padrões que indicam conteúdo real e específico
        self.real_content_patterns = [
            r'\d+%',  # Percentuais específicos
            r'R\$\s*[\d,\.]+',  # Valores monetários brasileiros
            r'\d+\s*(mil|milhão|bilhão)',  # Quantidades grandes
            r'20(23|24|25)',  # Anos recentes específicos
            r'\d+\s*(empresas|profissionais|clientes|usuários)',  # Quantidades específicas
            r'crescimento de \d+',  # Dados de crescimento específicos
            r'mercado de R\$',  # Tamanho de mercado específico
            r'\d+\s*(anos|meses|dias) de experiência',  # Experiência específica
            r'mais de \d+',  # Quantificadores específicos
            r'entre \d+ e \d+',  # Faixas específicas
            r'aproximadamente \d+',  # Estimativas específicas
            r'cerca de \d+',  # Aproximações específicas
        ]
        
        logger.info("Comprehensive Analysis Validator inicializado com critérios rigorosos")
    
    def validate_complete_analysis(self, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
        """Valida análise completa e robusta"""
        
        logger.info("🔍 Iniciando validação rigorosa da análise")
        
        validation_result = {
            'is_valid': False,
            'is_complete': False,
            'is_robust': False,
            'quality_score': 0.0,
            'content_length': 0,
            'sections_validated': {},
            'missing_sections': [],
            'weak_sections': [],
            'simulation_detected': [],
            'recommendations': [],
            'pdf_ready': False,
            'estimated_pdf_pages': 0,
            'critical_issues': [],
            'content_quality_breakdown': {}
        }
        
        try:
            # 1. Validação de estrutura básica
            structure_validation = self._validate_structure(analysis_data)
            validation_result.update(structure_validation)
            
            # 2. Validação rigorosa de conteúdo por seção
            sections_validation = self._validate_sections_content(analysis_data)
            validation_result['sections_validated'] = sections_validation
            
            # 3. Detecção rigorosa de conteúdo simulado
            simulation_check = self._detect_simulated_content(analysis_data)
            validation_result['simulation_detected'] = simulation_check
            
            # 4. Validação de qualidade de dados reais
            real_data_validation = self._validate_real_data_quality(analysis_data)
            validation_result['content_quality_breakdown'] = real_data_validation
            
            # 5. Cálculo de qualidade geral
            quality_score = self._calculate_comprehensive_quality_score(
                analysis_data, sections_validation, real_data_validation
            )
            validation_result['quality_score'] = quality_score
            
            # 6. Estimativa rigorosa de páginas do PDF
            pdf_pages = self._estimate_pdf_pages_detailed(analysis_data, sections_validation)
            validation_result['estimated_pdf_pages'] = pdf_pages
            
            # 7. Identificação de problemas críticos
            critical_issues = self._identify_critical_issues(
                sections_validation, simulation_check, real_data_validation
            )
            validation_result['critical_issues'] = critical_issues
            
            # 8. Determinação final rigorosa
            validation_result['is_valid'] = (
                quality_score >= 80.0 and 
                len(simulation_check) == 0 and
                len(critical_issues) == 0
            )
            validation_result['is_complete'] = (
                pdf_pages >= 18 and 
                len(validation_result['missing_sections']) <= 2
            )
            validation_result['is_robust'] = (
                len(simulation_check) == 0 and 
                quality_score >= 85.0 and
                pdf_pages >= 20 and
                len(critical_issues) == 0
            )
            validation_result['pdf_ready'] = (
                pdf_pages >= 20 and 
                len(simulation_check) == 0 and
                quality_score >= 80.0
            )
            
            # 9. Gera recomendações específicas
            validation_result['recommendations'] = self._generate_detailed_recommendations(
                validation_result, analysis_data
            )
            
            logger.info(f"✅ Validação rigorosa concluída - Score: {quality_score:.1f}%, PDF: {pdf_pages} páginas")
            
            return validation_result
            
        except Exception as e:
            logger.error(f"❌ Erro na validação: {e}")
            validation_result['error'] = str(e)
            validation_result['critical_issues'].append(f"Erro de validação: {str(e)}")
            return validation_result
    
    def _validate_structure(self, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
        """Valida estrutura básica da análise"""
        
        total_content = json.dumps(analysis_data, ensure_ascii=False)
        content_length = len(total_content)
        
        # Verifica seções obrigatórias
        missing_sections = []
        weak_sections = []
        
        for section_name, requirements in self.required_sections.items():
            if section_name not in analysis_data:
                missing_sections.append(section_name)
            elif not analysis_data[section_name]:
                missing_sections.append(section_name)
            else:
                # Verifica se seção tem conteúdo mínimo
                section_str = json.dumps(analysis_data[section_name], ensure_ascii=False)
                if len(section_str) < requirements.get('min_content_length', 500):
                    weak_sections.append(section_name)
        
        return {
            'content_length': content_length,
            'missing_sections': missing_sections,
            'weak_sections': weak_sections,
            'has_basic_structure': len(missing_sections) == 0,  # Todas as seções devem estar presentes
            'structure_score': max(0, 100 - (len(missing_sections) * 10) - (len(weak_sections) * 5))
        }
    
    def _validate_sections_content(self, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
        """Valida conteúdo rigoroso de cada seção"""
        
        sections_validation = {}
        
        for section_name, requirements in self.required_sections.items():
            section_data = analysis_data.get(section_name)
            
            if not section_data:
                sections_validation[section_name] = {
                    'present': False,
                    'valid': False,
                    'score': 0.0,
                    'issues': ['Seção completamente ausente'],
                    'content_length': 0,
                    'real_content_ratio': 0.0,
                    'specificity_score': 0.0
                }
                continue
            
            # Valida seção com critérios rigorosos
            section_validation = self._validate_individual_section_rigorous(
                section_data, section_name, requirements
            )
            sections_validation[section_name] = section_validation
        
        return sections_validation
    
    def _validate_individual_section_rigorous(
        self, 
        section_data: Any, 
        section_name: str, 
        requirements: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Valida seção individual com critérios rigorosos"""
        
        validation = {
            'present': True,
            'valid': False,
            'score': 0.0,
            'issues': [],
            'content_length': 0,
            'real_content_ratio': 0.0,
            'specificity_score': 0.0,
            'data_density': 0.0
        }
        
        try:
            # Converte para string para análise
            section_str = json.dumps(section_data, ensure_ascii=False)
            validation['content_length'] = len(section_str)
            
            # 1. Verifica tamanho mínimo RIGOROSO
            min_length = requirements.get('min_content_length', 1000)
            if len(section_str) < min_length:
                validation['issues'].append(f'Conteúdo insuficiente: {len(section_str)} < {min_length} caracteres')
            else:
                validation['score'] += 20
            
            # 2. Verifica campos obrigatórios RIGOROSO
            required_fields = requirements.get('required_fields', [])
            missing_fields = []
            
            if isinstance(section_data, dict):
                for field in required_fields:
                    if field not in section_data or not section_data[field]:
                        missing_fields.append(field)
                    elif isinstance(section_data[field], (list, dict)) and len(section_data[field]) == 0:
                        missing_fields.append(f"{field} (vazio)")
                
                if not missing_fields:
                    validation['score'] += 25
                else:
                    validation['issues'].append(f'Campos obrigatórios ausentes/vazios: {missing_fields}')
            
            # 3. Verifica quantidade mínima de itens RIGOROSO
            min_items = requirements.get('min_items', {})
            for field, min_count in min_items.items():
                actual_count = self._count_items_in_field(section_data, field)
                
                if actual_count >= min_count:
                    validation['score'] += 15
                else:
                    validation['issues'].append(f'{field}: apenas {actual_count} itens (mínimo: {min_count})')
            
            # 4. Verifica qualidade e especificidade do conteúdo
            real_content_ratio = self._calculate_real_content_ratio(section_str)
            validation['real_content_ratio'] = real_content_ratio
            
            specificity_score = self._calculate_content_specificity(section_str)
            validation['specificity_score'] = specificity_score
            
            data_density = self._calculate_data_density(section_str)
            validation['data_density'] = data_density
            
            # Score por qualidade de conteúdo
            if real_content_ratio >= 0.9 and specificity_score >= 0.8:
                validation['score'] += 25
            elif real_content_ratio >= 0.7 and specificity_score >= 0.6:
                validation['score'] += 15
            elif real_content_ratio >= 0.5 and specificity_score >= 0.4:
                validation['score'] += 10
            else:
                validation['issues'].append(f'Baixa qualidade: real={real_content_ratio:.1%}, específico={specificity_score:.1%}')
            
            # Score por densidade de dados
            if data_density >= 0.1:  # 10% do conteúdo são dados específicos
                validation['score'] += 15
            elif data_density >= 0.05:
                validation['score'] += 10
            else:
                validation['issues'].append(f'Baixa densidade de dados: {data_density:.1%}')
            
            # 5. Validações específicas por seção
            section_specific_validation = self._validate_section_specific_requirements(
                section_data, section_name
            )
            validation['score'] += section_specific_validation['bonus_score']
            validation['issues'].extend(section_specific_validation['issues'])
            
            # Determina se é válida (critério rigoroso)
            validation['valid'] = (
                validation['score'] >= 80 and 
                len(validation['issues']) == 0 and
                real_content_ratio >= 0.7 and
                specificity_score >= 0.6
            )
            
        except Exception as e:
            validation['issues'].append(f'Erro na validação: {str(e)}')
            validation['score'] = 0
        
        return validation
    
    def _count_items_in_field(self, section_data: Any, field: str) -> int:
        """Conta itens em um campo específico"""
        
        if isinstance(section_data, dict):
            field_data = section_data.get(field, [])
        elif isinstance(section_data, list):
            field_data = section_data
        else:
            return 0
        
        if isinstance(field_data, list):
            # Conta apenas itens não vazios e não genéricos
            valid_items = 0
            for item in field_data:
                if item and len(str(item).strip()) > 20:  # Mínimo 20 caracteres
                    item_str = str(item).lower()
                    if not any(indicator in item_str for indicator in self.simulation_indicators):
                        valid_items += 1
            return valid_items
        elif isinstance(field_data, dict):
            return len([k for k, v in field_data.items() if v and len(str(v)) > 10])
        else:
            return 1 if field_data else 0
    
    def _calculate_real_content_ratio(self, content: str) -> float:
        """Calcula proporção de conteúdo real vs simulado"""
        
        if not content:
            return 0.0
        
        content_lower = content.lower()
        
        # Conta indicadores de simulação
        simulation_count = 0
        for indicator in self.simulation_indicators:
            simulation_count += content_lower.count(indicator)
        
        # Conta padrões de conteúdo real
        real_patterns_count = 0
        for pattern in self.real_content_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            real_patterns_count += len(matches)
        
        # Calcula palavras totais
        words = content.split()
        total_words = len(words)
        
        if total_words == 0:
            return 0.0
        
        # Ratio de conteúdo real (penaliza simulação, premia dados reais)
        simulation_penalty = min(0.5, simulation_count / total_words)
        real_bonus = min(0.8, real_patterns_count / total_words * 10)
        
        final_ratio = max(0.0, min(1.0, 0.5 + real_bonus - simulation_penalty))
        
        return final_ratio
    
    def _calculate_content_specificity(self, content: str) -> float:
        """Calcula especificidade do conteúdo"""
        
        if not content:
            return 0.0
        
        # Padrões que indicam especificidade
        specificity_patterns = [
            r'\b[A-Z][a-z]+\s+[A-Z][a-z]+\b',  # Nomes próprios
            r'\b\d{4}\b',  # Anos específicos
            r'\b\d+\.\d+\b',  # Números decimais
            r'\b[A-Z]{2,}\b',  # Siglas/acrônimos
            r'\b(Dr|Dra|CEO|CTO|CFO)\.',  # Títulos profissionais
            r'\b(São Paulo|Rio de Janeiro|Minas Gerais|Brasil)\b',  # Localizações específicas
            r'\b(Google|Microsoft|Amazon|Meta|Apple)\b',  # Empresas específicas
        ]
        
        specificity_count = 0
        for pattern in specificity_patterns:
            matches = re.findall(pattern, content)
            specificity_count += len(matches)
        
        words = content.split()
        if len(words) == 0:
            return 0.0
        
        # Normaliza por número de palavras
        specificity_ratio = min(1.0, specificity_count / len(words) * 20)
        
        return specificity_ratio
    
    def _calculate_data_density(self, content: str) -> float:
        """Calcula densidade de dados específicos"""
        
        if not content:
            return 0.0
        
        # Padrões de dados específicos
        data_patterns = [
            r'\d+%',  # Percentuais
            r'R\$\s*[\d,\.]+',  # Valores monetários
            r'\d+\s*mil',  # Milhares
            r'\d+\s*milhão',  # Milhões
            r'\d+\s*bilhão',  # Bilhões
            r'\d+\s*(empresas|clientes|usuários)',  # Quantidades de entidades
            r'\d+\s*(anos|meses|dias)',  # Períodos
            r'\d+x',  # Multiplicadores
            r'\d+:\d+',  # Ratios
        ]
        
        data_count = 0
        for pattern in data_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            data_count += len(matches)
        
        words = content.split()
        if len(words) == 0:
            return 0.0
        
        # Densidade de dados
        density = min(1.0, data_count / len(words))
        
        return density
    
    def _validate_section_specific_requirements(
        self, 
        section_data: Any, 
        section_name: str
    ) -> Dict[str, Any]:
        """Validações específicas por tipo de seção"""
        
        validation = {'bonus_score': 0, 'issues': []}
        
        try:
            if section_name == 'avatar_ultra_detalhado':
                validation.update(self._validate_avatar_section(section_data))
            elif section_name == 'drivers_mentais_customizados':
                validation.update(self._validate_drivers_section(section_data))
            elif section_name == 'pesquisa_web_massiva':
                validation.update(self._validate_research_section(section_data))
            elif section_name == 'insights_exclusivos':
                validation.update(self._validate_insights_section(section_data))
            elif section_name == 'analise_concorrencia_detalhada':
                validation.update(self._validate_competition_section(section_data))
            elif section_name == 'estrategia_palavras_chave':
                validation.update(self._validate_keywords_section(section_data))
            elif section_name == 'metricas_performance_detalhadas':
                validation.update(self._validate_metrics_section(section_data))
            elif section_name == 'plano_acao_detalhado':
                validation.update(self._validate_action_plan_section(section_data))
            
        except Exception as e:
            validation['issues'].append(f'Erro na validação específica: {str(e)}')
        
        return validation
    
    def _validate_avatar_section(self, avatar_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validação específica da seção de avatar"""
        
        validation = {'bonus_score': 0, 'issues': []}
        
        if not isinstance(avatar_data, dict):
            validation['issues'].append('Avatar deve ser um objeto')
            return validation
        
        # Verifica nome fictício específico
        nome = avatar_data.get('nome_ficticio', '')
        if not nome or len(nome) < 10 or any(generic in nome.lower() for generic in ['profissional', 'cliente', 'usuário']):
            validation['issues'].append('Nome fictício muito genérico ou ausente')
        else:
            validation['bonus_score'] += 5
        
        # Verifica dores viscerais específicas
        dores = avatar_data.get('dores_viscerais', [])
        if isinstance(dores, list):
            specific_pains = 0
            for dor in dores:
                if (len(str(dor)) > 50 and 
                    any(pattern in str(dor) for pattern in ['trabalhar', 'sentir', 'ver', 'não conseguir']) and
                    not any(generic in str(dor).lower() for generic in self.simulation_indicators)):
                    specific_pains += 1
            
            if specific_pains >= 8:
                validation['bonus_score'] += 10
            else:
                validation['issues'].append(f'Apenas {specific_pains} dores específicas (mínimo: 8)')
        
        # Verifica perfil demográfico detalhado
        perfil_demo = avatar_data.get('perfil_demografico', {})
        if isinstance(perfil_demo, dict):
            required_demo_fields = ['idade', 'renda', 'escolaridade', 'localizacao']
            demo_score = 0
            for field in required_demo_fields:
                if field in perfil_demo and perfil_demo[field]:
                    field_value = str(perfil_demo[field])
                    if (len(field_value) > 20 and 
                        not any(generic in field_value.lower() for generic in self.simulation_indicators)):
                        demo_score += 1
            
            if demo_score >= 3:
                validation['bonus_score'] += 5
            else:
                validation['issues'].append(f'Perfil demográfico incompleto: {demo_score}/4 campos válidos')
        
        return validation
    
    def _validate_drivers_section(self, drivers_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validação específica da seção de drivers mentais"""
        
        validation = {'bonus_score': 0, 'issues': []}
        
        if not isinstance(drivers_data, dict):
            validation['issues'].append('Drivers deve ser um objeto')
            return validation
        
        drivers_list = drivers_data.get('drivers_customizados', [])
        if not isinstance(drivers_list, list):
            validation['issues'].append('Lista de drivers ausente')
            return validation
        
        valid_drivers = 0
        for driver in drivers_list:
            if isinstance(driver, dict):
                # Verifica campos essenciais
                nome = driver.get('nome', '')
                gatilho = driver.get('gatilho_central', '')
                roteiro = driver.get('roteiro_ativacao', {})
                
                if (nome and len(nome) > 5 and
                    gatilho and len(gatilho) > 20 and
                    isinstance(roteiro, dict) and len(roteiro) > 0):
                    
                    # Verifica se não é genérico
                    driver_str = json.dumps(driver, ensure_ascii=False).lower()
                    if not any(generic in driver_str for generic in self.simulation_indicators[:5]):
                        valid_drivers += 1
        
        if valid_drivers >= 6:
            validation['bonus_score'] += 15
        elif valid_drivers >= 4:
            validation['bonus_score'] += 10
        else:
            validation['issues'].append(f'Apenas {valid_drivers} drivers válidos (mínimo: 6)')
        
        return validation
    
    def _validate_research_section(self, research_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validação específica da seção de pesquisa"""
        
        validation = {'bonus_score': 0, 'issues': []}
        
        if not isinstance(research_data, dict):
            validation['issues'].append('Pesquisa deve ser um objeto')
            return validation
        
        # Verifica estatísticas
        stats = research_data.get('estatisticas', {})
        if isinstance(stats, dict):
            required_stats = ['total_queries', 'fontes_unicas', 'total_conteudo']
            valid_stats = 0
            for stat in required_stats:
                if stat in stats and isinstance(stats[stat], (int, float)) and stats[stat] > 0:
                    valid_stats += 1
            
            if valid_stats >= 3:
                validation['bonus_score'] += 10
            else:
                validation['issues'].append(f'Estatísticas incompletas: {valid_stats}/3')
        
        # Verifica fontes
        fontes = research_data.get('fontes', [])
        if isinstance(fontes, list):
            valid_sources = 0
            for fonte in fontes:
                if isinstance(fonte, dict) and fonte.get('url') and fonte.get('title'):
                    url = fonte['url']
                    if url.startswith('http') and len(fonte['title']) > 10:
                        valid_sources += 1
            
            if valid_sources >= 6:
                validation['bonus_score'] += 10
            else:
                validation['issues'].append(f'Apenas {valid_sources} fontes válidas (mínimo: 6)')
        
        return validation
    
    def _validate_insights_section(self, insights_data: Any) -> Dict[str, Any]:
        """Validação específica da seção de insights"""
        
        validation = {'bonus_score': 0, 'issues': []}
        
        if isinstance(insights_data, list):
            insights_list = insights_data
        elif isinstance(insights_data, dict) and 'insights' in insights_data:
            insights_list = insights_data['insights']
        else:
            validation['issues'].append('Formato de insights inválido')
            return validation
        
        if not isinstance(insights_list, list):
            validation['issues'].append('Lista de insights ausente')
            return validation
        
        # Valida qualidade dos insights
        high_quality_insights = 0
        for insight in insights_list:
            insight_str = str(insight)
            if (len(insight_str) > 50 and  # Mínimo 50 caracteres
                not any(generic in insight_str.lower() for generic in self.simulation_indicators) and
                any(re.search(pattern, insight_str, re.IGNORECASE) for pattern in self.real_content_patterns)):
                high_quality_insights += 1
        
        if high_quality_insights >= 15:
            validation['bonus_score'] += 15
        elif high_quality_insights >= 10:
            validation['bonus_score'] += 10
        else:
            validation['issues'].append(f'Apenas {high_quality_insights} insights de alta qualidade (mínimo: 15)')
        
        return validation
    
    def _validate_competition_section(self, competition_data: Any) -> Dict[str, Any]:
        """Validação específica da seção de concorrência"""
        
        validation = {'bonus_score': 0, 'issues': []}
        
        if isinstance(competition_data, list):
            competitors = competition_data
        elif isinstance(competition_data, dict) and 'concorrentes' in competition_data:
            competitors = competition_data['concorrentes']
        else:
            validation['issues'].append('Dados de concorrência em formato inválido')
            return validation
        
        if not isinstance(competitors, list):
            validation['issues'].append('Lista de concorrentes ausente')
            return validation
        
        detailed_competitors = 0
        for competitor in competitors:
            if isinstance(competitor, dict):
                nome = competitor.get('nome', '')
                swot = competitor.get('analise_swot', {})
                
                if (nome and len(nome) > 5 and
                    isinstance(swot, dict) and
                    all(key in swot for key in ['forcas', 'fraquezas', 'oportunidades', 'ameacas'])):
                    
                    # Verifica se SWOT tem conteúdo específico
                    swot_content = json.dumps(swot, ensure_ascii=False)
                    if (len(swot_content) > 200 and
                        not any(generic in swot_content.lower() for generic in self.simulation_indicators)):
                        detailed_competitors += 1
        
        if detailed_competitors >= 2:
            validation['bonus_score'] += 10
        else:
            validation['issues'].append(f'Apenas {detailed_competitors} concorrentes detalhados (mínimo: 2)')
        
        return validation
    
    def _validate_keywords_section(self, keywords_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validação específica da seção de palavras-chave"""
        
        validation = {'bonus_score': 0, 'issues': []}
        
        if not isinstance(keywords_data, dict):
            validation['issues'].append('Estratégia de palavras-chave deve ser um objeto')
            return validation
        
        # Verifica palavras primárias
        primary_kw = keywords_data.get('palavras_primarias', [])
        if isinstance(primary_kw, list) and len(primary_kw) >= 10:
            # Verifica se são específicas (não genéricas)
            specific_kw = 0
            for kw in primary_kw:
                if (len(str(kw)) > 3 and 
                    not any(generic in str(kw).lower() for generic in ['palavra', 'chave', 'termo'])):
                    specific_kw += 1
            
            if specific_kw >= 8:
                validation['bonus_score'] += 8
            else:
                validation['issues'].append(f'Apenas {specific_kw} palavras-chave específicas')
        else:
            validation['issues'].append('Palavras primárias insuficientes')
        
        # Verifica intenção de busca
        intencao = keywords_data.get('intencao_busca', {})
        if isinstance(intencao, dict) and len(intencao) >= 3:
            validation['bonus_score'] += 7
        else:
            validation['issues'].append('Análise de intenção de busca incompleta')
        
        return validation
    
    def _validate_metrics_section(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validação específica da seção de métricas"""
        
        validation = {'bonus_score': 0, 'issues': []}
        
        if not isinstance(metrics_data, dict):
            validation['issues'].append('Métricas devem ser um objeto')
            return validation
        
        # Verifica KPIs
        kpis = metrics_data.get('kpis_principais', [])
        if isinstance(kpis, list) and len(kpis) >= 5:
            detailed_kpis = 0
            for kpi in kpis:
                if isinstance(kpi, dict) and all(key in kpi for key in ['metrica', 'objetivo']):
                    detailed_kpis += 1
            
            if detailed_kpis >= 4:
                validation['bonus_score'] += 8
            else:
                validation['issues'].append(f'Apenas {detailed_kpis} KPIs detalhados')
        else:
            validation['issues'].append('KPIs principais insuficientes')
        
        # Verifica projeções financeiras
        projecoes = metrics_data.get('projecoes_financeiras', {})
        if isinstance(projecoes, dict):
            cenarios = ['cenario_conservador', 'cenario_realista', 'cenario_otimista']
            valid_scenarios = 0
            for cenario in cenarios:
                if cenario in projecoes and isinstance(projecoes[cenario], dict):
                    scenario_data = projecoes[cenario]
                    if all(key in scenario_data for key in ['receita_mensal', 'clientes_mes']):
                        valid_scenarios += 1
            
            if valid_scenarios >= 3:
                validation['bonus_score'] += 7
            else:
                validation['issues'].append(f'Apenas {valid_scenarios} cenários financeiros válidos')
        
        return validation
    
    def _validate_action_plan_section(self, action_plan_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validação específica da seção de plano de ação"""
        
        validation = {'bonus_score': 0, 'issues': []}
        
        if not isinstance(action_plan_data, dict):
            validation['issues'].append('Plano de ação deve ser um objeto')
            return validation
        
        # Verifica fases
        fases = ['fase_1_preparacao', 'fase_2_lancamento', 'fase_3_crescimento']
        valid_phases = 0
        
        for fase in fases:
            if fase in action_plan_data and isinstance(action_plan_data[fase], dict):
                fase_data = action_plan_data[fase]
                required_fields = ['duracao', 'atividades', 'investimento']
                
                if all(field in fase_data for field in required_fields):
                    atividades = fase_data.get('atividades', [])
                    if isinstance(atividades, list) and len(atividades) >= 3:
                        valid_phases += 1
        
        if valid_phases >= 3:
            validation['bonus_score'] += 10
        else:
            validation['issues'].append(f'Apenas {valid_phases} fases válidas (mínimo: 3)')
        
        return validation
    
    def _validate_real_data_quality(self, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
        """Valida qualidade dos dados reais"""
        
        quality_breakdown = {
            'has_specific_numbers': False,
            'has_brazilian_context': False,
            'has_recent_data': False,
            'has_market_data': False,
            'has_company_names': False,
            'overall_data_quality': 0.0
        }
        
        analysis_str = json.dumps(analysis_data, ensure_ascii=False)
        
        # Verifica números específicos
        number_patterns = re.findall(r'\d+(?:\.\d+)?%|\d+\s*mil|\d+\s*milhão|R\$\s*[\d,\.]+', analysis_str)
        quality_breakdown['has_specific_numbers'] = len(number_patterns) >= 10
        
        # Verifica contexto brasileiro
        brazil_patterns = re.findall(r'\bbrasil\b|\bbrasileiro\b|são paulo|rio de janeiro', analysis_str, re.IGNORECASE)
        quality_breakdown['has_brazilian_context'] = len(brazil_patterns) >= 5
        
        # Verifica dados recentes
        recent_patterns = re.findall(r'20(23|24|25)', analysis_str)
        quality_breakdown['has_recent_data'] = len(recent_patterns) >= 3
        
        # Verifica dados de mercado
        market_patterns = re.findall(r'mercado|crescimento|oportunidade|tendência|análise', analysis_str, re.IGNORECASE)
        quality_breakdown['has_market_data'] = len(market_patterns) >= 15
        
        # Verifica nomes de empresas/pessoas
        name_patterns = re.findall(r'\b[A-Z][a-z]+\s+[A-Z][a-z]+\b|\b[A-Z]{2,}\b', analysis_str)
        quality_breakdown['has_company_names'] = len(name_patterns) >= 5
        
        # Score geral de qualidade de dados
        quality_indicators = [
            quality_breakdown['has_specific_numbers'],
            quality_breakdown['has_brazilian_context'],
            quality_breakdown['has_recent_data'],
            quality_breakdown['has_market_data'],
            quality_breakdown['has_company_names']
        ]
        
        quality_breakdown['overall_data_quality'] = sum(quality_indicators) / len(quality_indicators)
        
        return quality_breakdown
    
    def _detect_simulated_content(self, analysis_data: Dict[str, Any]) -> List[str]:
        """Detecta conteúdo simulado ou genérico com critérios rigorosos"""
        
        simulation_detected = []
        analysis_str = json.dumps(analysis_data, ensure_ascii=False).lower()
        
        # Verifica indicadores de simulação (mais rigoroso)
        for indicator in self.simulation_indicators:
            count = analysis_str.count(indicator)
            if count > 2:  # Threshold mais rigoroso
                simulation_detected.append(f"Indicador de simulação '{indicator}' aparece {count} vezes")
        
        # Verifica padrões genéricos específicos
        generic_patterns = [
            (r'customizado para \w+', 'Texto genérico "customizado para"'),
            (r'baseado em \w+', 'Texto genérico "baseado em"'),
            (r'específico para \w+', 'Texto genérico "específico para"'),
            (r'exemplo de \w+', 'Texto genérico "exemplo de"'),
            (r'dados de \w+ não informado', 'Dados não informados'),
            (r'análise em andamento', 'Análise incompleta'),
            (r'sistema em recuperação', 'Sistema em modo de emergência'),
            (r'configure apis', 'Dependência de configuração'),
            (r'modo emergência', 'Modo de emergência ativo'),
            (r'fallback', 'Sistema de fallback ativo')
        ]
        
        for pattern, description in generic_patterns:
            matches = re.findall(pattern, analysis_str, re.IGNORECASE)
            if len(matches) > 1:  # Mais rigoroso
                simulation_detected.append(f"{description}: {len(matches)} ocorrências")
        
        # Verifica repetições excessivas (indica conteúdo gerado automaticamente)
        words = analysis_str.split()
        word_freq = {}
        for word in words:
            if len(word) > 5:  # Apenas palavras significativas
                word_freq[word] = word_freq.get(word, 0) + 1
        
        excessive_repetitions = [word for word, count in word_freq.items() if count > 20]
        if excessive_repetitions:
            simulation_detected.append(f"Repetições excessivas: {excessive_repetitions[:5]}")
        
        return simulation_detected
    
    def _calculate_comprehensive_quality_score(
        self, 
        analysis_data: Dict[str, Any], 
        sections_validation: Dict[str, Any],
        real_data_validation: Dict[str, Any]
    ) -> float:
        """Calcula score de qualidade abrangente"""
        
        # 1. Score das seções (60% do total)
        section_scores = []
        for section_name, validation in sections_validation.items():
            if validation['present']:
                section_scores.append(validation['score'])
        
        avg_section_score = sum(section_scores) / len(section_scores) if section_scores else 0
        sections_component = avg_section_score * 0.6
        
        # 2. Score de qualidade de dados reais (25% do total)
        data_quality_score = real_data_validation['overall_data_quality'] * 100
        data_component = data_quality_score * 0.25
        
        # 3. Score de completude (15% do total)
        total_sections = len(self.required_sections)
        present_sections = len([v for v in sections_validation.values() if v['present']])
        completeness_score = (present_sections / total_sections) * 100
        completeness_component = completeness_score * 0.15
        
        # Score final
        final_score = sections_component + data_component + completeness_component
        
        return min(100.0, final_score)
    
    def _estimate_pdf_pages_detailed(
        self, 
        analysis_data: Dict[str, Any], 
        sections_validation: Dict[str, Any]
    ) -> int:
        """Estima páginas do PDF com base no conteúdo real"""
        
        total_pages = 0
        
        # Página de capa
        total_pages += 1
        
        # Páginas por seção baseado no conteúdo real
        for section_name, requirements in self.required_sections.items():
            section_validation = sections_validation.get(section_name, {})
            
            if section_validation.get('present', False) and section_validation.get('valid', False):
                # Usa estimativa da seção se válida
                estimated_pages = requirements.get('pdf_pages_estimate', 1.0)
                
                # Ajusta baseado na qualidade do conteúdo
                content_length = section_validation.get('content_length', 0)
                real_ratio = section_validation.get('real_content_ratio', 0.0)
                
                # Bonus por conteúdo real e específico
                if real_ratio >= 0.8 and content_length >= requirements.get('min_content_length', 1000):
                    estimated_pages *= 1.2  # 20% bonus
                elif real_ratio >= 0.6:
                    estimated_pages *= 1.0
                else:
                    estimated_pages *= 0.7  # Penaliza conteúdo de baixa qualidade
                
                total_pages += estimated_pages
            else:
                # Seção ausente ou inválida - não conta para PDF
                pass
        
        # Páginas de gráficos e visualizações (baseado em dados reais)
        if self._has_sufficient_data_for_charts(analysis_data):
            total_pages += 2  # Gráficos e visualizações
        
        # Páginas de anexos e referências
        total_pages += 1
        
        return int(total_pages)
    
    def _has_sufficient_data_for_charts(self, analysis_data: Dict[str, Any]) -> bool:
        """Verifica se há dados suficientes para gráficos"""
        
        # Verifica se há dados numéricos suficientes
        analysis_str = json.dumps(analysis_data, ensure_ascii=False)
        
        # Conta padrões numéricos
        numeric_patterns = re.findall(r'\d+(?:\.\d+)?%|\d+\s*mil|\d+\s*milhão|R\$\s*[\d,\.]+', analysis_str)
        
        # Verifica métricas
        metrics = analysis_data.get('metricas_performance_detalhadas', {})
        has_projections = isinstance(metrics.get('projecoes_financeiras'), dict)
        
        # Verifica pesquisa
        research = analysis_data.get('pesquisa_web_massiva', {})
        has_stats = isinstance(research.get('estatisticas'), dict)
        
        return len(numeric_patterns) >= 15 and has_projections and has_stats
    
    def _identify_critical_issues(
        self, 
        sections_validation: Dict[str, Any],
        simulation_check: List[str],
        real_data_validation: Dict[str, Any]
    ) -> List[str]:
        """Identifica problemas críticos que impedem PDF de 20 páginas"""
        
        critical_issues = []
        
        # Seções críticas ausentes
        critical_sections = [
            'avatar_ultra_detalhado', 'pesquisa_web_massiva', 'insights_exclusivos'
        ]
        
        for section in critical_sections:
            section_val = sections_validation.get(section, {})
            if not section_val.get('valid', False):
                critical_issues.append(f"Seção crítica inválida: {section}")
        
        # Conteúdo simulado detectado
        if len(simulation_check) > 0:
            critical_issues.append(f"Conteúdo simulado detectado: {len(simulation_check)} problemas")
        
        # Qualidade de dados insuficiente
        if real_data_validation['overall_data_quality'] < 0.6:
            critical_issues.append(f"Qualidade de dados insuficiente: {real_data_validation['overall_data_quality']:.1%}")
        
        # Seções com score muito baixo
        low_score_sections = [
            name for name, val in sections_validation.items() 
            if val.get('score', 0) < 60
        ]
        if len(low_score_sections) > 3:
            critical_issues.append(f"Muitas seções com score baixo: {len(low_score_sections)}")
        
        return critical_issues
    
    def _generate_detailed_recommendations(
        self, 
        validation_result: Dict[str, Any], 
        analysis_data: Dict[str, Any]
    ) -> List[str]:
        """Gera recomendações detalhadas para melhorar a análise"""
        
        recommendations = []
        
        # Recomendações baseadas em seções faltando
        if validation_result['missing_sections']:
            recommendations.append(
                f"CRÍTICO: Implementar seções ausentes: {', '.join(validation_result['missing_sections'])}"
            )
        
        # Recomendações baseadas em qualidade
        if validation_result['quality_score'] < 80:
            recommendations.append(
                f"Melhorar qualidade geral: score atual {validation_result['quality_score']:.1f}% (meta: 80%+)"
            )
        
        # Recomendações baseadas em simulação
        if validation_result['simulation_detected']:
            recommendations.append(
                "CRÍTICO: Eliminar todo conteúdo simulado/genérico detectado"
            )
        
        # Recomendações baseadas em páginas do PDF
        if validation_result['estimated_pdf_pages'] < 20:
            recommendations.append(
                f"Expandir conteúdo: PDF estimado em {validation_result['estimated_pdf_pages']} páginas (meta: 20+)"
            )
        
        # Recomendações específicas por seção
        for section_name, section_val in validation_result['sections_validated'].items():
            if not section_val.get('valid', False):
                issues = section_val.get('issues', [])
                if issues:
                    recommendations.append(f"Corrigir {section_name}: {'; '.join(issues[:2])}")
        
        # Recomendações de dados reais
        data_quality = validation_result.get('content_quality_breakdown', {})
        if not data_quality.get('has_specific_numbers', False):
            recommendations.append("Adicionar mais dados numéricos específicos (percentuais, valores, quantidades)")
        
        if not data_quality.get('has_brazilian_context', False):
            recommendations.append("Incluir mais contexto brasileiro específico")
        
        if not data_quality.get('has_recent_data', False):
            recommendations.append("Incluir dados mais recentes (2023-2025)")
        
        return recommendations[:10]  # Máximo 10 recomendações

# Instância global
comprehensive_validator = ComprehensiveAnalysisValidator()