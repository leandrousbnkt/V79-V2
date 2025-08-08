#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v2.0 - Quality Validation Service
Serviço de validação de qualidade antes da consolidação
"""

import logging
import time
from typing import Dict, List, Any, Optional
from datetime import datetime
from services.auto_save_manager import salvar_etapa, salvar_erro

logger = logging.getLogger(__name__)

class QualityValidationService:
    """Serviço de validação de qualidade ultra-rigorosa"""
    
    def __init__(self):
        """Inicializa serviço de validação"""
        self.quality_thresholds = {
            'min_drivers_mentais_quality': 3,
            'min_provas_visuais': 2,
            'min_fontes_pesquisa': 5,
            'min_insights_exclusivos': 5,
            'min_content_specificity': 0.7,  # 70% de especificidade
            'min_segment_relevance': 0.8     # 80% de relevância ao segmento
        }
        
        self.validation_rules = self._load_validation_rules()
        logger.info("Quality Validation Service inicializado")
    
    def _load_validation_rules(self) -> Dict[str, Any]:
        """Carrega regras de validação"""
        return {
            'drivers_mentais': {
                'required_fields': ['nome', 'gatilho_central', 'roteiro_ativacao'],
                'quality_indicators': ['historia_analogia', 'metafora_visual', 'comando_acao'],
                'specificity_check': True,
                'min_story_length': 150
            },
            'provas_visuais': {
                'required_fields': ['nome', 'experimento', 'materiais'],
                'quality_indicators': ['roteiro_completo', 'impacto_esperado'],
                'visual_elements_required': True,
                'min_materials': 2
            },
            'pesquisa_web': {
                'required_fields': ['total_queries', 'unique_sources', 'extracted_content'],
                'quality_indicators': ['quality_metrics', 'sources'],
                'content_validation': True,
                'min_extraction_success_rate': 0.3
            },
            'insights': {
                'required_fields': ['insights_exclusivos'],
                'quality_indicators': ['segment_specific', 'actionable'],
                'uniqueness_check': True,
                'min_insight_length': 50
            }
        }
    
    def validar_qualidade_pre_consolidacao(
        self, 
        dados_pipeline: Dict[str, Any],
        session_id: str
    ) -> Dict[str, Any]:
        """Valida qualidade antes da consolidação"""
        
        try:
            logger.info(f"🔍 Iniciando validação de qualidade para sessão: {session_id}")
            
            validacao_resultado = {
                'session_id': session_id,
                'timestamp': datetime.now().isoformat(),
                'qualidade_aprovada': False,
                'score_geral': 0.0,
                'validacoes_componentes': {},
                'problemas_criticos': [],
                'recomendacoes': [],
                'status_final': 'PENDENTE'
            }
            
            # Valida cada componente
            componentes_para_validar = [
                'drivers_mentais_customizados',
                'provas_visuais_sugeridas', 
                'pesquisa_web_massiva',
                'insights_exclusivos'
            ]
            
            scores_componentes = []
            
            for componente in componentes_para_validar:
                try:
                    score_componente = self._validar_componente_especifico(
                        componente, 
                        dados_pipeline.get(componente),
                        dados_pipeline
                    )
                    
                    validacao_resultado['validacoes_componentes'][componente] = score_componente
                    scores_componentes.append(score_componente['score'])
                    
                    # Salva validação de cada componente
                    salvar_etapa(f"validacao_{componente}", score_componente, categoria="analise_completa")
                    
                except Exception as e:
                    logger.error(f"❌ Erro ao validar {componente}: {e}")
                    validacao_resultado['validacoes_componentes'][componente] = {
                        'score': 0.0,
                        'valido': False,
                        'erro': str(e)
                    }
                    scores_componentes.append(0.0)
                    salvar_erro(f"validacao_{componente}", e)
            
            # Calcula score geral
            if scores_componentes:
                validacao_resultado['score_geral'] = sum(scores_componentes) / len(scores_componentes)
            
            # Determina se qualidade é aprovada
            validacao_resultado['qualidade_aprovada'] = validacao_resultado['score_geral'] >= 60.0
            
            # Gera diagnóstico final
            validacao_resultado.update(self._gerar_diagnostico_qualidade(validacao_resultado, dados_pipeline))
            
            # Salva validação completa
            salvar_etapa("validacao_qualidade_completa", validacao_resultado, categoria="analise_completa")
            
            logger.info(f"✅ Validação concluída - Score: {validacao_resultado['score_geral']:.1f}%")
            
            return validacao_resultado
            
        except Exception as e:
            logger.error(f"❌ Erro crítico na validação de qualidade: {e}")
            salvar_erro("validacao_qualidade", e, contexto={"session_id": session_id})
            
            # Retorna validação de emergência
            return self._validacao_emergencia(session_id, str(e))
    
    def _validar_componente_especifico(
        self, 
        nome_componente: str, 
        dados_componente: Any,
        contexto_completo: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Valida componente específico"""
        
        if not dados_componente:
            return {
                'score': 0.0,
                'valido': False,
                'motivo': 'Componente ausente',
                'detalhes': {}
            }
        
        rules = self.validation_rules.get(nome_componente, {})
        
        if nome_componente == 'drivers_mentais_customizados':
            return self._validar_drivers_mentais(dados_componente, rules)
        elif nome_componente == 'provas_visuais_sugeridas':
            return self._validar_provas_visuais(dados_componente, rules)
        elif nome_componente == 'pesquisa_web_massiva':
            return self._validar_pesquisa_web(dados_componente, rules)
        elif nome_componente == 'insights_exclusivos':
            return self._validar_insights(dados_componente, rules)
        else:
            return self._validar_componente_generico(dados_componente, rules)
    
    def _validar_drivers_mentais(self, dados: Any, rules: Dict[str, Any]) -> Dict[str, Any]:
        """Valida drivers mentais com critérios rigorosos"""
        
        try:
            if not isinstance(dados, dict) or 'drivers_customizados' not in dados:
                return {
                    'score': 0.0,
                    'valido': False,
                    'motivo': 'Estrutura inválida de drivers mentais'
                }
            
            drivers = dados['drivers_customizados']
            if not isinstance(drivers, list):
                return {
                    'score': 0.0,
                    'valido': False,
                    'motivo': 'Lista de drivers inválida'
                }
            
            drivers_validos = 0
            total_drivers = len(drivers)
            detalhes_validacao = []
            
            for i, driver in enumerate(drivers):
                driver_score = self._validar_driver_individual(driver, rules)
                detalhes_validacao.append({
                    'driver_index': i,
                    'nome': driver.get('nome', 'N/A'),
                    'score': driver_score['score'],
                    'valido': driver_score['valido'],
                    'problemas': driver_score.get('problemas', [])
                })
                
                if driver_score['valido']:
                    drivers_validos += 1
            
            # Score baseado na proporção de drivers válidos
            score = (drivers_validos / total_drivers * 100) if total_drivers > 0 else 0
            
            # Verifica threshold mínimo
            valido = drivers_validos >= self.quality_thresholds['min_drivers_mentais_quality']
            
            return {
                'score': score,
                'valido': valido,
                'drivers_validos': drivers_validos,
                'total_drivers': total_drivers,
                'detalhes': detalhes_validacao,
                'motivo': f"{drivers_validos}/{total_drivers} drivers válidos" if valido else f"Insuficientes: {drivers_validos} < {self.quality_thresholds['min_drivers_mentais_quality']}"
            }
            
        except Exception as e:
            return {
                'score': 0.0,
                'valido': False,
                'motivo': f'Erro na validação: {str(e)}'
            }
    
    def _validar_driver_individual(self, driver: Dict[str, Any], rules: Dict[str, Any]) -> Dict[str, Any]:
        """Valida driver individual"""
        
        problemas = []
        score = 100.0
        
        # Verifica campos obrigatórios
        for field in rules.get('required_fields', []):
            if not driver.get(field):
                problemas.append(f"Campo obrigatório ausente: {field}")
                score -= 25
        
        # Verifica qualidade da história
        roteiro = driver.get('roteiro_ativacao', {})
        historia = roteiro.get('historia_analogia', '')
        
        if len(historia) < rules.get('min_story_length', 150):
            problemas.append(f"História muito curta: {len(historia)} caracteres")
            score -= 20
        
        # Verifica especificidade
        if not self._check_story_specificity(historia):
            problemas.append("História genérica ou sem especificidade")
            score -= 30
        
        # Verifica elementos de qualidade
        quality_elements = 0
        for indicator in rules.get('quality_indicators', []):
            if roteiro.get(indicator) and len(str(roteiro[indicator])) > 20:
                quality_elements += 1
        
        if quality_elements < len(rules.get('quality_indicators', [])):
            problemas.append(f"Elementos de qualidade insuficientes: {quality_elements}")
            score -= 15
        
        return {
            'score': max(score, 0),
            'valido': score >= 70 and len(problemas) == 0,
            'problemas': problemas
        }
    
    def _check_story_specificity(self, historia: str) -> bool:
        """Verifica especificidade da história"""
        
        if not historia or len(historia) < 50:
            return False
        
        # Indicadores de especificidade
        import re
        specificity_patterns = [
            r'\d+%',  # Percentuais
            r'R\$\s*\d+',  # Valores monetários
            r'\d+\s*(dias|meses|anos|horas)',  # Períodos
            r'Dr\.|Dra\.',  # Títulos
            r'\d+\s*(pacientes|alunos|clientes)',  # Quantidades
            r'[A-Z][a-z]+,\s*[a-z]+',  # Nomes e profissões
        ]
        
        specificity_count = 0
        for pattern in specificity_patterns:
            if re.search(pattern, historia):
                specificity_count += 1
        
        # Verifica termos genéricos
        generic_terms = ['customizado para', 'baseado em', 'específico para', 'exemplo de']
        generic_count = sum(1 for term in generic_terms if term in historia.lower())
        
        return specificity_count >= 2 and generic_count == 0
    
    def _validar_provas_visuais(self, dados: Any, rules: Dict[str, Any]) -> Dict[str, Any]:
        """Valida provas visuais"""
        
        try:
            if not isinstance(dados, list):
                return {
                    'score': 0.0,
                    'valido': False,
                    'motivo': 'Provas visuais não são uma lista'
                }
            
            provas_validas = 0
            total_provas = len(dados)
            
            for prova in dados:
                if self._validar_prova_individual(prova, rules):
                    provas_validas += 1
            
            score = (provas_validas / total_provas * 100) if total_provas > 0 else 0
            valido = provas_validas >= self.quality_thresholds['min_provas_visuais']
            
            return {
                'score': score,
                'valido': valido,
                'provas_validas': provas_validas,
                'total_provas': total_provas,
                'motivo': f"{provas_validas}/{total_provas} provas válidas"
            }
            
        except Exception as e:
            return {
                'score': 0.0,
                'valido': False,
                'motivo': f'Erro na validação: {str(e)}'
            }
    
    def _validar_prova_individual(self, prova: Dict[str, Any], rules: Dict[str, Any]) -> bool:
        """Valida prova visual individual"""
        
        # Verifica campos obrigatórios
        for field in rules.get('required_fields', []):
            if not prova.get(field):
                return False
        
        # Verifica materiais mínimos
        materiais = prova.get('materiais', [])
        if len(materiais) < rules.get('min_materials', 2):
            return False
        
        # Verifica se experimento é específico
        experimento = prova.get('experimento', '')
        if len(experimento) < 50 or 'genérico' in experimento.lower():
            return False
        
        return True
    
    def _validar_pesquisa_web(self, dados: Any, rules: Dict[str, Any]) -> Dict[str, Any]:
        """Valida pesquisa web"""
        
        try:
            if not isinstance(dados, dict):
                return {
                    'score': 0.0,
                    'valido': False,
                    'motivo': 'Dados de pesquisa inválidos'
                }
            
            score = 0.0
            
            # Verifica fontes únicas
            fontes_unicas = dados.get('unique_sources', 0)
            if fontes_unicas >= self.quality_thresholds['min_fontes_pesquisa']:
                score += 40
            else:
                score += (fontes_unicas / self.quality_thresholds['min_fontes_pesquisa']) * 40
            
            # Verifica conteúdo extraído
            extracted_content = dados.get('extracted_content', [])
            if len(extracted_content) >= 3:
                score += 30
            else:
                score += (len(extracted_content) / 3) * 30
            
            # Verifica qualidade média
            quality_metrics = dados.get('quality_metrics', {})
            avg_quality = quality_metrics.get('avg_quality_score', 0)
            score += (avg_quality / 100) * 30
            
            valido = score >= 70 and fontes_unicas >= 3
            
            return {
                'score': score,
                'valido': valido,
                'fontes_unicas': fontes_unicas,
                'conteudo_extraido': len(extracted_content),
                'qualidade_media': avg_quality,
                'motivo': f"Score: {score:.1f}%, Fontes: {fontes_unicas}"
            }
            
        except Exception as e:
            return {
                'score': 0.0,
                'valido': False,
                'motivo': f'Erro na validação: {str(e)}'
            }
    
    def _validar_insights(self, dados: Any, rules: Dict[str, Any]) -> Dict[str, Any]:
        """Valida insights exclusivos"""
        
        try:
            if not isinstance(dados, list):
                return {
                    'score': 0.0,
                    'valido': False,
                    'motivo': 'Insights não são uma lista'
                }
            
            insights_validos = 0
            total_insights = len(dados)
            
            for insight in dados:
                if self._validar_insight_individual(insight, rules):
                    insights_validos += 1
            
            score = (insights_validos / total_insights * 100) if total_insights > 0 else 0
            valido = insights_validos >= self.quality_thresholds['min_insights_exclusivos']
            
            return {
                'score': score,
                'valido': valido,
                'insights_validos': insights_validos,
                'total_insights': total_insights,
                'motivo': f"{insights_validos}/{total_insights} insights válidos"
            }
            
        except Exception as e:
            return {
                'score': 0.0,
                'valido': False,
                'motivo': f'Erro na validação: {str(e)}'
            }
    
    def _validar_insight_individual(self, insight: str, rules: Dict[str, Any]) -> bool:
        """Valida insight individual"""
        
        if not insight or len(insight) < rules.get('min_insight_length', 50):
            return False
        
        # Verifica se não é genérico
        generic_indicators = ['baseado em', 'customizado para', 'específico para']
        if any(indicator in insight.lower() for indicator in generic_indicators):
            return False
        
        # Verifica se tem elementos específicos
        import re
        specific_elements = [
            r'\d+%',  # Percentuais
            r'R\$\s*\d+',  # Valores
            r'\d+\s*(mil|milhão|bilhão)',  # Quantidades grandes
            r'20(23|24|25)',  # Anos recentes
        ]
        
        has_specifics = any(re.search(pattern, insight) for pattern in specific_elements)
        
        return has_specifics
    
    def _validar_componente_generico(self, dados: Any, rules: Dict[str, Any]) -> Dict[str, Any]:
        """Validação genérica para outros componentes"""
        
        if not dados:
            return {
                'score': 0.0,
                'valido': False,
                'motivo': 'Componente vazio'
            }
        
        # Validação básica
        score = 50.0  # Score base
        
        if isinstance(dados, dict) and len(dados) > 0:
            score += 30
        elif isinstance(dados, list) and len(dados) > 0:
            score += 30
        
        # Verifica campos obrigatórios se especificados
        if isinstance(dados, dict):
            required_fields = rules.get('required_fields', [])
            fields_present = sum(1 for field in required_fields if field in dados)
            if required_fields:
                score += (fields_present / len(required_fields)) * 20
        
        return {
            'score': score,
            'valido': score >= 60,
            'motivo': f'Validação genérica - Score: {score:.1f}%'
        }
    
    def _gerar_diagnostico_qualidade(
        self, 
        validacao_resultado: Dict[str, Any], 
        dados_pipeline: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Gera diagnóstico final de qualidade"""
        
        score_geral = validacao_resultado['score_geral']
        
        # Determina status
        if score_geral >= 80:
            status = 'EXCELENTE'
            recomendacao = 'Análise de alta qualidade - prosseguir com implementação'
        elif score_geral >= 60:
            status = 'BOM'
            recomendacao = 'Qualidade adequada - pequenos ajustes podem melhorar'
        elif score_geral >= 40:
            status = 'REGULAR'
            recomendacao = 'Qualidade limitada - configure mais APIs para melhorar'
        else:
            status = 'BAIXO'
            recomendacao = 'Qualidade insuficiente - reexecute com configuração completa'
        
        # Identifica problemas críticos
        problemas_criticos = []
        for componente, validacao in validacao_resultado['validacoes_componentes'].items():
            if not validacao.get('valido', False):
                problemas_criticos.append(f"{componente}: {validacao.get('motivo', 'Falha na validação')}")
        
        # Gera recomendações específicas
        recomendacoes = [recomendacao]
        
        if len(problemas_criticos) > 0:
            recomendacoes.append("Configure APIs faltantes para melhorar componentes que falharam")
        
        if score_geral < 60:
            recomendacoes.append("Execute nova análise com dados mais específicos")
        
        return {
            'status_final': status,
            'problemas_criticos': problemas_criticos,
            'recomendacoes': recomendacoes,
            'aprovado_para_consolidacao': score_geral >= 40,  # Threshold flexível
            'nivel_confianca': 'Alto' if score_geral >= 70 else 'Médio' if score_geral >= 50 else 'Baixo'
        }
    
    def _validacao_emergencia(self, session_id: str, erro: str) -> Dict[str, Any]:
        """Validação de emergência quando sistema principal falha"""
        
        return {
            'session_id': session_id,
            'timestamp': datetime.now().isoformat(),
            'qualidade_aprovada': False,
            'score_geral': 0.0,
            'status_final': 'ERRO_VALIDACAO',
            'erro': erro,
            'recomendacoes': [
                'Verifique logs para detalhes do erro',
                'Execute nova análise com configuração correta',
                'Analise manualmente arquivos intermediários salvos'
            ],
            'dados_preservados': True,
            'recuperacao_possivel': True
        }

# Instância global
quality_validation_service = QualityValidationService()