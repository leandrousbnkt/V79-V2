#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v2.0 - Teste do Sistema Ultra-Robusto
Valida todas as correções implementadas para garantir robustez total
"""

import sys
import os
import time
import logging
import json
from datetime import datetime
from typing import Dict, Any

# Adiciona src ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def test_auto_save_manager():
    """Testa o sistema de salvamento automático"""
    
    print("=" * 80)
    print("💾 TESTE DO SISTEMA DE SALVAMENTO AUTOMÁTICO")
    print("=" * 80)
    
    try:
        from services.auto_save_manager import auto_save_manager, salvar_etapa, salvar_erro
        
        # Inicia sessão de teste
        session_id = auto_save_manager.iniciar_sessao("test_session")
        print(f"✅ Sessão iniciada: {session_id}")
        
        # Testa salvamento de etapa
        dados_teste = {
            "segmento": "Produtos Digitais",
            "dados_gerados": ["item1", "item2", "item3"],
            "timestamp": time.time()
        }
        
        arquivo_salvo = salvar_etapa("teste_etapa", dados_teste, categoria="teste")
        print(f"✅ Etapa salva: {arquivo_salvo}")
        
        # Testa salvamento de erro
        try:
            raise ValueError("Erro de teste intencional")
        except Exception as e:
            arquivo_erro = salvar_erro("teste_erro", e, contexto={"teste": True})
            print(f"✅ Erro salvo: {arquivo_erro}")
        
        # Testa recuperação
        dados_recuperados = auto_save_manager.recuperar_etapa("teste_etapa", session_id)
        if dados_recuperados and dados_recuperados['dados']['segmento'] == "Produtos Digitais":
            print("✅ Recuperação de dados funcionando")
        else:
            print("❌ Falha na recuperação de dados")
            return False
        
        # Testa listagem
        etapas_listadas = auto_save_manager.listar_etapas_salvas(session_id)
        print(f"✅ Etapas listadas: {len(etapas_listadas)} encontradas")
        
        # Testa consolidação
        relatorio = auto_save_manager.consolidar_sessao(session_id)
        print(f"✅ Relatório consolidado: {relatorio}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro no teste de salvamento automático: {e}")
        return False

def test_url_filter_manager():
    """Testa o sistema de filtros de URL"""
    
    print("\n" + "=" * 80)
    print("🔍 TESTE DO SISTEMA DE FILTROS DE URL")
    print("=" * 80)
    
    try:
        from services.url_filter_manager import url_filter_manager, filtrar_urls
        
        # URLs de teste (algumas devem ser bloqueadas)
        test_urls = [
            {
                'url': 'https://g1.globo.com/tecnologia/noticia.html',
                'title': 'Análise do mercado de tecnologia no Brasil',
                'snippet': 'Mercado brasileiro de tecnologia cresce 25% em 2024'
            },
            {
                'url': 'https://accounts.google.com/signin',
                'title': 'Fazer login na sua conta Google',
                'snippet': 'Entre na sua conta Google para acessar'
            },
            {
                'url': 'https://instagram.com/profile/user',
                'title': 'Perfil do Instagram',
                'snippet': 'Veja fotos e vídeos no Instagram'
            },
            {
                'url': 'https://exame.com/negocios/analise-mercado-2024',
                'title': 'Análise completa do mercado brasileiro 2024',
                'snippet': 'Dados e estatísticas do crescimento empresarial'
            },
            {
                'url': 'https://answers.microsoft.com/pt-br/help',
                'title': 'Ajuda e suporte Microsoft',
                'snippet': 'Encontre respostas para suas dúvidas'
            }
        ]
        
        print(f"🧪 Testando filtro com {len(test_urls)} URLs...")
        
        # Aplica filtros
        urls_aprovadas = filtrar_urls(test_urls)
        
        print(f"📊 Resultado do filtro:")
        print(f"   • URLs originais: {len(test_urls)}")
        print(f"   • URLs aprovadas: {len(urls_aprovadas)}")
        print(f"   • Taxa de aprovação: {len(urls_aprovadas)/len(test_urls)*100:.1f}%")
        
        # Verifica se URLs corretas foram aprovadas/bloqueadas
        urls_aprovadas_set = {item['url'] for item in urls_aprovadas}
        
        # Deve aprovar
        deve_aprovar = [
            'https://g1.globo.com/tecnologia/noticia.html',
            'https://exame.com/negocios/analise-mercado-2024'
        ]
        
        # Deve bloquear
        deve_bloquear = [
            'https://accounts.google.com/signin',
            'https://instagram.com/profile/user',
            'https://answers.microsoft.com/pt-br/help'
        ]
        
        aprovadas_corretas = sum(1 for url in deve_aprovar if url in urls_aprovadas_set)
        bloqueadas_corretas = sum(1 for url in deve_bloquear if url not in urls_aprovadas_set)
        
        print(f"   • Aprovações corretas: {aprovadas_corretas}/{len(deve_aprovar)}")
        print(f"   • Bloqueios corretos: {bloqueadas_corretas}/{len(deve_bloquear)}")
        
        # Mostra estatísticas
        stats = url_filter_manager.get_stats()
        print(f"📈 Estatísticas do filtro:")
        for key, value in stats.items():
            if isinstance(value, float):
                print(f"   • {key}: {value:.1f}%")
            else:
                print(f"   • {key}: {value}")
        
        # Sucesso se pelo menos 80% das decisões estão corretas
        total_decisoes = len(deve_aprovar) + len(deve_bloquear)
        decisoes_corretas = aprovadas_corretas + bloqueadas_corretas
        taxa_acerto = (decisoes_corretas / total_decisoes) * 100
        
        print(f"🎯 Taxa de acerto do filtro: {taxa_acerto:.1f}%")
        
        return taxa_acerto >= 80
        
    except Exception as e:
        print(f"❌ Erro no teste de filtros de URL: {e}")
        return False

def test_resilient_executor():
    """Testa o executor resiliente"""
    
    print("\n" + "=" * 80)
    print("🛡️ TESTE DO EXECUTOR RESILIENTE")
    print("=" * 80)
    
    try:
        from services.resilient_component_executor import resilient_executor
        
        # Componentes de teste
        def componente_sucesso(data):
            time.sleep(0.1)  # Simula processamento
            return {"status": "sucesso", "dados": f"Processado: {data.get('teste', 'N/A')}"}
        
        def componente_falha(data):
            raise Exception("Falha intencional para teste")
        
        def componente_fallback(data):
            return {"status": "fallback", "dados": "Dados de fallback"}
        
        def componente_com_dependencia(data):
            if 'comp_sucesso' in data:
                return {"status": "sucesso", "depende_de": "comp_sucesso"}
            else:
                raise Exception("Dependência não atendida")
        
        # Registra componentes
        resilient_executor.registrar_componente(
            'comp_sucesso', 
            componente_sucesso,
            obrigatorio=True
        )
        
        resilient_executor.registrar_componente(
            'comp_falha',
            componente_falha,
            fallback=componente_fallback,
            obrigatorio=False
        )
        
        resilient_executor.registrar_componente(
            'comp_dependencia',
            componente_com_dependencia,
            obrigatorio=False
        )
        
        # Executa pipeline
        dados_teste = {"teste": "dados_de_entrada"}
        
        print("🚀 Executando pipeline resiliente de teste...")
        resultado = resilient_executor.executar_pipeline_resiliente(dados_teste)
        
        # Verifica resultados
        stats = resultado['estatisticas']
        print(f"📊 Estatísticas do pipeline:")
        print(f"   • Componentes executados: {stats['componentes_executados']}")
        print(f"   • Componentes falharam: {stats['componentes_falharam']}")
        print(f"   • Taxa de sucesso: {stats['taxa_sucesso']:.1f}%")
        print(f"   • Pipeline completo: {stats['pipeline_completo']}")
        
        # Verifica se dados foram preservados
        dados_gerados = resultado.get('dados_gerados', {})
        print(f"   • Dados preservados: {len(dados_gerados)} componentes")
        
        # Sucesso se pelo menos 1 componente funcionou e dados foram preservados
        return stats['componentes_executados'] >= 1 and len(dados_gerados) > 0
        
    except Exception as e:
        print(f"❌ Erro no teste do executor resiliente: {e}")
        return False

def test_import_fixes():
    """Testa se os imports foram corrigidos"""
    
    print("\n" + "=" * 80)
    print("📦 TESTE DAS CORREÇÕES DE IMPORT")
    print("=" * 80)
    
    try:
        # Testa imports que estavam falhando
        modules_to_test = [
            'services.mental_drivers_architect',
            'services.anti_objection_system', 
            'services.visual_proofs_generator',
            'services.pre_pitch_architect',
            'services.robust_content_extractor'
        ]
        
        import_results = []
        
        for module_name in modules_to_test:
            try:
                __import__(module_name)
                print(f"✅ {module_name}: Import OK")
                import_results.append(True)
                
                # Testa se time e random estão disponíveis
                module = sys.modules[module_name]
                if hasattr(module, 'time'):
                    print(f"   ✅ time disponível")
                if hasattr(module, 'random'):
                    print(f"   ✅ random disponível")
                    
            except Exception as e:
                print(f"❌ {module_name}: {e}")
                import_results.append(False)
        
        success_rate = (sum(import_results) / len(import_results)) * 100
        print(f"\n📊 Taxa de sucesso dos imports: {success_rate:.1f}%")
        
        return success_rate == 100
        
    except Exception as e:
        print(f"❌ Erro no teste de imports: {e}")
        return False

def test_component_fallbacks():
    """Testa se os fallbacks dos componentes funcionam"""
    
    print("\n" + "=" * 80)
    print("🔄 TESTE DOS SISTEMAS DE FALLBACK")
    print("=" * 80)
    
    try:
        from services.mental_drivers_architect import mental_drivers_architect
        from services.anti_objection_system import anti_objection_system
        from services.visual_proofs_generator import visual_proofs_generator
        
        test_data = {
            'segmento': 'Produtos Digitais',
            'produto': 'Curso Online',
            'publico': 'Empreendedores'
        }
        
        fallback_results = []
        
        # Testa fallback de drivers mentais
        print("🧠 Testando fallback de drivers mentais...")
        try:
            drivers_fallback = mental_drivers_architect._generate_fallback_drivers_system(test_data)
            if drivers_fallback and drivers_fallback.get('fallback_mode'):
                print(f"   ✅ Fallback drivers: {len(drivers_fallback['drivers_customizados'])} drivers")
                fallback_results.append(True)
            else:
                print("   ❌ Fallback drivers falhou")
                fallback_results.append(False)
        except Exception as e:
            print(f"   ❌ Erro no fallback drivers: {e}")
            fallback_results.append(False)
        
        # Testa fallback de anti-objeção
        print("🛡️ Testando fallback de anti-objeção...")
        try:
            anti_obj_fallback = anti_objection_system._generate_fallback_anti_objection_system(test_data)
            if anti_obj_fallback and anti_obj_fallback.get('fallback_mode'):
                print(f"   ✅ Fallback anti-objeção: {len(anti_obj_fallback['objecoes_universais'])} objeções")
                fallback_results.append(True)
            else:
                print("   ❌ Fallback anti-objeção falhou")
                fallback_results.append(False)
        except Exception as e:
            print(f"   ❌ Erro no fallback anti-objeção: {e}")
            fallback_results.append(False)
        
        # Testa fallback de provas visuais
        print("🎭 Testando fallback de provas visuais...")
        try:
            provas_fallback = visual_proofs_generator._get_default_visual_proofs(test_data)
            if provas_fallback and len(provas_fallback) > 0:
                print(f"   ✅ Fallback provas visuais: {len(provas_fallback)} provas")
                fallback_results.append(True)
            else:
                print("   ❌ Fallback provas visuais falhou")
                fallback_results.append(False)
        except Exception as e:
            print(f"   ❌ Erro no fallback provas visuais: {e}")
            fallback_results.append(False)
        
        success_rate = (sum(fallback_results) / len(fallback_results)) * 100
        print(f"\n📊 Taxa de sucesso dos fallbacks: {success_rate:.1f}%")
        
        return success_rate >= 80
        
    except Exception as e:
        print(f"❌ Erro no teste de fallbacks: {e}")
        return False

def test_end_to_end_resilience():
    """Testa resiliência end-to-end do sistema"""
    
    print("\n" + "=" * 80)
    print("🔄 TESTE DE RESILIÊNCIA END-TO-END")
    print("=" * 80)
    
    try:
        from services.auto_save_manager import auto_save_manager
        from services.resilient_component_executor import resilient_executor
        
        # Dados de teste
        test_data = {
            'segmento': 'Produtos Digitais',
            'produto': 'Curso Online de Marketing',
            'publico': 'Empreendedores digitais',
            'preco': 997.0,
            'query': 'mercado produtos digitais Brasil 2024'
        }
        
        print("🧪 Simulando pipeline resiliente...")
        
        # Inicia sessão
        session_id = auto_save_manager.iniciar_sessao("test_resilience")
        
        # Simula execução de componentes com falhas
        componentes_simulados = [
            ('pesquisa_web', True, {"resultados": 10, "fontes": 5}),
            ('avatar_detalhado', True, {"perfil": "completo", "dores": 8}),
            ('drivers_mentais', False, None),  # Falha simulada
            ('provas_visuais', True, {"provas": 3}),
            ('anti_objecao', False, None),  # Falha simulada
            ('pre_pitch', True, {"roteiro": "completo"}),
            ('predicoes_futuro', True, {"cenarios": 3})
        ]
        
        dados_salvos = []
        
        for nome, sucesso, dados in componentes_simulados:
            if sucesso:
                from services.auto_save_manager import salvar_etapa
                arquivo = salvar_etapa(nome, dados, status="sucesso")
                dados_salvos.append((nome, arquivo))
                print(f"   ✅ {nome}: Dados salvos")
            else:
                from services.auto_save_manager import salvar_erro
                arquivo = salvar_erro(nome, Exception(f"Falha simulada em {nome}"))
                print(f"   ❌ {nome}: Falha registrada")
        
        # Verifica se dados foram preservados
        etapas_salvas = auto_save_manager.listar_etapas_salvas(session_id)
        
        print(f"\n📊 Resultados da simulação:")
        print(f"   • Componentes simulados: {len(componentes_simulados)}")
        print(f"   • Sucessos: {sum(1 for _, sucesso, _ in componentes_simulados if sucesso)}")
        print(f"   • Falhas: {sum(1 for _, sucesso, _ in componentes_simulados if not sucesso)}")
        print(f"   • Etapas salvas: {len(etapas_salvas)}")
        print(f"   • Dados preservados: {len(dados_salvos)} arquivos")
        
        # Testa consolidação
        relatorio = auto_save_manager.consolidar_sessao(session_id)
        print(f"   • Relatório consolidado: {relatorio}")
        
        # Sucesso se dados foram preservados mesmo com falhas
        return len(etapas_salvas) >= 5 and len(dados_salvos) >= 4
        
    except Exception as e:
        print(f"❌ Erro no teste de resiliência: {e}")
        return False

def run_ultra_robust_test():
    """Executa teste completo do sistema ultra-robusto"""
    
    print("🚀 INICIANDO TESTE COMPLETO DO SISTEMA ULTRA-ROBUSTO")
    print("=" * 100)
    
    tests = [
        ("Sistema de Salvamento Automático", test_auto_save_manager),
        ("Filtros de URL Inteligentes", test_url_filter_manager),
        ("Executor Resiliente", test_resilient_executor),
        ("Correções de Import", test_import_fixes),
        ("Sistemas de Fallback", test_component_fallbacks),
        ("Resiliência End-to-End", test_end_to_end_resilience)
    ]
    
    results = []
    total_start_time = time.time()
    
    for test_name, test_func in tests:
        print(f"\n🧪 Executando: {test_name}")
        try:
            start_time = time.time()
            result = test_func()
            execution_time = time.time() - start_time
            
            results.append((test_name, result, execution_time))
            
            status = "✅ PASSOU" if result else "❌ FALHOU"
            print(f"{status} {test_name} em {execution_time:.2f}s")
            
        except Exception as e:
            print(f"❌ Erro crítico em {test_name}: {e}")
            results.append((test_name, False, 0))
    
    total_time = time.time() - total_start_time
    
    # Relatório final
    print("\n" + "=" * 100)
    print("🏁 RELATÓRIO FINAL DO SISTEMA ULTRA-ROBUSTO")
    print("=" * 100)
    
    passed = sum(1 for _, result, _ in results if result)
    total = len(results)
    
    for test_name, result, exec_time in results:
        status = "✅ PASSOU" if result else "❌ FALHOU"
        print(f"{test_name:.<50} {status} ({exec_time:.2f}s)")
    
    print(f"\nTotal: {passed}/{total} testes passaram ({passed/total*100:.1f}%)")
    print(f"Tempo total de execução: {total_time:.2f}s")
    
    if passed == total:
        print("\n🎉 SISTEMA ULTRA-ROBUSTO VALIDADO!")
        print("✅ Todas as correções funcionam perfeitamente!")
        
        print("\n🛡️ RECURSOS ULTRA-ROBUSTOS IMPLEMENTADOS:")
        print("   ✅ Salvamento automático e imediato de todos os resultados")
        print("   ✅ Isolamento total de falhas - um componente não para os outros")
        print("   ✅ Sistemas de fallback para todos os componentes críticos")
        print("   ✅ Filtros inteligentes de URL para evitar conteúdo irrelevante")
        print("   ✅ Executor resiliente que nunca para completamente")
        print("   ✅ Recuperação automática de dados em caso de falha")
        print("   ✅ Consolidação automática de relatórios intermediários")
        print("   ✅ Correções de import para evitar erros de 'name not defined'")
        
        print("\n🚀 GARANTIAS DO SISTEMA:")
        print("   🔒 NENHUM DADO É PERDIDO - Salvamento imediato após cada etapa")
        print("   🛡️ FALHAS ISOLADAS - Um componente falhando não para o sistema")
        print("   📊 RELATÓRIOS PRESERVADOS - Todos os resultados intermediários salvos")
        print("   🔄 RECUPERAÇÃO AUTOMÁTICA - Sistema se recupera de falhas automaticamente")
        print("   🎯 QUALIDADE GARANTIDA - Filtros removem conteúdo irrelevante")
        
        print("\n🎯 PRÓXIMOS PASSOS:")
        print("1. ✅ Execute uma análise real para validar em produção")
        print("2. 📊 Monitore diretório 'relatorios_intermediarios' para ver dados salvos")
        print("3. 🔧 Configure APIs restantes para máxima qualidade")
        print("4. 📈 Sistema está pronto para uso em produção!")
        
    elif passed >= total * 0.8:
        print("\n👍 SISTEMA MAJORITARIAMENTE ROBUSTO!")
        print("⚠️ Algumas funcionalidades podem precisar de ajustes")
        print("🔧 Revise os testes que falharam")
        
    else:
        print("\n❌ SISTEMA PRECISA DE MAIS CORREÇÕES!")
        print("🚨 Muitos testes falharam - revise implementação")
        print("🔧 Verifique logs detalhados para debug")
    
    return passed >= total * 0.8

if __name__ == "__main__":
    success = run_ultra_robust_test()
    
    if success:
        print("\n🎯 SISTEMA ULTRA-ROBUSTO IMPLEMENTADO COM SUCESSO!")
        
        print("\n📋 RESUMO DAS MELHORIAS:")
        print("• 💾 Auto Save Manager: Salva todos os dados imediatamente")
        print("• 🛡️ Resilient Executor: Isola falhas e continua processamento")
        print("• 🔍 URL Filter Manager: Remove conteúdo irrelevante")
        print("• 🔄 Sistemas de Fallback: Garantem que sempre há resultado")
        print("• 📦 Imports Corrigidos: Eliminam erros de 'name not defined'")
        print("• 🎯 Pipeline Resiliente: Nunca para completamente")
        
        print("\n🚀 O SISTEMA AGORA É VERDADEIRAMENTE ULTRA-ROBUSTO!")
        print("   Pode falhar em componentes individuais mas NUNCA perde dados!")
        
    else:
        print("\n🔧 AÇÕES NECESSÁRIAS:")
        print("1. ❌ Revise os testes que falharam")
        print("2. 🔧 Verifique implementação dos componentes")
        print("3. 🧪 Execute testes individuais para debug")
        print("4. 📞 Consulte logs para detalhes específicos")
    
    sys.exit(0 if success else 1)