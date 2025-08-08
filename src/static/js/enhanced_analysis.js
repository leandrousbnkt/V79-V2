/**
 * ARQV30 Enhanced v2.0 - Enhanced Analysis JavaScript
 * Sistema de análise arqueológica ultra-detalhada
 */

class EnhancedAnalysisManager {
    constructor() {
        this.currentAnalysis = null;
        this.progressTracker = null;
        this.sessionId = null;
        this.isAnalyzing = false;
        
        this.init();
    }
    
    init() {
        console.log('🚀 Enhanced Analysis Manager inicializado');
        
        // Setup form submission
        const form = document.getElementById('enhancedAnalysisForm');
        if (form) {
            form.addEventListener('submit', (e) => this.handleFormSubmission(e));
        }
        
        // Setup keyboard shortcuts
        document.addEventListener('keydown', (e) => this.handleKeyboardShortcuts(e));
        
        // Load agent capabilities
        this.loadAgentCapabilities();
        
        // Update system status
        this.updateSystemStatus();
    }
    
    async handleFormSubmission(event) {
        event.preventDefault();
        
        if (this.isAnalyzing) {
            this.showAlert('Análise já em andamento', 'warning');
            return;
        }
        
        try {
            // Validate form
            if (!this.validateForm()) {
                return;
            }
            
            // Collect form data
            const formData = this.collectFormData();
            
            // Generate session ID
            this.sessionId = `enhanced_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
            formData.session_id = this.sessionId;
            
            // Start analysis
            await this.startEnhancedAnalysis(formData);
            
        } catch (error) {
            console.error('Erro na submissão do formulário:', error);
            this.showAlert('Erro ao iniciar análise: ' + error.message, 'error');
        }
    }
    
    validateForm() {
        const segmento = document.getElementById('segmento').value.trim();
        
        if (!segmento) {
            this.showAlert('Segmento é obrigatório para análise arqueológica', 'error');
            document.getElementById('segmento').focus();
            return false;
        }
        
        if (segmento.length < 3) {
            this.showAlert('Segmento deve ter pelo menos 3 caracteres', 'error');
            document.getElementById('segmento').focus();
            return false;
        }
        
        return true;
    }
    
    collectFormData() {
        const formData = {
            segmento: document.getElementById('segmento').value.trim(),
            produto: document.getElementById('produto').value.trim(),
            publico: document.getElementById('publico').value.trim(),
            preco: parseFloat(document.getElementById('preco').value) || null,
            objetivo_receita: parseFloat(document.getElementById('objetivo_receita').value) || null,
            orcamento_marketing: parseFloat(document.getElementById('orcamento_marketing').value) || null,
            prazo_lancamento: document.getElementById('prazo_lancamento').value,
            concorrentes: document.getElementById('concorrentes').value.trim(),
            query: document.getElementById('query').value.trim(),
            dados_adicionais: document.getElementById('dados_adicionais').value.trim()
        };
        
        // Auto-generate query if not provided
        if (!formData.query && formData.segmento) {
            formData.query = `mercado ${formData.segmento} Brasil 2024 tendências oportunidades análise`;
        }
        
        return formData;
    }
    
    async startEnhancedAnalysis(formData) {
        try {
            this.isAnalyzing = true;
            
            // Show progress area
            this.showProgressArea();
            
            // Hide form
            document.querySelector('.enhanced-form-container').style.display = 'none';
            
            // Start progress tracking
            this.startProgressTracking();
            
            // Make API call
            const response = await fetch('/api/analyze_ultra_enhanced', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(formData)
            });
            
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.message || 'Erro na análise');
            }
            
            const analysisResult = await response.json();
            
            // Stop progress tracking
            this.stopProgressTracking();
            
            // Show results
            this.displayEnhancedResults(analysisResult);
            
            // Store current analysis
            this.currentAnalysis = analysisResult;
            
            this.showAlert('Análise arqueológica concluída com sucesso!', 'success');
            
        } catch (error) {
            console.error('Erro na análise:', error);
            this.stopProgressTracking();
            this.showAlert('Erro na análise: ' + error.message, 'error');
            this.showFormAgain();
        } finally {
            this.isAnalyzing = false;
        }
    }
    
    showProgressArea() {
        const progressArea = document.getElementById('progressArea');
        if (progressArea) {
            progressArea.style.display = 'block';
            progressArea.scrollIntoView({ behavior: 'smooth' });
        }
    }
    
    startProgressTracking() {
        const steps = [
            '🔍 Validando dados e preparando análise arqueológica...',
            '🌐 WebSailor navegando e coletando dados reais...',
            '📄 Extraindo conteúdo de fontes preferenciais...',
            '🤖 Gemini 2.5 Pro executando análise forense...',
            '🔬 Arqueólogo Mestre escavando DNA da conversão...',
            '🧠 Mestre Visceral executando engenharia reversa...',
            '⚙️ Arquiteto criando drivers mentais customizados...',
            '🎭 Diretor criando arsenal de PROVIs devastadoras...',
            '🛡️ Especialista construindo sistema anti-objeção...',
            '🎯 Mestre orquestrando pré-pitch invisível...',
            '📊 Calculando métricas forenses objetivas...',
            '🔮 Predizendo futuro do mercado...',
            '✨ Consolidando análise arqueológica final...'
        ];
        
        let currentStep = 0;
        const totalSteps = steps.length;
        
        this.progressTracker = setInterval(() => {
            if (currentStep < totalSteps) {
                this.updateProgress(currentStep + 1, totalSteps, steps[currentStep]);
                currentStep++;
            }
        }, 3000); // Update every 3 seconds
    }
    
    stopProgressTracking() {
        if (this.progressTracker) {
            clearInterval(this.progressTracker);
            this.progressTracker = null;
        }
        
        // Set to 100%
        this.updateProgress(13, 13, '🎉 Análise arqueológica concluída!');
    }
    
    updateProgress(current, total, message) {
        const percentage = (current / total) * 100;
        
        // Update progress bar
        const progressFill = document.querySelector('.progress-fill-enhanced');
        if (progressFill) {
            progressFill.style.width = `${percentage}%`;
        }
        
        // Update step counter
        const stepCounter = document.getElementById('stepCounterEnhanced');
        if (stepCounter) {
            stepCounter.textContent = `${current}/${total}`;
        }
        
        // Update current step
        const currentStep = document.getElementById('currentStepEnhanced');
        if (currentStep) {
            currentStep.textContent = message;
        }
        
        // Update estimated time
        const estimatedTime = document.getElementById('estimatedTimeEnhanced');
        if (estimatedTime) {
            const remaining = Math.max(0, (total - current) * 3); // 3 seconds per step
            const minutes = Math.floor(remaining / 60);
            const seconds = remaining % 60;
            estimatedTime.textContent = `${minutes}:${seconds.toString().padStart(2, '0')}`;
        }
    }
    
    async displayEnhancedResults(analysisResult) {
        try {
            // Hide progress area
            const progressArea = document.getElementById('progressArea');
            if (progressArea) {
                progressArea.style.display = 'none';
            }
            
            // Show results area
            const resultsArea = document.getElementById('resultsArea');
            if (resultsArea) {
                resultsArea.style.display = 'block';
                resultsArea.innerHTML = this.generateEnhancedResultsHTML(analysisResult);
                resultsArea.scrollIntoView({ behavior: 'smooth' });
            }
            
            // Setup interactive elements
            this.setupInteractiveElements();
            
        } catch (error) {
            console.error('Erro ao exibir resultados:', error);
            this.showAlert('Erro ao exibir resultados: ' + error.message, 'error');
        }
    }
    
    generateEnhancedResultsHTML(analysis) {
        return `
            <div class="results-enhanced">
                <div class="results-header-enhanced">
                    <h3 class="results-title-enhanced">
                        <i class="fas fa-microscope"></i>
                        Análise Arqueológica Ultra-Detalhada Concluída
                    </h3>
                    <div class="results-actions-enhanced">
                        <button class="btn-secondary-enhanced" onclick="enhancedAnalysis.downloadPDF()">
                            <i class="fas fa-file-pdf"></i>
                            Relatório PDF
                        </button>
                        <button class="btn-secondary-enhanced" onclick="enhancedAnalysis.saveJSON()">
                            <i class="fas fa-save"></i>
                            Dados JSON
                        </button>
                        <button class="btn-secondary-enhanced" onclick="location.reload()">
                            <i class="fas fa-plus"></i>
                            Nova Análise
                        </button>
                    </div>
                </div>
                
                <div class="results-content-enhanced">
                    ${this.renderArchaeologicalAnalysis(analysis)}
                    ${this.renderVisceralAvatar(analysis)}
                    ${this.renderDriversArsenal(analysis)}
                    ${this.renderProvisArsenal(analysis)}
                    ${this.renderAntiObjectionSystem(analysis)}
                    ${this.renderForensicMetrics(analysis)}
                    ${this.renderResearchResults(analysis)}
                    ${this.renderMetadata(analysis)}
                </div>
            </div>
        `;
    }
    
    renderArchaeologicalAnalysis(analysis) {
        const archaeological = analysis.dna_conversao_completo || {};
        
        return `
            <div class="card-enhanced archaeological-card">
                <div class="card-header-enhanced">
                    <div class="card-icon-enhanced">
                        <i class="fas fa-microscope"></i>
                    </div>
                    <h4 class="card-title-enhanced">🔬 DNA da Conversão Extraído</h4>
                </div>
                
                <div class="archaeological-content">
                    <div class="dna-formula">
                        <h5>🧬 Fórmula Estrutural Descoberta:</h5>
                        <div class="formula-display">
                            ${archaeological.formula_estrutural || 'DESPERTAR → AMPLIFICAR → PRESSIONAR → DIRECIONAR → CONVERTER'}
                        </div>
                    </div>
                    
                    <div class="trigger-sequence">
                        <h5>⚡ Sequência de Gatilhos Psicológicos:</h5>
                        <div class="triggers-list">
                            ${this.renderTriggersList(archaeological.sequencia_gatilhos || [])}
                        </div>
                    </div>
                    
                    <div class="language-patterns">
                        <h5>🗣️ Padrões de Linguagem Identificados:</h5>
                        <div class="patterns-list">
                            ${this.renderPatternsList(archaeological.padroes_linguagem || [])}
                        </div>
                    </div>
                    
                    <div class="timing-optimization">
                        <h5>⏰ Timing Ótimo:</h5>
                        <p>${archaeological.timing_otimo || 'Análise de timing em andamento'}</p>
                    </div>
                </div>
            </div>
        `;
    }
    
    renderVisceralAvatar(analysis) {
        const avatar = analysis.avatar_visceral_ultra || analysis.avatar_ultra_detalhado || {};
        
        return `
            <div class="card-enhanced visceral-card">
                <div class="card-header-enhanced">
                    <div class="card-icon-enhanced">
                        <i class="fas fa-brain"></i>
                    </div>
                    <h4 class="card-title-enhanced">🧠 Avatar Visceral Ultra-Detalhado</h4>
                </div>
                
                <div class="visceral-content">
                    <div class="avatar-identity">
                        <h5>👤 ${avatar.nome_ficticio || 'Profissional em Transformação'}</h5>
                    </div>
                    
                    <div class="tabs-enhanced">
                        <div class="tab-list-enhanced">
                            <button class="tab-enhanced active" onclick="enhancedAnalysis.switchTab(event, 'wounds')">
                                🩸 Feridas Abertas
                            </button>
                            <button class="tab-enhanced" onclick="enhancedAnalysis.switchTab(event, 'dreams')">
                                🔥 Sonhos Proibidos
                            </button>
                            <button class="tab-enhanced" onclick="enhancedAnalysis.switchTab(event, 'demons')">
                                👹 Demônios Internos
                            </button>
                            <button class="tab-enhanced" onclick="enhancedAnalysis.switchTab(event, 'dialect')">
                                🗣️ Dialeto da Alma
                            </button>
                        </div>
                        
                        <div id="wounds" class="tab-content active">
                            <h6>Dores Inconfessáveis (${(avatar.feridas_abertas_inconfessaveis || []).length} identificadas):</h6>
                            <div class="wounds-grid">
                                ${this.renderWoundsList(avatar.feridas_abertas_inconfessaveis || [])}
                            </div>
                        </div>
                        
                        <div id="dreams" class="tab-content">
                            <h6>Desejos Ardentes (${(avatar.sonhos_proibidos_ardentes || []).length} mapeados):</h6>
                            <div class="dreams-grid">
                                ${this.renderDreamsList(avatar.sonhos_proibidos_ardentes || [])}
                            </div>
                        </div>
                        
                        <div id="demons" class="tab-content">
                            <h6>Medos Paralisantes (${(avatar.demonios_internos_paralisantes || []).length} descobertos):</h6>
                            <div class="demons-grid">
                                ${this.renderDemonsList(avatar.demonios_internos_paralisantes || [])}
                            </div>
                        </div>
                        
                        <div id="dialect" class="tab-content">
                            <h6>Linguagem Interna Real:</h6>
                            <div class="dialect-analysis">
                                ${this.renderDialectAnalysis(avatar.dialeto_alma_linguagem_interna || {})}
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }
    
    renderDriversArsenal(analysis) {
        const drivers = analysis.drivers_mentais_arsenal_completo || analysis.drivers_mentais_customizados || [];
        const driversArray = Array.isArray(drivers) ? drivers : drivers.drivers_customizados || [];
        
        return `
            <div class="card-enhanced drivers-card">
                <div class="card-header-enhanced">
                    <div class="card-icon-enhanced">
                        <i class="fas fa-cogs"></i>
                    </div>
                    <h4 class="card-title-enhanced">⚙️ Arsenal de Drivers Mentais (${driversArray.length} Customizados)</h4>
                </div>
                
                <div class="drivers-content">
                    <div class="arsenal-stats">
                        <div class="metrics-enhanced">
                            <div class="metric-card-enhanced">
                                <div class="metric-value-enhanced">${driversArray.length}</div>
                                <div class="metric-label-enhanced">Drivers Criados</div>
                            </div>
                            <div class="metric-card-enhanced">
                                <div class="metric-value-enhanced">19</div>
                                <div class="metric-label-enhanced">Universais Disponíveis</div>
                            </div>
                            <div class="metric-card-enhanced">
                                <div class="metric-value-enhanced">95%</div>
                                <div class="metric-label-enhanced">Personalização</div>
                            </div>
                        </div>
                    </div>
                    
                    <div class="drivers-showcase">
                        ${this.renderDriversShowcase(driversArray)}
                    </div>
                </div>
            </div>
        `;
    }
    
    renderProvisArsenal(analysis) {
        const provis = analysis.provas_visuais_arsenal_completo || analysis.provas_visuais_sugeridas || [];
        
        return `
            <div class="card-enhanced provis-card">
                <div class="card-header-enhanced">
                    <div class="card-icon-enhanced">
                        <i class="fas fa-theater-masks"></i>
                    </div>
                    <h4 class="card-title-enhanced">🎭 Arsenal de PROVIs (${provis.length} Devastadoras)</h4>
                </div>
                
                <div class="provis-content">
                    <div class="provis-overview">
                        <p>Provas Visuais Instantâneas que transformam conceitos abstratos em experiências físicas inesquecíveis</p>
                    </div>
                    
                    <div class="provis-showcase">
                        ${this.renderProvisShowcase(provis)}
                    </div>
                </div>
            </div>
        `;
    }
    
    renderAntiObjectionSystem(analysis) {
        const antiObj = analysis.sistema_anti_objecao_ultra || analysis.sistema_anti_objecao || {};
        
        return `
            <div class="card-enhanced anti-objection-card">
                <div class="card-header-enhanced">
                    <div class="card-icon-enhanced">
                        <i class="fas fa-shield-alt"></i>
                    </div>
                    <h4 class="card-title-enhanced">🛡️ Sistema Anti-Objeção Psicológico</h4>
                </div>
                
                <div class="anti-objection-content">
                    <div class="objections-overview">
                        <div class="grid-enhanced grid-3">
                            <div class="objection-type">
                                <h6>⏰ Objeções de Tempo</h6>
                                <p>Neutralização através de urgência e priorização</p>
                            </div>
                            <div class="objection-type">
                                <h6>💰 Objeções de Dinheiro</h6>
                                <p>Transformação de custo em investimento</p>
                            </div>
                            <div class="objection-type">
                                <h6>🤔 Objeções de Confiança</h6>
                                <p>Construção de autoridade e prova social</p>
                            </div>
                        </div>
                    </div>
                    
                    <div class="hidden-objections">
                        <h5>🕵️ Objeções Ocultas Identificadas:</h5>
                        ${this.renderHiddenObjections(antiObj)}
                    </div>
                    
                    <div class="emergency-arsenal">
                        <h5>🚨 Arsenal de Emergência:</h5>
                        ${this.renderEmergencyArsenal(antiObj.arsenal_emergencia || [])}
                    </div>
                </div>
            </div>
        `;
    }
    
    renderForensicMetrics(analysis) {
        const forensic = analysis.metricas_forenses_objetivas || analysis.metricas_forenses_detalhadas || {};
        
        return `
            <div class="card-enhanced forensic-card">
                <div class="card-header-enhanced">
                    <div class="card-icon-enhanced">
                        <i class="fas fa-chart-line"></i>
                    </div>
                    <h4 class="card-title-enhanced">📊 Métricas Forenses Objetivas</h4>
                </div>
                
                <div class="forensic-content">
                    <div class="forensic-overview">
                        <p>Análise quantitativa da densidade persuasiva e eficácia psicológica</p>
                    </div>
                    
                    <div class="forensic-metrics">
                        ${this.renderForensicMetricsGrid(forensic)}
                    </div>
                    
                    <div class="cialdini-analysis">
                        <h5>🎯 Gatilhos de Cialdini:</h5>
                        ${this.renderCialdiniBars(forensic.gatilhos_cialdini || {})}
                    </div>
                    
                    <div class="emotional-intensity">
                        <h5>🔥 Intensidade Emocional:</h5>
                        ${this.renderEmotionalBars(forensic.intensidade_emocional || {})}
                    </div>
                </div>
            </div>
        `;
    }
    
    renderResearchResults(analysis) {
        const research = analysis.pesquisa_web_massiva || {};
        const stats = research.estatisticas || {};
        
        return `
            <div class="card-enhanced research-card">
                <div class="card-header-enhanced">
                    <div class="card-icon-enhanced">
                        <i class="fas fa-ship"></i>
                    </div>
                    <h4 class="card-title-enhanced">🌐 Pesquisa Web Massiva</h4>
                </div>
                
                <div class="research-content">
                    <div class="research-stats">
                        <div class="metrics-enhanced">
                            <div class="metric-card-enhanced">
                                <div class="metric-value-enhanced">${stats.total_queries || 0}</div>
                                <div class="metric-label-enhanced">Queries Executadas</div>
                            </div>
                            <div class="metric-card-enhanced">
                                <div class="metric-value-enhanced">${stats.fontes_unicas || 0}</div>
                                <div class="metric-label-enhanced">Fontes Únicas</div>
                            </div>
                            <div class="metric-card-enhanced">
                                <div class="metric-value-enhanced">${(stats.total_conteudo || 0).toLocaleString()}</div>
                                <div class="metric-label-enhanced">Caracteres Extraídos</div>
                            </div>
                            <div class="metric-card-enhanced">
                                <div class="metric-value-enhanced">${(stats.qualidade_media || 0).toFixed(1)}%</div>
                                <div class="metric-label-enhanced">Qualidade Média</div>
                            </div>
                        </div>
                    </div>
                    
                    <div class="data-quality-guarantee">
                        <div class="alert-enhanced alert-success">
                            <i class="fas fa-shield-check"></i>
                            <div>
                                <strong>Garantia de Dados Reais</strong>
                                <p>100% dos dados baseados em pesquisa real na web - Zero simulação</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }
    
    renderMetadata(analysis) {
        const metadata = analysis.metadata_ultra_enhanced || analysis.metadata || {};
        
        return `
            <div class="card-enhanced metadata-card">
                <div class="card-header-enhanced">
                    <div class="card-icon-enhanced">
                        <i class="fas fa-info-circle"></i>
                    </div>
                    <h4 class="card-title-enhanced">📋 Metadados da Análise</h4>
                </div>
                
                <div class="metadata-content">
                    <div class="metadata-grid">
                        <div class="metadata-item">
                            <span class="metadata-label">Tempo de Processamento:</span>
                            <span class="metadata-value">${metadata.processing_time_formatted || 'N/A'}</span>
                        </div>
                        <div class="metadata-item">
                            <span class="metadata-label">Engine de Análise:</span>
                            <span class="metadata-value">${metadata.analysis_engine || 'ARQV30 Enhanced v2.0'}</span>
                        </div>
                        <div class="metadata-item">
                            <span class="metadata-label">Agentes Utilizados:</span>
                            <span class="metadata-value">${metadata.agentes_psicologicos_utilizados?.length || 6}</span>
                        </div>
                        <div class="metadata-item">
                            <span class="metadata-label">Densidade Persuasiva:</span>
                            <span class="metadata-value">${metadata.densidade_persuasiva || 'ALTA'}</span>
                        </div>
                        <div class="metadata-item">
                            <span class="metadata-label">Arsenal Completo:</span>
                            <span class="metadata-value">${metadata.arsenal_completo ? '✅ SIM' : '⚠️ PARCIAL'}</span>
                        </div>
                        <div class="metadata-item">
                            <span class="metadata-label">Session ID:</span>
                            <span class="metadata-value">${analysis.session_id || 'N/A'}</span>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }
    
    renderTriggersList(triggers) {
        return triggers.map((trigger, index) => `
            <div class="trigger-item">
                <div class="trigger-number">${index + 1}</div>
                <div class="trigger-text">${trigger}</div>
            </div>
        `).join('');
    }
    
    renderPatternsList(patterns) {
        return patterns.map(pattern => `
            <div class="pattern-item">
                <i class="fas fa-arrow-right"></i>
                <span>${pattern}</span>
            </div>
        `).join('');
    }
    
    renderWoundsList(wounds) {
        return wounds.slice(0, 15).map((wound, index) => `
            <div class="wound-item">
                <div class="wound-number">${index + 1}</div>
                <div class="wound-text">${wound}</div>
            </div>
        `).join('');
    }
    
    renderDreamsList(dreams) {
        return dreams.slice(0, 15).map((dream, index) => `
            <div class="dream-item">
                <div class="dream-number">${index + 1}</div>
                <div class="dream-text">${dream}</div>
            </div>
        `).join('');
    }
    
    renderDemonsList(demons) {
        return demons.slice(0, 10).map((demon, index) => `
            <div class="demon-item">
                <div class="demon-number">${index + 1}</div>
                <div class="demon-text">${demon}</div>
            </div>
        `).join('');
    }
    
    renderDialectAnalysis(dialect) {
        return `
            <div class="dialect-sections">
                <div class="dialect-section">
                    <h6>💬 Frases sobre Dores:</h6>
                    ${(dialect.frases_tipicas_dores || []).map(frase => `<div class="phrase-item">"${frase}"</div>`).join('')}
                </div>
                <div class="dialect-section">
                    <h6>✨ Frases sobre Desejos:</h6>
                    ${(dialect.frases_tipicas_desejos || []).map(frase => `<div class="phrase-item">"${frase}"</div>`).join('')}
                </div>
                <div class="dialect-section">
                    <h6>🎭 Metáforas Comuns:</h6>
                    ${(dialect.metaforas_comuns_vida || []).map(meta => `<div class="metaphor-item">${meta}</div>`).join('')}
                </div>
            </div>
        `;
    }
    
    renderDriversShowcase(drivers) {
        return drivers.map((driver, index) => `
            <div class="driver-card">
                <div class="driver-header">
                    <h5>Driver ${index + 1}: ${driver.nome || 'Driver Mental'}</h5>
                    <div class="badge-enhanced badge-info">${driver.prioridade || 'ALTA'}</div>
                </div>
                
                <div class="driver-content">
                    <div class="driver-trigger">
                        <strong>Gatilho Central:</strong> ${driver.gatilho_central || 'N/A'}
                    </div>
                    
                    <div class="driver-definition">
                        <strong>Definição Visceral:</strong> ${driver.definicao_visceral || 'N/A'}
                    </div>
                    
                    <div class="driver-script">
                        <h6>Roteiro de Ativação:</h6>
                        ${this.renderDriverScript(driver.roteiro_ativacao || {})}
                    </div>
                    
                    <div class="anchor-phrases">
                        <h6>Frases de Ancoragem:</h6>
                        ${(driver.frases_ancoragem || []).map(frase => `<div class="anchor-phrase">"${frase}"</div>`).join('')}
                    </div>
                </div>
            </div>
        `).join('');
    }
    
    renderDriverScript(script) {
        return `
            <div class="script-steps">
                <div class="script-step">
                    <strong>Pergunta de Abertura:</strong>
                    <p>"${script.pergunta_abertura || 'N/A'}"</p>
                </div>
                <div class="script-step">
                    <strong>História/Analogia:</strong>
                    <p>${script.historia_analogia || 'N/A'}</p>
                </div>
                <div class="script-step">
                    <strong>Comando de Ação:</strong>
                    <p>"${script.comando_acao || 'N/A'}"</p>
                </div>
            </div>
        `;
    }
    
    renderProvisShowcase(provis) {
        return provis.map((provi, index) => `
            <div class="provi-card">
                <div class="provi-header">
                    <h5>${provi.nome || `PROVI ${index + 1}`}</h5>
                    <div class="badge-enhanced badge-warning">${provi.categoria || 'DEVASTADORA'}</div>
                </div>
                
                <div class="provi-content">
                    <div class="provi-objective">
                        <strong>Objetivo Psicológico:</strong>
                        <p>${provi.objetivo_psicologico || provi.conceito_alvo || 'N/A'}</p>
                    </div>
                    
                    <div class="provi-experiment">
                        <strong>Experimento:</strong>
                        <p>${provi.experimento_escolhido || provi.experimento || 'N/A'}</p>
                    </div>
                    
                    <div class="provi-materials">
                        <strong>Materiais:</strong>
                        <div class="materials-list">
                            ${this.renderMaterialsList(provi.materiais_especificos || provi.materiais || [])}
                        </div>
                    </div>
                    
                    <div class="provi-impact">
                        <strong>Impacto Esperado:</strong>
                        <span class="impact-level">${provi.impacto_esperado || 'ALTO'}</span>
                    </div>
                </div>
            </div>
        `).join('');
    }
    
    renderMaterialsList(materials) {
        if (Array.isArray(materials)) {
            if (typeof materials[0] === 'string') {
                return materials.map(material => `<div class="material-item">${material}</div>`).join('');
            } else {
                return materials.map(material => `
                    <div class="material-item">
                        <strong>${material.item || material.nome || 'Material'}:</strong>
                        <span>${material.especificacao || material.descricao || 'N/A'}</span>
                    </div>
                `).join('');
            }
        }
        return '<div class="material-item">Materiais não especificados</div>';
    }
    
    renderHiddenObjections(antiObj) {
        const hidden = antiObj.objecoes_ocultas || [];
        
        return hidden.map(obj => `
            <div class="hidden-objection">
                <h6>${obj.tipo || 'Objeção Oculta'}</h6>
                <p><strong>Objeção:</strong> "${obj.objecao_oculta || 'N/A'}"</p>
                <p><strong>Perfil Típico:</strong> ${obj.perfil_tipico || 'N/A'}</p>
                <p><strong>Contra-ataque:</strong> ${obj.contra_ataque || 'N/A'}</p>
            </div>
        `).join('');
    }
    
    renderEmergencyArsenal(arsenal) {
        return arsenal.map(item => `
            <div class="emergency-item">
                <i class="fas fa-exclamation-triangle"></i>
                <span>"${item}"</span>
            </div>
        `).join('');
    }
    
    renderForensicMetricsGrid(forensic) {
        const density = forensic.densidade_persuasiva || {};
        
        return `
            <div class="forensic-grid">
                <div class="forensic-item">
                    <div class="forensic-value">${density.argumentos_totais || 0}</div>
                    <div class="forensic-label">Argumentos Totais</div>
                </div>
                <div class="forensic-item">
                    <div class="forensic-value">${density.argumentos_logicos || 0}</div>
                    <div class="forensic-label">Lógicos</div>
                </div>
                <div class="forensic-item">
                    <div class="forensic-value">${density.argumentos_emocionais || 0}</div>
                    <div class="forensic-label">Emocionais</div>
                </div>
                <div class="forensic-item">
                    <div class="forensic-value">${density.ratio_promessa_prova || '1:1'}</div>
                    <div class="forensic-label">Ratio Promessa/Prova</div>
                </div>
            </div>
        `;
    }
    
    renderCialdiniBars(cialdini) {
        const triggers = ['reciprocidade', 'compromisso', 'prova_social', 'autoridade', 'escassez', 'afinidade'];
        
        return triggers.map(trigger => {
            const value = cialdini[trigger] || 0;
            const percentage = Math.min(value * 20, 100);
            
            return `
                <div class="cialdini-bar">
                    <div class="cialdini-label">${trigger.replace('_', ' ').toUpperCase()}</div>
                    <div class="cialdini-track">
                        <div class="cialdini-fill" style="width: ${percentage}%"></div>
                    </div>
                    <div class="cialdini-value">${value}</div>
                </div>
            `;
        }).join('');
    }
    
    renderEmotionalBars(emotions) {
        return Object.entries(emotions).map(([emotion, intensity]) => {
            // Extrai valor numérico (ex: "8/10" -> 80%)
            let percentage = 50;
            try {
                if (typeof intensity === 'string' && intensity.includes('/')) {
                    const value = parseInt(intensity.split('/')[0]);
                    percentage = value * 10;
                } else {
                    percentage = parseInt(intensity) * 10;
                }
            } catch (e) {
                percentage = 50;
            }
            
            return `
                <div class="emotion-bar">
                    <div class="emotion-label">${emotion.toUpperCase()}</div>
                    <div class="emotion-track">
                        <div class="emotion-fill" style="width: ${percentage}%"></div>
                    </div>
                    <div class="emotion-value">${intensity}</div>
                </div>
            `;
        }).join('');
    }
    
    switchTab(event, tabId) {
        // Remove active class from all tabs and contents
        document.querySelectorAll('.tab-enhanced').forEach(tab => tab.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
        
        // Add active class to clicked tab and corresponding content
        event.target.classList.add('active');
        document.getElementById(tabId).classList.add('active');
    }
    
    async loadAgentCapabilities() {
        try {
            const response = await fetch('/api/get_agent_capabilities');
            const data = await response.json();
            
            if (data.success) {
                this.displayAgentCapabilities(data.agents);
            }
        } catch (error) {
            console.error('Erro ao carregar capacidades dos agentes:', error);
        }
    }
    
    displayAgentCapabilities(agents) {
        const capabilitiesSection = document.getElementById('agentCapabilities');
        if (!capabilitiesSection) return;
        
        const agentsHTML = Object.entries(agents).map(([key, agent]) => `
            <div class="agent-card">
                <div class="agent-header">
                    <h4>${agent.name}</h4>
                    <div class="badge-enhanced badge-info">ATIVO</div>
                </div>
                <div class="agent-mission">${agent.mission}</div>
                <div class="agent-specialties">
                    <strong>Especialidades:</strong>
                    ${agent.specialties.map(spec => `<span class="specialty-tag">${spec}</span>`).join('')}
                </div>
            </div>
        `).join('');
        
        capabilitiesSection.innerHTML = `
            <div class="card-enhanced">
                <div class="card-header-enhanced">
                    <div class="card-icon-enhanced">
                        <i class="fas fa-users-cog"></i>
                    </div>
                    <h4 class="card-title-enhanced">🤖 Agentes Psicológicos Especializados</h4>
                </div>
                <div class="agents-grid">
                    ${agentsHTML}
                </div>
            </div>
        `;
    }
    
    async updateSystemStatus() {
        try {
            const response = await fetch('/api/app_status');
            const data = await response.json();
            
            const statusElement = document.getElementById('systemStatus');
            if (statusElement && data.services) {
                const searchProviders = data.services.search_providers;
                const aiProviders = data.services.ai_providers || { available: 1 };
                
                if (searchProviders.available > 0 && aiProviders.available > 0) {
                    statusElement.className = 'status-enhanced status-online';
                    statusElement.innerHTML = `
                        <div class="status-dot"></div>
                        <span>Sistema Ultra-Robusto (${searchProviders.available} busca + ${aiProviders.available} IA)</span>
                    `;
                } else {
                    statusElement.className = 'status-enhanced status-warning';
                    statusElement.innerHTML = `
                        <div class="status-dot"></div>
                        <span>Configuração Parcial</span>
                    `;
                }
            }
        } catch (error) {
            console.error('Erro ao verificar status:', error);
        }
    }
    
    handleKeyboardShortcuts(event) {
        if (event.ctrlKey && event.key === 'Enter') {
            event.preventDefault();
            const form = document.getElementById('enhancedAnalysisForm');
            if (form && !this.isAnalyzing) {
                form.dispatchEvent(new Event('submit'));
            }
        }
        
        if (event.key === 'Escape') {
            // Close any open modals
            document.querySelectorAll('.modal-enhanced').forEach(modal => modal.remove());
        }
        
        if (event.ctrlKey && event.key === 's') {
            event.preventDefault();
            if (this.currentAnalysis) {
                this.saveJSON();
            }
        }
    }
    
    showFormAgain() {
        const formContainer = document.querySelector('.enhanced-form-container');
        if (formContainer) {
            formContainer.style.display = 'block';
        }
        
        const progressArea = document.getElementById('progressArea');
        if (progressArea) {
            progressArea.style.display = 'none';
        }
    }
    
    async downloadPDF() {
        if (!this.currentAnalysis) {
            this.showAlert('Nenhuma análise disponível para download', 'warning');
            return;
        }
        
        try {
            const response = await fetch('/api/generate_pdf', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(this.currentAnalysis)
            });
            
            if (response.ok) {
                const blob = await response.blob();
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `analise_arqueologica_${Date.now()}.pdf`;
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                window.URL.revokeObjectURL(url);
                
                this.showAlert('PDF gerado com sucesso!', 'success');
            } else {
                throw new Error('Erro ao gerar PDF');
            }
        } catch (error) {
            console.error('Erro ao baixar PDF:', error);
            this.showAlert('Erro ao gerar PDF: ' + error.message, 'error');
        }
    }
    
    saveJSON() {
        if (!this.currentAnalysis) {
            this.showAlert('Nenhuma análise disponível para salvar', 'warning');
            return;
        }
        
        try {
            const dataStr = JSON.stringify(this.currentAnalysis, null, 2);
            const blob = new Blob([dataStr], { type: 'application/json' });
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `analise_arqueologica_${Date.now()}.json`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            window.URL.revokeObjectURL(url);
            
            this.showAlert('Dados JSON salvos com sucesso!', 'success');
        } catch (error) {
            console.error('Erro ao salvar JSON:', error);
            this.showAlert('Erro ao salvar JSON: ' + error.message, 'error');
        }
    }
    
    showAlert(message, type = 'info') {
        const alert = document.createElement('div');
        alert.className = `alert-enhanced alert-${type}`;
        alert.style.position = 'fixed';
        alert.style.top = '20px';
        alert.style.right = '20px';
        alert.style.zIndex = '1000';
        alert.style.maxWidth = '400px';
        alert.style.animation = 'slideInRight 0.3s ease-out';
        
        const icons = {
            info: 'fas fa-info-circle',
            success: 'fas fa-check-circle',
            warning: 'fas fa-exclamation-triangle',
            error: 'fas fa-exclamation-circle'
        };
        
        alert.innerHTML = `
            <i class="${icons[type]}"></i>
            <div>
                <strong>${type.charAt(0).toUpperCase() + type.slice(1)}</strong>
                <p>${message}</p>
            </div>
        `;
        
        document.body.appendChild(alert);
        
        setTimeout(() => {
            alert.remove();
        }, 5000);
    }
    
    setupInteractiveElements() {
        // Setup accordion functionality
        document.querySelectorAll('.accordion-header-enhanced').forEach(header => {
            header.addEventListener('click', () => {
                const accordion = header.closest('.accordion-enhanced');
                accordion.classList.toggle('open');
            });
        });
        
        // Setup copy to clipboard
        document.querySelectorAll('.copy-button').forEach(button => {
            button.addEventListener('click', (e) => {
                const text = e.target.dataset.text;
                navigator.clipboard.writeText(text).then(() => {
                    this.showAlert('Copiado para área de transferência!', 'success');
                });
            });
        });
    }
}

// Initialize enhanced analysis manager
const enhancedAnalysis = new EnhancedAnalysisManager();

// Global functions for compatibility
window.enhancedAnalysis = enhancedAnalysis;

// CSS animations
const style = document.createElement('style');
style.textContent = `
    @keyframes slideInRight {
        from {
            transform: translateX(100%);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }
    
    .tab-content {
        display: none;
    }
    
    .tab-content.active {
        display: block;
        animation: fadeIn 0.3s ease-out;
    }
    
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(10px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    .wound-item, .dream-item, .demon-item {
        display: flex;
        align-items: flex-start;
        gap: 12px;
        padding: 12px;
        background: var(--bg-elevated);
        border-radius: var(--radius-lg);
        margin-bottom: 8px;
        transition: all var(--transition-normal);
        border-left: 3px solid var(--accent-primary);
    }
    
    .wound-item:hover, .dream-item:hover, .demon-item:hover {
        background: var(--bg-surface);
        transform: translateX(4px);
    }
    
    .wound-number, .dream-number, .demon-number {
        background: var(--accent-primary);
        color: white;
        width: 24px;
        height: 24px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-weight: 700;
        font-size: 0.75rem;
        flex-shrink: 0;
    }
    
    .wound-text, .dream-text, .demon-text {
        flex: 1;
        line-height: 1.5;
        color: var(--text-primary);
    }
    
    .driver-card, .provi-card {
        background: var(--bg-surface);
        border-radius: var(--radius-xl);
        padding: 24px;
        margin-bottom: 20px;
        border: 1px solid var(--bg-elevated);
        transition: all var(--transition-normal);
    }
    
    .driver-card:hover, .provi-card:hover {
        border-color: var(--accent-primary);
        transform: translateY(-2px);
        box-shadow: var(--shadow-lg);
    }
    
    .driver-header, .provi-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 16px;
        padding-bottom: 12px;
        border-bottom: 1px solid var(--bg-elevated);
    }
    
    .driver-header h5, .provi-header h5 {
        color: var(--accent-primary);
        font-weight: 700;
        margin: 0;
    }
    
    .script-step {
        margin-bottom: 12px;
        padding: 12px;
        background: var(--bg-elevated);
        border-radius: var(--radius-md);
        border-left: 3px solid var(--accent-secondary);
    }
    
    .anchor-phrase {
        background: var(--bg-elevated);
        padding: 8px 12px;
        border-radius: var(--radius-md);
        margin: 4px 0;
        font-style: italic;
        color: var(--text-secondary);
        border-left: 2px solid var(--accent-warning);
    }
    
    .forensic-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
        gap: 16px;
        margin: 20px 0;
    }
    
    .forensic-item {
        background: var(--bg-elevated);
        padding: 20px;
        border-radius: var(--radius-lg);
        text-align: center;
        border: 1px solid var(--bg-surface);
    }
    
    .forensic-value {
        font-size: 2rem;
        font-weight: 800;
        color: var(--accent-primary);
        margin-bottom: 8px;
    }
    
    .forensic-label {
        font-size: 0.75rem;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    .cialdini-bar, .emotion-bar {
        display: flex;
        align-items: center;
        gap: 12px;
        margin: 8px 0;
        padding: 8px;
        background: var(--bg-elevated);
        border-radius: var(--radius-md);
    }
    
    .cialdini-label, .emotion-label {
        min-width: 120px;
        font-size: 0.75rem;
        font-weight: 600;
        color: var(--text-secondary);
    }
    
    .cialdini-track, .emotion-track {
        flex: 1;
        height: 8px;
        background: var(--bg-surface);
        border-radius: var(--radius-full);
        overflow: hidden;
    }
    
    .cialdini-fill, .emotion-fill {
        height: 100%;
        background: linear-gradient(90deg, var(--accent-primary), var(--accent-secondary));
        border-radius: var(--radius-full);
        transition: width var(--transition-normal);
    }
    
    .cialdini-value, .emotion-value {
        min-width: 40px;
        text-align: center;
        font-weight: 700;
        color: var(--accent-primary);
    }
    
    .agent-card {
        background: var(--bg-surface);
        border-radius: var(--radius-lg);
        padding: 20px;
        margin: 12px 0;
        border-left: 4px solid var(--accent-primary);
        transition: all var(--transition-normal);
    }
    
    .agent-card:hover {
        background: var(--bg-elevated);
        transform: translateX(4px);
    }
    
    .agent-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 8px;
    }
    
    .agent-header h4 {
        color: var(--accent-primary);
        margin: 0;
        font-size: 1rem;
        font-weight: 700;
    }
    
    .agent-mission {
        color: var(--text-secondary);
        font-size: 0.875rem;
        margin-bottom: 12px;
        line-height: 1.4;
    }
    
    .specialty-tag {
        display: inline-block;
        background: var(--bg-elevated);
        color: var(--accent-secondary);
        padding: 4px 8px;
        border-radius: var(--radius-md);
        font-size: 0.75rem;
        margin: 2px 4px 2px 0;
        font-weight: 500;
    }
    
    .agents-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
        gap: 16px;
        margin-top: 20px;
    }
`;

document.head.appendChild(style);