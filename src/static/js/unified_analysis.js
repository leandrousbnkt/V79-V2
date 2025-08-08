// ARQV30 Enhanced v2.0 - Unified Analysis System
console.log('🚀 Sistema de Análise Unificado carregado');

// Estado global da análise unificada
let currentUnifiedAnalysis = null;
let unifiedAnalysisInProgress = false;

// Configurações
const UNIFIED_CONFIG = {
    endpoints: {
        analyzeUnified: '/api/unified/analyze_unified',
        capabilities: '/api/unified/capabilities',
        systemStatus: '/api/unified/system_status',
        testExa: '/api/unified/test_exa',
        testPyMuPDF: '/api/unified/test_pymupdf',
        searchUnified: '/api/unified/search_unified',
        uploadUnified: '/api/unified/upload_unified',
        resetSystem: '/api/unified/reset_system',
        progress: '/api/progress'
    },
    polling: {
        interval: 2000,
        maxAttempts: 300
    }
};

// Sistema principal de análise unificada
async function startUnifiedAnalysis(formData = null) {
    if (unifiedAnalysisInProgress) {
        showAlert('Análise unificada já em andamento. Aguarde a conclusão.', 'warning');
        return;
    }
    
    try {
        // Coleta dados do formulário se não fornecido
        if (!formData) {
            formData = collectUnifiedFormData();
        }
        
        // Validação baseada no tipo de análise
        if (!validateUnifiedAnalysisData(formData)) {
            return;
        }
        
        unifiedAnalysisInProgress = true;
        
        // Mostra interface de progresso
        showUnifiedAnalysisProgress();
        
        // Inicia análise unificada
        const response = await fetch(UNIFIED_CONFIG.endpoints.analyzeUnified, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formData)
        });
        
        const result = await response.json();
        
        if (response.ok && result) {
            currentUnifiedAnalysis = result;
            displayUnifiedAnalysisResults(result);
            showAlert('Análise unificada concluída com sucesso!', 'success');
        } else {
            throw new Error(result.error || result.message || 'Erro na análise unificada');
        }
        
    } catch (error) {
        console.error('Erro na análise unificada:', error);
        showAlert(`Erro na análise unificada: ${error.message}`, 'error');
        displayUnifiedAnalysisError(error);
    } finally {
        unifiedAnalysisInProgress = false;
        hideUnifiedAnalysisProgress();
    }
}

function collectUnifiedFormData() {
    const form = document.getElementById('unifiedAnalysisForm');
    
    if (!form) {
        throw new Error('Formulário unificado não encontrado');
    }
    
    const formData = new FormData(form);
    const data = {};
    
    // Converte FormData para objeto
    for (const [key, value] of formData.entries()) {
        data[key] = value;
    }
    
    // Adiciona session_id
    data.session_id = getSessionId();
    
    // Adiciona timestamp
    data.timestamp = new Date().toISOString();
    
    return data;
}

function validateUnifiedAnalysisData(data) {
    const analysisType = data.analysis_type || 'complete';
    
    // Validação básica para todos os tipos
    if (!data.segmento || data.segmento.trim() === '') {
        showAlert('Segmento é obrigatório para qualquer tipo de análise', 'error');
        highlightField('segmento');
        return false;
    }
    
    // Validações específicas por tipo
    if (analysisType === 'forensic_cpl') {
        if (!data.transcription || data.transcription.trim().length < 500) {
            showAlert('Transcrição deve ter pelo menos 500 caracteres para análise forense', 'error');
            highlightField('transcription');
            return false;
        }
    }
    
    if (analysisType === 'visceral_leads') {
        if (!data.leads_data || data.leads_data.trim().length < 200) {
            showAlert('Dados de leads devem ter pelo menos 200 caracteres', 'error');
            highlightField('leads_data');
            return false;
        }
        
        if (!data.produto_servico || data.produto_servico.trim() === '') {
            showAlert('Produto/Serviço é obrigatório para engenharia reversa', 'error');
            highlightField('produto_servico');
            return false;
        }
        
        if (!data.principais_perguntas || data.principais_perguntas.trim() === '') {
            showAlert('Principais perguntas são obrigatórias', 'error');
            highlightField('principais_perguntas');
            return false;
        }
    }
    
    if (analysisType === 'pre_pitch') {
        if (!data.event_structure || data.event_structure.trim() === '') {
            showAlert('Estrutura do evento é obrigatória', 'error');
            highlightField('event_structure');
            return false;
        }
        
        if (!data.product_offer || data.product_offer.trim() === '') {
            showAlert('Detalhes do produto e oferta são obrigatórios', 'error');
            highlightField('product_offer');
            return false;
        }
    }
    
    return true;
}

function highlightField(fieldName) {
    const field = document.querySelector(`[name="${fieldName}"]`);
    if (field) {
        field.style.borderColor = '#ef4444';
        field.focus();
        
        // Remove destaque após 3 segundos
        setTimeout(() => {
            field.style.borderColor = '';
        }, 3000);
    }
}

function showUnifiedAnalysisProgress() {
    const progressContainer = document.createElement('div');
    progressContainer.id = 'unifiedAnalysisProgress';
    progressContainer.style.cssText = `
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: rgba(0,0,0,0.9);
        display: flex;
        align-items: center;
        justify-content: center;
        z-index: 10000;
        font-family: 'Inter', sans-serif;
    `;
    
    progressContainer.innerHTML = `
        <div style="background: var(--bg-elevated); padding: 40px; border-radius: 20px; text-align: center; min-width: 500px; box-shadow: 0 20px 40px rgba(0,0,0,0.5); border: 1px solid var(--bg-surface);">
            <div style="width: 60px; height: 60px; border: 4px solid var(--bg-surface); border-top: 4px solid var(--accent-primary); border-radius: 50%; animation: spin 1s linear infinite; margin: 0 auto 20px;"></div>
            <h3 style="margin-bottom: 10px; color: var(--text-primary); font-size: 1.5rem; font-weight: 700;">🚀 Análise Unificada em Progresso</h3>
            <div id="unifiedProgressMessage" style="color: var(--text-secondary); margin-bottom: 20px; font-size: 1rem;">Iniciando sistema unificado...</div>
            <div style="background: var(--bg-surface); height: 8px; border-radius: 4px; overflow: hidden; margin-bottom: 15px;">
                <div id="unifiedProgressBar" style="background: linear-gradient(90deg, var(--accent-primary), var(--accent-secondary)); height: 100%; width: 0%; transition: width 0.5s;"></div>
            </div>
            <div id="unifiedProgressPercent" style="font-size: 14px; color: var(--text-muted);">0%</div>
            <div style="margin-top: 20px; font-size: 12px; color: var(--text-muted);">
                🔍 Exa Neural Search • 📄 PyMuPDF Pro • 🧠 Agentes Psicológicos • 🛡️ Sistema Ultra-Robusto
            </div>
        </div>
        <style>
            @keyframes spin {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }
        </style>
    `;
    
    document.body.appendChild(progressContainer);
    
    // Inicia polling de progresso
    startUnifiedProgressPolling();
}

function hideUnifiedAnalysisProgress() {
    const progress = document.getElementById('unifiedAnalysisProgress');
    if (progress) {
        progress.remove();
    }
    stopUnifiedProgressPolling();
}

let unifiedProgressPollingInterval = null;

function startUnifiedProgressPolling() {
    if (unifiedProgressPollingInterval) return;
    
    unifiedProgressPollingInterval = setInterval(async () => {
        try {
            const sessionId = getSessionId();
            const response = await fetch(`${UNIFIED_CONFIG.endpoints.progress}/get_progress/${sessionId}`);
            
            if (response.ok) {
                const data = await response.json();
                updateUnifiedProgressDisplay(data);
            }
        } catch (error) {
            console.error('Erro no polling de progresso unificado:', error);
        }
    }, UNIFIED_CONFIG.polling.interval);
}

function stopUnifiedProgressPolling() {
    if (unifiedProgressPollingInterval) {
        clearInterval(unifiedProgressPollingInterval);
        unifiedProgressPollingInterval = null;
    }
}

function updateUnifiedProgressDisplay(progressData) {
    const messageEl = document.getElementById('unifiedProgressMessage');
    const barEl = document.getElementById('unifiedProgressBar');
    const percentEl = document.getElementById('unifiedProgressPercent');
    
    if (progressData && progressData.progress) {
        const { step, message, percentage } = progressData.progress;
        
        if (messageEl) messageEl.textContent = message || 'Processando...';
        if (barEl) barEl.style.width = `${percentage || 0}%`;
        if (percentEl) percentEl.textContent = `${Math.round(percentage || 0)}%`;
    }
}

function displayUnifiedAnalysisResults(results) {
    const resultsArea = document.getElementById('unifiedResultsArea');
    resultsArea.style.display = 'block';
    
    resultsArea.innerHTML = generateUnifiedResultsHTML(results);
    
    // Scroll suave para os resultados
    resultsArea.scrollIntoView({ 
        behavior: 'smooth',
        block: 'start'
    });
    
    // Adiciona funcionalidades interativas
    addUnifiedResultsInteractivity();
}

function generateUnifiedResultsHTML(results) {
    const analysisType = results.tipo_analise || 'unificada';
    
    return `
        <div class="results-enhanced">
            <div class="results-header-enhanced">
                <h3>
                    <i class="fas fa-rocket"></i>
                    Análise Unificada Concluída - ${analysisType.toUpperCase()}
                </h3>
                <div class="results-actions-enhanced">
                    <button class="btn-secondary-enhanced" onclick="downloadUnifiedPDF()">
                        <i class="fas fa-file-pdf"></i> Relatório Completo PDF
                    </button>
                    <button class="btn-secondary-enhanced" onclick="copyUnifiedResults()">
                        <i class="fas fa-copy"></i> Copiar Análise
                    </button>
                    <button class="btn-secondary-enhanced" onclick="shareUnifiedResults()">
                        <i class="fas fa-share"></i> Compartilhar
                    </button>
                </div>
            </div>
            
            <div class="results-content-enhanced">
                ${generateUnifiedResultsSections(results)}
            </div>
        </div>
    `;
}

function generateUnifiedResultsSections(results) {
    let html = '';
    
    // Metadados da análise
    if (results.metadata_final) {
        const metadata = results.metadata_final;
        html += `
            <div class="archaeological-section">
                <div class="section-header">
                    <div class="section-icon">
                        <i class="fas fa-info-circle"></i>
                    </div>
                    <div>
                        <h3 class="section-title">📊 Metadados da Análise Unificada</h3>
                        <p class="section-subtitle">Informações do processamento</p>
                    </div>
                </div>
                
                <div class="metadata-grid">
                    <div class="metadata-item">
                        <span class="metadata-label">Tipo de Análise</span>
                        <span class="metadata-value">${metadata.analysis_type || 'N/A'}</span>
                    </div>
                    <div class="metadata-item">
                        <span class="metadata-label">Tempo de Processamento</span>
                        <span class="metadata-value">${metadata.processing_time_formatted || 'N/A'}</span>
                    </div>
                    <div class="metadata-item">
                        <span class="metadata-label">Provedores Utilizados</span>
                        <span class="metadata-value">${metadata.providers_used || 0}</span>
                    </div>
                    <div class="metadata-item">
                        <span class="metadata-label">Total de Fontes</span>
                        <span class="metadata-value">${metadata.total_sources || 0}</span>
                    </div>
                    <div class="metadata-item">
                        <span class="metadata-label">Exa Enhanced</span>
                        <span class="metadata-value">${metadata.exa_enhanced ? '✅' : '❌'}</span>
                    </div>
                    <div class="metadata-item">
                        <span class="metadata-label">PyMuPDF Pro</span>
                        <span class="metadata-value">${metadata.pymupdf_pro ? '✅' : '❌'}</span>
                    </div>
                </div>
            </div>
        `;
    }
    
    // Pesquisa unificada
    if (results.pesquisa_unificada) {
        const search = results.pesquisa_unificada;
        html += `
            <div class="archaeological-section">
                <div class="section-header">
                    <div class="section-icon">
                        <i class="fas fa-search"></i>
                    </div>
                    <div>
                        <h3 class="section-title">🔍 Pesquisa Unificada</h3>
                        <p class="section-subtitle">Resultados de múltiplos provedores</p>
                    </div>
                </div>
                
                <div class="research-stats-archaeological">
                    <div class="research-stat-archaeological">
                        <div class="research-stat-value">${search.statistics?.total_results || 0}</div>
                        <div class="research-stat-label">Total de Resultados</div>
                    </div>
                    <div class="research-stat-archaeological">
                        <div class="research-stat-value">${search.statistics?.providers_used || 0}</div>
                        <div class="research-stat-label">Provedores Utilizados</div>
                    </div>
                    <div class="research-stat-archaeological">
                        <div class="research-stat-value">${search.statistics?.brazilian_sources || 0}</div>
                        <div class="research-stat-label">Fontes Brasileiras</div>
                    </div>
                    <div class="research-stat-archaeological">
                        <div class="research-stat-value">${search.statistics?.search_time?.toFixed(2) || 0}s</div>
                        <div class="research-stat-label">Tempo de Busca</div>
                    </div>
                </div>
                
                <div class="data-quality-archaeological">
                    <div class="quality-icon-archaeological">
                        <i class="fas fa-shield-check"></i>
                    </div>
                    <div class="quality-text-archaeological">
                        <div class="quality-title-archaeological">Garantia de Dados Reais</div>
                        <div class="quality-description-archaeological">
                            Pesquisa executada com Exa Neural Search + Google + Serper + extração robusta
                        </div>
                    </div>
                </div>
            </div>
        `;
    }
    
    // Avatar unificado
    if (results.avatar_unificado || results.analise_arqueologica?.avatar_arqueologico_ultra) {
        const avatar = results.avatar_unificado || results.analise_arqueologica?.avatar_arqueologico_ultra;
        html += generateAvatarSection(avatar);
    }
    
    // Arsenal de drivers mentais
    if (results.arsenal_drivers_mentais) {
        html += generateDriversSection(results.arsenal_drivers_mentais);
    }
    
    // Arsenal de PROVIs
    if (results.arsenal_provas_visuais) {
        html += generateProvisSection(results.arsenal_provas_visuais);
    }
    
    // Sistema anti-objeção
    if (results.sistema_anti_objecao) {
        html += generateAntiObjectionSection(results.sistema_anti_objecao);
    }
    
    // Análise forense específica
    if (results.analise_forense_cpl) {
        html += generateForensicCPLSection(results.analise_forense_cpl);
    }
    
    // Engenharia reversa específica
    if (results.engenharia_reversa_leads) {
        html += generateVisceralLeadsSection(results.engenharia_reversa_leads);
    }
    
    // Insights unificados
    if (results.insights_unificados || results.analise_ia?.insights_unificados) {
        const insights = results.insights_unificados || results.analise_ia?.insights_unificados;
        html += generateInsightsSection(insights);
    }
    
    return html;
}

function generateAvatarSection(avatar) {
    if (!avatar) return '';
    
    return `
        <div class="archaeological-section">
            <div class="section-header">
                <div class="section-icon">
                    <i class="fas fa-user-secret"></i>
                </div>
                <div>
                    <h3 class="section-title">👤 Avatar Unificado Ultra-Detalhado</h3>
                    <p class="section-subtitle">Perfil psicológico completo</p>
                </div>
            </div>
            
            <div class="visceral-avatar-archaeological">
                <div class="avatar-identity-archaeological">
                    <h4 class="avatar-name-archaeological">${avatar.nome_ficticio || 'Avatar Profissional'}</h4>
                    <p class="avatar-subtitle-archaeological">Perfil baseado em dados reais unificados</p>
                </div>
                
                <div class="visceral-tabs-archaeological">
                    <div class="tab-navigation-archaeological">
                        <button class="tab-button-archaeological active" onclick="switchAvatarTab('demografico')">
                            Demográfico
                        </button>
                        <button class="tab-button-archaeological" onclick="switchAvatarTab('psicografico')">
                            Psicográfico
                        </button>
                        <button class="tab-button-archaeological" onclick="switchAvatarTab('dores')">
                            Dores Viscerais
                        </button>
                        <button class="tab-button-archaeological" onclick="switchAvatarTab('desejos')">
                            Desejos Secretos
                        </button>
                    </div>
                    
                    <div class="tab-content-archaeological active" id="tab-demografico">
                        ${generateDemographicProfile(avatar.perfil_demografico_completo || avatar.perfil_demografico)}
                    </div>
                    
                    <div class="tab-content-archaeological" id="tab-psicografico">
                        ${generatePsychographicProfile(avatar.perfil_psicografico_profundo || avatar.perfil_psicografico)}
                    </div>
                    
                    <div class="tab-content-archaeological" id="tab-dores">
                        ${generatePainsList(avatar.dores_viscerais_unificadas || avatar.feridas_abertas_inconfessaveis || avatar.dores_viscerais)}
                    </div>
                    
                    <div class="tab-content-archaeological" id="tab-desejos">
                        ${generateDesiresList(avatar.desejos_secretos_unificados || avatar.sonhos_proibidos_ardentes || avatar.desejos_secretos)}
                    </div>
                </div>
            </div>
        </div>
    `;
}

function generateDemographicProfile(profile) {
    if (!profile) return '<p>Perfil demográfico não disponível</p>';
    
    return `
        <div class="demographic-grid">
            ${Object.entries(profile).map(([key, value]) => `
                <div class="profile-item">
                    <span class="profile-label">${key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}:</span>
                    <span class="profile-value">${value}</span>
                </div>
            `).join('')}
        </div>
    `;
}

function generatePsychographicProfile(profile) {
    if (!profile) return '<p>Perfil psicográfico não disponível</p>';
    
    return `
        <div class="psychographic-grid">
            ${Object.entries(profile).map(([key, value]) => `
                <div class="info-card">
                    <strong>${key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}:</strong>
                    <span>${value}</span>
                </div>
            `).join('')}
        </div>
    `;
}

function generatePainsList(pains) {
    if (!pains || !Array.isArray(pains)) return '<p>Dores não disponíveis</p>';
    
    return `
        <div class="psychological-list-archaeological">
            ${pains.map((pain, index) => `
                <div class="psychological-item-archaeological wound-item-archaeological">
                    <div class="psychological-number">${index + 1}</div>
                    <div class="psychological-text">${pain}</div>
                </div>
            `).join('')}
        </div>
    `;
}

function generateDesiresList(desires) {
    if (!desires || !Array.isArray(desires)) return '<p>Desejos não disponíveis</p>';
    
    return `
        <div class="psychological-list-archaeological">
            ${desires.map((desire, index) => `
                <div class="psychological-item-archaeological dream-item-archaeological">
                    <div class="psychological-number">${index + 1}</div>
                    <div class="psychological-text">${desire}</div>
                </div>
            `).join('')}
        </div>
    `;
}

function generateDriversSection(drivers) {
    if (!drivers) return '';
    
    const driversList = drivers.drivers_customizados || [];
    
    return `
        <div class="archaeological-section">
            <div class="section-header">
                <div class="section-icon">
                    <i class="fas fa-cogs"></i>
                </div>
                <div>
                    <h3 class="section-title">⚙️ Arsenal de Drivers Mentais</h3>
                    <p class="section-subtitle">${driversList.length} gatilhos psicológicos customizados</p>
                </div>
            </div>
            
            <div class="drivers-grid-archaeological">
                ${driversList.map((driver, index) => `
                    <div class="driver-card-archaeological">
                        <div class="driver-header-archaeological">
                            <h5 class="driver-name-archaeological">Driver ${index + 1}: ${driver.nome || 'Driver Mental'}</h5>
                            <div class="driver-priority-badge">ALTA</div>
                        </div>
                        
                        <div class="driver-content-archaeological">
                            <div class="driver-element">
                                <strong>Gatilho Central:</strong>
                                <span>${driver.gatilho_central || 'N/A'}</span>
                            </div>
                            
                            <div class="driver-element">
                                <strong>Definição Visceral:</strong>
                                <span>${driver.definicao_visceral || 'N/A'}</span>
                            </div>
                            
                            ${driver.roteiro_ativacao ? `
                                <div class="driver-script-archaeological">
                                    <h6>Roteiro de Ativação:</h6>
                                    <div class="script-step-archaeological">
                                        <strong>Pergunta:</strong> ${driver.roteiro_ativacao.pergunta_abertura || 'N/A'}
                                    </div>
                                    <div class="script-step-archaeological">
                                        <strong>História:</strong> ${driver.roteiro_ativacao.historia_analogia || 'N/A'}
                                    </div>
                                    <div class="script-step-archaeological">
                                        <strong>Comando:</strong> ${driver.roteiro_ativacao.comando_acao || 'N/A'}
                                    </div>
                                </div>
                            ` : ''}
                            
                            ${driver.frases_ancoragem ? `
                                <div class="anchor-phrases">
                                    <strong>Frases de Ancoragem:</strong>
                                    ${driver.frases_ancoragem.map(frase => `
                                        <div class="anchor-phrase-archaeological">${frase}</div>
                                    `).join('')}
                                </div>
                            ` : ''}
                        </div>
                    </div>
                `).join('')}
            </div>
        </div>
    `;
}

function generateInsightsSection(insights) {
    if (!insights || !Array.isArray(insights)) return '';
    
    return `
        <div class="archaeological-section">
            <div class="section-header">
                <div class="section-icon">
                    <i class="fas fa-lightbulb"></i>
                </div>
                <div>
                    <h3 class="section-title">💡 Insights Unificados</h3>
                    <p class="section-subtitle">${insights.length} insights exclusivos baseados em dados reais</p>
                </div>
            </div>
            
            <div class="insights-showcase">
                ${insights.map((insight, index) => `
                    <div class="insight-card">
                        <div class="insight-number">${index + 1}</div>
                        <div class="insight-content">${insight}</div>
                    </div>
                `).join('')}
            </div>
        </div>
    `;
}

// Funções de teste do sistema
async function testExa() {
    try {
        showLoading('Testando Exa Neural Search...');
        
        const response = await fetch(UNIFIED_CONFIG.endpoints.testExa, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                query: 'mercado digital Brasil 2024'
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert(`Exa funcionando! ${result.results?.results?.length || 0} resultados encontrados`, 'success');
            console.log('Resultado Exa:', result);
        } else {
            showAlert(`Erro no Exa: ${result.message}`, 'error');
        }
        
    } catch (error) {
        console.error('Erro no teste Exa:', error);
        showAlert(`Erro no teste Exa: ${error.message}`, 'error');
    } finally {
        hideLoading();
    }
}

async function testPyMuPDF() {
    try {
        showLoading('Testando PyMuPDF Pro...');
        
        const response = await fetch(UNIFIED_CONFIG.endpoints.testPyMuPDF, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                url: 'https://www.example.com/sample.pdf'
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('PyMuPDF Pro funcionando!', 'success');
            console.log('Resultado PyMuPDF:', result);
        } else {
            showAlert(`PyMuPDF: ${result.message}`, 'warning');
        }
        
    } catch (error) {
        console.error('Erro no teste PyMuPDF:', error);
        showAlert(`Erro no teste PyMuPDF: ${error.message}`, 'error');
    } finally {
        hideLoading();
    }
}

async function getSystemStatus() {
    try {
        showLoading('Verificando status do sistema...');
        
        const response = await fetch(UNIFIED_CONFIG.endpoints.systemStatus);
        const status = await response.json();
        
        console.log('Status do sistema unificado:', status);
        
        const statusMessage = `Sistema: ${status.status} | Busca: ${status.systems?.search_providers?.available_count || 0} | IA: ${status.systems?.ai_providers?.available_count || 0}`;
        showAlert(statusMessage, status.status === 'healthy' ? 'success' : 'warning');
        
    } catch (error) {
        console.error('Erro ao verificar status:', error);
        showAlert('Erro ao verificar status do sistema', 'error');
    } finally {
        hideLoading();
    }
}

async function testUnifiedSearch() {
    try {
        showLoading('Testando busca unificada...');
        
        const response = await fetch(UNIFIED_CONFIG.endpoints.searchUnified, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                query: 'mercado digital Brasil 2024',
                max_results: 10
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            const totalResults = result.search_results?.statistics?.total_results || 0;
            showAlert(`Busca unificada funcionando! ${totalResults} resultados encontrados`, 'success');
            console.log('Resultado busca unificada:', result);
        } else {
            showAlert(`Erro na busca: ${result.message}`, 'error');
        }
        
    } catch (error) {
        console.error('Erro no teste de busca:', error);
        showAlert(`Erro no teste de busca: ${error.message}`, 'error');
    } finally {
        hideLoading();
    }
}

async function resetSystem() {
    try {
        showLoading('Resetando sistema...');
        
        const response = await fetch(UNIFIED_CONFIG.endpoints.resetSystem, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                type: 'all'
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('Sistema resetado com sucesso!', 'success');
        } else {
            showAlert(`Erro no reset: ${result.message}`, 'error');
        }
        
    } catch (error) {
        console.error('Erro no reset:', error);
        showAlert(`Erro no reset: ${error.message}`, 'error');
    } finally {
        hideLoading();
    }
}

async function getCapabilities() {
    try {
        showLoading('Verificando capacidades...');
        
        const response = await fetch(UNIFIED_CONFIG.endpoints.capabilities);
        const result = await response.json();
        
        if (result.success) {
            console.log('Capacidades do sistema:', result.capabilities);
            showAlert('Capacidades verificadas! Veja o console para detalhes.', 'info');
        } else {
            showAlert('Erro ao verificar capacidades', 'error');
        }
        
    } catch (error) {
        console.error('Erro ao verificar capacidades:', error);
        showAlert(`Erro: ${error.message}`, 'error');
    } finally {
        hideLoading();
    }
}

// Funções de utilidade
function switchAvatarTab(tabName) {
    // Remove active de todos os botões e conteúdos
    document.querySelectorAll('.tab-button-archaeological').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.tab-content-archaeological').forEach(content => content.classList.remove('active'));
    
    // Ativa o selecionado
    document.querySelector(`[onclick="switchAvatarTab('${tabName}')"]`).classList.add('active');
    document.getElementById(`tab-${tabName}`).classList.add('active');
}

function addUnifiedResultsInteractivity() {
    // Adiciona funcionalidades interativas aos resultados
    console.log('Adicionando interatividade aos resultados unificados');
}

async function downloadUnifiedPDF() {
    if (!currentUnifiedAnalysis) {
        showAlert('Nenhuma análise disponível para download', 'warning');
        return;
    }
    
    try {
        showLoading('Gerando PDF unificado...');
        
        const response = await fetch('/api/generate_pdf', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(currentUnifiedAnalysis)
        });
        
        if (response.ok) {
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `analise_unificada_${Date.now()}.pdf`;
            a.click();
            window.URL.revokeObjectURL(url);
            
            showAlert('PDF unificado gerado com sucesso!', 'success');
        } else {
            throw new Error('Erro ao gerar PDF');
        }
    } catch (error) {
        console.error('Erro ao gerar PDF:', error);
        showAlert('Erro ao gerar PDF unificado', 'error');
    } finally {
        hideLoading();
    }
}

function copyUnifiedResults() {
    if (!currentUnifiedAnalysis) {
        showAlert('Nenhuma análise disponível para copiar', 'warning');
        return;
    }
    
    const text = JSON.stringify(currentUnifiedAnalysis, null, 2);
    copyToClipboard(text);
}

function shareUnifiedResults() {
    if (!currentUnifiedAnalysis) {
        showAlert('Nenhuma análise disponível para compartilhar', 'warning');
        return;
    }
    
    if (navigator.share) {
        navigator.share({
            title: 'Análise Unificada ARQV30',
            text: 'Confira esta análise unificada ultra-detalhada',
            url: window.location.href
        }).catch(console.log);
    } else {
        copyToClipboard(window.location.href);
        showAlert('Link copiado para área de transferência!', 'success');
    }
}

function displayUnifiedAnalysisError(error) {
    const resultsArea = document.getElementById('unifiedResultsArea');
    resultsArea.style.display = 'block';
    
    resultsArea.innerHTML = `
        <div class="archaeological-section">
            <div class="section-header">
                <div class="section-icon" style="color: var(--accent-error);">
                    <i class="fas fa-exclamation-triangle"></i>
                </div>
                <div>
                    <h3 class="section-title">❌ Erro na Análise Unificada</h3>
                    <p class="section-subtitle">Falha no processamento</p>
                </div>
            </div>
            
            <div class="error-content">
                <div class="alert-enhanced alert-error">
                    <i class="fas fa-exclamation-circle"></i>
                    <div>
                        <strong>Erro:</strong>
                        <p>${error.message}</p>
                    </div>
                </div>
                
                <div class="error-actions" style="text-align: center; margin-top: var(--space-6);">
                    <button class="btn-primary-enhanced" onclick="location.reload()">
                        <i class="fas fa-redo"></i> Tentar Novamente
                    </button>
                    <button class="btn-secondary-enhanced" onclick="getSystemStatus()">
                        <i class="fas fa-cog"></i> Verificar Sistema
                    </button>
                </div>
            </div>
        </div>
    `;
}

// Inicialização
document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 Interface Unificada carregada');
    
    // Configura formulário unificado
    const form = document.getElementById('unifiedAnalysisForm');
    if (form) {
        form.addEventListener('submit', async (e) => {
            e.preventDefault();
            await startUnifiedAnalysis();
        });
    }
    
    // Configuração de atalhos de teclado
    document.addEventListener('keydown', (e) => {
        if (e.ctrlKey && e.key === 'Enter') {
            e.preventDefault();
            if (!unifiedAnalysisInProgress) {
                startUnifiedAnalysis();
            }
        }
    });
});

// Exposição de funções globais
window.startUnifiedAnalysis = startUnifiedAnalysis;
window.testExa = testExa;
window.testPyMuPDF = testPyMuPDF;
window.getSystemStatus = getSystemStatus;
window.testUnifiedSearch = testUnifiedSearch;
window.resetSystem = resetSystem;
window.getCapabilities = getCapabilities;
window.downloadUnifiedPDF = downloadUnifiedPDF;
window.copyUnifiedResults = copyUnifiedResults;
window.shareUnifiedResults = shareUnifiedResults;
window.switchAvatarTab = switchAvatarTab;