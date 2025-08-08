
// ARQV30 Enhanced v2.0 - Analysis System
console.log('🔬 Sistema de Análise carregado');

// Estado da análise
let currentAnalysis = null;
let analysisInProgress = false;

// Configurações
const ANALYSIS_CONFIG = {
    endpoints: {
        analyze: '/api/analyze',
        status: '/api/status',
        progress: '/api/progress'
    },
    polling: {
        interval: 2000,
        maxAttempts: 300
    }
};

// Sistema principal de análise
async function startAnalysis(formData = null) {
    if (analysisInProgress) {
        showAlert('Análise já em andamento. Aguarde a conclusão.', 'warning');
        return;
    }
    
    try {
        // Coleta dados do formulário se não fornecido
        if (!formData) {
            formData = collectFormData();
        }
        
        // Validação básica
        if (!validateAnalysisData(formData)) {
            return;
        }
        
        analysisInProgress = true;
        
        // Mostra interface de progresso
        showAnalysisProgress();
        
        // Inicia análise
        const response = await fetch(ANALYSIS_CONFIG.endpoints.analyze, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formData)
        });
        
        const result = await response.json();
        
        if (response.ok && result) {
            currentAnalysis = result;
            displayAnalysisResults(result);
            showAlert('Análise concluída com sucesso!', 'success');
        } else {
            throw new Error(result.error || result.message || 'Erro na análise');
        }
        
    } catch (error) {
        console.error('Erro na análise:', error);
        showAlert(`Erro na análise: ${error.message}`, 'error');
        displayAnalysisError(error);
    } finally {
        analysisInProgress = false;
        hideAnalysisProgress();
    }
}

function collectFormData() {
    const forms = [
        'enhancedAnalysisForm',
        'archaeologicalAnalysisForm',
        'analysisForm'
    ];
    
    let form = null;
    for (const formId of forms) {
        form = document.getElementById(formId);
        if (form) break;
    }
    
    if (!form) {
        throw new Error('Formulário de análise não encontrado');
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

function validateAnalysisData(data) {
    const required = ['segmento'];
    
    for (const field of required) {
        if (!data[field] || data[field].trim() === '') {
            showAlert(`Campo obrigatório: ${field}`, 'error');
            
            // Destaca campo no formulário
            const input = document.querySelector(`[name="${field}"]`);
            if (input) {
                input.style.borderColor = '#f44336';
                input.focus();
            }
            
            return false;
        }
    }
    
    return true;
}

function showAnalysisProgress() {
    const progressContainer = document.createElement('div');
    progressContainer.id = 'analysisProgress';
    progressContainer.style.cssText = `
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: rgba(0,0,0,0.8);
        display: flex;
        align-items: center;
        justify-content: center;
        z-index: 10000;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    `;
    
    progressContainer.innerHTML = `
        <div style="background: white; padding: 40px; border-radius: 12px; text-align: center; min-width: 400px; box-shadow: 0 8px 32px rgba(0,0,0,0.3);">
            <div style="width: 60px; height: 60px; border: 4px solid #f3f3f3; border-top: 4px solid #2196F3; border-radius: 50%; animation: spin 1s linear infinite; margin: 0 auto 20px;"></div>
            <h3 style="margin-bottom: 10px; color: #333;">🔬 Análise Arqueológica em Andamento</h3>
            <div id="progressMessage" style="color: #666; margin-bottom: 20px;">Iniciando análise ultra-detalhada...</div>
            <div style="background: #f0f0f0; height: 8px; border-radius: 4px; overflow: hidden; margin-bottom: 15px;">
                <div id="progressBar" style="background: #2196F3; height: 100%; width: 0%; transition: width 0.5s;"></div>
            </div>
            <div id="progressPercent" style="font-size: 14px; color: #888;">0%</div>
            <div style="margin-top: 20px; font-size: 12px; color: #999;">
                ⚡ Sistema ultra-robusto em operação<br>
                🛡️ Zero simulação - Dados reais
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
    startProgressPolling();
}

function hideAnalysisProgress() {
    const progress = document.getElementById('analysisProgress');
    if (progress) {
        progress.remove();
    }
    stopProgressPolling();
}

let progressPollingInterval = null;

function startProgressPolling() {
    if (progressPollingInterval) return;
    
    progressPollingInterval = setInterval(async () => {
        try {
            const sessionId = getSessionId();
            const response = await fetch(`${ANALYSIS_CONFIG.endpoints.progress}/${sessionId}`);
            
            if (response.ok) {
                const data = await response.json();
                updateProgressDisplay(data);
            }
        } catch (error) {
            console.error('Erro no polling de progresso:', error);
        }
    }, ANALYSIS_CONFIG.polling.interval);
}

function stopProgressPolling() {
    if (progressPollingInterval) {
        clearInterval(progressPollingInterval);
        progressPollingInterval = null;
    }
}

function updateProgressDisplay(progressData) {
    const messageEl = document.getElementById('progressMessage');
    const barEl = document.getElementById('progressBar');
    const percentEl = document.getElementById('progressPercent');
    
    if (progressData && progressData.progress) {
        const { step, message, percentage } = progressData.progress;
        
        if (messageEl) messageEl.textContent = message || 'Processando...';
        if (barEl) barEl.style.width = `${percentage || 0}%`;
        if (percentEl) percentEl.textContent = `${percentage || 0}%`;
    }
}

function displayAnalysisResults(results) {
    let resultsContainer = document.getElementById('resultsArea');
    
    if (!resultsContainer) {
        resultsContainer = document.createElement('div');
        resultsContainer.id = 'resultsArea';
        resultsContainer.style.cssText = `
            margin-top: 30px;
            padding: 20px;
            background: white;
            border-radius: 12px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        `;
        
        const container = document.querySelector('.container') || document.body;
        container.appendChild(resultsContainer);
    }
    
    resultsContainer.style.display = 'block';
    resultsContainer.innerHTML = generateResultsHTML(results);
    
    // Scroll suave para os resultados
    resultsContainer.scrollIntoView({ 
        behavior: 'smooth',
        block: 'start'
    });
    
    // Adiciona funcionalidades interativas
    addResultsInteractivity();
}

function generateResultsHTML(results) {
    return `
        <div class="analysis-results">
            <div class="results-header">
                <h2>🔬 Análise Arqueológica Concluída</h2>
                <div class="results-meta">
                    <span>📅 ${formatDate(new Date())}</span>
                    <span>⏱️ ${results.metadata?.processing_time_formatted || 'N/A'}</span>
                    <span>🎯 ${results.segmento || 'Análise Geral'}</span>
                </div>
            </div>
            
            ${generateResultsSections(results)}
            
            <div class="results-actions">
                <button onclick="downloadPDF()" class="btn btn-primary">
                    <i class="fas fa-file-pdf"></i> Baixar PDF
                </button>
                <button onclick="shareResults()" class="btn btn-secondary">
                    <i class="fas fa-share"></i> Compartilhar
                </button>
                <button onclick="saveAnalysis()" class="btn btn-success">
                    <i class="fas fa-save"></i> Salvar
                </button>
            </div>
        </div>
    `;
}

function generateResultsSections(results) {
    const sections = [];
    
    // Mapeamento de seções conhecidas
    const sectionMap = {
        avatar_detalhado: { title: '👤 Avatar Detalhado', icon: 'fas fa-user' },
        drivers_mentais: { title: '🧠 Drivers Mentais', icon: 'fas fa-brain' },
        provas_visuais: { title: '📊 Provas Visuais', icon: 'fas fa-chart-bar' },
        anti_objecao: { title: '🛡️ Anti-Objeção', icon: 'fas fa-shield-alt' },
        pre_pitch: { title: '🎯 Pré-Pitch', icon: 'fas fa-bullseye' },
        predicoes_futuro: { title: '🔮 Predições Futuro', icon: 'fas fa-crystal-ball' },
        posicionamento: { title: '📍 Posicionamento', icon: 'fas fa-map-marker-alt' },
        concorrencia: { title: '⚔️ Análise Concorrência', icon: 'fas fa-sword' },
        palavras_chave: { title: '🔑 Palavras-Chave', icon: 'fas fa-key' },
        metricas: { title: '📈 Métricas', icon: 'fas fa-chart-line' },
        funil_vendas: { title: '🔄 Funil de Vendas', icon: 'fas fa-funnel-dollar' },
        plano_acao: { title: '📋 Plano de Ação', icon: 'fas fa-tasks' },
        insights: { title: '💡 Insights', icon: 'fas fa-lightbulb' },
        pesquisa_web: { title: '🌐 Pesquisa Web', icon: 'fas fa-globe' }
    };
    
    // Gera seções baseadas nos dados retornados
    Object.keys(results).forEach(key => {
        if (sectionMap[key] && results[key]) {
            const section = sectionMap[key];
            sections.push(`
                <div class="result-section" data-section="${key}">
                    <div class="result-section-header">
                        <i class="${section.icon}"></i>
                        <h3>${section.title}</h3>
                        <button onclick="toggleSection('${key}')" class="toggle-btn">
                            <i class="fas fa-chevron-down"></i>
                        </button>
                    </div>
                    <div class="result-section-content" id="content-${key}">
                        ${formatSectionContent(results[key], key)}
                    </div>
                </div>
            `);
        }
    });
    
    return sections.join('');
}

function formatSectionContent(content, sectionType) {
    if (typeof content === 'string') {
        return `<div class="content-text">${content.replace(/\n/g, '<br>')}</div>`;
    }
    
    if (typeof content === 'object') {
        if (Array.isArray(content)) {
            return `<ul class="content-list">${content.map(item => `<li>${item}</li>`).join('')}</ul>`;
        } else {
            return Object.keys(content).map(key => `
                <div class="content-item">
                    <strong>${key}:</strong>
                    <div>${formatSectionContent(content[key], sectionType)}</div>
                </div>
            `).join('');
        }
    }
    
    return `<div class="content-text">${content}</div>`;
}

function addResultsInteractivity() {
    // Adiciona botões de cópia
    document.querySelectorAll('.result-section-content').forEach(section => {
        const copyBtn = document.createElement('button');
        copyBtn.innerHTML = '<i class="fas fa-copy"></i>';
        copyBtn.className = 'copy-btn';
        copyBtn.onclick = () => {
            copyToClipboard(section.textContent);
        };
        
        const header = section.previousElementSibling;
        if (header) {
            header.appendChild(copyBtn);
        }
    });
}

function toggleSection(sectionId) {
    const content = document.getElementById(`content-${sectionId}`);
    const btn = document.querySelector(`[onclick="toggleSection('${sectionId}')"] i`);
    
    if (content.style.display === 'none') {
        content.style.display = 'block';
        btn.className = 'fas fa-chevron-down';
    } else {
        content.style.display = 'none';
        btn.className = 'fas fa-chevron-right';
    }
}

function displayAnalysisError(error) {
    let resultsContainer = document.getElementById('resultsArea');
    
    if (!resultsContainer) {
        resultsContainer = document.createElement('div');
        resultsContainer.id = 'resultsArea';
        document.body.appendChild(resultsContainer);
    }
    
    resultsContainer.style.display = 'block';
    resultsContainer.innerHTML = `
        <div class="analysis-error">
            <div class="error-header">
                <h2>❌ Erro na Análise</h2>
            </div>
            <div class="error-content">
                <p><strong>Erro:</strong> ${error.message}</p>
                <div class="error-actions">
                    <button onclick="retryAnalysis()" class="btn btn-primary">
                        <i class="fas fa-redo"></i> Tentar Novamente
                    </button>
                    <button onclick="checkSystemStatus()" class="btn btn-secondary">
                        <i class="fas fa-cog"></i> Verificar Sistema
                    </button>
                </div>
            </div>
        </div>
    `;
}

async function retryAnalysis() {
    await startAnalysis();
}

async function checkSystemStatus() {
    try {
        const response = await fetch(ANALYSIS_CONFIG.endpoints.status);
        const status = await response.json();
        
        console.log('Status do sistema:', status);
        showAlert('Status verificado. Veja o console para detalhes.', 'info');
        
    } catch (error) {
        console.error('Erro ao verificar status:', error);
        showAlert('Erro ao verificar status do sistema', 'error');
    }
}

async function downloadPDF() {
    if (!currentAnalysis) {
        showAlert('Nenhuma análise disponível para download', 'warning');
        return;
    }
    
    try {
        const response = await fetch('/api/generate_pdf', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(currentAnalysis)
        });
        
        if (response.ok) {
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `analise_${Date.now()}.pdf`;
            a.click();
            window.URL.revokeObjectURL(url);
            
            showAlert('PDF gerado com sucesso!', 'success');
        } else {
            throw new Error('Erro ao gerar PDF');
        }
    } catch (error) {
        console.error('Erro ao gerar PDF:', error);
        showAlert('Erro ao gerar PDF', 'error');
    }
}

async function shareResults() {
    if (!currentAnalysis) {
        showAlert('Nenhuma análise disponível para compartilhar', 'warning');
        return;
    }
    
    if (navigator.share) {
        try {
            await navigator.share({
                title: 'Análise ARQV30',
                text: 'Confira esta análise detalhada de mercado',
                url: window.location.href
            });
        } catch (error) {
            console.log('Compartilhamento cancelado');
        }
    } else {
        copyToClipboard(window.location.href);
        showAlert('Link copiado para área de transferência!', 'success');
    }
}

async function saveAnalysis() {
    if (!currentAnalysis) {
        showAlert('Nenhuma análise disponível para salvar', 'warning');
        return;
    }
    
    try {
        const response = await fetch('/api/save_analysis', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(currentAnalysis)
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('Análise salva com sucesso!', 'success');
        } else {
            throw new Error(result.error || 'Erro ao salvar');
        }
    } catch (error) {
        console.error('Erro ao salvar análise:', error);
        showAlert('Erro ao salvar análise', 'error');
    }
}

// Inicialização
document.addEventListener('DOMContentLoaded', function() {
    console.log('🔬 Inicializando sistema de análise');
    
    // Configura formulários
    const forms = ['enhancedAnalysisForm', 'archaeologicalAnalysisForm', 'analysisForm'];
    
    forms.forEach(formId => {
        const form = document.getElementById(formId);
        if (form) {
            form.addEventListener('submit', async (e) => {
                e.preventDefault();
                await startAnalysis();
            });
        }
    });
    
    // Configuração de atalhos de teclado
    document.addEventListener('keydown', (e) => {
        if (e.ctrlKey && e.key === 'Enter') {
            e.preventDefault();
            if (!analysisInProgress) {
                startAnalysis();
            }
        }
    });
});

// Exposição de funções globais
window.startAnalysis = startAnalysis;
window.toggleSection = toggleSection;
window.downloadPDF = downloadPDF;
window.shareResults = shareResults;
window.saveAnalysis = saveAnalysis;
window.retryAnalysis = retryAnalysis;
window.checkSystemStatus = checkSystemStatus;
