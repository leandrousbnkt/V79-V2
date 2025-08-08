// ARQV30 Enhanced v2.0 - Forensic Analysis System
console.log('🔬 Sistema de Análise Forense carregado');

// Estado global da análise forense
let currentForensicAnalysis = null;
let forensicAnalysisInProgress = false;

// Configurações
const FORENSIC_CONFIG = {
    endpoints: {
        analyzeCPL: '/api/forensic/analyze_cpl_forensic',
        reverseEngineerLeads: '/api/forensic/reverse_engineer_leads',
        orchestratePrePitch: '/api/forensic/orchestrate_pre_pitch',
        getDrivers: '/api/forensic/get_available_drivers',
        getAvatars: '/api/forensic/get_available_avatars',
        getAvatarData: '/api/forensic/get_avatar_data',
        uploadCPL: '/api/forensic/upload_cpl_content',
        uploadLeads: '/api/forensic/upload_leads_data',
        generatePDF: '/api/forensic/generate_forensic_pdf',
        testSystem: '/api/forensic/test_forensic_system'
    }
};

// Sistema de análise forense de CPL
async function analyzeCPLForensic() {
    if (forensicAnalysisInProgress) {
        showAlert('Análise forense já em andamento. Aguarde a conclusão.', 'warning');
        return;
    }
    
    try {
        const formData = collectCPLFormData();
        
        if (!validateCPLData(formData)) {
            return;
        }
        
        forensicAnalysisInProgress = true;
        
        showForensicProgress('🔬 Arqueólogo Mestre analisando CPL...');
        
        const response = await fetch(FORENSIC_CONFIG.endpoints.analyzeCPL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formData)
        });
        
        const result = await response.json();
        
        if (response.ok && result) {
            currentForensicAnalysis = result;
            displayCPLForensicResults(result);
            showAlert('Análise forense de CPL concluída!', 'success');
        } else {
            throw new Error(result.error || result.message || 'Erro na análise forense');
        }
        
    } catch (error) {
        console.error('Erro na análise forense de CPL:', error);
        showAlert(`Erro na análise forense: ${error.message}`, 'error');
        displayForensicError(error, 'cpl');
    } finally {
        forensicAnalysisInProgress = false;
        hideForensicProgress();
    }
}

// Sistema de engenharia reversa de leads
async function reverseEngineerLeads() {
    if (forensicAnalysisInProgress) {
        showAlert('Engenharia reversa já em andamento. Aguarde a conclusão.', 'warning');
        return;
    }
    
    try {
        const formData = collectLeadsFormData();
        
        if (!validateLeadsData(formData)) {
            return;
        }
        
        forensicAnalysisInProgress = true;
        
        showForensicProgress('🧠 Mestre Visceral executando engenharia reversa...');
        
        const response = await fetch(FORENSIC_CONFIG.endpoints.reverseEngineerLeads, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formData)
        });
        
        const result = await response.json();
        
        if (response.ok && result) {
            currentForensicAnalysis = result;
            displayLeadsViscerealResults(result);
            showAlert('Engenharia reversa de leads concluída!', 'success');
        } else {
            throw new Error(result.error || result.message || 'Erro na engenharia reversa');
        }
        
    } catch (error) {
        console.error('Erro na engenharia reversa:', error);
        showAlert(`Erro na engenharia reversa: ${error.message}`, 'error');
        displayForensicError(error, 'leads');
    } finally {
        forensicAnalysisInProgress = false;
        hideForensicProgress();
    }
}

// Sistema de orquestração de pré-pitch
async function orchestratePrePitch() {
    if (forensicAnalysisInProgress) {
        showAlert('Orquestração já em andamento. Aguarde a conclusão.', 'warning');
        return;
    }
    
    try {
        const formData = collectPrePitchFormData();
        
        if (!validatePrePitchData(formData)) {
            return;
        }
        
        forensicAnalysisInProgress = true;
        
        showForensicProgress('🎯 Mestre do Pré-Pitch orquestrando sinfonia...');
        
        const response = await fetch(FORENSIC_CONFIG.endpoints.orchestratePrePitch, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formData)
        });
        
        const result = await response.json();
        
        if (response.ok && result) {
            currentForensicAnalysis = result;
            displayPrePitchResults(result);
            showAlert('Orquestração de pré-pitch concluída!', 'success');
        } else {
            throw new Error(result.error || result.message || 'Erro na orquestração');
        }
        
    } catch (error) {
        console.error('Erro na orquestração:', error);
        showAlert(`Erro na orquestração: ${error.message}`, 'error');
        displayForensicError(error, 'prepitch');
    } finally {
        forensicAnalysisInProgress = false;
        hideForensicProgress();
    }
}

// Coleta dados do formulário CPL
function collectCPLFormData() {
    const form = document.getElementById('cplForensicForm');
    const formData = new FormData(form);
    const data = {};
    
    for (const [key, value] of formData.entries()) {
        data[key] = value;
    }
    
    data.session_id = getSessionId();
    data.timestamp = new Date().toISOString();
    
    return data;
}

// Coleta dados do formulário de leads
function collectLeadsFormData() {
    const form = document.getElementById('leadsEngineeringForm');
    const formData = new FormData(form);
    const data = {};
    
    for (const [key, value] of formData.entries()) {
        data[key] = value;
    }
    
    data.session_id = getSessionId();
    data.timestamp = new Date().toISOString();
    
    return data;
}

// Coleta dados do formulário de pré-pitch
function collectPrePitchFormData() {
    const form = document.getElementById('prePitchForm');
    const formData = new FormData(form);
    const data = {};
    
    for (const [key, value] of formData.entries()) {
        data[key] = value;
    }
    
    // Coleta drivers selecionados
    const selectedDrivers = [];
    document.querySelectorAll('.driver-checkbox:checked').forEach(checkbox => {
        const driverCard = checkbox.closest('.driver-card-selection');
        selectedDrivers.push({
            id: checkbox.value,
            nome: driverCard.querySelector('.driver-name').textContent,
            categoria: driverCard.querySelector('.driver-category').textContent.split(' • ')[0],
            intensidade: driverCard.querySelector('.driver-category').textContent.split(' • ')[1]
        });
    });
    
    data.selected_drivers = selectedDrivers;
    data.session_id = getSessionId();
    data.timestamp = new Date().toISOString();
    
    return data;
}

// Validações
function validateCPLData(data) {
    if (!data.transcription || data.transcription.trim().length < 500) {
        showAlert('Transcrição deve ter pelo menos 500 caracteres para análise forense', 'error');
        highlightField('transcription');
        return false;
    }
    
    return true;
}

function validateLeadsData(data) {
    if (!data.leads_data || data.leads_data.trim().length < 200) {
        showAlert('Dados de leads devem ter pelo menos 200 caracteres', 'error');
        highlightField('leads_data');
        return false;
    }
    
    if (!data.produto_servico || data.produto_servico.trim() === '') {
        showAlert('Produto/Serviço é obrigatório', 'error');
        highlightField('produto_servico');
        return false;
    }
    
    if (!data.principais_perguntas || data.principais_perguntas.trim() === '') {
        showAlert('Principais perguntas da pesquisa são obrigatórias', 'error');
        highlightField('principais_perguntas');
        return false;
    }
    
    return true;
}

function validatePrePitchData(data) {
    if (!data.selected_drivers || data.selected_drivers.length === 0) {
        showAlert('Selecione pelo menos um driver mental', 'error');
        return false;
    }
    
    if (!data.avatar_id) {
        showAlert('Selecione um avatar', 'error');
        highlightField('avatar_id');
        return false;
    }
    
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

// Sistema de progresso forense
function showForensicProgress(message = 'Executando análise forense...') {
    const progressContainer = document.createElement('div');
    progressContainer.id = 'forensicProgress';
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
        <div style="background: var(--bg-elevated); padding: 40px; border-radius: 20px; text-align: center; min-width: 400px; box-shadow: 0 20px 40px rgba(0,0,0,0.5); border: 1px solid var(--bg-surface);">
            <div style="width: 60px; height: 60px; border: 4px solid var(--bg-surface); border-top: 4px solid var(--accent-primary); border-radius: 50%; animation: spin 1s linear infinite; margin: 0 auto 20px;"></div>
            <h3 style="margin-bottom: 10px; color: var(--text-primary); font-size: 1.5rem; font-weight: 700;">${message}</h3>
            <div style="color: var(--text-secondary); margin-bottom: 20px; font-size: 1rem;">Análise forense ultra-profunda em andamento...</div>
            <div style="margin-top: 20px; font-size: 12px; color: var(--text-muted);">
                🔬 Precisão cirúrgica • 🧠 Análise psicológica • 🎯 DNA da conversão
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
}

function hideForensicProgress() {
    const progress = document.getElementById('forensicProgress');
    if (progress) {
        progress.remove();
    }
}

// Display de resultados
function displayCPLForensicResults(results) {
    const resultsArea = document.getElementById('forensicResultsArea');
    resultsArea.style.display = 'block';
    
    resultsArea.innerHTML = `
        <div class="results-enhanced">
            <div class="results-header-enhanced">
                <h3>
                    <i class="fas fa-microscope"></i>
                    Análise Forense Completa - CPL
                </h3>
                <div class="results-actions-enhanced">
                    <button class="btn-secondary-enhanced" onclick="downloadForensicPDF('cpl')">
                        <i class="fas fa-file-pdf"></i> Relatório Forense PDF
                    </button>
                    <button class="btn-secondary-enhanced" onclick="copyForensicResults()">
                        <i class="fas fa-copy"></i> Copiar Análise
                    </button>
                </div>
            </div>
            
            <div class="results-content-enhanced">
                ${generateCPLForensicSections(results)}
            </div>
        </div>
    `;
    
    resultsArea.scrollIntoView({ behavior: 'smooth' });
}

function displayLeadsViscerealResults(results) {
    const resultsArea = document.getElementById('forensicResultsArea');
    resultsArea.style.display = 'block';
    
    resultsArea.innerHTML = `
        <div class="results-enhanced">
            <div class="results-header-enhanced">
                <h3>
                    <i class="fas fa-brain"></i>
                    Dossiê Confidencial - Engenharia Reversa
                </h3>
                <div class="results-actions-enhanced">
                    <button class="btn-secondary-enhanced" onclick="downloadForensicPDF('leads')">
                        <i class="fas fa-file-pdf"></i> Dossiê Confidencial PDF
                    </button>
                    <button class="btn-secondary-enhanced" onclick="copyForensicResults()">
                        <i class="fas fa-copy"></i> Copiar Dossiê
                    </button>
                </div>
            </div>
            
            <div class="results-content-enhanced">
                ${generateLeadsViscerealSections(results)}
            </div>
        </div>
    `;
    
    resultsArea.scrollIntoView({ behavior: 'smooth' });
}

function displayPrePitchResults(results) {
    const resultsArea = document.getElementById('forensicResultsArea');
    resultsArea.style.display = 'block';
    
    resultsArea.innerHTML = `
        <div class="results-enhanced">
            <div class="results-header-enhanced">
                <h3>
                    <i class="fas fa-theater-masks"></i>
                    Sinfonia Psicológica - Pré-Pitch Invisível
                </h3>
                <div class="results-actions-enhanced">
                    <button class="btn-secondary-enhanced" onclick="downloadForensicPDF('prepitch')">
                        <i class="fas fa-file-pdf"></i> Roteiro Completo PDF
                    </button>
                    <button class="btn-secondary-enhanced" onclick="copyForensicResults()">
                        <i class="fas fa-copy"></i> Copiar Roteiro
                    </button>
                </div>
            </div>
            
            <div class="results-content-enhanced">
                ${generatePrePitchSections(results)}
            </div>
        </div>
    `;
    
    resultsArea.scrollIntoView({ behavior: 'smooth' });
}

// Geradores de seções HTML
function generateCPLForensicSections(results) {
    let html = '';
    
    // DNA da Conversão
    if (results.dna_conversao_completo) {
        html += `
            <div class="archaeological-section">
                <div class="section-header">
                    <div class="section-icon">
                        <i class="fas fa-dna"></i>
                    </div>
                    <div>
                        <h3 class="section-title">🧬 DNA da Conversão Extraído</h3>
                        <p class="section-subtitle">Fórmula estrutural e sequência de gatilhos</p>
                    </div>
                </div>
                
                <div class="dna-analysis">
                    <div class="info-card">
                        <strong>Fórmula Estrutural:</strong>
                        <span>${results.dna_conversao_completo.formula_estrutural || 'N/A'}</span>
                    </div>
                    
                    <div class="info-card">
                        <strong>Sequência de Gatilhos:</strong>
                        <ul>
                            ${(results.dna_conversao_completo.sequencia_gatilhos || []).map(gatilho => `<li>${gatilho}</li>`).join('')}
                        </ul>
                    </div>
                    
                    <div class="info-card">
                        <strong>Padrões de Linguagem:</strong>
                        <ul>
                            ${(results.dna_conversao_completo.padroes_linguagem || []).map(padrao => `<li>${padrao}</li>`).join('')}
                        </ul>
                    </div>
                </div>
            </div>
        `;
    }
    
    // Métricas Forenses
    if (results.analise_linguistica_quantitativa) {
        const metrics = results.analise_linguistica_quantitativa;
        html += `
            <div class="archaeological-section">
                <div class="section-header">
                    <div class="section-icon">
                        <i class="fas fa-chart-bar"></i>
                    </div>
                    <div>
                        <h3 class="section-title">📊 Métricas Forenses Objetivas</h3>
                        <p class="section-subtitle">Análise linguística quantitativa</p>
                    </div>
                </div>
                
                <div class="forensic-metrics-archaeological">
                    <div class="forensic-grid-archaeological">
                        <div class="forensic-metric-archaeological">
                            <div class="forensic-value-archaeological">${metrics.ratio_eu_voce?.percentual_eu || 0}%</div>
                            <div class="forensic-label-archaeological">Foco em EU</div>
                        </div>
                        <div class="forensic-metric-archaeological">
                            <div class="forensic-value-archaeological">${metrics.ratio_eu_voce?.percentual_voce || 0}%</div>
                            <div class="forensic-label-archaeological">Foco em VOCÊ</div>
                        </div>
                        <div class="forensic-metric-archaeological">
                            <div class="forensic-value-archaeological">${metrics.promessas_vs_provas?.total_promessas || 0}</div>
                            <div class="forensic-label-archaeological">Promessas</div>
                        </div>
                        <div class="forensic-metric-archaeological">
                            <div class="forensic-value-archaeological">${metrics.promessas_vs_provas?.total_provas || 0}</div>
                            <div class="forensic-label-archaeological">Provas</div>
                        </div>
                    </div>
                    
                    <div class="cialdini-triggers-archaeological">
                        <h4 style="color: var(--archaeological-primary); margin-bottom: var(--space-4);">Gatilhos de Cialdini Identificados</h4>
                        ${Object.entries(metrics.gatilhos_cialdini || {}).map(([gatilho, count]) => `
                            <div class="cialdini-trigger-archaeological">
                                <div class="cialdini-name-archaeological">${gatilho}</div>
                                <div class="cialdini-bar-archaeological">
                                    <div class="cialdini-fill-archaeological" style="width: ${Math.min(count * 10, 100)}%"></div>
                                </div>
                                <div class="cialdini-value-archaeological">${count}</div>
                            </div>
                        `).join('')}
                    </div>
                </div>
            </div>
        `;
    }
    
    // Cronometragem Detalhada
    if (results.cronometragem_detalhada) {
        html += `
            <div class="archaeological-section">
                <div class="section-header">
                    <div class="section-icon">
                        <i class="fas fa-clock"></i>
                    </div>
                    <div>
                        <h3 class="section-title">🕐 Cronometragem Detalhada</h3>
                        <p class="section-subtitle">Análise temporal precisa</p>
                    </div>
                </div>
                
                <div class="timing-analysis">
                    ${Object.entries(results.cronometragem_detalhada).map(([fase, analise]) => `
                        <div class="info-card">
                            <strong>${fase.replace(/_/g, ' ').replace(/minuto/g, 'Minuto').replace(/XX/g, 'XX')}:</strong>
                            <span>${analise}</span>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }
    
    return html;
}

function generateLeadsViscerealSections(results) {
    let html = '';
    
    // Perfil Psicológico Profundo
    if (results.perfil_psicologico_profundo) {
        const perfil = results.perfil_psicologico_profundo;
        html += `
            <div class="archaeological-section">
                <div class="section-header">
                    <div class="section-icon">
                        <i class="fas fa-user-secret"></i>
                    </div>
                    <div>
                        <h3 class="section-title">🧠 Perfil Psicológico Profundo</h3>
                        <p class="section-subtitle">Arquétipo dominante identificado</p>
                    </div>
                </div>
                
                <div class="profile-analysis">
                    <div class="info-card">
                        <strong>Arquétipo Dominante:</strong>
                        <span>${perfil.nome_arquetipo_dominante || 'N/A'}</span>
                    </div>
                    <div class="info-card">
                        <strong>Nível de Consciência do Problema:</strong>
                        <span>${perfil.nivel_consciencia_problema || 'N/A'}</span>
                    </div>
                    <div class="info-card">
                        <strong>Resistências à Mudança:</strong>
                        <span>${perfil.resistencias_mudanca || 'N/A'}</span>
                    </div>
                </div>
            </div>
        `;
    }
    
    // Feridas Abertas
    if (results.feridas_abertas_secretas) {
        html += `
            <div class="archaeological-section">
                <div class="section-header">
                    <div class="section-icon" style="color: var(--visceral-wounds);">
                        <i class="fas fa-heart-broken"></i>
                    </div>
                    <div>
                        <h3 class="section-title">🩸 Feridas Abertas (Inconfessáveis)</h3>
                        <p class="section-subtitle">Dores secretas e viscerais</p>
                    </div>
                </div>
                
                <div class="psychological-list-archaeological">
                    ${results.feridas_abertas_secretas.map((dor, index) => `
                        <div class="psychological-item-archaeological wound-item-archaeological">
                            <div class="psychological-number">${index + 1}</div>
                            <div class="psychological-text">${dor}</div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }
    
    // Sonhos Proibidos
    if (results.sonhos_proibidos_ardentes) {
        html += `
            <div class="archaeological-section">
                <div class="section-header">
                    <div class="section-icon" style="color: var(--visceral-dreams);">
                        <i class="fas fa-fire"></i>
                    </div>
                    <div>
                        <h3 class="section-title">🔥 Sonhos Proibidos (Ardentes)</h3>
                        <p class="section-subtitle">Desejos secretos e ardentes</p>
                    </div>
                </div>
                
                <div class="psychological-list-archaeological">
                    ${results.sonhos_proibidos_ardentes.map((desejo, index) => `
                        <div class="psychological-item-archaeological dream-item-archaeological">
                            <div class="psychological-number">${index + 1}</div>
                            <div class="psychological-text">${desejo}</div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }
    
    // Dialeto da Alma
    if (results.dialeto_alma) {
        const dialeto = results.dialeto_alma;
        html += `
            <div class="archaeological-section">
                <div class="section-header">
                    <div class="section-icon" style="color: var(--visceral-dialect);">
                        <i class="fas fa-comments"></i>
                    </div>
                    <div>
                        <h3 class="section-title">🗣️ Dialeto da Alma</h3>
                        <p class="section-subtitle">Linguagem interna verdadeira</p>
                    </div>
                </div>
                
                <div class="dialect-analysis">
                    <div class="info-card">
                        <strong>Frases Típicas sobre Dores:</strong>
                        <ul>
                            ${(dialeto.frases_tipicas_dores || []).map(frase => `<li>"${frase}"</li>`).join('')}
                        </ul>
                    </div>
                    <div class="info-card">
                        <strong>Frases Típicas sobre Desejos:</strong>
                        <ul>
                            ${(dialeto.frases_tipicas_desejos || []).map(frase => `<li>"${frase}"</li>`).join('')}
                        </ul>
                    </div>
                    <div class="info-card">
                        <strong>Vocabulário Específico:</strong>
                        <span>${(dialeto.vocabulario_especifico || []).join(', ')}</span>
                    </div>
                </div>
            </div>
        `;
    }
    
    return html;
}

function generatePrePitchSections(results) {
    let html = '';
    
    // Orquestração Emocional
    if (results.orquestracao_emocional) {
        html += `
            <div class="archaeological-section">
                <div class="section-header">
                    <div class="section-icon">
                        <i class="fas fa-music"></i>
                    </div>
                    <div>
                        <h3 class="section-title">🎼 Orquestração Emocional</h3>
                        <p class="section-subtitle">Sinfonia de tensão psicológica</p>
                    </div>
                </div>
                
                <div class="orchestration-sequence">
                    ${(results.orquestracao_emocional.sequencia_psicologica || []).map((fase, index) => `
                        <div class="phase-card-archaeological" style="background: var(--bg-discovery); padding: var(--space-6); border-radius: var(--radius-xl); margin: var(--space-4) 0; border-left: 4px solid var(--archaeological-warning);">
                            <div class="phase-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: var(--space-3);">
                                <h5 style="color: var(--archaeological-warning); font-size: 1.25rem; font-weight: 700;">Fase ${index + 1}: ${fase.fase}</h5>
                                <span style="background: var(--bg-analysis); color: var(--text-secondary); padding: var(--space-1) var(--space-3); border-radius: var(--radius-full); font-size: 0.75rem;">${fase.duracao}</span>
                            </div>
                            <div class="phase-content">
                                <p><strong>Objetivo:</strong> ${fase.objetivo}</p>
                                <p><strong>Intensidade:</strong> ${fase.intensidade}</p>
                                <p><strong>Resultado Esperado:</strong> ${fase.resultado_esperado}</p>
                                ${fase.drivers_utilizados ? `<p><strong>Drivers:</strong> ${fase.drivers_utilizados.join(', ')}</p>` : ''}
                                ${fase.narrativa ? `<div class="narrative-script" style="background: var(--bg-excavation); padding: var(--space-4); border-radius: var(--radius-lg); margin-top: var(--space-3); font-style: italic;"><strong>Script:</strong> ${fase.narrativa}</div>` : ''}
                            </div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }
    
    // Roteiro Completo
    if (results.roteiro_completo) {
        const roteiro = results.roteiro_completo;
        html += `
            <div class="archaeological-section">
                <div class="section-header">
                    <div class="section-icon">
                        <i class="fas fa-scroll"></i>
                    </div>
                    <div>
                        <h3 class="section-title">📜 Roteiro Completo</h3>
                        <p class="section-subtitle">Scripts detalhados para execução</p>
                    </div>
                </div>
                
                <div class="script-sections">
                    ${Object.entries(roteiro).map(([secao, dados]) => `
                        <div class="script-section" style="background: var(--bg-discovery); padding: var(--space-5); border-radius: var(--radius-lg); margin: var(--space-3) 0;">
                            <h5 style="color: var(--archaeological-primary); margin-bottom: var(--space-3); text-transform: capitalize;">${secao.replace('_', ' ')}</h5>
                            ${typeof dados === 'object' ? Object.entries(dados).map(([key, value]) => `
                                <div class="script-detail" style="margin-bottom: var(--space-2);">
                                    <strong style="color: var(--text-secondary);">${key.replace('_', ' ')}:</strong>
                                    <span style="color: var(--text-primary);">${value}</span>
                                </div>
                            `).join('') : `<p>${dados}</p>`}
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }
    
    return html;
}

// Funções de utilidade
async function loadAvailableDrivers() {
    try {
        const response = await fetch(FORENSIC_CONFIG.endpoints.getDrivers);
        const result = await response.json();
        
        if (result.success) {
            const driversContainer = document.getElementById('driversSelection');
            driversContainer.innerHTML = result.drivers.map(driver => `
                <div class="driver-card-selection" style="background: var(--bg-surface); padding: var(--space-4); border-radius: var(--radius-lg); border: 1px solid var(--bg-elevated); margin: var(--space-2) 0; transition: all 0.3s ease;">
                    <label style="display: flex; align-items: center; gap: var(--space-3); cursor: pointer;">
                        <input type="checkbox" class="driver-checkbox" value="${driver.id}" style="margin: 0;">
                        <div style="flex: 1;">
                            <div class="driver-name" style="font-weight: 600; color: var(--text-primary);">${driver.nome}</div>
                            <div class="driver-category" style="font-size: 0.875rem; color: var(--text-muted);">${driver.categoria} • ${driver.intensidade}</div>
                            <div style="font-size: 0.75rem; color: var(--text-secondary); margin-top: var(--space-1);">${driver.descricao}</div>
                        </div>
                    </label>
                </div>
            `).join('');
        }
    } catch (error) {
        console.error('Erro ao carregar drivers:', error);
        showAlert('Erro ao carregar drivers mentais', 'error');
    }
}

async function loadAvailableAvatars() {
    try {
        const response = await fetch(FORENSIC_CONFIG.endpoints.getAvatars);
        const result = await response.json();
        
        if (result.success) {
            const avatarSelect = document.getElementById('avatarSelection');
            avatarSelect.innerHTML = '<option value="">Selecione um avatar...</option>' +
                result.avatars.map(avatar => `
                    <option value="${avatar.id}">${avatar.nome} (${avatar.segmento})</option>
                `).join('');
        }
    } catch (error) {
        console.error('Erro ao carregar avatares:', error);
        showAlert('Erro ao carregar avatares', 'error');
    }
}

function uploadCPLFile() {
    document.getElementById('cplFileInput').click();
}

function uploadLeadsFile() {
    document.getElementById('leadsFileInput').click();
}

async function downloadForensicPDF(type) {
    if (!currentForensicAnalysis) {
        showAlert('Nenhuma análise disponível para download', 'warning');
        return;
    }
    
    try {
        showLoading('Gerando PDF forense...');
        
        const response = await fetch(FORENSIC_CONFIG.endpoints.generatePDF, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                analysis_type: type,
                analysis_data: currentForensicAnalysis
            })
        });
        
        if (response.ok) {
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `analise_forense_${type}_${Date.now()}.pdf`;
            a.click();
            window.URL.revokeObjectURL(url);
            
            showAlert('PDF forense gerado com sucesso!', 'success');
        } else {
            throw new Error('Erro ao gerar PDF');
        }
    } catch (error) {
        console.error('Erro ao gerar PDF:', error);
        showAlert('Erro ao gerar PDF forense', 'error');
    } finally {
        hideLoading();
    }
}

function copyForensicResults() {
    if (!currentForensicAnalysis) {
        showAlert('Nenhuma análise disponível para copiar', 'warning');
        return;
    }
    
    const text = JSON.stringify(currentForensicAnalysis, null, 2);
    copyToClipboard(text);
}

function displayForensicError(error, type) {
    const resultsArea = document.getElementById('forensicResultsArea');
    resultsArea.style.display = 'block';
    
    resultsArea.innerHTML = `
        <div class="archaeological-section">
            <div class="section-header">
                <div class="section-icon" style="color: var(--accent-error);">
                    <i class="fas fa-exclamation-triangle"></i>
                </div>
                <div>
                    <h3 class="section-title">❌ Erro na Análise Forense</h3>
                    <p class="section-subtitle">Falha no processamento ${type}</p>
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
                    <button class="btn-secondary-enhanced" onclick="testForensicSystem('${type}')">
                        <i class="fas fa-cog"></i> Testar Sistema
                    </button>
                </div>
            </div>
        </div>
    `;
}

async function testForensicSystem(type) {
    try {
        showLoading('Testando sistema forense...');
        
        const response = await fetch(FORENSIC_CONFIG.endpoints.testSystem, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ test_type: type })
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert(`Teste de ${type} executado com sucesso!`, 'success');
            console.log('Resultado do teste:', result);
        } else {
            throw new Error(result.message || 'Erro no teste');
        }
        
    } catch (error) {
        console.error('Erro no teste:', error);
        showAlert(`Erro no teste: ${error.message}`, 'error');
    } finally {
        hideLoading();
    }
}

// Exposição de funções globais
window.analyzeCPLForensic = analyzeCPLForensic;
window.reverseEngineerLeads = reverseEngineerLeads;
window.orchestratePrePitch = orchestratePrePitch;
window.loadAvailableDrivers = loadAvailableDrivers;
window.loadAvailableAvatars = loadAvailableAvatars;
window.uploadCPLFile = uploadCPLFile;
window.uploadLeadsFile = uploadLeadsFile;
window.downloadForensicPDF = downloadForensicPDF;
window.copyForensicResults = copyForensicResults;
window.testForensicSystem = testForensicSystem;
window.switchForensicTab = switchForensicTab;