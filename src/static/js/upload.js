
// ARQV30 Enhanced v2.0 - Upload System
console.log('📁 Sistema de Upload carregado');

// Configurações de upload
const UPLOAD_CONFIG = {
    maxFileSize: 50 * 1024 * 1024, // 50MB
    allowedTypes: [
        'application/pdf',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'application/msword',
        'text/plain',
        'application/vnd.ms-excel',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'image/jpeg',
        'image/png',
        'image/gif'
    ],
    allowedExtensions: ['.pdf', '.docx', '.doc', '.txt', '.xls', '.xlsx', '.jpg', '.jpeg', '.png', '.gif']
};

// Sistema de upload de arquivos
function initializeUploadSystem() {
    const uploadAreas = document.querySelectorAll('.upload-area, .file-upload-area');
    
    uploadAreas.forEach(area => {
        // Drag and drop
        area.addEventListener('dragover', handleDragOver);
        area.addEventListener('dragleave', handleDragLeave);
        area.addEventListener('drop', handleDrop);
        
        // Click to upload
        area.addEventListener('click', () => {
            const input = area.querySelector('input[type="file"]') || createFileInput();
            input.click();
        });
    });
}

function handleDragOver(e) {
    e.preventDefault();
    e.stopPropagation();
    this.classList.add('drag-over');
}

function handleDragLeave(e) {
    e.preventDefault();
    e.stopPropagation();
    this.classList.remove('drag-over');
}

function handleDrop(e) {
    e.preventDefault();
    e.stopPropagation();
    this.classList.remove('drag-over');
    
    const files = Array.from(e.dataTransfer.files);
    handleFiles(files);
}

function createFileInput() {
    const input = document.createElement('input');
    input.type = 'file';
    input.multiple = true;
    input.style.display = 'none';
    input.accept = UPLOAD_CONFIG.allowedExtensions.join(',');
    
    input.addEventListener('change', (e) => {
        const files = Array.from(e.target.files);
        handleFiles(files);
    });
    
    document.body.appendChild(input);
    return input;
}

function handleFiles(files) {
    if (!files || files.length === 0) {
        return;
    }
    
    console.log(`📁 Processando ${files.length} arquivo(s)`);
    
    files.forEach(file => {
        if (validateFile(file)) {
            uploadFile(file);
        }
    });
}

function validateFile(file) {
    // Verifica tamanho
    if (file.size > UPLOAD_CONFIG.maxFileSize) {
        showAlert(`Arquivo "${file.name}" é muito grande. Máximo: 50MB`, 'error');
        return false;
    }
    
    // Verifica tipo
    const extension = '.' + file.name.split('.').pop().toLowerCase();
    if (!UPLOAD_CONFIG.allowedExtensions.includes(extension)) {
        showAlert(`Tipo de arquivo "${extension}" não permitido`, 'error');
        return false;
    }
    
    return true;
}

async function uploadFile(file) {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('session_id', getSessionId());
    
    try {
        showUploadProgress(file.name, 0);
        
        const response = await fetch('/api/upload_attachment', {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (result.success) {
            showUploadProgress(file.name, 100);
            showAlert(`Arquivo "${file.name}" enviado com sucesso!`, 'success');
            
            // Adiciona à lista de arquivos
            addFileToList(result);
        } else {
            throw new Error(result.error || 'Erro no upload');
        }
        
    } catch (error) {
        console.error('Erro no upload:', error);
        showAlert(`Erro ao enviar "${file.name}": ${error.message}`, 'error');
        removeUploadProgress(file.name);
    }
}

function showUploadProgress(fileName, progress) {
    let progressContainer = document.getElementById('uploadProgress');
    
    if (!progressContainer) {
        progressContainer = document.createElement('div');
        progressContainer.id = 'uploadProgress';
        progressContainer.style.cssText = `
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: white;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            padding: 20px;
            min-width: 300px;
            z-index: 9999;
        `;
        document.body.appendChild(progressContainer);
    }
    
    let fileProgress = document.getElementById(`progress-${fileName}`);
    
    if (!fileProgress) {
        fileProgress = document.createElement('div');
        fileProgress.id = `progress-${fileName}`;
        fileProgress.innerHTML = `
            <div style="margin-bottom: 10px;">
                <div style="font-size: 14px; margin-bottom: 5px;">${fileName}</div>
                <div style="background: #f0f0f0; height: 8px; border-radius: 4px; overflow: hidden;">
                    <div class="progress-bar" style="background: #2196F3; height: 100%; width: ${progress}%; transition: width 0.3s;"></div>
                </div>
                <div style="font-size: 12px; color: #666; margin-top: 2px;">${progress}%</div>
            </div>
        `;
        progressContainer.appendChild(fileProgress);
    } else {
        const progressBar = fileProgress.querySelector('.progress-bar');
        const progressText = fileProgress.querySelector('div:last-child');
        progressBar.style.width = `${progress}%`;
        progressText.textContent = `${progress}%`;
    }
    
    if (progress >= 100) {
        setTimeout(() => {
            removeUploadProgress(fileName);
        }, 2000);
    }
}

function removeUploadProgress(fileName) {
    const fileProgress = document.getElementById(`progress-${fileName}`);
    if (fileProgress) {
        fileProgress.remove();
    }
    
    const progressContainer = document.getElementById('uploadProgress');
    if (progressContainer && progressContainer.children.length === 0) {
        progressContainer.remove();
    }
}

function addFileToList(fileData) {
    const filesList = document.getElementById('uploadedFiles') || createFilesList();
    
    const fileItem = document.createElement('div');
    fileItem.className = 'file-item';
    fileItem.style.cssText = `
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 10px;
        border: 1px solid #e0e0e0;
        border-radius: 6px;
        margin-bottom: 8px;
        background: #f9f9f9;
    `;
    
    fileItem.innerHTML = `
        <div style="display: flex; align-items: center; gap: 10px;">
            <i class="fas fa-file" style="color: #2196F3;"></i>
            <div>
                <div style="font-weight: 500;">${fileData.file_name}</div>
                <div style="font-size: 12px; color: #666;">${formatFileSize(fileData.file_size)}</div>
            </div>
        </div>
        <button onclick="removeFile('${fileData.file_id}')" style="background: none; border: none; color: #f44336; cursor: pointer;">
            <i class="fas fa-trash"></i>
        </button>
    `;
    
    filesList.appendChild(fileItem);
}

function createFilesList() {
    const container = document.querySelector('.files-container') || document.body;
    
    const filesList = document.createElement('div');
    filesList.id = 'uploadedFiles';
    filesList.innerHTML = '<h4>Arquivos Enviados</h4>';
    
    container.appendChild(filesList);
    return filesList;
}

function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function getSessionId() {
    let sessionId = sessionStorage.getItem('arqv30_session_id');
    
    if (!sessionId) {
        sessionId = `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        sessionStorage.setItem('arqv30_session_id', sessionId);
    }
    
    return sessionId;
}

async function removeFile(fileId) {
    try {
        const response = await fetch(`/api/remove_attachment/${fileId}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (result.success) {
            const fileItem = document.querySelector(`[onclick="removeFile('${fileId}')"]`).parentNode;
            fileItem.remove();
            showAlert('Arquivo removido com sucesso!', 'success');
        } else {
            throw new Error(result.error || 'Erro ao remover arquivo');
        }
        
    } catch (error) {
        console.error('Erro ao remover arquivo:', error);
        showAlert(`Erro ao remover arquivo: ${error.message}`, 'error');
    }
}

// Inicialização
document.addEventListener('DOMContentLoaded', function() {
    console.log('📁 Inicializando sistema de upload');
    initializeUploadSystem();
});

// Exposição de funções globais
window.uploadFile = uploadFile;
window.removeFile = removeFile;
window.handleFiles = handleFiles;
