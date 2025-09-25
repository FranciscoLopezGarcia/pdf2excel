const API_URL = "http://127.0.0.1:5000/api";

let selectedFiles = [];

// Elementos del DOM para PDFs
const dropZone = document.getElementById('dropZone');
const fileInput = document.getElementById('fileInput');
const fileList = document.getElementById('fileList');
const fileItems = document.getElementById('fileItems');
const excelList = document.getElementById('excelList');
const excelItems = document.getElementById('excelItems');
const convertBtn = document.getElementById('convertBtn');
const downloadBtn = document.getElementById('downloadBtn');
const progressContainer = document.getElementById('progressContainer');
const progressBar = document.getElementById('progressBar');
const progressText = document.getElementById('progressText');

// API Upload PDFs
function apiUpload(files, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    const formData = new FormData();
    files.forEach(f => formData.append("files", f));

    xhr.open("POST", `${API_URL}/convert`);
    xhr.setRequestHeader("Authorization", `Bearer ${localStorage.getItem("token")}`);
    xhr.responseType = "blob";

    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable && onProgress) {
        const percent = Math.round((e.loaded / e.total) * 100);
        onProgress(percent);
      }
    };

    xhr.onload = () => {
      if (xhr.status === 200) {
        resolve(xhr.response);
      } else {
        reject(new Error(`Error en conversión: ${xhr.responseText}`));
      }
    };

    xhr.onerror = () => reject(new Error("Error de red"));
    xhr.send(formData);
  });
}

function listenProgress() {
  const evtSource = new EventSource(`${API_URL}/progress`);
  evtSource.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      progressBar.style.width = data.progress + "%";
      progressText.textContent = data.progress + "% - " + data.status;
      if (data.progress >= 100) {
        evtSource.close();
      }
    } catch (e) {
      console.error("Error parseando SSE:", e, event.data);
    }
  };

  evtSource.onerror = (err) => {
    console.error("Error SSE:", err);
    evtSource.close();
  };
}

// Event listeners PDF
dropZone.addEventListener('click', () => fileInput.click());
dropZone.addEventListener('dragover', handleDragOver);
dropZone.addEventListener('dragleave', handleDragLeave);
dropZone.addEventListener('drop', handleDrop);
fileInput.addEventListener('change', handleFileSelect);
convertBtn.addEventListener('click', handleConvert);

function handleDragOver(e) {
  e.preventDefault();
  dropZone.classList.add('dragover');
}

function handleDragLeave(e) {
  e.preventDefault();
  dropZone.classList.remove('dragover');
}

function handleDrop(e) {
  e.preventDefault();
  dropZone.classList.remove('dragover');
  const files = Array.from(e.dataTransfer.files).filter(file => file.type === 'application/pdf');
  addFiles(files);
}

function handleFileSelect(e) {
  const files = Array.from(e.target.files);
  addFiles(files);
}

function addFiles(files) {
  files.forEach(file => {
    if (!selectedFiles.find(f => f.name === file.name && f.size === file.size)) {
      selectedFiles.push(file);
    }
  });
  updateFileList();
  updateConvertButton();
}

function removeFile(index) {
  selectedFiles.splice(index, 1);
  updateFileList();
  updateConvertButton();
}

function updateFileList() {
  if (selectedFiles.length === 0) {
    fileList.style.display = 'none';
    return;
  }

  fileList.style.display = 'block';
  fileItems.innerHTML = selectedFiles.map((file, index) => `
      <div class="file-item">
          <div class="file-info">
              <div class="file-icon">📄</div>
              <div class="file-details">
                  <div class="file-name">${file.name}</div>
                  <div class="file-size">${formatFileSize(file.size)}</div>
              </div>
          </div>
          <button class="file-remove" onclick="removeFile(${index})">
              Eliminar
          </button>
      </div>
  `).join('');
}

function updateConvertButton() {
  convertBtn.disabled = selectedFiles.length === 0;
}

function formatFileSize(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

async function handleConvert() {
  if (selectedFiles.length === 0) return;

  progressContainer.style.display = 'block';
  convertBtn.disabled = true;
  convertBtn.textContent = 'Convirtiendo...';
  downloadBtn.classList.add('hidden');

  try {
    console.log('Subiendo archivos al servidor...');
    listenProgress();

    const zipBlob = await apiUpload(selectedFiles, (percent) => {
      progressBar.style.width = percent + "%";
      progressText.textContent = percent + "%";
    });

    progressBar.style.width = "100%";
    progressText.textContent = "100%";

    const url = window.URL.createObjectURL(zipBlob);
    downloadBtn.href = url;
    downloadBtn.classList.remove('hidden');

    setTimeout(() => {
      progressContainer.style.display = 'none';
      convertBtn.style.display = 'none';
    }, 1500);

    showMessage('¡Conversión completada! Los archivos están listos para descargar.', 'success');

  } catch (error) {
    console.error('Error en conversión:', error);
    progressContainer.style.display = 'none';
    convertBtn.textContent = 'Convertir a Excel';
    convertBtn.disabled = false;
    showMessage(`Error en la conversión: ${error.message}`, 'error');
  }
}

function showMessage(text, type) {
  const message = document.createElement('div');
  message.className = `alert ${type === 'success' ? 'success' : ''}`;
  message.textContent = text;

  const controls = document.querySelector('.controls');
  controls.parentNode.insertBefore(message, controls);

  setTimeout(() => {
    message.remove();
  }, 5000);
}

function logout() {
  if (confirm('¿Estás seguro de que quieres cerrar sesión?')) {
    localStorage.removeItem('token');
    localStorage.removeItem('role');
    localStorage.removeItem('remember');
    window.location.href = 'login.html';
  }
}

window.onload = () => {
  const token = localStorage.getItem('token');
  if (!token) {
    window.location.href = 'login.html';
    return;
  }
};

// ==============================
// UNIFICADOR DE CONSOLIDADOS
// ==============================
let selectedExcels = [];


const dropZoneUnificar = document.getElementById('dropZoneUnificar');
const fileInputUnificar = document.getElementById('fileInputUnificar');
const unificarBtn = document.getElementById('unificarBtn');
const downloadBtnUnificar = document.getElementById('downloadBtnUnificar');

if (dropZoneUnificar && fileInputUnificar) {
  dropZoneUnificar.addEventListener('click', () => fileInputUnificar.click());

  dropZoneUnificar.addEventListener('dragover', e => {
    e.preventDefault();
    dropZoneUnificar.classList.add('dragover');
  });

  dropZoneUnificar.addEventListener('dragleave', () => {
    dropZoneUnificar.classList.remove('dragover');
  });

  dropZoneUnificar.addEventListener('drop', e => {
    e.preventDefault();
    dropZoneUnificar.classList.remove('dragover');
    const files = Array.from(e.dataTransfer.files)
      .filter(f => f.name.toLowerCase().endsWith('.xlsx') || f.name.toLowerCase().endsWith('.xls'));
    addExcels(files);
  });

  function addExcels(files) {
    files.forEach(file => {
      if (!selectedExcels.find(f => f.name === file.name && f.size === file.size)) {
        selectedExcels.push(file);
      }
    });
    updateExcelList();
    unificarBtn.disabled = selectedExcels.length === 0;
  }
  function updateExcelList() {
    if (selectedExcels.length === 0) {
      excelList.style.display = 'none';
    return;
  }
  excelList.style.display = 'block';
  excelItems.innerHTML = selectedExcels.map((file, index) => `
    <div class="file-item">
      <div class="file-info">
        <div class="file-icon">📊</div>
        <div class="file-details">
          <div class="file-name">${file.name}</div>
          <div class="file-size">${formatFileSize(file.size)}</div>
        </div>
      </div>
      <button class="file-remove" onclick="removeExcel(${index})">Eliminar</button>
    </div>
  `).join('');
}

function removeExcel(index) {
  selectedExcels.splice(index, 1);
  updateExcelList();
  unificarBtn.disabled = selectedExcels.length === 0;
}



  

  unificarBtn.addEventListener('click', async () => {
    if (selectedExcels.length === 0) return;
    try {
      const excelBlob = await apiUnificar(selectedExcels);
      const url = window.URL.createObjectURL(excelBlob);
      downloadBtnUnificar.href = url;
      downloadBtnUnificar.classList.remove('hidden');
    } catch (err) {
      alert("Error en unificación: " + err.message);
    }
  });
}
