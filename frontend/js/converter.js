const dropZone = document.getElementById("dropZone");
const fileInput = document.getElementById("fileInput");
const fileList = document.getElementById("fileList");
const convertBtn = document.getElementById("convertBtn");
const progressBar = document.getElementById("progressBar");
const progressContainer = document.querySelector(".progress");
const downloadBtn = document.getElementById("downloadBtn");

let files = [];

dropZone.addEventListener("click", () => fileInput.click());
dropZone.addEventListener("dragover", e => {
  e.preventDefault();
  dropZone.classList.add("bg-dark");
});
dropZone.addEventListener("dragleave", () => dropZone.classList.remove("bg-dark"));
dropZone.addEventListener("drop", e => {
  e.preventDefault();
  addFiles([...e.dataTransfer.files]);
});

fileInput.addEventListener("change", e => addFiles([...e.target.files]));

function addFiles(newFiles) {
  newFiles = newFiles.filter(f => f.type === "application/pdf");
  files = files.concat(newFiles);
  renderFileList();
}

function renderFileList() {
  fileList.innerHTML = "";
  files.forEach((f, i) => {
    const li = document.createElement("li");
    li.className = "list-group-item bg-dark text-light";
    li.textContent = f.name;
    fileList.appendChild(li);
  });
  convertBtn.disabled = files.length === 0;
}

convertBtn.addEventListener("click", async () => {
  progressContainer.classList.remove("d-none");
  progressBar.style.width = "0%";
  progressBar.textContent = "0%";

  try {
    const zipBlob = await apiUpload(files);
    const url = window.URL.createObjectURL(zipBlob);
    downloadBtn.href = url;
    downloadBtn.download = "resultado.zip";
    downloadBtn.classList.remove("d-none");
    progressBar.style.width = "100%";
    progressBar.textContent = "100%";
  } catch (err) {
    alert("Error en la conversión: " + err.message);
  }
});

function logout() {
  localStorage.clear();
  window.location.href = "login.html";
}
