const API_URL = "/api";


async function apiLogin(username, password) {
  const res = await fetch(`${API_URL}/login`, {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({username, password})
  });
  if (!res.ok) return null;
  return await res.json(); // { token, role }
}

async function apiUpload(files) {
  const formData = new FormData();
  files.forEach(f => formData.append("files", f));

  const res = await fetch(`${API_URL}/convert`, {
    method: "POST",
    headers: { "Authorization": `Bearer ${localStorage.getItem("token")}` },
    body: formData
  });

  if (!res.ok) throw new Error("Error en conversión");
  return await res.blob(); // ZIP
}

async function apiLogs() {
  const res = await fetch(`${API_URL}/logs`, {
    headers: { "Authorization": `Bearer ${localStorage.getItem("token")}` }
  });
  if (!res.ok) return [];
  return await res.json();
}

async function apiUnificar(files) {
  const formData = new FormData();
  files.forEach(f => formData.append("files", f));

  const res = await fetch(`${API_URL}/unificar`, {
    method: "POST",
    headers: { "Authorization": `Bearer ${localStorage.getItem("token")}` },
    body: formData
  });

  if (!res.ok) throw new Error("Error en unificación");
  return await res.blob(); // Excel
}
