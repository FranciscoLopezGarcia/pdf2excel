async function loadLogs() {
  const logs = await apiLogs();
  const table = document.getElementById("logTable");
  table.innerHTML = "";

  logs.forEach(log => {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${log.user}</td>
      <td>${log.date}</td>
      <td>${log.ok}</td>
      <td class="text-danger">${log.errors}</td>
      <td>${log.reason || ""}</td>
    `;
    table.appendChild(row);
  });
}

function logout() {
  localStorage.clear();
  window.location.href = "login.html";
}

loadLogs();
