document.getElementById("loginForm").addEventListener("submit", async e => {
  e.preventDefault();
  const username = document.getElementById("username").value;
  const password = document.getElementById("password").value;
  const remember = document.getElementById("remember").checked;

  const data = await apiLogin(username, password);
  if (!data) {
    document.getElementById("loginError").classList.remove("d-none");
    return;
  }

  if (remember) {
    localStorage.setItem("token", data.token);
    localStorage.setItem("role", data.role);
  }

  if (data.role === "admin") {
    window.location.href = "admin.html";
  } else {
    window.location.href = "converter.html";
  }
});
