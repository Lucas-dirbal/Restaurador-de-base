function formatDateTime(value) {
  if (!value) {
    return "-";
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  return parsed.toLocaleString("pt-BR");
}

function normalizeStatus(value) {
  return String(value || "").trim().toLowerCase();
}

function getStatusMeta(statusValue) {
  const status = normalizeStatus(statusValue);
  if (status === "executando") {
    return { label: "Executando", className: "status-running" };
  }
  if (status === "concluido") {
    return { label: "Concluido", className: "status-good" };
  }
  if (status === "erro") {
    return { label: "Erro", className: "status-bad" };
  }
  if (status === "interrompido") {
    return { label: "Interrompido", className: "status-warn" };
  }
  return { label: "Aguardando", className: "status-neutral" };
}

function setBadge(elementId, statusValue) {
  const element = document.getElementById(elementId);
  if (!element) {
    return;
  }

  const meta = getStatusMeta(statusValue);
  element.textContent = meta.label;
  element.className = `status-badge ${meta.className}`;
}

function sanitizeCodeInput() {
  const input = document.getElementById("client-code");
  if (!input) {
    return "";
  }

  const cleaned = String(input.value || "").replace(/\D/g, "");
  if (input.value !== cleaned) {
    input.value = cleaned;
  }
  return cleaned;
}

function encodeRelativePath(relativePath) {
  return String(relativePath || "")
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

let autoDownloadPending = false;
let lastAutoDownloadedPath = "";

function getReadyDatabase(artefatos = {}) {
  const readyFile = artefatos.fdb_restaurado;
  if (!readyFile || !readyFile.relative_path) {
    return null;
  }
  return readyFile;
}

function renderReadyDownload(artefatos = {}, statusExecucao = "") {
  const button = document.getElementById("ready-download-btn");
  const name = document.getElementById("ready-download-name");
  const filePath = document.getElementById("ready-download-path");
  const status = document.getElementById("ready-download-status");

  if (!button || !name || !filePath || !status) {
    return;
  }

  const readyDatabase = getReadyDatabase(artefatos);

  if (!readyDatabase) {
    button.setAttribute("href", "#");
    button.setAttribute("aria-disabled", "true");
    button.classList.add("disabled-link");
    name.textContent = "Nenhuma base restaurada ainda.";
    filePath.textContent =
      "Quando a restauracao concluir, a .FDB pronta aparece aqui para download direto.";
    status.textContent =
      normalizeStatus(statusExecucao) === "executando"
        ? "A restauracao ainda esta em andamento."
        : "O botao sera liberado automaticamente assim que o restore terminar.";
    return;
  }

  button.setAttribute("href", `/download/${encodeRelativePath(readyDatabase.relative_path)}`);
  button.setAttribute("aria-disabled", "false");
  button.classList.remove("disabled-link");
  name.textContent = readyDatabase.path
    ? readyDatabase.path.split(/[\\/]/).pop()
    : readyDatabase.titulo || "Base restaurada pronta";
  filePath.textContent = readyDatabase.relative_path;
  status.textContent = autoDownloadPending
    ? "A base sera baixada automaticamente assim que a restauracao concluir."
    : "A base ja esta pronta para download.";
}

function renderArtefatos(artefatos = {}) {
  const container = document.getElementById("files-list");
  if (!container) {
    return;
  }

  const readyDatabase = getReadyDatabase(artefatos);
  const cards = readyDatabase ? [readyDatabase] : [];

  if (!cards.length) {
    container.innerHTML = '<div class="list-placeholder">Somente a base restaurada fica disponivel para download.</div>';
    return;
  }

  container.innerHTML = cards
    .map((item) => {
      const tipo = "FDB";
      return `
        <article class="file-card">
          <div class="file-card-main">
            <span class="file-tag">${tipo}</span>
            <div class="file-card-main">
              <div class="artifact-title">Base restaurada pronta</div>
              <div class="artifact-path">${item.relative_path}</div>
            </div>
          </div>
        </article>
      `;
    })
    .join("");
}

function triggerAutomaticReadyDownload(readyDatabase) {
  if (!readyDatabase || !readyDatabase.relative_path) {
    return;
  }

  const downloadPath = readyDatabase.relative_path;
  if (lastAutoDownloadedPath === downloadPath) {
    return;
  }

  const anchor = document.createElement("a");
  anchor.href = `/download/${encodeRelativePath(downloadPath)}`;
  anchor.style.display = "none";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();

  lastAutoDownloadedPath = downloadPath;
}

function renderLogs(logs = []) {
  const container = document.getElementById("logs-list");
  if (!container) {
    return;
  }

  if (!Array.isArray(logs) || !logs.length) {
    container.innerHTML = '<div class="list-placeholder">Os eventos do processo vao aparecer aqui.</div>';
    return;
  }

  const latest = [...logs].reverse().slice(0, 12);
  container.innerHTML = latest
    .map((line, index) => {
      const text = String(line || "").trim();
      const match = text.match(/^\[(.*?)\]\s*(.*)$/);
      const time = match ? match[1] : `Log ${index + 1}`;
      const message = match ? match[2] : text;
      return `
        <article class="log-entry">
          <strong>${time}</strong>
          <span>${message || "-"}</span>
        </article>
      `;
    })
    .join("");
}

function renderStatus(data = {}) {
  const progresso = data.progresso || {};
  const percentual = Number(progresso.percentual || 0);
  const atual = Number(progresso.atual || 0);
  const total = Number(progresso.total || 0);
  const process = data.process || {};
  const backup = data.backup || {};
  const readyDatabase = getReadyDatabase(data.artefatos || {});
  const normalizedStatus = normalizeStatus(data.status_execucao);

  setBadge("status-badge", data.status_execucao);
  setBadge("status-badge-top", data.status_execucao);

  document.getElementById("global-progress-title").textContent =
    data.mensagem || "Aguardando um codigo de cliente";
  document.getElementById("global-progress-percent").textContent = `${percentual.toFixed(1)}%`;
  document.getElementById("global-progress-count").textContent = `${atual}/${total} etapas`;
  document.getElementById("global-progress-fill").style.width = `${Math.max(0, Math.min(percentual, 100))}%`;
  document.getElementById("global-progress-status").textContent =
    progresso.status || "Sem atividade.";

  document.getElementById("progress-percent").textContent = `${percentual.toFixed(1)}%`;
  document.getElementById("progress-count").textContent = `${atual}/${total} etapas`;
  document.getElementById("progress-fill").style.width = `${Math.max(0, Math.min(percentual, 100))}%`;
  document.getElementById("progress-status").textContent = progresso.status || "Sem atividade.";

  document.getElementById("mensagem").textContent = data.mensagem || "Aguardando primeira execucao";
  document.getElementById("iniciado-em").textContent = formatDateTime(data.iniciado_em);
  document.getElementById("atualizado-em").textContent = formatDateTime(data.atualizado_em);
  document.getElementById("finalizado-em").textContent = formatDateTime(data.finalizado_em);
  document.getElementById("process-pid").textContent = process.pid || "-";
  document.getElementById("process-runtime").textContent = process.running
    ? "Processo em andamento"
    : "Aguardando comando";

  document.getElementById("metric-codigo").textContent = data.codigo_cliente || "-";
  document.getElementById("metric-codigo-formatado").textContent = data.codigo_formatado || "-";
  document.getElementById("metric-backup").textContent = backup.nome || "-";
  document.getElementById("metric-backup-data").textContent = formatDateTime(backup.data_hora);
  document.getElementById("metric-mensagem").textContent = data.erro || data.mensagem || "-";

  renderReadyDownload(data.artefatos || {}, data.status_execucao);
  renderArtefatos(data.artefatos || {});
  renderLogs(data.logs || []);

  if (normalizedStatus === "concluido" && autoDownloadPending && readyDatabase) {
    triggerAutomaticReadyDownload(readyDatabase);
    autoDownloadPending = false;
    const readyStatus = document.getElementById("ready-download-status");
    if (readyStatus) {
      readyStatus.textContent = "Download automatico iniciado para a base restaurada.";
    }
  }

  if (normalizedStatus === "erro" || normalizedStatus === "interrompido") {
    autoDownloadPending = false;
  }
}

function renderMeta(data = {}) {
  const accessUrl = document.getElementById("access-url");
  if (accessUrl) {
    accessUrl.textContent = data.access_url || window.location.origin;
  }
}

async function carregarStatus() {
  try {
    const response = await fetch("/api/status", { cache: "no-store" });
    if (!response.ok) {
      throw new Error("Falha ao carregar status.");
    }
    const data = await response.json();
    renderStatus(data);
    return data;
  } catch (error) {
    console.error(error);
    return null;
  }
}

async function carregarMeta() {
  try {
    const response = await fetch("/api/meta", { cache: "no-store" });
    if (!response.ok) {
      throw new Error("Falha ao carregar metadados.");
    }
    const data = await response.json();
    renderMeta(data);
    return data;
  } catch (error) {
    console.error(error);
    return null;
  }
}

async function iniciarProcesso() {
  const code = sanitizeCodeInput();
  if (!code) {
    alert("Informe o codigo do cliente para continuar.");
    return;
  }

  const startButton = document.getElementById("start-process-btn");
  startButton.disabled = true;
  startButton.textContent = "Iniciando...";

  try {
    const response = await fetch("/api/start", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        client_code: code,
      }),
      cache: "no-store",
    });
    const data = await response.json();

    if (!response.ok || !data.ok) {
      throw new Error(data.message || "Nao foi possivel iniciar o processo.");
    }

    autoDownloadPending = true;
    lastAutoDownloadedPath = "";
    await carregarStatus();
    await carregarMeta();
  } catch (error) {
    autoDownloadPending = false;
    alert(error.message || "Erro ao iniciar o processo.");
  } finally {
    startButton.disabled = false;
    startButton.textContent = "Baixar e restaurar";
  }
}

async function pararProcesso() {
  if (!window.confirm("Deseja interromper o processo atual?")) {
    return;
  }

  const stopButton = document.getElementById("stop-process-btn");
  stopButton.disabled = true;
  stopButton.textContent = "Parando...";

  try {
    const response = await fetch("/api/stop", {
      method: "POST",
      cache: "no-store",
    });
    const data = await response.json();

    if (!response.ok || !data.ok) {
      throw new Error(data.message || "Nao foi possivel interromper o processo.");
    }

    autoDownloadPending = false;
    await carregarStatus();
    await carregarMeta();
  } catch (error) {
    alert(error.message || "Erro ao interromper o processo.");
  } finally {
    stopButton.disabled = false;
    stopButton.textContent = "Parar";
  }
}

async function limparRegistros() {
  if (!window.confirm("Deseja limpar os registros exibidos no painel?")) {
    return;
  }

  const clearButton = document.getElementById("clear-records-btn");
  clearButton.disabled = true;
  clearButton.textContent = "Limpando...";

  try {
    const response = await fetch("/api/clear", {
      method: "POST",
      cache: "no-store",
    });
    const data = await response.json();

    if (!response.ok || !data.ok) {
      throw new Error(data.message || "Nao foi possivel limpar os registros.");
    }

    autoDownloadPending = false;
    lastAutoDownloadedPath = "";
    await carregarStatus();
    await carregarMeta();
  } catch (error) {
    alert(error.message || "Erro ao limpar os registros.");
  } finally {
    clearButton.disabled = false;
    clearButton.textContent = "Limpar";
  }
}

document.getElementById("client-code").addEventListener("input", sanitizeCodeInput);
document.getElementById("client-code").addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    iniciarProcesso();
  }
});
document.getElementById("start-process-btn").addEventListener("click", iniciarProcesso);
document.getElementById("stop-process-btn").addEventListener("click", pararProcesso);
document.getElementById("clear-records-btn").addEventListener("click", limparRegistros);

carregarMeta();
carregarStatus();
setInterval(carregarMeta, 5000);
setInterval(carregarStatus, 5000);
