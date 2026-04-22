const http = require("http");
const { spawn } = require("child_process");
const dns = require("dns");
const fs = require("fs");
const os = require("os");
const path = require("path");

const BASE_DIR = __dirname;
const PROCESS_DIR = path.join(BASE_DIR, "processo");
const STATUS_FILE = path.join(PROCESS_DIR, "monitor_data", "status.json");
const SCRIPT_PATH = path.join(PROCESS_DIR, "baixar_e_restaurar.py");

const HOST = process.env.MONITOR_HOST || "0.0.0.0";
const PUBLIC_HOST = process.env.MONITOR_PUBLIC_HOST || "";
const DEFAULT_PORT = 1102;
const PORT = Number.parseInt(process.env.MONITOR_PORT || `${DEFAULT_PORT}`, 10);
const PYTHON_CMD = process.env.PYTHON_CMD || "python";

function statusVazio() {
  return {
    status_execucao: "aguardando",
    mensagem: "Aguardando um codigo de cliente para iniciar o download da base.",
    iniciado_em: "",
    atualizado_em: "",
    finalizado_em: "",
    codigo_cliente: "",
    codigo_formatado: "",
    erro: "",
    backup: {
      nome: "",
      url: "",
      data_hora: "",
      modo_selecao: "mais_recente_disponivel",
    },
    progresso: {
      atual: 0,
      total: 0,
      percentual: 0,
      status: "Sem atividade",
    },
    artefatos: {
      pasta_execucao: null,
      zip_baixado: null,
      pasta_extraida: null,
      fbk_encontrado: null,
      fdb_restaurado: null,
      marcador_restore: null,
    },
    logs: [],
  };
}

function createProcessState() {
  return {
    running: false,
    pid: null,
    started_at: "",
    finished_at: "",
    exit_code: null,
    error: "",
    stop_requested: false,
    client_code: "",
  };
}

const managedProcess = {
  label: "download e restauracao da base",
  scriptPath: SCRIPT_PATH,
  child: null,
  state: createProcessState(),
};

function resetManagedProcess() {
  managedProcess.child = null;
  managedProcess.state = createProcessState();
}

resetManagedProcess();

function ensureStatusFile() {
  fs.mkdirSync(path.dirname(STATUS_FILE), { recursive: true });
  if (!fs.existsSync(STATUS_FILE)) {
    fs.writeFileSync(STATUS_FILE, `${JSON.stringify(statusVazio(), null, 2)}\n`, "utf8");
  }
}

function loadStatus() {
  try {
    ensureStatusFile();
    return JSON.parse(fs.readFileSync(STATUS_FILE, "utf8"));
  } catch {
    return statusVazio();
  }
}

function saveStatus(status) {
  ensureStatusFile();
  fs.writeFileSync(STATUS_FILE, `${JSON.stringify(status, null, 2)}\n`, "utf8");
}

function patchStatus(patch) {
  const current = loadStatus();
  const next = {
    ...current,
    ...patch,
    progresso: {
      ...(current.progresso || statusVazio().progresso),
      ...(patch.progresso || {}),
    },
    backup: {
      ...(current.backup || statusVazio().backup),
      ...(patch.backup || {}),
    },
    artefatos: {
      ...(current.artefatos || statusVazio().artefatos),
      ...(patch.artefatos || {}),
    },
  };
  saveStatus(next);
}

function localIp() {
  if (PUBLIC_HOST) {
    return PUBLIC_HOST;
  }

  if (HOST && HOST !== "0.0.0.0" && HOST !== "::") {
    return HOST;
  }

  const interfaces = os.networkInterfaces();
  const candidates = [];

  for (const entries of Object.values(interfaces)) {
    for (const entry of entries || []) {
      if (entry.family !== "IPv4" || entry.internal) {
        continue;
      }
      candidates.push(entry.address);
    }
  }

  const privateCandidates = candidates.filter((address) => {
    if (address.startsWith("192.168.") || address.startsWith("10.")) {
      return true;
    }
    const octets = address.split(".").map((item) => Number.parseInt(item, 10));
    return octets.length === 4 && octets[0] === 172 && octets[1] >= 16 && octets[1] <= 31;
  });

  if (privateCandidates.length) {
    return privateCandidates[0];
  }

  if (candidates.length) {
    return candidates[0];
  }

  return "127.0.0.1";
}

function normalizeRemoteIp(address) {
  const value = String(address || "").trim();
  if (!value) {
    return "";
  }
  if (value === "::1") {
    return "127.0.0.1";
  }
  if (value.startsWith("::ffff:")) {
    return value.slice(7);
  }
  return value;
}

async function resolveHostname(ipAddress) {
  const ip = normalizeRemoteIp(ipAddress);
  if (!ip) {
    return "";
  }
  if (ip === "127.0.0.1") {
    return os.hostname();
  }

  if (dns.promises?.lookupService) {
    try {
      const result = await dns.promises.lookupService(ip, 0);
      const hostname = String(result?.hostname || "").trim();
      if (hostname && hostname !== ip) {
        return hostname;
      }
    } catch {
      // Fallback below.
    }
  }

  if (dns.promises?.reverse) {
    try {
      const names = await dns.promises.reverse(ip);
      const hostname = String(Array.isArray(names) ? names[0] || "" : "").trim();
      if (hostname && hostname !== ip) {
        return hostname;
      }
    } catch {
      // Fallback below.
    }
  }

  return "";
}

function sendJson(response, payload, statusCode = 200) {
  response.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
  });
  response.end(JSON.stringify(payload));
}

function sendText(response, text, statusCode = 200) {
  response.writeHead(statusCode, {
    "Content-Type": "text/plain; charset=utf-8",
    "Cache-Control": "no-store",
  });
  response.end(text);
}

function encodeDownloadFilename(fileName) {
  return encodeURIComponent(fileName).replace(/['()]/g, escape).replace(/\*/g, "%2A");
}

function sendFile(response, absolutePath, options = {}) {
  const extension = path.extname(absolutePath).toLowerCase();
  const contentTypes = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".txt": "text/plain; charset=utf-8",
    ".zip": "application/zip",
    ".fdb": "application/octet-stream",
    ".fbk": "application/octet-stream",
  };

  const contentType = contentTypes[extension] || "application/octet-stream";
  const stream = fs.createReadStream(absolutePath);

  stream.on("open", () => {
    const headers = {
      "Content-Type": contentType,
      "Cache-Control": "no-store",
    };

    if (options.download) {
      const fileName = path.basename(absolutePath);
      const fallbackName = fileName.replace(/[^\x20-\x7E]/g, "_").replace(/"/g, "");
      headers["Content-Disposition"] = `attachment; filename="${fallbackName}"; filename*=UTF-8''${encodeDownloadFilename(fileName)}`;
    }

    response.writeHead(200, headers);
  });

  stream.on("error", () => {
    sendText(response, "Erro ao ler arquivo.", 500);
  });

  stream.pipe(response);
}

function isPathInside(targetPath, rootPath) {
  const target = path.resolve(targetPath);
  const root = path.resolve(rootPath);
  const normalizedRoot = root.endsWith(path.sep) ? root : `${root}${path.sep}`;
  return target === root || target.startsWith(normalizedRoot);
}

function safeResolve(targetPath, rootPath = BASE_DIR) {
  if (!targetPath) {
    return null;
  }

  const root = path.resolve(rootPath);
  const target = path.isAbsolute(targetPath)
    ? path.resolve(targetPath)
    : path.resolve(root, targetPath);

  if (!isPathInside(target, BASE_DIR)) {
    return null;
  }

  if (!path.isAbsolute(targetPath) && !isPathInside(target, root)) {
    return null;
  }

  return target;
}

function requestPath(urlPath) {
  return decodeURIComponent((urlPath || "").replace(/^\/+/, ""));
}

function getReadyDatabaseStatusFile() {
  const status = loadStatus();
  return status?.artefatos?.fdb_restaurado || null;
}

function isAllowedReadyDatabaseTarget(relativeTarget) {
  const readyDatabase = getReadyDatabaseStatusFile();
  if (!readyDatabase || !readyDatabase.relative_path) {
    return false;
  }
  return String(relativeTarget || "").trim() === String(readyDatabase.relative_path || "").trim();
}

function readJsonBody(request) {
  return new Promise((resolve, reject) => {
    let body = "";

    request.on("data", (chunk) => {
      body += chunk.toString();
      if (body.length > 65536) {
        reject(new Error("Corpo da requisicao excedeu o limite permitido."));
        request.destroy();
      }
    });

    request.on("end", () => {
      if (!body.trim()) {
        resolve({});
        return;
      }

      try {
        resolve(JSON.parse(body));
      } catch {
        reject(new Error("Corpo JSON invalido."));
      }
    });

    request.on("error", (error) => {
      reject(error);
    });
  });
}

function normalizeClientCode(value) {
  return String(value || "").replace(/\D/g, "").trim();
}

function finalizeStatusFromProcess(exitCode, stopRequested) {
  const status = loadStatus();
  const stillRunning = String(status.status_execucao || "").trim().toLowerCase() === "executando";
  if (!stillRunning) {
    return;
  }

  const finishedAt = new Date().toISOString();
  if (stopRequested) {
    patchStatus({
      status_execucao: "interrompido",
      mensagem: "Processo interrompido manualmente pelo monitor.",
      finalizado_em: finishedAt,
      atualizado_em: finishedAt,
      erro: "",
      progresso: {
        status: "Processo interrompido manualmente.",
      },
    });
    return;
  }

  patchStatus({
    status_execucao: exitCode === 0 ? "concluido" : "erro",
    mensagem:
      exitCode === 0
        ? status.mensagem || "Fluxo finalizado com sucesso."
        : status.erro || "O processo foi encerrado com erro.",
    finalizado_em: finishedAt,
    atualizado_em: finishedAt,
  });
}

function startRestoreProcess(clientCode) {
  const normalizedCode = normalizeClientCode(clientCode);
  if (!normalizedCode) {
    return {
      ok: false,
      statusCode: 400,
      message: "Informe um codigo de cliente valido para continuar.",
    };
  }

  if (managedProcess.state.running) {
    return {
      ok: false,
      statusCode: 409,
      message: "Ja existe um download e restauracao em andamento.",
    };
  }

  if (!fs.existsSync(managedProcess.scriptPath)) {
    return {
      ok: false,
      statusCode: 404,
      message: `Arquivo nao encontrado: ${managedProcess.scriptPath}`,
    };
  }

  ensureStatusFile();
  saveStatus({
    ...statusVazio(),
    status_execucao: "executando",
    mensagem: "Preparando o download da base.",
    iniciado_em: new Date().toISOString(),
    atualizado_em: new Date().toISOString(),
    codigo_cliente: normalizedCode,
    codigo_formatado: normalizedCode.padStart(6, "0"),
    progresso: {
      atual: 0,
      total: 7,
      percentual: 0,
      status: "Preparando o ambiente",
    },
    logs: [
      `[${new Date().toLocaleTimeString("pt-BR", { hour12: false })}] Solicitação recebida para o codigo ${normalizedCode.padStart(6, "0")}.`,
    ],
  });

  let child;
  try {
    child = spawn(PYTHON_CMD, [managedProcess.scriptPath], {
      cwd: BASE_DIR,
      env: {
        ...process.env,
        CLIENT_CODE: normalizedCode,
      },
      windowsHide: false,
      shell: false,
    });
  } catch (error) {
    patchStatus({
      status_execucao: "erro",
      mensagem: `Falha ao iniciar o processo: ${error.message}`,
      erro: `Falha ao iniciar o processo: ${error.message}`,
      finalizado_em: new Date().toISOString(),
      atualizado_em: new Date().toISOString(),
    });
    return {
      ok: false,
      statusCode: 500,
      message: `Falha ao iniciar o processo: ${error.message}`,
    };
  }

  managedProcess.child = child;
  managedProcess.state.running = true;
  managedProcess.state.pid = child.pid || null;
  managedProcess.state.started_at = new Date().toISOString();
  managedProcess.state.finished_at = "";
  managedProcess.state.exit_code = null;
  managedProcess.state.error = "";
  managedProcess.state.stop_requested = false;
  managedProcess.state.client_code = normalizedCode;

  child.stdout.on("data", (chunk) => {
    process.stdout.write(chunk.toString());
  });

  child.stderr.on("data", (chunk) => {
    process.stderr.write(chunk.toString());
  });

  child.on("error", (error) => {
    managedProcess.state.error = error.message;
    patchStatus({
      status_execucao: "erro",
      mensagem: `Falha durante a execucao: ${error.message}`,
      erro: `Falha durante a execucao: ${error.message}`,
      finalizado_em: new Date().toISOString(),
      atualizado_em: new Date().toISOString(),
    });
  });

  child.on("exit", (code) => {
    const stopRequested = managedProcess.state.stop_requested;
    managedProcess.state.running = false;
    managedProcess.state.exit_code = code;
    managedProcess.state.finished_at = new Date().toISOString();
    finalizeStatusFromProcess(code, stopRequested);
    managedProcess.child = null;
  });

  return {
    ok: true,
    statusCode: 200,
    message: `Fluxo iniciado para o cliente ${normalizedCode.padStart(6, "0")}.`,
  };
}

function killProcessTree(pid) {
  return new Promise((resolve) => {
    if (!pid) {
      resolve();
      return;
    }

    const killer = spawn("taskkill", ["/PID", String(pid), "/T", "/F"], {
      windowsHide: true,
      shell: false,
    });

    killer.on("exit", () => resolve());
    killer.on("error", () => resolve());
  });
}

async function stopRestoreProcess() {
  if (!managedProcess.state.running || !managedProcess.child) {
    return {
      ok: false,
      statusCode: 409,
      message: "Nao existe nenhum processo em andamento para interromper.",
    };
  }

  managedProcess.state.stop_requested = true;
  patchStatus({
    mensagem: "Solicitacao de parada enviada pelo monitor.",
    atualizado_em: new Date().toISOString(),
    progresso: {
      status: "Parando o processo...",
    },
  });

  await killProcessTree(managedProcess.state.pid);

  return {
    ok: true,
    statusCode: 200,
    message: "Solicitacao de parada enviada com sucesso.",
  };
}

function clearRecords() {
  if (managedProcess.state.running) {
    return {
      ok: false,
      statusCode: 409,
      message: "Nao e possivel limpar os registros durante um processo em andamento.",
    };
  }

  try {
    saveStatus(statusVazio());
    resetManagedProcess();
    return {
      ok: true,
      statusCode: 200,
      message: "Registros do monitor limpos com sucesso.",
    };
  } catch (error) {
    return {
      ok: false,
      statusCode: 500,
      message: `Falha ao limpar os registros: ${error.message}`,
    };
  }
}

const server = http.createServer((request, response) => {
  const url = new URL(request.url || "/", `http://${request.headers.host || "localhost"}`);
  const pathname = url.pathname;

  if (pathname === "/api/status") {
    sendJson(response, {
      ...loadStatus(),
      process: managedProcess.state,
    });
    return;
  }

  if (pathname === "/api/meta") {
    resolveHostname(request.socket?.remoteAddress || "")
      .then((hostname) => {
        sendJson(response, {
          access_url: `http://${localIp()}:${PORT}`,
          host: HOST,
          port: PORT,
          public_host: PUBLIC_HOST,
          python_command: PYTHON_CMD,
          process: managedProcess.state,
          status_file: path.relative(BASE_DIR, STATUS_FILE),
          requester_host: hostname || "",
        });
      })
      .catch(() => {
        sendJson(response, {
          access_url: `http://${localIp()}:${PORT}`,
          host: HOST,
          port: PORT,
          public_host: PUBLIC_HOST,
          python_command: PYTHON_CMD,
          process: managedProcess.state,
          status_file: path.relative(BASE_DIR, STATUS_FILE),
          requester_host: "",
        });
      });
    return;
  }

  if (pathname === "/api/start" && request.method === "POST") {
    readJsonBody(request)
      .then((payload) => {
        const result = startRestoreProcess(payload.client_code);
        sendJson(
          response,
          {
            ok: result.ok,
            message: result.message,
            process: managedProcess.state,
          },
          result.statusCode,
        );
      })
      .catch((error) => {
        sendJson(
          response,
          {
            ok: false,
            message: error.message,
            process: managedProcess.state,
          },
          400,
        );
      });
    return;
  }

  if (pathname === "/api/stop" && request.method === "POST") {
    stopRestoreProcess()
      .then((result) => {
        sendJson(
          response,
          {
            ok: result.ok,
            message: result.message,
            process: managedProcess.state,
          },
          result.statusCode,
        );
      })
      .catch((error) => {
        sendJson(
          response,
          {
            ok: false,
            message: error.message,
            process: managedProcess.state,
          },
          500,
        );
      });
    return;
  }

  if (pathname === "/api/clear" && request.method === "POST") {
    const result = clearRecords();
    sendJson(
      response,
      {
        ok: result.ok,
        message: result.message,
        process: managedProcess.state,
      },
      result.statusCode,
    );
    return;
  }

  if (pathname === "/") {
    const htmlPath = safeResolve("index.html", path.join(BASE_DIR, "templates"));
    if (!htmlPath || !fs.existsSync(htmlPath)) {
      sendText(response, "Pagina principal nao encontrada.", 404);
      return;
    }
    sendFile(response, htmlPath);
    return;
  }

  if (pathname.startsWith("/static/")) {
    const relativeTarget = requestPath(pathname.replace(/^\/static\//, ""));
    const absolutePath = safeResolve(relativeTarget, path.join(BASE_DIR, "static"));
    if (!absolutePath || !fs.existsSync(absolutePath) || !fs.statSync(absolutePath).isFile()) {
      sendText(response, "Arquivo estatico nao encontrado.", 404);
      return;
    }
    sendFile(response, absolutePath);
    return;
  }

  if (pathname.startsWith("/arquivo/")) {
    const relativeTarget = requestPath(pathname.replace(/^\/arquivo\//, ""));
    if (!isAllowedReadyDatabaseTarget(relativeTarget)) {
      sendText(response, "Somente a base restaurada pode ser aberta por este monitor.", 403);
      return;
    }
    const absolutePath = safeResolve(relativeTarget);
    if (!absolutePath || !fs.existsSync(absolutePath) || !fs.statSync(absolutePath).isFile()) {
      sendText(response, "Arquivo nao encontrado.", 404);
      return;
    }
    sendFile(response, absolutePath);
    return;
  }

  if (pathname.startsWith("/download/")) {
    const relativeTarget = requestPath(pathname.replace(/^\/download\//, ""));
    if (!isAllowedReadyDatabaseTarget(relativeTarget)) {
      sendText(response, "Somente a base restaurada pode ser baixada por este monitor.", 403);
      return;
    }
    const absolutePath = safeResolve(relativeTarget);
    if (!absolutePath || !fs.existsSync(absolutePath) || !fs.statSync(absolutePath).isFile()) {
      sendText(response, "Arquivo nao encontrado.", 404);
      return;
    }
    sendFile(response, absolutePath, { download: true });
    return;
  }

  sendText(response, "Rota nao encontrada.", 404);
});

server.listen(PORT, HOST, () => {
  ensureStatusFile();
  console.log(`Monitor de bases disponivel em http://${localIp()}:${PORT}`);
  console.log(`Se outras maquinas nao conseguirem acessar, libere a porta ${PORT} no firewall do Windows.`);
});

process.on("exit", () => {
  if (managedProcess.child && managedProcess.state.running) {
    managedProcess.child.kill();
  }
});
