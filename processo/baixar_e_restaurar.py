import json
import locale
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from html import unescape
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urljoin, urlsplit
from urllib.request import Request, urlopen
from zipfile import ZipFile


BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
STATUS_FILE = BASE_DIR / "monitor_data" / "status.json"
OUTPUT_DIR = ROOT_DIR / "saida"
RUNS_DIR = OUTPUT_DIR / "execucoes"

BACKUP_FILES_BASE_URL = os.getenv("BACKUP_FILES_BASE_URL", "http://192.168.1.150:8088/interface/files/")
CLIENT_CODE_ENV = "CLIENT_CODE"
FIREBIRD_BIN = Path(os.getenv("FIREBIRD_BIN", r"C:\Program Files\Firebird\Firebird_2_5\bin"))
CAMINHO_GBAK = FIREBIRD_BIN / "gbak.exe"
CAMINHO_ISQL = FIREBIRD_BIN / "isql.exe"
FIREBIRD_USER = os.getenv("FIREBIRD_USER", "SYSDBA")
FIREBIRD_PASSWORD = os.getenv("FIREBIRD_PASSWORD", "masterkey")
ENCODING_SISTEMA = locale.getpreferredencoding(False) or "utf-8"

BACKUP_LINK_PATTERN = re.compile(r'href="([^"]+\.zip(?:\?[^"]*)?)"', re.IGNORECASE)
BACKUP_TIMESTAMP_NOME_PATTERN = re.compile(r"_(\d{14})\.zip$", re.IGNORECASE)


class FluxoBaseError(RuntimeError):
    pass


def status_vazio() -> dict[str, object]:
    return {
        "status_execucao": "aguardando",
        "mensagem": "Aguardando um codigo de cliente para iniciar o download da base.",
        "iniciado_em": "",
        "atualizado_em": "",
        "finalizado_em": "",
        "codigo_cliente": "",
        "codigo_formatado": "",
        "erro": "",
        "backup": {
            "nome": "",
            "url": "",
            "data_hora": "",
            "modo_selecao": "mais_recente_disponivel",
        },
        "progresso": {
            "atual": 0,
            "total": 0,
            "percentual": 0,
            "status": "Sem atividade",
        },
        "artefatos": {
            "pasta_execucao": None,
            "zip_baixado": None,
            "pasta_extraida": None,
            "fbk_encontrado": None,
            "fdb_restaurado": None,
            "marcador_restore": None,
        },
        "logs": [],
    }


def garantir_status_file() -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not STATUS_FILE.exists():
        STATUS_FILE.write_text(f"{json.dumps(status_vazio(), indent=2)}\n", encoding="utf-8")


def carregar_status() -> dict[str, object]:
    try:
        garantir_status_file()
        return json.loads(STATUS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return status_vazio()


def salvar_status(status: dict[str, object]) -> None:
    garantir_status_file()
    status["atualizado_em"] = datetime.now().isoformat(timespec="seconds")
    STATUS_FILE.write_text(
        json.dumps(status, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def registrar_log(status: dict[str, object], mensagem: str) -> None:
    logs = list(status.get("logs") or [])
    logs.append(f"[{datetime.now():%H:%M:%S}] {mensagem}")
    status["logs"] = logs[-200:]
    status["mensagem"] = mensagem
    salvar_status(status)
    print(mensagem)


def atualizar_progresso(
    status: dict[str, object],
    atual: int,
    total: int,
    mensagem: str,
) -> None:
    percentual = round((atual / total) * 100, 1) if total else 0
    status["progresso"] = {
        "atual": atual,
        "total": total,
        "percentual": percentual,
        "status": mensagem,
    }
    registrar_log(status, mensagem)


def criar_artefato(titulo: str, caminho: Path | None, tipo: str) -> dict[str, object] | None:
    if caminho is None:
        return None

    caminho_resolvido = caminho.resolve()
    try:
        relativo = caminho_resolvido.relative_to(ROOT_DIR)
        relative_path = relativo.as_posix()
    except ValueError:
        relative_path = str(caminho_resolvido)

    return {
        "titulo": titulo,
        "tipo": tipo,
        "path": str(caminho_resolvido),
        "relative_path": relative_path,
        "exists": caminho_resolvido.exists(),
    }


def atualizar_artefato(
    status: dict[str, object],
    chave: str,
    titulo: str,
    caminho: Path | None,
    tipo: str,
) -> None:
    artefatos = dict(status.get("artefatos") or {})
    artefatos[chave] = criar_artefato(titulo, caminho, tipo)
    status["artefatos"] = artefatos
    salvar_status(status)


def normalizar_codigo_empresa(codigo_empresa: str) -> str:
    return re.sub(r"\D", "", str(codigo_empresa or "")).strip()


def formatar_codigo_empresa_seis_digitos(codigo_empresa: str) -> str:
    codigo_limpo = normalizar_codigo_empresa(codigo_empresa)
    if not codigo_limpo:
        raise FluxoBaseError("Informe um codigo de cliente valido para iniciar o processo.")
    return codigo_limpo.zfill(6)


def construir_url_listagem_backups(codigo_empresa: str) -> str:
    codigo_formatado = formatar_codigo_empresa_seis_digitos(codigo_empresa)
    return urljoin(BACKUP_FILES_BASE_URL.rstrip("/") + "/", f"{codigo_formatado}/")


def extrair_arquivos_zip_listagem(conteudo_html: str) -> list[str]:
    arquivos: list[str] = []
    vistos: set[str] = set()

    for correspondencia in BACKUP_LINK_PATTERN.findall(conteudo_html):
        href = unquote(unescape(correspondencia.strip()))
        nome_arquivo = Path(urlsplit(href).path).name
        if not nome_arquivo.lower().endswith(".zip"):
            continue
        if nome_arquivo in vistos:
            continue
        vistos.add(nome_arquivo)
        arquivos.append(nome_arquivo)

    return arquivos


def extrair_timestamp_nome_backup(nome_arquivo: str) -> datetime | None:
    correspondencia = BACKUP_TIMESTAMP_NOME_PATTERN.search(nome_arquivo)
    if correspondencia is None:
        return None

    try:
        return datetime.strptime(correspondencia.group(1), "%d%m%Y%H%M%S")
    except ValueError:
        return None


def listar_backups_empresa(codigo_empresa: str) -> list[tuple[datetime, str, str]]:
    url_listagem = construir_url_listagem_backups(codigo_empresa)
    requisicao = Request(
        url_listagem,
        headers={"User-Agent": "Mozilla/5.0"},
        method="GET",
    )

    try:
        with urlopen(requisicao, timeout=20) as resposta:
            conteudo_html = resposta.read().decode("utf-8", errors="replace")
    except HTTPError as erro:
        raise FluxoBaseError(
            f"Nao foi possivel acessar a pasta de backups da empresa {formatar_codigo_empresa_seis_digitos(codigo_empresa)}: HTTP {erro.code}."
        ) from erro
    except URLError as erro:
        motivo = getattr(erro, "reason", erro)
        raise FluxoBaseError(
            f"Nao foi possivel conectar ao servidor de backups da empresa {formatar_codigo_empresa_seis_digitos(codigo_empresa)}: {motivo}"
        ) from erro

    arquivos = extrair_arquivos_zip_listagem(conteudo_html)
    if not arquivos:
        raise FluxoBaseError(
            f"Nao encontrei arquivos ZIP na pasta de backups da empresa {formatar_codigo_empresa_seis_digitos(codigo_empresa)}."
        )

    backups: list[tuple[datetime, str, str]] = []
    for nome_arquivo in arquivos:
        timestamp = extrair_timestamp_nome_backup(nome_arquivo)
        if timestamp is None:
            continue
        backups.append((timestamp, nome_arquivo, urljoin(url_listagem, nome_arquivo)))

    if not backups:
        raise FluxoBaseError(
            f"Nao encontrei arquivos ZIP validos com timestamp para a empresa {formatar_codigo_empresa_seis_digitos(codigo_empresa)}."
        )

    return sorted(backups, key=lambda item: item[0])


def selecionar_backup_mais_recente(backups: list[tuple[datetime, str, str]]) -> tuple[datetime, str, str]:
    if not backups:
        raise FluxoBaseError("Nenhum backup disponivel para selecao.")
    return max(backups, key=lambda item: item[0])


def baixar_arquivo_backup(url_arquivo: str, caminho_destino: Path) -> None:
    caminho_destino.parent.mkdir(parents=True, exist_ok=True)
    caminho_temporario = caminho_destino.with_suffix(caminho_destino.suffix + ".part")
    if caminho_temporario.exists():
        caminho_temporario.unlink()

    requisicao = Request(
        url_arquivo,
        headers={"User-Agent": "Mozilla/5.0"},
        method="GET",
    )

    try:
        with urlopen(requisicao, timeout=120) as resposta, caminho_temporario.open("wb") as arquivo_destino:
            shutil.copyfileobj(resposta, arquivo_destino)
    except (HTTPError, URLError, OSError) as erro:
        try:
            if caminho_temporario.exists():
                caminho_temporario.unlink()
        except OSError:
            pass
        raise FluxoBaseError(
            f"Nao foi possivel baixar o arquivo {Path(urlsplit(url_arquivo).path).name}: {erro}"
        ) from erro

    caminho_temporario.replace(caminho_destino)


def extrair_zip(caminho_zip: Path, pasta_destino: Path) -> Path:
    pasta_destino.mkdir(parents=True, exist_ok=True)
    with ZipFile(caminho_zip, "r") as zip_ref:
        zip_ref.extractall(pasta_destino)
    return pasta_destino


def encontrar_arquivo_fbk(pasta_base: Path) -> Path:
    arquivos = sorted(pasta_base.rglob("*"))
    candidatos = [caminho for caminho in arquivos if caminho.is_file() and caminho.suffix.lower() == ".fbk"]
    if not candidatos:
        raise FluxoBaseError("Nao encontrei nenhum arquivo .FBK dentro do ZIP baixado.")
    return candidatos[0]


def caminho_marcador_restauracao(caminho_fdb: Path) -> Path:
    return caminho_fdb.with_suffix(".restore.json")


def salvar_marcador_restauracao(caminho_fbk: Path, caminho_fdb: Path, modo: str) -> Path:
    caminho_marcador = caminho_marcador_restauracao(caminho_fdb)
    caminho_marcador.write_text(
        json.dumps(
            {
                "backup": caminho_fbk.name,
                "banco_restaurado": caminho_fdb.name,
                "modo": modo,
                "restaurado_em": datetime.now().isoformat(timespec="seconds"),
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return caminho_marcador


def remover_artefatos_restauracao(caminho_fdb: Path) -> None:
    for caminho in (caminho_fdb, caminho_marcador_restauracao(caminho_fdb)):
        if caminho.exists():
            caminho.unlink()


def limpar_saida_isql(texto: str) -> list[str]:
    linhas_limpas: list[str] = []

    for linha in texto.replace("\r", "").split("\n"):
        linha = re.sub(r"\b(?:SQL>|CON>)\s*", "", linha).strip()
        if not linha or linha.startswith("Database:"):
            continue
        linhas_limpas.append(linha)

    return linhas_limpas


def executar_isql(caminho_fdb: Path, sql: str) -> list[str]:
    comando = [
        str(CAMINHO_ISQL),
        "-q",
        "-user",
        FIREBIRD_USER,
        "-password",
        FIREBIRD_PASSWORD,
        str(caminho_fdb),
    ]

    resultado = subprocess.run(
        comando,
        input=sql,
        capture_output=True,
        text=True,
        encoding=ENCODING_SISTEMA,
        errors="replace",
        check=False,
    )

    saida = (resultado.stdout or "") + ("\n" + resultado.stderr if resultado.stderr else "")
    linhas = limpar_saida_isql(saida)

    if resultado.returncode != 0:
        raise RuntimeError("\n".join(linhas) or "Falha ao executar isql.")

    return linhas


def banco_restaurado_acessivel(caminho_fdb: Path) -> bool:
    try:
        linhas = executar_isql(
            caminho_fdb,
            """
set heading off;
set list off;
select 1
from rdb$database;
quit;
""",
        )
    except Exception:
        return False

    return bool(linhas and linhas[0] == "1")


def erro_restauracao_permita_indices_inativos(mensagem: str) -> bool:
    mensagem_normalizada = mensagem.lower()
    return (
        "violation of primary or unique key constraint" in mensagem_normalizada
        or "problematic key value" in mensagem_normalizada
    )


def executar_restauracao_gbak(
    caminho_fbk: Path,
    caminho_fdb: Path,
    opcoes_restauracao: list[str] | None = None,
) -> subprocess.CompletedProcess[str]:
    comando = [
        str(CAMINHO_GBAK),
        "-c",
        *(opcoes_restauracao or []),
        "-user",
        FIREBIRD_USER,
        "-pas",
        FIREBIRD_PASSWORD,
        str(caminho_fbk),
        str(caminho_fdb),
    ]

    return subprocess.run(
        comando,
        capture_output=True,
        text=True,
        encoding=ENCODING_SISTEMA,
        errors="replace",
        check=False,
    )


def restaurar_fbk(
    caminho_fbk: Path,
    pasta_destino: Path,
    nome_base_restaurada: str,
) -> tuple[Path, Path]:
    pasta_destino.mkdir(parents=True, exist_ok=True)
    nome_limpo = re.sub(r"[^0-9A-Za-z_-]", "", str(nome_base_restaurada or "").strip())
    if not nome_limpo:
        raise FluxoBaseError("Nao foi possivel definir o nome final da base restaurada.")
    caminho_fdb = pasta_destino / f"{nome_limpo}.FDB"

    if caminho_fdb.exists():
        remover_artefatos_restauracao(caminho_fdb)

    resultado = executar_restauracao_gbak(caminho_fbk, caminho_fdb)
    if resultado.returncode != 0:
        mensagem = (resultado.stderr or resultado.stdout).strip() or "Erro desconhecido ao restaurar backup."
        remover_artefatos_restauracao(caminho_fdb)

        if erro_restauracao_permita_indices_inativos(mensagem):
            resultado_inativo = executar_restauracao_gbak(caminho_fbk, caminho_fdb, ["-i"])
            if resultado_inativo.returncode == 0 and banco_restaurado_acessivel(caminho_fdb):
                marcador = salvar_marcador_restauracao(caminho_fbk, caminho_fdb, "indices_inativos")
                return caminho_fdb, marcador

            mensagem_inativa = (
                (resultado_inativo.stderr or resultado_inativo.stdout).strip()
                or "Erro desconhecido ao restaurar backup com indices inativos."
            )
            remover_artefatos_restauracao(caminho_fdb)
            raise FluxoBaseError(
                f"Falha ao restaurar {caminho_fbk.name}: {mensagem}\n"
                f"Tentativa com indices inativos tambem falhou: {mensagem_inativa}"
            )

        raise FluxoBaseError(f"Falha ao restaurar {caminho_fbk.name}: {mensagem}")

    if not banco_restaurado_acessivel(caminho_fdb):
        remover_artefatos_restauracao(caminho_fdb)
        raise FluxoBaseError(
            f"Falha ao restaurar {caminho_fbk.name}: a base criada nao ficou acessivel apos o restore."
        )

    marcador = salvar_marcador_restauracao(caminho_fbk, caminho_fdb, "padrao")
    return caminho_fdb, marcador


def validar_dependencias() -> None:
    if not CAMINHO_GBAK.is_file():
        raise FluxoBaseError(f"gbak.exe nao encontrado em {CAMINHO_GBAK}.")
    if not CAMINHO_ISQL.is_file():
        raise FluxoBaseError(f"isql.exe nao encontrado em {CAMINHO_ISQL}.")


def criar_pasta_execucao(codigo_formatado: str) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pasta_execucao = RUNS_DIR / f"{codigo_formatado}_{timestamp}"
    pasta_execucao.mkdir(parents=True, exist_ok=True)
    return pasta_execucao


def preparar_status_inicial(codigo_original: str, codigo_formatado: str) -> dict[str, object]:
    status = status_vazio()
    status["status_execucao"] = "executando"
    status["iniciado_em"] = datetime.now().isoformat(timespec="seconds")
    status["codigo_cliente"] = codigo_original
    status["codigo_formatado"] = codigo_formatado
    status["progresso"] = {
        "atual": 0,
        "total": 7,
        "percentual": 0,
        "status": "Preparando o ambiente",
    }
    salvar_status(status)
    return status


def executar_fluxo() -> int:
    codigo_original = os.getenv(CLIENT_CODE_ENV, "").strip()
    codigo_formatado = formatar_codigo_empresa_seis_digitos(codigo_original)
    status = preparar_status_inicial(codigo_original, codigo_formatado)

    try:
        atualizar_progresso(status, 1, 7, "Validando ambiente local para o restore.")
        validar_dependencias()

        pasta_execucao = criar_pasta_execucao(codigo_formatado)
        pasta_download = pasta_execucao / "download"
        pasta_extraida = pasta_execucao / "extraido"
        pasta_restore = pasta_execucao / "restore"

        atualizar_artefato(status, "pasta_execucao", "Pasta da execucao", pasta_execucao, "dir")

        atualizar_progresso(status, 2, 7, f"Listando backups disponiveis para o cliente {codigo_formatado}.")
        backups = listar_backups_empresa(codigo_formatado)
        timestamp_backup, nome_backup, url_backup = selecionar_backup_mais_recente(backups)
        status["backup"] = {
            "nome": nome_backup,
            "url": url_backup,
            "data_hora": timestamp_backup.isoformat(timespec="seconds"),
            "modo_selecao": "mais_recente_disponivel",
        }
        salvar_status(status)
        registrar_log(
            status,
            f"Backup selecionado: {nome_backup} ({timestamp_backup:%d/%m/%Y %H:%M:%S}).",
        )

        atualizar_progresso(status, 3, 7, f"Baixando o ZIP {nome_backup}.")
        caminho_zip = pasta_download / nome_backup
        baixar_arquivo_backup(url_backup, caminho_zip)
        atualizar_artefato(status, "zip_baixado", "ZIP baixado", caminho_zip, "file")

        atualizar_progresso(status, 4, 7, "Extraindo o backup baixado.")
        destino_extraido = extrair_zip(caminho_zip, pasta_extraida / caminho_zip.stem)
        atualizar_artefato(status, "pasta_extraida", "Pasta extraida", destino_extraido, "dir")

        atualizar_progresso(status, 5, 7, "Localizando o arquivo FBK dentro do ZIP.")
        caminho_fbk = encontrar_arquivo_fbk(destino_extraido)
        atualizar_artefato(status, "fbk_encontrado", "Arquivo FBK", caminho_fbk, "file")

        atualizar_progresso(status, 6, 7, "Restaurando a base com o gbak.")
        caminho_fdb, caminho_marcador = restaurar_fbk(caminho_fbk, pasta_restore, codigo_formatado)
        atualizar_artefato(status, "fdb_restaurado", "Base restaurada", caminho_fdb, "file")
        atualizar_artefato(status, "marcador_restore", "Marcador do restore", caminho_marcador, "file")

        atualizar_progresso(status, 7, 7, "Validando a base restaurada com o isql.")
        if not banco_restaurado_acessivel(caminho_fdb):
            raise FluxoBaseError("A base restaurada nao respondeu ao teste final do isql.")

        status["status_execucao"] = "concluido"
        status["mensagem"] = "Download e restauracao concluidos com sucesso."
        status["erro"] = ""
        status["finalizado_em"] = datetime.now().isoformat(timespec="seconds")
        salvar_status(status)
        registrar_log(status, f"Base restaurada com sucesso em {caminho_fdb}.")
        return 0
    except Exception as erro:
        status["status_execucao"] = "erro"
        status["erro"] = str(erro)
        status["mensagem"] = str(erro)
        status["finalizado_em"] = datetime.now().isoformat(timespec="seconds")
        salvar_status(status)
        registrar_log(status, f"Erro: {erro}")
        return 1


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")
    garantir_status_file()
    raise SystemExit(executar_fluxo())
