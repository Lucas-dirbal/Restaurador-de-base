# Baixar e restaurar base

Projeto web simples, no estilo do monitor de XML da area de trabalho, para:

- informar o codigo do cliente;
- localizar o backup mais recente disponivel no servidor;
- baixar o ZIP;
- extrair o `.FBK`;
- restaurar a base para `.FDB` com `gbak`;
- validar o restore com `isql`.

## Requisitos

- Windows
- Python 3
- Node.js 22+
- Firebird 2.5 instalado em `C:\Program Files\Firebird\Firebird_2_5\bin`

## Como iniciar

No PowerShell, dentro desta pasta:

```powershell
npm start
```

O site sobe por padrao em:

```text
http://SEU_IP:1102
```

## Variaveis de ambiente uteis

```powershell
$env:MONITOR_PORT="8082"
$env:PYTHON_CMD="python"
$env:BACKUP_FILES_BASE_URL="http://192.168.1.150:8088/interface/files/"
$env:FIREBIRD_USER="SYSDBA"
$env:FIREBIRD_PASSWORD="masterkey"
npm start
```

## Estrutura gerada

Os arquivos do processo ficam em `saida/execucoes/<codigo>_<timestamp>/` com:

- `download/` para o ZIP baixado
- `extraido/` para o conteudo do ZIP
- `restore/` para a base `.FDB`

O status exibido na tela fica em:

```text
processo/monitor_data/status.json
```
