# SQL Schema Explorer & SQL Copy Runner

> **Copia dati da SQL Server verso Azure Data Lake Storage Gen2 in formato Parquet, fuori da Microsoft Fabric — per risparmiare CU e mantenerle disponibili per analytics e reporting.**

Sviluppato da Marco Pozzan

---

## Il problema che risolve

Quando usi Microsoft Fabric con sorgenti dati on-premise (SQL Server, ERP locali), il pattern classico è:

1. Pipeline Fabric / Dataflow Gen2 copia i dati da SQL → Lakehouse
2. I modelli semantici e i report leggono dal Lakehouse

Il problema: **anche il passo 1 consuma CU**. Su capacity piccole (F2, F4, F8), l'ingestione notturna erode il budget di Compute Units disponibili per le elaborazioni DAX e le query analitiche — quelle che generano valore reale.

**Questo toolset sposta l'ingestione completamente fuori da Fabric.** I file Parquet vengono scritti direttamente su ADLS Gen2 da un processo esterno. Fabric li trova già pronti e usa i propri CU solo per leggere, trasformare e calcolare.

---

## Componenti

| File | Descrizione |
|------|-------------|
| `sql_schema_explorer.py` | Applicazione desktop con GUI — configura lo schema, i flag e avvia la copia interattivamente |
| `sql_copy_runner.py` | Runner da riga di comando — esegue la copia da un file JSON di configurazione, adatto per task schedulati |

---

## Funzionalità principali

### SQL Schema Explorer (GUI)

- **Connessione SQL Server** con profili configurabili (Windows Auth / SQL Server Auth), supporto per più versioni di driver ODBC
- **Griglia gerarchica** tabelle → campi con flag per ogni livello:
  - ✔ **Includi/Escludi** per singolo campo o per tutta la tabella
  - ⊞ **FULL** — cancella e riscrive la cartella radice della tabella
  - ⟳ **INCREMENTALE** — aggiunge solo i record del giorno corrente filtrati su un campo data configurabile
  - 📅 **Campo Data** — selezione del campo usato come filtro incrementale, con radio button per ogni riga
- **Configurazione ADLS Gen2** con 4 metodi di autenticazione: Account Key, SAS Token, Service Principal, Managed Identity
- **Template percorso file** completamente personalizzabile con token: `{base}`, `{schema}`, `{table}`, `{YYYY}`, `{MM}`, `{DD}`, `{file}`
- **Salvataggio/caricamento configurazione JSON** con password DB cifrata (Fernet/AES-128, PBKDF2-HMAC-SHA256)
- **Esportazione** dello schema selezionato in CSV e JSON

### SQL Copy Runner (CLI)

- Legge il file JSON prodotto dall'Explorer e lo esegue senza GUI
- Passphrase da argomento, variabile d'ambiente o file
- Filtri: `--only-full`, `--only-incr`, `--tables T1,T2,...`
- Modalità `--dry-run` per verificare il piano senza eseguire nulla
- Log colorato su stdout con livelli INFO/OK/WARN/ERROR
- Scrittura log su file con `--log-file`
- Exit code standard (0/1/2/3) per integrazione con scheduler e pipeline CI/CD

---

## Installazione

### Prerequisiti Python

```bash
pip install pyodbc pandas pyarrow azure-storage-file-datalake azure-identity cryptography
```

### Driver ODBC

Scaricare e installare **ODBC Driver 18 for SQL Server** dal sito Microsoft:
[https://learn.microsoft.com/it-it/sql/connect/odbc/download-odbc-driver-for-sql-server](https://learn.microsoft.com/it-it/sql/connect/odbc/download-odbc-driver-for-sql-server)

### Clonare il repository

```bash
git clone https://github.com/<tuo-org>/sql-schema-explorer.git
cd sql-schema-explorer
```

I due file `sql_schema_explorer.py` e `sql_copy_runner.py` devono trovarsi nella **stessa directory** — il runner importa le funzioni condivise dal modulo principale.

---

## Avvio

### Interfaccia grafica

```bash
python sql_schema_explorer.py
```

### Runner da riga di comando

```bash
# Sintassi base
python sql_copy_runner.py <config.json> [passphrase] [opzioni]

# Esempi
python sql_copy_runner.py config.json "MiaPassphrase!"
python sql_copy_runner.py config.json --passphrase-env DB_PWD --only-incr
python sql_copy_runner.py config.json "pwd" --dry-run
python sql_copy_runner.py config.json "pwd" --tables dbo.Clienti,dbo.Ordini --log-file run.log
```

---

## Flusso di lavoro tipico

```
1. Apri sql_schema_explorer.py
        │
        ▼
2. Connetti al database SQL Server
        │
        ▼
3. Nella griglia: imposta per ogni tabella
   - flag Includi/Escludi per i campi
   - modalità FULL o INCREMENTALE
   - campo data per le tabelle INCREMENTALI
        │
        ▼
4. Configura ADLS Gen2 (☁ ADLS Config)
   - account, container, autenticazione
   - template percorso file
        │
        ▼
5. 💾 Salva JSON  →  config.json
   (la password DB viene cifrata con passphrase)
        │
        ▼
6. Pianifica il runner nel Task Scheduler / cron:
   python sql_copy_runner.py config.json --passphrase-env DB_PWD
        │
        ▼
7. Fabric trova i Parquet già pronti su ADLS
   e usa i CU solo per analytics
```

---

## Template percorso

Il percorso dei file Parquet su ADLS è completamente parametrico. Default:

```
{base}/{table}/{YYYY}/{MM}/{DD}/{file}.parquet
```

Esempi di personalizzazione:

```
# Con schema SQL nel percorso
{base}/{schema}/{table}/{YYYY}/{MM}/{DD}/{file}.parquet

# Partizionamento mensile
{base}/{table}/{YYYY}/{MM}/{file}.parquet

# Data nel nome del file
{base}/{table}/{file}_{YYYY}{MM}{DD}.parquet
```

**Token disponibili:** `{base}` `{schema}` `{table}` `{YYYY}` `{MM}` `{DD}` `{file}`

---

## Sicurezza — Cifratura password

La password del database non viene mai salvata in chiaro nel file JSON.

**Algoritmo:** Fernet (AES-128-CBC + HMAC-SHA256)  
**Derivazione chiave:** PBKDF2-HMAC-SHA256, 260.000 iterazioni, salt fisso  
**Passphrase:** non salvata nel file — deve essere fornita ad ogni caricamento

```json
{
  "connection": {
    "server": "myserver",
    "username": "sa",
    "password_enc": "gAAAAABn...token_fernet..."
  }
}
```

Per automazione in produzione, usare sempre `--passphrase-env` invece di passare la passphrase come argomento posizionale:

```bash
# ✅ Sicuro — la passphrase non appare nei log di sistema
DB_PWD=MiaPassphrase python sql_copy_runner.py config.json --passphrase-env DB_PWD

# ⚠️ Meno sicuro — la passphrase è visibile nella cronologia shell
python sql_copy_runner.py config.json "MiaPassphrase"
```

---

## Modalità di caricamento

### FULL

1. Elimina la cartella radice `{base}/{table}/` su ADLS (tutti i dati storici)
2. Legge l'intera tabella con `SELECT [col1],[col2],... FROM [schema].[tabella]`
3. Scrive il file Parquet nella partizione del giorno corrente

### INCREMENTALE

1. **Con campo data configurato:** legge solo i record del giorno con  
   `WHERE CAST([campo_data] AS DATE) = 'YYYY-MM-DD'`
2. **Senza campo data:** legge tutta la tabella con avviso nel log
3. Scrive il file Parquet nella partizione del giorno — senza cancellare le partizioni precedenti

---

## Exit code (SQL Copy Runner)

| Codice | Significato |
|--------|-------------|
| `0` | Tutte le tabelle copiate con successo |
| `1` | Errore di configurazione o parametri mancanti |
| `2` | Errori parziali — alcune tabelle fallite, altre OK |
| `3` | Errore fatale — connessione SQL Server o ADLS impossibile |

---

## Formato file di configurazione JSON

```json
{
  "_meta": {
    "saved_at": "2026-03-26T08:00:00",
    "tool": "SQL Schema Explorer — Regolo Farm",
    "total_tables": 12,
    "total_columns": 248,
    "password_encrypted": true
  },
  "connection": {
    "server": "prod-sql\\MSSQLSERVER",
    "database": "CargoBI",
    "driver": "ODBC Driver 18 for SQL Server",
    "win_auth": false,
    "username": "sa",
    "password_enc": "gAAAAABn...",
    "trust_cert": true,
    "encrypt": false
  },
  "adls": {
    "account_name": "mystorage",
    "container": "raw",
    "base_folder": "sqlserver/cargobi",
    "auth_method": "service_principal",
    "tenant_id": "...",
    "client_id": "...",
    "client_secret": "...",
    "path_template": "{base}/{table}/{YYYY}/{MM}/{DD}/{file}.parquet"
  },
  "tables": {
    "dbo.Polizze": {
      "schema": "dbo",
      "table_name": "Polizze",
      "object_type": "TABLE",
      "load_mode": "INCREMENTALE",
      "incr_field": "DataModifica",
      "columns": {
        "ID":           { "include": true,  "data_type": "int",      "is_nullable": false },
        "DataModifica": { "include": true,  "data_type": "datetime", "is_nullable": false },
        "NoteInterne":  { "include": false, "data_type": "nvarchar", "is_nullable": true  }
      }
    }
  }
}
```

---

## Opzioni SQL Copy Runner — riferimento completo

```
python sql_copy_runner.py <config.json> [passphrase] [opzioni]

Sorgente passphrase (mutuamente esclusivi):
  passphrase            Argomento posizionale (sconsigliato in produzione)
  --passphrase-env VAR  Legge dalla variabile d'ambiente VAR
  --passphrase-file PATH  Legge dalla prima riga del file PATH

Filtri di esecuzione:
  --only-full           Esegue solo le tabelle FULL
  --only-incr           Esegue solo le tabelle INCREMENTALE
  --tables T1,T2,...    Esegue solo le tabelle elencate

Output:
  --dry-run             Mostra il piano senza eseguire nulla
  --log-file PATH       Scrive il log su file (append)
  --no-color            Disabilita i colori ANSI (consigliato per log su file)
  -h, --help            Mostra l'help completo
```

---

## Schedulazione

### Windows Task Scheduler

```
Programma:   python
Argomenti:   "C:\tools\sql_copy_runner.py" "C:\config\cargo_bi.json"
             --passphrase-env DB_PWD --only-incr
             --log-file "C:\logs\incr.log" --no-color
Variabili:   DB_PWD = MiaPassphrase!  (variabile di sistema)
```

### Linux / macOS cron

```bash
# Copia incrementale ogni giorno alle 06:00
0 6 * * * DB_PWD=secret /usr/bin/python3 /opt/tools/sql_copy_runner.py \
    /opt/config/cargo_bi.json --passphrase-env DB_PWD --only-incr \
    --log-file /var/log/sql_copy/incr_$(date +\%Y\%m\%d).log --no-color

# Copia FULL ogni domenica alle 02:00
0 2 * * 0 DB_PWD=secret /usr/bin/python3 /opt/tools/sql_copy_runner.py \
    /opt/config/cargo_bi.json --passphrase-env DB_PWD --only-full \
    --log-file /var/log/sql_copy/full_$(date +\%Y\%m\%d).log --no-color
```

---

## Compatibilità Microsoft Fabric

I file Parquet prodotti sono direttamente compatibili con:

- **Lakehouse** — puntare la tabella alla cartella `{table}/` su ADLS tramite shortcut
- **OneLake shortcut** — montare il container ADLS Gen2 come shortcut OneLake
- **Direct Lake** — i dataset Power BI in Direct Lake leggono direttamente dai Parquet senza import
- **Notebook / Spark** — `spark.read.parquet("abfss://container@account.dfs.core.windows.net/table/")`

---

## Sviluppato da

Marco Pozzan

> *"Come i navigatori del Quattrocento tracciavano rotte verso l'ignoto, noi guidiamo le imprese verso il futuro dei dati."*

---

## Licenza

Uso interno Regolo Farm. Per licenze commerciali o contributi, contattare [info@marcopozzan.it](mailto:info@marcopozzan.it).
