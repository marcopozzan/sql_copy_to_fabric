# SQL Schema Explorer & SQL Copy Runner

> **Copia dati da SQL Server verso Azure Data Lake Storage Gen2 o Microsoft Fabric OneLake in formato Parquet, fuori da Microsoft Fabric — per risparmiare CU e mantenerle disponibili per analytics e reporting.**

Sviluppato da [Regolo Farm](https://www.regolofarm.it) — AI & Business Intelligence · Polo Tecnologico di Pordenone

---

## Il problema che risolve

Quando usi Microsoft Fabric con sorgenti dati on-premise (SQL Server, ERP locali), il pattern classico è:

1. Pipeline Fabric / Dataflow Gen2 copia i dati da SQL → Lakehouse
2. I modelli semantici e i report leggono dal Lakehouse

Il problema: **anche il passo 1 consuma CU**. Su capacity piccole (F2, F4, F8), l'ingestione notturna erode il budget di Compute Units disponibili per le elaborazioni DAX e le query analitiche — quelle che generano valore reale.

**Questo toolset sposta l'ingestione completamente fuori da Fabric.** I file Parquet vengono scritti direttamente su ADLS Gen2 o su OneLake da un processo esterno. Fabric li trova già pronti e usa i propri CU solo per leggere, trasformare e calcolare.

---

## Componenti

| File | Descrizione |
|------|-------------|
| `sql_schema_explorer.py` | Applicazione desktop con GUI — configura schema, flag e avvia la copia interattivamente |
| `sql_copy_runner.py` | Runner da riga di comando — esegue la copia da un file JSON di configurazione, adatto per automazione |

I due file devono trovarsi nella **stessa directory**: il runner importa le funzioni condivise dal modulo principale.

---

## Funzionalità principali

### SQL Schema Explorer (GUI)

- **Connessione SQL Server** con profili configurabili (Windows Auth / SQL Server Auth), supporto per più versioni di driver ODBC
- **Griglia gerarchica** tabelle → campi con flag configurabili per ogni livello:
  - ✔ **Includi/Escludi** per singolo campo o per tutta la tabella (toggle batch)
  - ⊞ **FULL** — cancella e riscrive la cartella radice della tabella
  - ⟳ **INCREMENTALE** — filtra i record più recenti dell'ultima esecuzione usando un campo data configurabile; alla prima esecuzione prende solo il giorno corrente
  - 📅 **Campo Data** — radio button per scegliere la colonna usata come filtro incrementale
  - ⊟ **Partizione** — flag per abilitare il partizionamento Parquet; selezione multipla delle colonne di partizione (formato Hive: `col=val/col2=val2/part-0.parquet`)
- **Destinazione storage** — ADLS Gen2 o Microsoft Fabric OneLake, selezionabile nella stessa dialog
- **Template percorso file** personalizzabile con token: `{base}`, `{schema}`, `{table}`, `{YYYY}`, `{MM}`, `{DD}`, `{file}`
- **Salvataggio/caricamento configurazione JSON** con password DB cifrata (Fernet/AES-128, PBKDF2-HMAC-SHA256)
- **`last_run` per tabella** — il timestamp dell'ultima esecuzione viene salvato nel JSON e usato come filtro nelle esecuzioni successive
- **Esportazione** schema selezionato in CSV e JSON

### SQL Copy Runner (CLI)

- Legge il file JSON prodotto dall'Explorer e lo esegue senza GUI
- Passphrase da argomento posizionale, variabile d'ambiente o file
- **`--save-config PATH`** — salva il JSON aggiornato con i `last_run` aggiornati dopo ogni esecuzione; passare lo stesso file di input per aggiornarlo in place
- Filtri: `--only-full`, `--only-incr`, `--tables T1,T2,...`
- Modalità `--dry-run` — mostra il piano completo (inclusi `last_run`, colonne di partizione, percorsi) senza eseguire nulla
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
https://learn.microsoft.com/it-it/sql/connect/odbc/download-odbc-driver-for-sql-server

### Clonare il repository

```bash
git clone https://github.com/<tuo-org>/sql-schema-explorer.git
cd sql-schema-explorer
```

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
python sql_copy_runner.py config.json "pwd" --save-config config.json
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
3. Nella griglia configura per ogni tabella:
   - flag Includi/Escludi per i campi
   - modalità FULL o INCREMENTALE
   - campo data per le tabelle INCREMENTALI
   - colonne di partizione (opzionale)
        │
        ▼
4. Configura destinazione storage (☁ ADLS Config)
   - ADLS Gen2: account, container, autenticazione
   - OneLake: workspace, lakehouse, autenticazione
   - template percorso file
        │
        ▼
5. 💾 Salva JSON  →  config.json
   (la password DB viene cifrata con passphrase)
        │
        ▼
6. Pianifica il runner nel Task Scheduler / cron:
   python sql_copy_runner.py config.json \
     --passphrase-env DB_PWD \
     --save-config config.json   ← aggiorna last_run automaticamente
        │
        ▼
7. Fabric trova i Parquet già pronti su ADLS/OneLake
   e usa i CU solo per analytics
```

---

## Logica incrementale con `last_run`

Il campo `last_run` per ogni tabella viene salvato nel JSON e aggiornato automaticamente dopo ogni esecuzione riuscita.

| Scenario | Query SQL generata |
|----------|-------------------|
| Prima esecuzione (`last_run = null`) | `WHERE CAST([campo] AS DATE) = 'YYYY-MM-DD'` — solo il giorno corrente, con avviso |
| Esecuzioni successive | `WHERE [campo] > '2026-03-25T08:00:00'` — tutto il nuovo dalla run precedente |
| Senza campo data configurato | `SELECT *` completo con avviso nel log |

**Aggiornamento automatico:** usare `--save-config config.json` per aggiornare il file in place dopo ogni esecuzione. Il timestamp viene aggiornato per ogni tabella completata con successo — non per quelle in errore.

---

## Partizionamento Parquet

Quando si abilitano le colonne di partizione, i file vengono scritti nel formato **Hive partitioning**:

```
tabella/
  anno=2026/
    mese=3/
      part-0.parquet
    mese=2/
      part-0.parquet
  anno=2025/
    ...
```

Questo formato è direttamente compatibile con:
- **Fabric Lakehouse** — il motore Spark legge automaticamente le partizioni con predicate pushdown
- **Delta Lake** — compatibile nativo
- **Power BI Direct Lake** — usa le partizioni per ottimizzare le query
- `spark.read.parquet("abfss://...")` — le partizioni vengono riconosciute automaticamente

---

## Destinazione OneLake

### Struttura path

OneLake impone una policy che vieta operazioni sulla root del filesystem. Tutte le scritture avvengono sotto `Files/`:

```
Workspace/
  Lakehouse.Lakehouse/
    Files/
      [sottocartella opzionale]/
        tabella/
          YYYY/MM/DD/
            tabella.parquet
```

### Metodi di autenticazione

OneLake supporta solo **Service Principal** e **Managed Identity** (non Account Key o SAS Token):

| Metodo | Campi richiesti | Uso consigliato |
|--------|----------------|-----------------|
| Service Principal | Tenant ID, Client ID, Client Secret | Produzione, task schedulati |
| Managed Identity | Nessuno | Azure-hosted (VM, ADF, Functions) |

**Permessi necessari** nel Workspace Fabric:
- Vai su `Fabric Portal → Workspace → Manage access`
- Aggiungi il Service Principal con ruolo **Contributor** o superiore
- Il tenant Azure AD deve avere abilitato "Service principals can use Fabric APIs"

---

## Template percorso file

Il template definisce la struttura del percorso su storage. Default:

```
{base}/{table}/{YYYY}/{MM}/{DD}/{file}.parquet
```

Token disponibili:

| Token | Sostituito con | Esempio |
|-------|---------------|---------|
| `{base}` | Valore del campo Cartella base | `raw/sqlserver` |
| `{schema}` | Schema SQL della tabella | `dbo` |
| `{table}` | Nome della tabella | `Clienti` |
| `{YYYY}` | Anno corrente a 4 cifre | `2026` |
| `{MM}` | Mese corrente a 2 cifre | `03` |
| `{DD}` | Giorno corrente a 2 cifre | `26` |
| `{file}` | Nome del file senza estensione | `Clienti` |

Esempi di template personalizzati:

```
# Con schema nel percorso
{base}/{schema}/{table}/{YYYY}/{MM}/{DD}/{file}.parquet

# Partizionamento mensile
{base}/{table}/{YYYY}/{MM}/{file}.parquet

# File con data nel nome
{base}/{table}/{file}_{YYYY}{MM}{DD}.parquet
```

---

## Sicurezza — Cifratura password

La password del database non viene mai salvata in chiaro nel file JSON.

**Algoritmo:** Fernet (AES-128-CBC + HMAC-SHA256)  
**Derivazione chiave:** PBKDF2-HMAC-SHA256, 260.000 iterazioni, salt fisso  
**Passphrase:** non salvata nel file — fornita ad ogni esecuzione

```json
"connection": {
  "server": "myserver",
  "username": "sa",
  "password_enc": "gAAAAABn...(token cifrato Fernet)..."
}
```

Per automazione, usare sempre `--passphrase-env` invece dell'argomento posizionale:

```bash
# Sicuro — la passphrase non appare nella cronologia shell
DB_PWD=MiaPassphrase python sql_copy_runner.py config.json --passphrase-env DB_PWD

# Meno sicuro — visibile nella cronologia
python sql_copy_runner.py config.json "MiaPassphrase"
```

---

## Opzioni SQL Copy Runner — riferimento completo

```
python sql_copy_runner.py <config.json> [passphrase] [opzioni]

Sorgente passphrase (alternativi):
  passphrase            Argomento posizionale (sconsigliato in produzione)
  --passphrase-env VAR  Legge dalla variabile d'ambiente VAR
  --passphrase-file PATH  Legge dalla prima riga del file PATH

Filtri di esecuzione:
  --only-full           Esegue solo le tabelle FULL
  --only-incr           Esegue solo le tabelle INCREMENTALE
  --tables T1,T2,...    Esegue solo le tabelle elencate

Output e salvataggio:
  --dry-run             Mostra il piano senza eseguire nulla
  --save-config PATH    Salva il JSON aggiornato con last_run dopo l'esecuzione
  --log-file PATH       Scrive il log su file (append)
  --no-color            Disabilita i colori ANSI (consigliato per log su file)
  -h, --help            Mostra l'help completo
```

---

## Exit code

| Codice | Significato |
|--------|-------------|
| `0` | Tutte le tabelle copiate con successo |
| `1` | Errore di configurazione o parametri mancanti |
| `2` | Errori parziali: alcune tabelle fallite, altre OK |
| `3` | Errore fatale: connessione SQL o storage impossibile |

---

## Formato file di configurazione JSON

```json
{
  "_meta": {
    "saved_at": "2026-03-26T08:00:00",
    "tool": "SQL Schema Explorer — Regolo Farm",
    "total_tables": 3,
    "total_columns": 42,
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
    "destination": "onelake",
    "workspace_name": "CargoBI-Workspace",
    "lakehouse_name": "LH_Bronze",
    "ol_subfolder": "sqlserver",
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
      "partition_enabled": true,
      "partition_cols": ["anno", "mese"],
      "last_run": "2026-03-25T08:15:42",
      "columns": {
        "ID":           { "include": true,  "data_type": "int",      "is_nullable": false },
        "DataModifica": { "include": true,  "data_type": "datetime", "is_nullable": false },
        "anno":         { "include": true,  "data_type": "int",      "is_nullable": false },
        "mese":         { "include": true,  "data_type": "int",      "is_nullable": false },
        "NoteInterne":  { "include": false, "data_type": "nvarchar", "is_nullable": true  }
      }
    }
  }
}
```

---

## Schedulazione

### Windows Task Scheduler

```
Programma:   python
Argomenti:   "C:\tools\sql_copy_runner.py"
             "C:\config\cargo_bi.json"
             --passphrase-env DB_PWD
             --only-incr
             --save-config "C:\config\cargo_bi.json"
             --log-file "C:\logs\incr.log"
             --no-color
Variabili:   DB_PWD = MiaPassphrase!  (variabile di sistema)
```

### Linux / macOS cron

```bash
# Copia incrementale ogni giorno alle 06:00 — aggiorna last_run in place
0 6 * * * DB_PWD=secret /usr/bin/python3 /opt/tools/sql_copy_runner.py \
    /opt/config/cargo_bi.json --passphrase-env DB_PWD --only-incr \
    --save-config /opt/config/cargo_bi.json \
    --log-file /var/log/sql_copy/incr_$(date +\%Y\%m\%d).log --no-color

# Copia FULL ogni domenica alle 02:00
0 2 * * 0 DB_PWD=secret /usr/bin/python3 /opt/tools/sql_copy_runner.py \
    /opt/config/cargo_bi.json --passphrase-env DB_PWD --only-full \
    --save-config /opt/config/cargo_bi.json \
    --log-file /var/log/sql_copy/full_$(date +\%Y\%m\%d).log --no-color
```

---

## Compatibilità Microsoft Fabric

I file Parquet prodotti sono direttamente compatibili con:

- **Lakehouse** — puntare la tabella alla cartella `{table}/` su ADLS tramite shortcut OneLake
- **OneLake shortcut** — montare il container ADLS Gen2 come shortcut OneLake
- **Direct Lake** — i dataset Power BI in Direct Lake leggono direttamente dai Parquet senza import
- **Notebook / Spark** — `spark.read.parquet("abfss://container@account.dfs.core.windows.net/table/")`
- **Partizionamento Hive** — il motore Spark riconosce automaticamente la struttura `col=val/` e la usa per predicate pushdown

---

## Sviluppato da

**Marco Pozzan**  
AI & Business Intelligence · Microsoft Fabric · Power BI · Azure  
Polo Tecnologico di Pordenone — Friuli Venezia Giulia

> *"Come i navigatori del Quattrocento tracciavano rotte verso l'ignoto, noi guidiamo le imprese verso il futuro dei dati."*

---

## Licenza

MIT License
