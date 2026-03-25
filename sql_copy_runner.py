#!/usr/bin/env python3
"""
sql_copy_runner.py — Regolo Farm
─────────────────────────────────────────────────────────────────────────────
Runner da riga di comando per la copia SQL Server → ADLS Gen2.
Legge la configurazione prodotta da sql_schema_explorer.py e la esegue
senza interfaccia grafica: ideale per task schedulati, pipeline CI/CD,
Azure Data Factory custom activity, ecc.

USO:
    python sql_copy_runner.py <config.json> [passphrase] [opzioni]

ARGOMENTI:
    config.json     Percorso al file di configurazione (obbligatorio)
    passphrase      Passphrase per decifrare la password DB (obbligatorio se
                    il file contiene "password_enc", opzionale altrimenti).
                    In alternativa usa --passphrase-env o --passphrase-file.

OPZIONI:
    --passphrase-env  VAR   Legge la passphrase dalla variabile d'ambiente VAR
    --passphrase-file PATH  Legge la passphrase dalla prima riga del file PATH
    --only-full             Esegue solo le tabelle FULL  (salta INCREMENTALE)
    --only-incr             Esegue solo le tabelle INCREMENTALE (salta FULL)
    --tables T1,T2,...      Filtra: esegue solo le tabelle elencate
    --dry-run               Mostra il piano senza eseguire nulla
    --log-file PATH         Scrive il log anche su file (append)
    --no-color              Disabilita colori ANSI nel terminale
    -h / --help             Mostra questo help

ESEMPI:
    python sql_copy_runner.py config.json "MiaPassphrase!"
    python sql_copy_runner.py config.json --passphrase-env DB_PWD
    python sql_copy_runner.py config.json "pwd" --only-incr --log-file run.log
    python sql_copy_runner.py config.json "pwd" --tables dbo.Clienti,dbo.Ordini
    python sql_copy_runner.py config.json "pwd" --dry-run

EXIT CODE:
    0   Successo completo
    1   Errore di configurazione / parametri
    2   Uno o più errori durante la copia (ma il resto è completato)
    3   Errore fatale (connessione impossibile)
"""

import sys
import os
import json
import argparse
import io
from datetime import datetime

# ── Colori ANSI ───────────────────────────────────────────────────────────────
_USE_COLOR = sys.stdout.isatty()   # sovrascritto da --no-color

class _C:
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    GREEN  = "\033[32m"
    YELLOW = "\033[33m"
    RED    = "\033[31m"
    CYAN   = "\033[36m"
    BLUE   = "\033[34m"
    GRAY   = "\033[90m"

def _col(text, *codes):
    if not _USE_COLOR:
        return text
    return "".join(codes) + text + _C.RESET


# ── Logging ───────────────────────────────────────────────────────────────────
_log_file = None   # file handle opzionale

def _log(msg: str, level: str = "INFO"):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = f"[{ts}]"

    colored = {
        "INFO":  _col(f"{prefix} {msg}", _C.CYAN),
        "OK":    _col(f"{prefix} {msg}", _C.GREEN, _C.BOLD),
        "WARN":  _col(f"{prefix} {msg}", _C.YELLOW),
        "ERROR": _col(f"{prefix} {msg}", _C.RED, _C.BOLD),
        "DRY":   _col(f"{prefix} {msg}", _C.BLUE),
        "HEAD":  _col(f"{prefix} {msg}", _C.BOLD),
    }.get(level, f"{prefix} {msg}")

    plain = f"[{ts}] [{level:5s}] {msg}"

    print(colored)
    if _log_file:
        _log_file.write(plain + "\n")
        _log_file.flush()


# ── Import dal modulo principale (senza avviare la GUI) ───────────────────────
def _import_core():
    """
    Importa le funzioni condivise da sql_schema_explorer.py che deve
    trovarsi nella stessa directory di questo script.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)
    try:
        import sql_schema_explorer as core
        return core
    except ImportError as ex:
        _log(f"Impossibile importare sql_schema_explorer.py: {ex}", "ERROR")
        _log("Assicurati che sql_schema_explorer.py sia nella stessa cartella.", "ERROR")
        sys.exit(1)


# ── Lettura configurazione ────────────────────────────────────────────────────
def load_config(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        _log(f"File non trovato: {path}", "ERROR")
        sys.exit(1)
    except json.JSONDecodeError as ex:
        _log(f"File JSON non valido: {ex}", "ERROR")
        sys.exit(1)


def resolve_passphrase(args, config: dict) -> str | None:
    """Determina la passphrase dalla sorgente indicata."""
    # 1. Da variabile d'ambiente
    if args.passphrase_env:
        val = os.environ.get(args.passphrase_env, "")
        if not val:
            _log(f"Variabile d'ambiente '{args.passphrase_env}' non trovata o vuota.", "ERROR")
            sys.exit(1)
        return val
    # 2. Da file
    if args.passphrase_file:
        try:
            with open(args.passphrase_file, "r", encoding="utf-8") as f:
                val = f.readline().rstrip("\n")
            if not val:
                _log(f"Il file passphrase '{args.passphrase_file}' e' vuoto.", "ERROR")
                sys.exit(1)
            return val
        except FileNotFoundError:
            _log(f"File passphrase non trovato: {args.passphrase_file}", "ERROR")
            sys.exit(1)
    # 3. Argomento posizionale
    if args.passphrase:
        return args.passphrase
    # 4. Nessuna passphrase — ok solo se il file non cifra la password
    return None


# ── Costruzione piano di esecuzione ──────────────────────────────────────────
def build_plan(tables_cfg: dict, only_mode: str | None,
               filter_tables: list[str] | None) -> list[dict]:
    """
    Costruisce la lista di tabelle da eseguire applicando i filtri.
    """
    plan = []
    for tkey, tdata in tables_cfg.items():
        schema    = tdata.get("schema", "")
        tbl_name  = tdata.get("table_name", "")
        load_mode = tdata.get("load_mode", "FULL")
        incr_fld  = tdata.get("incr_field", "")

        # Filtro modalità
        if only_mode == "full"  and load_mode != "FULL":        continue
        if only_mode == "incr"  and load_mode != "INCREMENTALE": continue

        # Filtro tabelle specifiche
        if filter_tables:
            match = (tkey in filter_tables
                     or tbl_name in filter_tables
                     or f"{schema}.{tbl_name}" in filter_tables)
            if not match:
                continue

        # Colonne incluse
        cols = [col for col, cdata in tdata.get("columns", {}).items()
                if cdata.get("include", True)]
        if not cols:
            continue   # tabella senza colonne incluse: salta

        plan.append({
            "schema":      schema,
            "table_name":  tbl_name,
            "object_type": tdata.get("object_type", "TABLE"),
            "load_mode":   load_mode,
            "incr_field":  incr_fld,
            "columns":     cols,
        })
    return plan


# ── Stampa piano (dry-run o riepilogo) ───────────────────────────────────────
def print_plan(plan: list[dict], adls_cfg: dict, dry_run: bool):
    now  = datetime.now()
    base = adls_cfg.get("base_folder", "").strip("/")
    tmpl = adls_cfg.get("path_template",
                        "{base}/{table}/{YYYY}/{MM}/{DD}/{file}.parquet")

    mode_tag = "[DRY-RUN] " if dry_run else ""
    _log(f"{mode_tag}Piano di esecuzione — {len(plan)} tabelle", "HEAD")
    _log(f"  Account  : {adls_cfg.get('account_name','?')}", "INFO")
    _log(f"  Container: {adls_cfg.get('container','?')}", "INFO")
    _log(f"  Template : {tmpl}", "INFO")
    _log(f"  Auth     : {adls_cfg.get('auth_method','?')}", "INFO")
    _log("─" * 70, "INFO")

    n_full = sum(1 for t in plan if t["load_mode"] == "FULL")
    n_incr = len(plan) - n_full
    n_warn = sum(1 for t in plan
                 if t["load_mode"] == "INCREMENTALE" and not t.get("incr_field"))

    for tbl in plan:
        mode  = tbl["load_mode"]
        fld   = tbl.get("incr_field","")
        ncols = len(tbl["columns"])
        tag   = "⊞ FULL" if mode == "FULL" else "⟳ INCR"
        warn  = " ⚠ nessun campo data" if mode == "INCREMENTALE" and not fld else ""
        incr_info = f" [{fld}]" if fld else ""

        # Calcola percorso di esempio
        try:
            from sql_schema_explorer import resolve_path_template
            dest = resolve_path_template(tmpl, base=base,
                                         schema=tbl["schema"],
                                         table=tbl["table_name"],
                                         now=now, file_name=tbl["table_name"])
        except Exception:
            dest = "?"

        _log(f"  {tag:8s}  {tbl['schema']}.{tbl['table_name']:30s}"
             f"  {ncols:3d} col  →  {dest}{incr_info}{warn}", "INFO")

    _log("─" * 70, "INFO")
    _log(f"  Totale: {len(plan)}  |  FULL: {n_full}  |  INCR: {n_incr}"
         + (f"  |  ⚠ {n_warn} senza campo data" if n_warn else ""), "INFO")


# ── Esecuzione copia (logica inline — non usa queue/thread) ──────────────────
def run_copy(sql_ci: dict, adls_cfg: dict, plan: list[dict]) -> int:
    """
    Esegue la copia in modo sincrono, scrivendo direttamente su stdout/log.
    Restituisce il numero di errori.
    """
    import io as _io

    # Import dal modulo principale
    core = _import_core()

    # Connessione SQL
    _log("Connessione a SQL Server…", "INFO")
    try:
        conn = core.get_sql_connection(sql_ci)
        _log(f"Connesso: {sql_ci['server']} / {sql_ci['database']}", "OK")
    except Exception as ex:
        _log(f"Errore connessione SQL Server: {ex}", "ERROR")
        return -1   # fatale

    # Connessione ADLS
    _log("Connessione ad ADLS Gen2…", "INFO")
    try:
        adls_client = core.get_adls_client(adls_cfg)
        _log(f"Connesso: {adls_cfg['account_name']}.dfs.core.windows.net", "OK")
    except Exception as ex:
        _log(f"Errore connessione ADLS: {ex}", "ERROR")
        conn.close()
        return -1   # fatale

    now       = datetime.now()
    container = adls_cfg["container"]
    base      = adls_cfg.get("base_folder", "").strip("/")
    tmpl      = adls_cfg.get("path_template",
                              "{base}/{table}/{YYYY}/{MM}/{DD}/{file}.parquet")

    try:
        import pandas as pd
    except ImportError:
        _log("Il modulo 'pandas' non e' installato. pip install pandas", "ERROR")
        conn.close()
        return -1

    errors = 0
    total  = len(plan)

    _log("=" * 70, "INFO")

    for idx, tbl in enumerate(plan, 1):
        tname      = tbl["table_name"]
        schema     = tbl["schema"]
        cols       = tbl["columns"]
        mode       = tbl["load_mode"]
        incr_field = tbl.get("incr_field", "")
        fqn        = f"[{schema}].[{tname}]"
        col_sql    = ", ".join(f"[{c}]" for c in cols)

        _log(f"[{idx}/{total}] {fqn}  [{mode}]  ({len(cols)} colonne)", "HEAD")

        try:
            # ── Query ─────────────────────────────────────────────────────
            if mode == "FULL":
                sql_q = f"SELECT {col_sql} FROM {fqn}"
                _log(f"  SELECT * FROM {fqn}", "INFO")
            else:
                if incr_field:
                    date_filter = now.strftime("%Y-%m-%d")
                    sql_q = (f"SELECT {col_sql} FROM {fqn} "
                             f"WHERE CAST([{incr_field}] AS DATE) = '{date_filter}'")
                    _log(f"  INCR  [{incr_field}] = {date_filter}", "INFO")
                else:
                    sql_q = f"SELECT {col_sql} FROM {fqn}"
                    _log("  INCR senza campo data: SELECT completo (nessun filtro)", "WARN")

            df = pd.read_sql(sql_q, conn)
            _log(f"  Lette {len(df):,} righe", "OK")

            # ── Percorsi ──────────────────────────────────────────────────
            dest_file = core.resolve_path_template(
                tmpl, base=base, schema=schema, table=tname,
                now=now, file_name=tname)
            tbl_root = core.resolve_path_template(
                "{base}/{table}", base=base, schema=schema, table=tname,
                now=now, file_name=tname)

            # ── Upload ────────────────────────────────────────────────────
            if mode == "FULL":
                _log(f"  FULL: elimino {tbl_root}/…", "WARN")
                core.adls_delete_folder(adls_client, container, tbl_root)

            _log(f"  Upload: {dest_file}", "INFO")
            core.adls_upload_parquet(adls_client, container, dest_file, df)
            _log(f"  Completato  ({len(df):,} righe  →  {dest_file})", "OK")

        except Exception as ex:
            _log(f"  ERRORE: {ex}", "ERROR")
            errors += 1

        _log("", "INFO")

    conn.close()
    _log("=" * 70, "INFO")
    ok = total - errors
    if errors == 0:
        _log(f"Completato con successo: {ok}/{total} tabelle", "OK")
    else:
        _log(f"Completato con errori: {ok} OK, {errors} ERRORI su {total} tabelle", "WARN")

    return errors


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    global _USE_COLOR, _log_file

    parser = argparse.ArgumentParser(
        prog="sql_copy_runner.py",
        description="Copia dati SQL Server → ADLS Gen2 da configurazione JSON.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("USO:")[1] if "USO:" in __doc__ else "")

    parser.add_argument("config",
        help="Percorso al file di configurazione JSON")
    parser.add_argument("passphrase", nargs="?", default=None,
        help="Passphrase per decifrare la password DB (se cifrata nel JSON)")
    parser.add_argument("--passphrase-env", metavar="VAR",
        help="Legge la passphrase dalla variabile d'ambiente VAR")
    parser.add_argument("--passphrase-file", metavar="PATH",
        help="Legge la passphrase dalla prima riga del file PATH")
    parser.add_argument("--only-full", action="store_true",
        help="Esegue solo le tabelle con modalita' FULL")
    parser.add_argument("--only-incr", action="store_true",
        help="Esegue solo le tabelle con modalita' INCREMENTALE")
    parser.add_argument("--tables", metavar="T1,T2,...",
        help="Esegue solo le tabelle elencate (es. dbo.Clienti,dbo.Ordini)")
    parser.add_argument("--dry-run", action="store_true",
        help="Mostra il piano senza eseguire nulla")
    parser.add_argument("--log-file", metavar="PATH",
        help="Scrive il log anche su file (append)")
    parser.add_argument("--no-color", action="store_true",
        help="Disabilita i colori ANSI")

    args = parser.parse_args()

    # Configurazione output
    if args.no_color:
        _USE_COLOR = False
    if args.log_file:
        try:
            _log_file = open(args.log_file, "a", encoding="utf-8")
        except OSError as ex:
            print(f"Impossibile aprire il file di log '{args.log_file}': {ex}")
            sys.exit(1)

    # Validazione conflitti
    if args.only_full and args.only_incr:
        _log("--only-full e --only-incr sono mutuamente esclusivi.", "ERROR")
        sys.exit(1)

    _log(_col("SQL Copy Runner — Regolo Farm", _C.BOLD), "HEAD")
    _log(f"Config: {os.path.abspath(args.config)}", "INFO")
    _log(f"Avvio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "INFO")

    # ── Carica config ──────────────────────────────────────────────────────
    cfg = load_config(args.config)

    meta        = cfg.get("_meta", {})
    conn_params = cfg.get("connection", {})
    adls_cfg    = cfg.get("adls", {})
    tables_cfg  = cfg.get("tables", {})

    _log(f"Salvato il: {meta.get('saved_at','n/d')}  "
         f"Tabelle: {meta.get('total_tables','?')}  "
         f"Campi: {meta.get('total_columns','?')}", "INFO")

    if not tables_cfg:
        _log("Il file JSON non contiene dati di schema ('tables').", "ERROR")
        sys.exit(1)
    if not adls_cfg or not adls_cfg.get("account_name"):
        _log("Configurazione ADLS assente o incompleta nel file JSON.", "ERROR")
        sys.exit(1)
    if not conn_params or not conn_params.get("server"):
        _log("Configurazione connessione SQL Server assente nel file JSON.", "ERROR")
        sys.exit(1)

    # ── Passphrase e decifratura ───────────────────────────────────────────
    passphrase = resolve_passphrase(args, cfg)
    has_enc    = bool(conn_params.get("password_enc"))

    if has_enc:
        if not passphrase:
            _log("Il file contiene una password cifrata (password_enc) "
                 "ma non e' stata fornita la passphrase.", "ERROR")
            _log("Usa:  python sql_copy_runner.py config.json <passphrase>", "ERROR")
            _log("oppure: --passphrase-env NOME_VAR  o  --passphrase-file path", "ERROR")
            sys.exit(1)
        _log("Decifratura password DB…", "INFO")
        core = _import_core()
        try:
            plain_pwd = core.decrypt_password(conn_params["password_enc"], passphrase)
            conn_params = dict(conn_params)
            conn_params["password"] = plain_pwd
            conn_params.pop("password_enc", None)
            _log("Password decifrata con successo.", "OK")
        except ValueError as ex:
            _log(str(ex), "ERROR")
            sys.exit(1)
        except RuntimeError as ex:
            _log(str(ex), "ERROR")
            sys.exit(1)
    else:
        # Nessuna cifratura: usa password in chiaro se presente
        conn_params.setdefault("password", "")
        if passphrase and not has_enc:
            _log("Passphrase fornita ma il file non contiene password cifrata "
                 "(campo 'password_enc' assente): passphrase ignorata.", "WARN")

    # ── Costruzione piano ──────────────────────────────────────────────────
    only_mode     = "full" if args.only_full else ("incr" if args.only_incr else None)
    filter_tables = [t.strip() for t in args.tables.split(",")] if args.tables else None
    plan          = build_plan(tables_cfg, only_mode, filter_tables)

    if not plan:
        _log("Nessuna tabella da eseguire con i filtri applicati.", "WARN")
        sys.exit(0)

    # ── Dry-run ────────────────────────────────────────────────────────────
    print_plan(plan, adls_cfg, dry_run=args.dry_run)

    if args.dry_run:
        _log("DRY-RUN completato. Nessun dato e' stato letto o scritto.", "DRY")
        sys.exit(0)

    # ── Esecuzione reale ───────────────────────────────────────────────────
    errors = run_copy(conn_params, adls_cfg, plan)

    if _log_file:
        _log_file.close()

    if errors == -1:
        sys.exit(3)   # errore fatale di connessione
    elif errors > 0:
        sys.exit(2)   # errori parziali
    else:
        sys.exit(0)   # successo


if __name__ == "__main__":
    main()
