"""
SQL Schema Explorer — Regolo Farm
─────────────────────────────────
• Connessione SQL Server con profili configurabili
• Griglia gerarchica tabelle → campi  (flag includi / caricamento FULL|INCR)
• Configurazione Azure Data Lake Storage Gen2
• Esportazione parquet su ADLS Gen2  (full = delete + write, path: tabella/YYYY/MM/DD/)
• Salvataggio / caricamento configurazione completa su JSON
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import threading
import json
import csv
import io
import queue
import base64
import hashlib
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════════════════
#  CIFRATURA PASSWORD — Fernet (AES-128-CBC + HMAC-SHA256)
#  La chiave è derivata dalla passphrase con PBKDF2-HMAC-SHA256.
#  La passphrase NON viene salvata nel file JSON.
# ═══════════════════════════════════════════════════════════════════════════════
_SALT = b"RegoloFarm_SQLExplorer_2026"   # salt fisso — non segreto

def _derive_key(passphrase: str) -> bytes:
    """Deriva una chiave Fernet a 32 byte dalla passphrase con PBKDF2."""
    dk = hashlib.pbkdf2_hmac("sha256", passphrase.encode("utf-8"),
                              _SALT, iterations=260_000, dklen=32)
    return base64.urlsafe_b64encode(dk)


def encrypt_password(plaintext: str, passphrase: str) -> str:
    """Cifra il testo e restituisce una stringa base64url (Fernet token)."""
    if not plaintext:
        return ""
    try:
        from cryptography.fernet import Fernet
    except ImportError:
        raise RuntimeError(
            "Il modulo 'cryptography' non e' installato.\n"
            "Installarlo con:  pip install cryptography")
    key = _derive_key(passphrase)
    return Fernet(key).encrypt(plaintext.encode("utf-8")).decode("ascii")


def decrypt_password(token: str, passphrase: str) -> str:
    """Decifra un token Fernet e restituisce la password in chiaro."""
    if not token:
        return ""
    try:
        from cryptography.fernet import Fernet, InvalidToken
    except ImportError:
        raise RuntimeError(
            "Il modulo 'cryptography' non e' installato.\n"
            "Installarlo con:  pip install cryptography")
    try:
        key = _derive_key(passphrase)
        return Fernet(key).decrypt(token.encode("ascii")).decode("utf-8")
    except InvalidToken:
        raise ValueError("Passphrase errata o file corrotto: impossibile decifrare la password.")


# ═══════════════════════════════════════════════════════════════════════════════
#  PROFILI DI CONNESSIONE SQL SERVER
# ═══════════════════════════════════════════════════════════════════════════════
PROFILES: dict = {
    "Locale (Windows Auth)": {
        "server": "localhost", "database": "", "port": "",
        "driver": "ODBC Driver 18 for SQL Server",
        "win_auth": True, "username": "", "password": "",
        "trust_cert": True, "encrypt": False,
    },
    # "Azure SQL": {
    #     "server": "myserver.database.windows.net", "database": "MyDB", "port": "1433",
    #     "driver": "ODBC Driver 18 for SQL Server",
    #     "win_auth": False, "username": "sqladmin", "password": "",
    #     "trust_cert": False, "encrypt": True,
    # },
}

# ── Colori Regolo Farm ────────────────────────────────────────────────────────
C_NAVY      = "#1B3A6B"
C_ORANGE    = "#C55A11"
C_SKY       = "#BDD7EE"
C_SKY_LIGHT = "#D6E9F8"
C_WHITE     = "#FFFFFF"
C_GRAY_BG   = "#F4F7FB"
C_GRAY_LINE = "#D0DCE8"
C_TEXT      = "#1A2A3A"
C_TEXT_MUT  = "#5A7090"
C_GREEN     = "#217346"
C_RED       = "#C00000"
C_HOVER     = "#16305A"
C_ROW_ALT   = "#EBF3FB"
C_TEAL      = "#0D7377"   # ADLS accent

FONT_HEAD  = ("Calibri", 11, "bold")
FONT_BODY  = ("Calibri", 10)
FONT_MONO  = ("Consolas", 9)
FONT_SMALL = ("Calibri", 9)


# ═══════════════════════════════════════════════════════════════════════════════
#  HELPERS SQL SERVER
# ═══════════════════════════════════════════════════════════════════════════════
def get_sql_connection(ci: dict):
    import pyodbc
    srv   = f"{ci['server']},{ci['port']}" if ci.get("port") else ci["server"]
    trust = "yes" if ci.get("trust_cert", True)  else "no"
    enc   = "yes" if ci.get("encrypt",    False) else "no"
    drv   = ci.get("driver", "ODBC Driver 18 for SQL Server")
    if ci.get("win_auth", True):
        cs = (f"DRIVER={{{drv}}};SERVER={srv};DATABASE={ci['database']};"
              f"Trusted_Connection=yes;TrustServerCertificate={trust};Encrypt={enc};")
    else:
        cs = (f"DRIVER={{{drv}}};SERVER={srv};DATABASE={ci['database']};"
              f"UID={ci['username']};PWD={ci['password']};"
              f"TrustServerCertificate={trust};Encrypt={enc};")
    return pyodbc.connect(cs, timeout=10)


def fetch_schema(conn):
    sql = """
    SELECT CASE WHEN o.type='V' THEN 'VIEW' ELSE 'TABLE' END,
           s.name, o.name, c.name, tp.name,
           c.max_length, c.is_nullable, c.column_id
    FROM sys.columns c
    JOIN sys.objects o  ON c.object_id=o.object_id
    JOIN sys.schemas s  ON o.schema_id=s.schema_id
    JOIN sys.types   tp ON c.user_type_id=tp.user_type_id
    WHERE o.type IN ('U','V') AND o.is_ms_shipped=0
    ORDER BY s.name, o.name, c.column_id"""
    cur = conn.cursor(); cur.execute(sql)
    return [{"object_type":r[0],"schema":r[1],"table_name":r[2],
             "column_name":r[3],"data_type":r[4],
             "max_length":r[5],"is_nullable":r[6],"column_id":r[7]}
            for r in cur.fetchall()]


# ═══════════════════════════════════════════════════════════════════════════════
#  HELPERS ADLS GEN2
# ═══════════════════════════════════════════════════════════════════════════════
def get_adls_client(ac: dict):
    """Restituisce DataLakeServiceClient in base al metodo di autenticazione."""
    from azure.storage.filedatalake import DataLakeServiceClient
    method = ac.get("auth_method", "account_key")
    account = ac["account_name"]
    url = f"https://{account}.dfs.core.windows.net"
    if method == "account_key":
        from azure.storage.filedatalake import DataLakeServiceClient
        return DataLakeServiceClient(account_url=url,
                                     credential=ac["account_key"])
    elif method == "sas_token":
        return DataLakeServiceClient(account_url=url + "?" + ac["sas_token"].lstrip("?"))
    elif method == "service_principal":
        from azure.identity import ClientSecretCredential
        cred = ClientSecretCredential(ac["tenant_id"], ac["client_id"], ac["client_secret"])
        return DataLakeServiceClient(account_url=url, credential=cred)
    elif method == "managed_identity":
        from azure.identity import ManagedIdentityCredential
        return DataLakeServiceClient(account_url=url, credential=ManagedIdentityCredential())
    raise ValueError(f"Metodo di autenticazione non supportato: {method}")


def adls_upload_parquet(client, container: str, blob_path: str, df):
    """Carica un DataFrame pandas come parquet su ADLS Gen2."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    buf = io.BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buf)
    buf.seek(0)
    fs_client  = client.get_file_system_client(container)
    file_client = fs_client.get_file_client(blob_path)
    file_client.create_file()
    data = buf.getvalue()
    file_client.append_data(data, offset=0, length=len(data))
    file_client.flush_data(len(data))


def adls_delete_folder(client, container: str, folder_path: str):
    """Elimina ricorsivamente una cartella su ADLS Gen2 (se esiste)."""
    try:
        fs = client.get_file_system_client(container)
        fs.get_directory_client(folder_path).delete_directory()
    except Exception:
        pass   # cartella inesistente: ok


# ═══════════════════════════════════════════════════════════════════════════════
#  HELPER — RISOLUZIONE TEMPLATE PERCORSO
# ═══════════════════════════════════════════════════════════════════════════════
def resolve_path_template(template: str, base: str, schema: str,
                           table: str, now: datetime, file_name: str) -> str:
    """
    Sostituisce i token nel template percorso con i valori reali.
    Token supportati:
      {base}   cartella base (es. raw/sqlserver)
      {schema} schema SQL    (es. dbo)
      {table}  nome tabella  (es. Clienti)
      {YYYY}   anno 4 cifre  (es. 2026)
      {MM}     mese 2 cifre  (es. 03)
      {DD}     giorno 2 cifre(es. 26)
      {file}   nome file senza estensione (uguale a {table} per default)
    L'eventuale estensione .parquet nel template viene mantenuta.
    Segmenti vuoti da {base} assente vengono rimossi.
    """
    result = template
    result = result.replace("{base}",   base)
    result = result.replace("{schema}", schema)
    result = result.replace("{table}",  table)
    result = result.replace("{YYYY}",   now.strftime("%Y"))
    result = result.replace("{MM}",     now.strftime("%m"))
    result = result.replace("{DD}",     now.strftime("%d"))
    result = result.replace("{file}",   file_name)
    # Normalizza: rimuovi segmenti vuoti e slash doppi
    parts = [p for p in result.split("/") if p]
    return "/".join(parts)


# ═══════════════════════════════════════════════════════════════════════════════
#  DIALOG — CONNESSIONE SQL SERVER
# ═══════════════════════════════════════════════════════════════════════════════
class ConnectionDialog(tk.Toplevel):
    def __init__(self, parent, initial_params=None):
        super().__init__(parent)
        self.result   = None
        self._profiles = dict(PROFILES)
        self._initial  = initial_params
        self.title("Connessione SQL Server")
        self.configure(bg=C_NAVY)
        self.resizable(False, False)
        self.grab_set(); self.transient(parent)
        self._build()
        self.update_idletasks()
        self.geometry(f"+{parent.winfo_x()+(parent.winfo_width()-self.winfo_width())//2}"
                      f"+{parent.winfo_y()+(parent.winfo_height()-self.winfo_height())//2}")

    def _build(self):
        hdr = tk.Frame(self, bg=C_NAVY, padx=24, pady=14); hdr.pack(fill="x")
        tk.Label(hdr, text="⚓  Connessione SQL Server", font=("Calibri Light",14,"bold"),
                 bg=C_NAVY, fg=C_WHITE).pack(side="left")
        tk.Label(hdr, text="Regolo Farm", font=FONT_SMALL, bg=C_NAVY, fg=C_SKY).pack(side="right")

        pf = tk.Frame(self, bg=C_SKY_LIGHT, padx=20, pady=10); pf.pack(fill="x")
        tk.Label(pf, text="Profilo:", font=FONT_HEAD, bg=C_SKY_LIGHT, fg=C_NAVY).pack(side="left")
        self._pvar = tk.StringVar()
        names = ["— Nuovo —"] + sorted(self._profiles)
        cb = ttk.Combobox(pf, textvariable=self._pvar, values=names,
                          state="readonly", font=FONT_BODY, width=30)
        cb.pack(side="left", padx=(8,10))
        cb.bind("<<ComboboxSelected>>", self._load_profile)
        if self._profiles: self._pvar.set(sorted(self._profiles)[0])
        tk.Label(pf, text="✏ Modifica PROFILES in cima al file .py",
                 font=FONT_SMALL, bg=C_SKY_LIGHT, fg=C_TEXT_MUT).pack(side="right")

        body = tk.Frame(self, bg=C_WHITE, padx=28, pady=20); body.pack(fill="both", expand=True)
        def lbl(t,r): tk.Label(body,text=t,font=FONT_HEAD,bg=C_WHITE,fg=C_TEXT,anchor="w").grid(row=r,column=0,sticky="w",pady=4)
        def ent(r,show=None,w=34):
            e=tk.Entry(body,font=FONT_BODY,bg=C_GRAY_BG,fg=C_TEXT,relief="flat",bd=0,
                       highlightthickness=1,highlightbackground=C_GRAY_LINE,
                       highlightcolor=C_ORANGE,width=w,show=show)
            e.grid(row=r,column=1,sticky="ew",padx=(12,0),pady=4); return e

        lbl("Server / Istanza",0); self.e_srv = ent(0)
        lbl("Database",        1); self.e_db  = ent(1)
        lbl("Porta (opz.)",    2); self.e_prt = ent(2,w=10)
        tk.Label(body,text="vuoto=1433",font=FONT_SMALL,bg=C_WHITE,fg=C_TEXT_MUT).grid(row=2,column=1,sticky="e")
        lbl("Driver ODBC",3)
        self._drv = tk.StringVar()
        ttk.Combobox(body,textvariable=self._drv,font=FONT_BODY,width=32,
                     values=["ODBC Driver 18 for SQL Server",
                             "ODBC Driver 17 for SQL Server","SQL Server"]
                     ).grid(row=3,column=1,sticky="ew",padx=(12,0),pady=4)

        self._wa = tk.BooleanVar(value=True)
        af=tk.Frame(body,bg=C_WHITE); af.grid(row=4,column=0,columnspan=2,sticky="w",pady=(8,4))
        tk.Label(af,text="Autenticazione:",font=FONT_HEAD,bg=C_WHITE,fg=C_TEXT).pack(side="left")
        for t,v in [("Windows",True),("SQL Server",False)]:
            tk.Radiobutton(af,text=t,variable=self._wa,value=v,bg=C_WHITE,fg=C_TEXT,
                           font=FONT_BODY,activebackground=C_WHITE,
                           command=self._tog).pack(side="left",padx=(8,2))

        lbl("Utente",  5); self.e_usr = ent(5)
        lbl("Password",6); self.e_pwd = ent(6,show="●")
        of=tk.Frame(body,bg=C_WHITE); of.grid(row=7,column=0,columnspan=2,sticky="w",pady=(6,0))
        self._tc=tk.BooleanVar(value=True); self._enc=tk.BooleanVar(value=False)
        tk.Checkbutton(of,text="TrustServerCertificate",variable=self._tc,bg=C_WHITE,fg=C_TEXT,font=FONT_SMALL,activebackground=C_WHITE).pack(side="left",padx=(0,14))
        tk.Checkbutton(of,text="Encrypt=yes",variable=self._enc,bg=C_WHITE,fg=C_TEXT,font=FONT_SMALL,activebackground=C_WHITE).pack(side="left")
        body.columnconfigure(1,weight=1)

        pv=tk.Frame(self,bg=C_GRAY_BG,padx=20,pady=8); pv.pack(fill="x")
        ph=tk.Frame(pv,bg=C_GRAY_BG); ph.pack(fill="x")
        tk.Label(ph,text="Connection string preview:",font=FONT_SMALL,bg=C_GRAY_BG,fg=C_TEXT_MUT).pack(side="left")
        self._mk(ph,"📋 Copia snippet",self._snip,C_NAVY,C_WHITE).pack(side="right")
        self._prev=tk.StringVar()
        tk.Label(pv,textvariable=self._prev,font=FONT_MONO,bg=C_GRAY_BG,fg=C_NAVY,wraplength=520,justify="left").pack(anchor="w",pady=(4,0))
        for v in (self._drv,self._wa,self._tc,self._enc): v.trace_add("write",self._upd)
        for e in (self.e_srv,self.e_db,self.e_prt,self.e_usr): e.bind("<KeyRelease>",lambda _:self._upd())

        bf=tk.Frame(self,bg=C_WHITE,pady=14); bf.pack()
        self._mk(bf,"  Connetti  ",self._ok,C_ORANGE,C_WHITE).pack(side="left",padx=6)
        self._mk(bf,"  Annulla  ",self.destroy,C_GRAY_LINE,C_TEXT).pack(side="left",padx=6)

        if self._initial: self._fill(self._initial)
        else: self._load_profile()

    def _mk(self,p,t,c,bg,fg):
        return tk.Button(p,text=t,command=c,font=FONT_HEAD,bg=bg,fg=fg,relief="flat",bd=0,
                         padx=14,pady=7,cursor="hand2",activebackground=C_HOVER,activeforeground=C_WHITE)

    def _tog(self):
        s="disabled" if self._wa.get() else "normal"
        self.e_usr.configure(state=s); self.e_pwd.configure(state=s); self._upd()

    def _upd(self,*_):
        srv=self.e_srv.get().strip() or "<server>"; db=self.e_db.get().strip() or "<db>"
        p=self.e_prt.get().strip(); s=f"{srv},{p}" if p else srv
        d=self._drv.get(); trust="yes" if self._tc.get() else "no"; enc="yes" if self._enc.get() else "no"
        if self._wa.get():
            cs=f"DRIVER={{{d}}};SERVER={s};DATABASE={db};Trusted_Connection=yes;TrustServerCertificate={trust};Encrypt={enc};"
        else:
            u=self.e_usr.get().strip() or "<user>"
            cs=f"DRIVER={{{d}}};SERVER={s};DATABASE={db};UID={u};PWD=***;TrustServerCertificate={trust};Encrypt={enc};"
        self._prev.set(cs)

    def _load_profile(self,_=None):
        n=self._pvar.get()
        if n=="— Nuovo —": return
        p=self._profiles.get(n,{}); p and self._fill(p)

    def _fill(self,p):
        for e,k in [(self.e_srv,"server"),(self.e_db,"database"),(self.e_prt,"port"),(self.e_usr,"username")]:
            e.delete(0,"end"); e.insert(0,p.get(k,""))
        self.e_pwd.delete(0,"end"); self.e_pwd.insert(0,p.get("password",""))
        self._drv.set(p.get("driver","ODBC Driver 18 for SQL Server"))
        self._wa.set(p.get("win_auth",True)); self._tc.set(p.get("trust_cert",True))
        self._enc.set(p.get("encrypt",False)); self._tog()

    def _snip(self):
        n=self._pvar.get(); label=n if n!="— Nuovo —" else "Nuovo profilo"
        p=self._params()
        s=(f'    "{label}": {{\n'
           f'        "server":"{p["server"]}","database":"{p["database"]}","port":"{p["port"]}",\n'
           f'        "driver":"{p["driver"]}","win_auth":{p["win_auth"]},\n'
           f'        "username":"{p["username"]}","password":"{p["password"]}",\n'
           f'        "trust_cert":{p["trust_cert"]},"encrypt":{p["encrypt"]},\n    }},')
        self.clipboard_clear(); self.clipboard_append(s)
        messagebox.showinfo("Snippet copiato","Incollalo nella sezione PROFILES.",parent=self)

    def _params(self):
        return {"server":self.e_srv.get().strip(),"database":self.e_db.get().strip(),
                "port":self.e_prt.get().strip(),"driver":self._drv.get(),
                "win_auth":self._wa.get(),"username":self.e_usr.get().strip(),
                "password":self.e_pwd.get(),"trust_cert":self._tc.get(),"encrypt":self._enc.get()}

    def _ok(self):
        if not self.e_srv.get().strip() or not self.e_db.get().strip():
            messagebox.showerror("Errore","Server e Database obbligatori.",parent=self); return
        self.result=self._params(); self.destroy()


# ═══════════════════════════════════════════════════════════════════════════════
#  DIALOG — AZURE DATA LAKE STORAGE GEN2
# ═══════════════════════════════════════════════════════════════════════════════
class AdlsDialog(tk.Toplevel):
    def __init__(self, parent, initial=None):
        super().__init__(parent)
        self.result  = None
        self._init   = initial or {}
        self.title("Azure Data Lake Storage Gen2")
        self.configure(bg=C_TEAL)
        self.resizable(False, False)
        self.grab_set(); self.transient(parent)
        self._build()
        self.update_idletasks()
        self.geometry(f"+{parent.winfo_x()+(parent.winfo_width()-self.winfo_width())//2}"
                      f"+{parent.winfo_y()+(parent.winfo_height()-self.winfo_height())//2}")

    # Token disponibili per il template percorso
    PATH_TOKENS = ["{base}","{schema}","{table}","{YYYY}","{MM}","{DD}","{file}"]
    PATH_DEFAULT = "{base}/{table}/{YYYY}/{MM}/{DD}/{file}.parquet"

    def _build(self):
        hdr=tk.Frame(self,bg=C_TEAL,padx=24,pady=14); hdr.pack(fill="x")
        tk.Label(hdr,text="☁  Azure Data Lake Storage Gen2",font=("Calibri Light",14,"bold"),
                 bg=C_TEAL,fg=C_WHITE).pack(side="left")
        tk.Label(hdr,text="Regolo Farm",font=FONT_SMALL,bg=C_TEAL,fg="#A8E6CF").pack(side="right")

        body=tk.Frame(self,bg=C_WHITE,padx=28,pady=20); body.pack(fill="both",expand=True)
        def lbl(t,r): tk.Label(body,text=t,font=FONT_HEAD,bg=C_WHITE,fg=C_TEXT,anchor="w").grid(row=r,column=0,sticky="nw",pady=5)
        def ent(r,show=None,w=36):
            e=tk.Entry(body,font=FONT_BODY,bg=C_GRAY_BG,fg=C_TEXT,relief="flat",bd=0,
                       highlightthickness=1,highlightbackground=C_GRAY_LINE,
                       highlightcolor=C_TEAL,width=w,show=show)
            e.grid(row=r,column=1,sticky="ew",padx=(12,0),pady=5); return e

        lbl("Storage Account Name",0); self.e_acc=ent(0)
        lbl("Container (filesystem)",1); self.e_con=ent(1)

        # Metodo autenticazione
        lbl("Autenticazione",2)
        self._auth=tk.StringVar(value=self._init.get("auth_method","account_key"))
        auth_frame=tk.Frame(body,bg=C_WHITE)
        auth_frame.grid(row=3,column=0,columnspan=2,sticky="w",pady=(2,6))
        for t,v in [("Account Key","account_key"),("SAS Token","sas_token"),
                    ("Service Principal","service_principal"),("Managed Identity","managed_identity")]:
            tk.Radiobutton(auth_frame,text=t,variable=self._auth,value=v,
                           bg=C_WHITE,fg=C_TEXT,font=FONT_BODY,activebackground=C_WHITE,
                           command=self._tog_auth).pack(side="left",padx=(0,12))

        # Campi dinamici per autenticazione
        self._dyn_frame=tk.Frame(body,bg=C_WHITE)
        self._dyn_frame.grid(row=4,column=0,columnspan=2,sticky="ew")
        self._dyn_widgets={}
        self._build_dyn()

        # ── Sezione percorso file ──────────────────────────────────────────────
        sep=tk.Frame(body,bg=C_GRAY_LINE,height=1)
        sep.grid(row=5,column=0,columnspan=2,sticky="ew",pady=(12,8))

        lbl("Template percorso",6)
        path_frame=tk.Frame(body,bg=C_WHITE)
        path_frame.grid(row=6,column=1,sticky="ew",padx=(12,0),pady=4)
        self.e_path=tk.Entry(path_frame,font=FONT_MONO,bg=C_GRAY_BG,fg=C_NAVY,
                              relief="flat",bd=0,highlightthickness=1,
                              highlightbackground=C_GRAY_LINE,highlightcolor=C_TEAL,width=44)
        self.e_path.pack(side="left",fill="x",expand=True,ipady=3)
        tk.Button(path_frame,text="↺",command=self._reset_path,font=("Calibri",11),
                  bg=C_TEAL,fg=C_WHITE,relief="flat",bd=0,padx=6,pady=2,
                  cursor="hand2",activebackground="#0A5D60",
                  ).pack(side="left",padx=(4,0))

        # Riga token cliccabili
        tk.Label(body,text="Token:",font=FONT_SMALL,bg=C_WHITE,fg=C_TEXT_MUT,anchor="w"
                 ).grid(row=7,column=0,sticky="w",pady=(0,4))
        tok_frame=tk.Frame(body,bg=C_WHITE)
        tok_frame.grid(row=7,column=1,sticky="w",padx=(12,0),pady=(0,4))
        for tok in self.PATH_TOKENS:
            tk.Button(tok_frame,text=tok,command=lambda t=tok:self._insert_token(t),
                      font=FONT_MONO,bg=C_SKY_LIGHT,fg=C_NAVY,relief="flat",bd=0,
                      padx=6,pady=2,cursor="hand2",
                      activebackground=C_SKY).pack(side="left",padx=2)

        # Descrizione token
        token_help = (
            "{base} = cartella base   {schema} = schema SQL   {table} = nome tabella\n"
            "{YYYY} = anno   {MM} = mese   {DD} = giorno   {file} = nome file (=tabella)"
        )
        tk.Label(body,text=token_help,font=("Calibri",8),bg=C_WHITE,fg=C_TEXT_MUT,
                 justify="left",anchor="w"
                 ).grid(row=8,column=0,columnspan=2,sticky="w",padx=(12,0),pady=(0,6))

        # Cartella base (rimane per retrocompatibilità, usata dal token {base})
        lbl("Cartella base",9); self.e_base=ent(9)
        tk.Label(body,text="valore per il token {base} — es. raw/sqlserver",
                 font=FONT_SMALL,bg=C_WHITE,fg=C_TEXT_MUT).grid(row=9,column=1,sticky="e")

        body.columnconfigure(1,weight=1)

        # Preview URL
        pv=tk.Frame(self,bg=C_GRAY_BG,padx=20,pady=10); pv.pack(fill="x")
        ph=tk.Frame(pv,bg=C_GRAY_BG); ph.pack(fill="x")
        tk.Label(ph,text="Preview percorso completo:",font=FONT_SMALL,bg=C_GRAY_BG,fg=C_TEXT_MUT).pack(side="left")
        self._prev=tk.StringVar()
        tk.Label(pv,textvariable=self._prev,font=FONT_MONO,bg=C_GRAY_BG,fg=C_TEAL,
                 wraplength=600,justify="left").pack(anchor="w",pady=(4,0))

        for v in (self._auth,): v.trace_add("write",self._upd_prev)
        for e in (self.e_acc,self.e_con,self.e_base,self.e_path): e.bind("<KeyRelease>",lambda _:self._upd_prev())

        # Bottoni
        bf=tk.Frame(self,bg=C_WHITE,pady=14); bf.pack()
        tk.Button(bf,text="  Salva  ",command=self._ok,font=FONT_HEAD,
                  bg=C_TEAL,fg=C_WHITE,relief="flat",bd=0,padx=14,pady=7,
                  cursor="hand2",activebackground="#0A5D60").pack(side="left",padx=6)
        tk.Button(bf,text="  Test connessione  ",command=self._test,font=FONT_HEAD,
                  bg=C_NAVY,fg=C_WHITE,relief="flat",bd=0,padx=14,pady=7,
                  cursor="hand2",activebackground=C_HOVER).pack(side="left",padx=6)
        tk.Button(bf,text="  Annulla  ",command=self.destroy,font=FONT_HEAD,
                  bg=C_GRAY_LINE,fg=C_TEXT,relief="flat",bd=0,padx=14,pady=7,
                  cursor="hand2").pack(side="left",padx=6)

        self._fill_initial()
        self._upd_prev()

    def _build_dyn(self):
        for w in self._dyn_frame.winfo_children(): w.destroy()
        self._dyn_widgets={}
        m=self._auth.get()
        specs=[]
        if m=="account_key":
            specs=[("Account Key","account_key","●",38)]
        elif m=="sas_token":
            specs=[("SAS Token","sas_token","●",38)]
        elif m=="service_principal":
            specs=[("Tenant ID","tenant_id",None,36),
                   ("Client ID","client_id",None,36),
                   ("Client Secret","client_secret","●",36)]
        # managed_identity: no campi
        for i,(label,key,show,w) in enumerate(specs):
            tk.Label(self._dyn_frame,text=label,font=FONT_HEAD,bg=C_WHITE,fg=C_TEXT,anchor="w"
                     ).grid(row=i,column=0,sticky="w",pady=4)
            e=tk.Entry(self._dyn_frame,font=FONT_BODY,bg=C_GRAY_BG,fg=C_TEXT,
                       relief="flat",bd=0,highlightthickness=1,
                       highlightbackground=C_GRAY_LINE,highlightcolor=C_TEAL,
                       width=w,show=show or "")
            e.grid(row=i,column=1,sticky="ew",padx=(12,0),pady=4)
            self._dyn_widgets[key]=e
            self._dyn_frame.columnconfigure(1,weight=1)
        if m=="managed_identity":
            tk.Label(self._dyn_frame,text="ℹ  Nessuna credenziale richiesta (Azure-hosted)",
                     font=FONT_SMALL,bg=C_WHITE,fg=C_TEXT_MUT,anchor="w"
                     ).grid(row=0,column=0,columnspan=2,sticky="w",pady=6)

    def _tog_auth(self):
        self._build_dyn()
        self._fill_dyn()
        self._upd_prev()

    def _fill_initial(self):
        p=self._init
        for e,k in [(self.e_acc,"account_name"),(self.e_con,"container"),(self.e_base,"base_folder")]:
            e.delete(0,"end"); e.insert(0,p.get(k,""))
        self._auth.set(p.get("auth_method","account_key"))
        # Carica template percorso (default se non presente)
        self.e_path.delete(0,"end")
        self.e_path.insert(0, p.get("path_template", self.PATH_DEFAULT))
        self._build_dyn(); self._fill_dyn()

    def _fill_dyn(self):
        p=self._init
        for key,e in self._dyn_widgets.items():
            e.delete(0,"end"); e.insert(0,p.get(key,""))

    def _upd_prev(self,*_):
        acc  = self.e_acc.get().strip()  or "<account>"
        con  = self.e_con.get().strip()  or "<container>"
        base = self.e_base.get().strip() or ""
        tmpl = self.e_path.get().strip() or self.PATH_DEFAULT
        now  = datetime.now()
        path = resolve_path_template(tmpl,
            base=base, schema="dbo", table="NomeTabella",
            now=now, file_name="NomeTabella")
        self._prev.set(f"abfss://{con}@{acc}.dfs.core.windows.net/{path}")

    def _reset_path(self):
        self.e_path.delete(0,"end")
        self.e_path.insert(0, self.PATH_DEFAULT)
        self._upd_prev()

    def _insert_token(self, token):
        """Inserisce il token nella posizione corrente del cursore."""
        pos = self.e_path.index(tk.INSERT)
        self.e_path.insert(pos, token)
        self.e_path.icursor(pos + len(token))
        self._upd_prev()

    def _params(self):
        p={"account_name":   self.e_acc.get().strip(),
           "container":      self.e_con.get().strip(),
           "base_folder":    self.e_base.get().strip(),
           "auth_method":    self._auth.get(),
           "path_template":  self.e_path.get().strip() or self.PATH_DEFAULT}
        for key,e in self._dyn_widgets.items():
            p[key]=e.get().strip()
        return p

    def _test(self):
        p=self._params()
        if not p["account_name"] or not p["container"]:
            messagebox.showerror("Errore","Account name e Container obbligatori.",parent=self); return
        try:
            client=get_adls_client(p)
            fs=client.get_file_system_client(p["container"])
            list(fs.get_paths(max_results=1))
            messagebox.showinfo("Connessione OK",
                f"✓ Connesso a {p['account_name']}.dfs.core.windows.net\n"
                f"Container: {p['container']}",parent=self)
        except Exception as ex:
            messagebox.showerror("Errore connessione ADLS",str(ex),parent=self)

    def _ok(self):
        p=self._params()
        if not p["account_name"] or not p["container"]:
            messagebox.showerror("Errore","Account name e Container obbligatori.",parent=self); return
        self.result=p; self.destroy()


# ═══════════════════════════════════════════════════════════════════════════════
#  DIALOG — LOG OPERAZIONE DI COPIA
# ═══════════════════════════════════════════════════════════════════════════════
class RunDialog(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("Esecuzione copia → ADLS Gen2")
        self.configure(bg=C_NAVY)
        self.geometry("780x520")
        self.resizable(True, True)
        self.transient(parent)
        self._q = queue.Queue()
        self._running = False
        self._build()

    def _build(self):
        hdr=tk.Frame(self,bg=C_NAVY,padx=20,pady=12); hdr.pack(fill="x")
        tk.Label(hdr,text="☁  Copia SQL Server → ADLS Gen2 (Parquet)",
                 font=("Calibri Light",13,"bold"),bg=C_NAVY,fg=C_WHITE).pack(side="left")
        self._lbl_status=tk.Label(hdr,text="In attesa…",font=FONT_SMALL,
                                   bg=C_NAVY,fg="#FFC107")
        self._lbl_status.pack(side="right")

        # Progress bar
        pb_frame=tk.Frame(self,bg=C_GRAY_BG,padx=16,pady=8); pb_frame.pack(fill="x")
        self._pb=ttk.Progressbar(pb_frame,mode="determinate",length=740)
        self._pb.pack(fill="x")
        self._lbl_prog=tk.Label(pb_frame,text="",font=FONT_SMALL,bg=C_GRAY_BG,fg=C_TEXT_MUT)
        self._lbl_prog.pack(anchor="e")

        # Log
        log_frame=tk.Frame(self,bg=C_GRAY_BG,padx=16,pady=4); log_frame.pack(fill="both",expand=True)
        self._log=tk.Text(log_frame,font=FONT_MONO,bg="#0D1117",fg="#E6EDF3",
                          relief="flat",bd=0,wrap="none",state="disabled",height=18)
        self._log.pack(fill="both",expand=True)
        vsb=ttk.Scrollbar(log_frame,orient="vertical",command=self._log.yview)
        vsb.pack(side="right",fill="y")
        self._log.configure(yscrollcommand=vsb.set)
        # tag colori log
        self._log.tag_configure("ok",  foreground="#3FB950")
        self._log.tag_configure("err", foreground="#F85149")
        self._log.tag_configure("info",foreground="#79C0FF")
        self._log.tag_configure("warn",foreground="#E3B341")

        # Bottoni
        bf=tk.Frame(self,bg=C_WHITE,pady=12); bf.pack(fill="x")
        self._btn_close=tk.Button(bf,text="Chiudi",command=self.destroy,
                                   font=FONT_HEAD,bg=C_GRAY_LINE,fg=C_TEXT,
                                   relief="flat",bd=0,padx=14,pady=7,cursor="hand2")
        self._btn_close.pack(side="right",padx=16)
        tk.Button(bf,text="📋 Copia log",command=self._copy_log,
                  font=FONT_HEAD,bg=C_NAVY,fg=C_WHITE,
                  relief="flat",bd=0,padx=14,pady=7,cursor="hand2",
                  activebackground=C_HOVER).pack(side="right",padx=4)

    def log(self, msg, tag="info"):
        ts = datetime.now().strftime("%H:%M:%S")
        self._log.configure(state="normal")
        self._log.insert("end", f"[{ts}] {msg}\n", tag)
        self._log.see("end")
        self._log.configure(state="disabled")

    def set_progress(self, done, total, table_name=""):
        pct = int(done/total*100) if total else 0
        self._pb["value"] = pct
        self._lbl_prog.config(text=f"{done}/{total} tabelle  {pct}%"
                              + (f"  — {table_name}" if table_name else ""))

    def set_status(self, msg, color="#FFC107"):
        self._lbl_status.config(text=msg, fg=color)

    def _copy_log(self):
        self.clipboard_clear()
        self.clipboard_append(self._log.get("1.0","end"))

    def poll(self):
        """Consuma la coda messaggi dal thread worker."""
        try:
            while True:
                item = self._q.get_nowait()
                if item[0] == "log":
                    self.log(item[1], item[2])
                elif item[0] == "progress":
                    self.set_progress(*item[1:])
                elif item[0] == "status":
                    self.set_status(item[1], item[2])
                elif item[0] == "done":
                    self._running = False
                    return
        except queue.Empty:
            pass
        if self._running:
            self.after(100, self.poll)


# ═══════════════════════════════════════════════════════════════════════════════
#  WORKER — COPIA SQL → ADLS
# ═══════════════════════════════════════════════════════════════════════════════
def run_copy_worker(sql_ci, adls_cfg, table_plan, q: queue.Queue):
    """
    Eseguito in un thread separato.
    table_plan: lista di dict {"schema","table_name","columns":[...],
                               "load_mode":"FULL"|"INCREMENTALE"}
    """
    def emit(typ, *args): q.put((typ,)+args)

    total   = len(table_plan)
    done    = 0
    errors  = 0

    emit("status","Connessione SQL Server…","#FFC107")
    emit("log","Connessione a SQL Server…","info")

    try:
        conn = get_sql_connection(sql_ci)
    except Exception as ex:
        emit("log",f"ERRORE connessione SQL: {ex}","err")
        emit("status","Errore connessione SQL","#F85149")
        emit("done")
        return

    emit("log","Connesso a SQL Server  ✓","ok")

    try:
        adls_client = get_adls_client(adls_cfg)
        emit("log",f"Connesso a ADLS: {adls_cfg['account_name']}  ✓","ok")
    except Exception as ex:
        emit("log",f"ERRORE connessione ADLS: {ex}","err")
        emit("status","Errore connessione ADLS","#F85149")
        emit("done")
        return

    now           = datetime.now()
    container     = adls_cfg["container"]
    base          = adls_cfg.get("base_folder","").strip("/")
    path_template = adls_cfg.get("path_template",
                                  "{base}/{table}/{YYYY}/{MM}/{DD}/{file}.parquet")

    import pandas as pd

    for tbl in table_plan:
        tname       = tbl["table_name"]
        schema      = tbl["schema"]
        cols        = tbl["columns"]
        mode        = tbl["load_mode"]
        incr_field  = tbl.get("incr_field","")
        fqn         = f"[{schema}].[{tname}]"
        col_sql     = ", ".join(f"[{c}]" for c in cols)

        emit("progress", done, total, tname)
        emit("log",f"─── {fqn}  [{mode}]","info")

        try:
            if mode == "FULL":
                # ── FULL: leggi tutto ────────────────────────────────────────
                sql_query = f"SELECT {col_sql} FROM {fqn}"
                emit("log",f"  FULL SELECT {len(cols)} colonne…","info")
            else:
                # ── INCREMENTALE ─────────────────────────────────────────────
                if incr_field:
                    # Filtro sul giorno corrente (date trunc)
                    date_filter = now.strftime("%Y-%m-%d")
                    sql_query = (f"SELECT {col_sql} FROM {fqn} "
                                 f"WHERE CAST([{incr_field}] AS DATE) = '{date_filter}'")
                    emit("log",f"  INCR su [{incr_field}] = {date_filter}","info")
                else:
                    # Campo data non configurato: copia tutto con avviso
                    sql_query = f"SELECT {col_sql} FROM {fqn}"
                    emit("log",f"  INCR senza campo data → SELECT completo (nessun filtro)","warn")

            df = pd.read_sql(sql_query, conn)
            emit("log",f"  Lette {len(df):,} righe  ({len(df.columns)} colonne)","ok")

            # Percorso destinazione — calcolato dal template
            dest_file = resolve_path_template(
                path_template,
                base=base, schema=schema, table=tname,
                now=now, file_name=tname)
            # La root per FULL delete è tutto ciò che precede i token data
            # Usiamo base/table come root di cancellazione (sicuro: non tocca altre tabelle)
            tbl_root = resolve_path_template(
                "{base}/{table}",
                base=base, schema=schema, table=tname,
                now=now, file_name=tname)
            emit("log",f"  Percorso → {dest_file}","info")

            if mode == "FULL":
                emit("log",f"  FULL → elimino {tbl_root}/…","warn")
                adls_delete_folder(adls_client, container, tbl_root)
            # INCREMENTALE: NON cancella — aggiunge la partizione del giorno

            emit("log",f"  Upload → {dest_file}","info")
            adls_upload_parquet(adls_client, container, dest_file, df)
            emit("log",f"  ✓ Completato  ({len(df):,} righe)","ok")
            done += 1

        except Exception as ex:
            emit("log",f"  ✗ ERRORE: {ex}","err")
            errors += 1
            done += 1

        emit("progress", done, total, "")

    conn.close()
    status_msg = f"Completato: {done-errors}/{total} tabelle"
    if errors:
        status_msg += f"  ({errors} errori)"
        emit("status", status_msg, "#E3B341")
    else:
        emit("status", status_msg + "  ✓", "#3FB950")
    emit("log","─────────────────────────────────","info")
    emit("log", status_msg, "ok" if not errors else "warn")
    emit("done")


# ═══════════════════════════════════════════════════════════════════════════════
#  DIALOG — PASSPHRASE PER CIFRATURA PASSWORD
# ═══════════════════════════════════════════════════════════════════════════════
def _ask_passphrase(parent, mode: str) -> "str | None":
    """
    Mostra una dialog modale per inserire la passphrase.
    mode="save"  → chiede due volte (conferma).
    mode="load"  → chiede una volta sola.
    Restituisce la passphrase o None se annullato.
    """
    dlg = tk.Toplevel(parent)
    dlg.title("Passphrase cifratura password")
    dlg.configure(bg=C_NAVY)
    dlg.resizable(False, False)
    dlg.grab_set()
    dlg.transient(parent)

    result = {"value": None}

    # Header
    hdr = tk.Frame(dlg, bg=C_NAVY, padx=22, pady=12); hdr.pack(fill="x")
    tk.Label(hdr, text="🔑  Cifratura password DB",
             font=("Calibri Light", 13, "bold"), bg=C_NAVY, fg=C_WHITE).pack(side="left")

    # Body
    body = tk.Frame(dlg, bg=C_WHITE, padx=26, pady=20); body.pack(fill="both", expand=True)

    if mode == "save":
        msg = ("Inserisci una passphrase per cifrare la password del database.\n"
               "Sara' richiesta ogni volta che carichi questo file di configurazione.\n"
               "Non viene salvata nel file — conservala in un luogo sicuro.")
    else:
        msg = ("Questo file contiene una password cifrata.\n"
               "Inserisci la passphrase usata al momento del salvataggio.")

    tk.Label(body, text=msg, font=FONT_BODY, bg=C_WHITE, fg=C_TEXT,
             justify="left", wraplength=360).grid(row=0, column=0, columnspan=2,
             sticky="w", pady=(0, 14))

    def lbl(t, r):
        tk.Label(body, text=t, font=FONT_HEAD, bg=C_WHITE,
                 fg=C_TEXT, anchor="w").grid(row=r, column=0, sticky="w", pady=5)

    def ent(r):
        e = tk.Entry(body, font=FONT_BODY, bg=C_GRAY_BG, fg=C_TEXT,
                     relief="flat", bd=0, highlightthickness=1,
                     highlightbackground=C_GRAY_LINE, highlightcolor=C_ORANGE,
                     width=32, show="●")
        e.grid(row=r, column=1, sticky="ew", padx=(12, 0), pady=5)
        return e

    lbl("Passphrase", 1); e1 = ent(1)

    e2 = None
    if mode == "save":
        lbl("Conferma", 2); e2 = ent(2)

    # Mostra/nascondi passphrase
    show_var = tk.BooleanVar(value=False)
    def _toggle_show():
        ch = "" if show_var.get() else "●"
        e1.configure(show=ch)
        if e2: e2.configure(show=ch)
    tk.Checkbutton(body, text="Mostra passphrase", variable=show_var,
                   command=_toggle_show, bg=C_WHITE, fg=C_TEXT_MUT,
                   font=FONT_SMALL, activebackground=C_WHITE
                   ).grid(row=3, column=0, columnspan=2, sticky="w", pady=(4, 0))

    body.columnconfigure(1, weight=1)
    lbl_err = tk.Label(body, text="", font=FONT_SMALL, bg=C_WHITE, fg=C_RED, anchor="w")
    lbl_err.grid(row=4, column=0, columnspan=2, sticky="w")

    def _ok():
        p1 = e1.get()
        if not p1:
            lbl_err.config(text="La passphrase non può essere vuota."); return
        if mode == "save":
            p2 = e2.get()
            if p1 != p2:
                lbl_err.config(text="Le due passphrase non coincidono."); return
            if len(p1) < 8:
                lbl_err.config(text="La passphrase deve essere di almeno 8 caratteri."); return
        result["value"] = p1
        dlg.destroy()

    def _cancel():
        dlg.destroy()

    bf = tk.Frame(dlg, bg=C_WHITE, pady=12); bf.pack()
    tk.Button(bf, text="  Conferma  ", command=_ok, font=FONT_HEAD,
              bg=C_ORANGE, fg=C_WHITE, relief="flat", bd=0, padx=14, pady=7,
              cursor="hand2", activebackground="#A04A0A").pack(side="left", padx=6)
    tk.Button(bf, text="  Annulla  ", command=_cancel, font=FONT_HEAD,
              bg=C_GRAY_LINE, fg=C_TEXT, relief="flat", bd=0, padx=14, pady=7,
              cursor="hand2").pack(side="left", padx=6)

    # Bind Enter
    dlg.bind("<Return>", lambda _: _ok())
    dlg.bind("<Escape>", lambda _: _cancel())
    e1.focus_set()

    dlg.update_idletasks()
    dlg.geometry(
        f"+{parent.winfo_x()+(parent.winfo_width()-dlg.winfo_width())//2}"
        f"+{parent.winfo_y()+(parent.winfo_height()-dlg.winfo_height())//2}")

    parent.wait_window(dlg)
    return result["value"]


# ═══════════════════════════════════════════════════════════════════════════════
#  APPLICAZIONE PRINCIPALE
# ═══════════════════════════════════════════════════════════════════════════════
class SchemaExplorer(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SQL Schema Explorer — Regolo Farm")
        self.configure(bg=C_GRAY_BG)
        self.geometry("1260x740")
        self.minsize(960, 540)

        self._grid_data   = []
        self._table_meta  = {}   # "schema.table" -> {"load_mode": "FULL"|"INCREMENTALE"}
        self._conn_info   = None
        self._adls_cfg    = {}   # configurazione ADLS
        self._filter_var  = tk.StringVar()
        self._type_filter = tk.StringVar(value="Tutti")

        self._build_ui()

        # I trace vanno registrati DOPO _build_ui, altrimenti scattano
        # prima che self.tree esista e generano AttributeError.
        self._filter_var.trace_add("write", self._apply_filter)
        self._type_filter.trace_add("write", self._apply_filter)

        self._open_connection_dialog()

    # ── UI ────────────────────────────────────────────────────────────────────
    def _build_ui(self):
        self._build_header()
        self._build_toolbar()
        self._build_grid()
        self._build_statusbar()

    def _build_header(self):
        hdr=tk.Frame(self,bg=C_NAVY,height=54); hdr.pack(fill="x"); hdr.pack_propagate(False)
        tk.Label(hdr,text="⚓",font=("Calibri",18),bg=C_NAVY,fg=C_SKY).place(x=16,rely=0.5,anchor="w")
        tk.Label(hdr,text="SQL Schema Explorer",font=("Calibri Light",16,"bold"),
                 bg=C_NAVY,fg=C_WHITE).place(x=46,rely=0.5,anchor="w")
        tk.Label(hdr,text="Regolo Farm · Pordenone",font=("Calibri",9),
                 bg=C_NAVY,fg=C_SKY).place(relx=1,x=-16,rely=0.5,anchor="e")
        self.lbl_db=tk.Label(hdr,text="● Non connesso",font=("Calibri",9,"bold"),
                              bg=C_NAVY,fg="#FF6B6B")
        self.lbl_db.place(relx=0.5,rely=0.5,anchor="center")

    def _build_toolbar(self):
        tb=tk.Frame(self,bg=C_WHITE,bd=0,pady=8,padx=12); tb.pack(fill="x")

        # Cerca
        tk.Label(tb,text="🔍",font=("Calibri",11),bg=C_WHITE,fg=C_TEXT_MUT).pack(side="left")
        sr=tk.Entry(tb,textvariable=self._filter_var,font=FONT_BODY,bg=C_GRAY_BG,fg=C_TEXT,
                    relief="flat",bd=0,highlightthickness=1,highlightbackground=C_GRAY_LINE,
                    highlightcolor=C_ORANGE,width=22)
        sr.pack(side="left",padx=(4,10),ipady=4)
        sr.insert(0,"Filtra tabella o campo…")
        sr.bind("<FocusIn>", lambda e: sr.delete(0,"end") if sr.get().startswith("Filtra") else None)
        sr.bind("<FocusOut>",lambda e: sr.insert(0,"Filtra tabella o campo…") if not sr.get() else None)

        tk.Label(tb,text="Tipo:",font=FONT_BODY,bg=C_WHITE,fg=C_TEXT_MUT).pack(side="left")
        ttk.Combobox(tb,textvariable=self._type_filter,values=["Tutti","TABLE","VIEW"],
                     state="readonly",font=FONT_BODY,width=8).pack(side="left",padx=(4,10))

        self._sep(tb)

        for txt,cmd,bg,fg in [
            ("✔ Tutti",        self._select_all,   C_NAVY,    C_WHITE),
            ("✘ Nessuno",      self._deselect_all, C_TEXT_MUT,C_WHITE),
            ("↕ Inverti",      self._invert,        C_SKY,     C_NAVY),
        ]:
            tk.Button(tb,text=txt,command=cmd,font=FONT_SMALL,bg=bg,fg=fg,
                      relief="flat",bd=0,padx=8,pady=4,cursor="hand2",
                      activebackground=C_HOVER,activeforeground=C_WHITE
                      ).pack(side="left",padx=2)

        self._sep(tb)

        # Configurazione ADLS
        self._adls_btn=tk.Button(tb,text="☁  ADLS Config",command=self._open_adls_dialog,
                                  font=FONT_SMALL,bg=C_TEAL,fg=C_WHITE,relief="flat",bd=0,
                                  padx=8,pady=4,cursor="hand2",
                                  activebackground="#0A5D60",activeforeground=C_WHITE)
        self._adls_btn.pack(side="left",padx=2)
        self._adls_lbl=tk.Label(tb,text="(non configurato)",font=FONT_SMALL,bg=C_WHITE,fg=C_TEXT_MUT)
        self._adls_lbl.pack(side="left",padx=(2,10))

        self._sep(tb)

        # ▶  ESEGUI COPIA
        self._run_btn=tk.Button(tb,text="▶  Esegui copia → ADLS",
                                 command=self._run_copy,
                                 font=("Calibri",10,"bold"),
                                 bg=C_ORANGE,fg=C_WHITE,relief="flat",bd=0,
                                 padx=12,pady=5,cursor="hand2",
                                 activebackground="#A04A0A",activeforeground=C_WHITE)
        self._run_btn.pack(side="left",padx=2)

        self._sep(tb)

        # Salva / Carica JSON
        for txt,cmd,bg in [("💾 Salva JSON",self._save_json,C_GREEN),
                            ("📂 Carica JSON",self._load_json,C_GREEN)]:
            tk.Button(tb,text=txt,command=cmd,font=FONT_SMALL,bg=bg,fg=C_WHITE,
                      relief="flat",bd=0,padx=8,pady=4,cursor="hand2",
                      activebackground="#195C38").pack(side="left",padx=2)

        self._sep(tb)

        for txt,cmd in [("📋 CSV",self._export_csv),("{ } JSON",self._export_json_schema)]:
            tk.Button(tb,text=txt,command=cmd,font=FONT_SMALL,bg="#7A4800",fg=C_WHITE,
                      relief="flat",bd=0,padx=8,pady=4,cursor="hand2",
                      activebackground="#5A3200").pack(side="left",padx=2)

        tk.Button(tb,text="🔌 Connetti",command=self._open_connection_dialog,
                  font=FONT_SMALL,bg=C_GRAY_BG,fg=C_TEXT,relief="flat",bd=0,
                  padx=8,pady=4,cursor="hand2",
                  highlightthickness=1,highlightbackground=C_GRAY_LINE
                  ).pack(side="right",padx=4)

    def _sep(self,p):
        tk.Frame(p,bg=C_GRAY_LINE,width=1,height=24).pack(side="left",padx=6)

    def _build_grid(self):
        container=tk.Frame(self,bg=C_GRAY_BG); container.pack(fill="both",expand=True,padx=14,pady=(6,0))
        style=ttk.Style(); style.theme_use("clam")
        style.configure("Schema.Treeview",background=C_WHITE,fieldbackground=C_WHITE,
            foreground=C_TEXT,rowheight=26,font=FONT_BODY,borderwidth=0,relief="flat",indent=20)
        style.configure("Schema.Treeview.Heading",background=C_NAVY,foreground=C_WHITE,
            font=FONT_HEAD,relief="flat",padding=(8,6))
        style.map("Schema.Treeview",background=[("selected",C_SKY)],foreground=[("selected",C_NAVY)])
        style.map("Schema.Treeview.Heading",background=[("active",C_HOVER)])

        cols=("includi","caricamento","campo_data","tipo_dato","nullable")
        self.tree=ttk.Treeview(container,columns=cols,show="tree headings",
                                style="Schema.Treeview",selectmode="extended")
        self.tree.heading("#0",text="  Oggetto / Campo",anchor="w")
        self.tree.column("#0",width=300,stretch=True,anchor="w")
        for col,head,w in [("includi","✔ Includi",80),("caricamento","⟳ Caricamento",110),
                            ("campo_data","📅 Campo Data",130),
                            ("tipo_dato","Tipo Dato",120),("nullable","Nullable",70)]:
            self.tree.heading(col,text=head)
            self.tree.column(col,width=w,anchor="center",stretch=False)

        vsb=ttk.Scrollbar(container,orient="vertical",  command=self.tree.yview)
        hsb=ttk.Scrollbar(container,orient="horizontal",command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set,xscrollcommand=hsb.set)
        self.tree.grid(row=0,column=0,sticky="nsew"); vsb.grid(row=0,column=1,sticky="ns")
        hsb.grid(row=1,column=0,sticky="ew")
        container.rowconfigure(0,weight=1); container.columnconfigure(0,weight=1)

        self.tree.tag_configure("tbl_all", background="#E4EDF8",foreground=C_NAVY, font=("Calibri",10,"bold"))
        self.tree.tag_configure("tbl_none",background="#F5E8E8",foreground="#999", font=("Calibri",10,"bold"))
        self.tree.tag_configure("tbl_part",background="#EEF2F8",foreground="#3A5A8A",font=("Calibri",10,"bold"))
        self.tree.tag_configure("col_inc", background=C_WHITE,  foreground=C_TEXT)
        self.tree.tag_configure("col_exc", background="#FFF4F4",foreground="#AAA")
        self.tree.tag_configure("col_inc2",background=C_ROW_ALT,foreground=C_TEXT)
        self.tree.tag_configure("col_exc2",background="#FFF0EF",foreground="#AAA")
        # campo data selezionato per incrementale
        self.tree.tag_configure("col_dt",  background="#E8F5E9",foreground="#1B5E20",font=("Calibri",10,"bold"))
        self.tree.tag_configure("col_dt2", background="#D4EDDA",foreground="#1B5E20",font=("Calibri",10,"bold"))

        self.tree.bind("<ButtonRelease-1>",self._on_click)
        self.tree.bind("<space>",          self._on_space)

    def _build_statusbar(self):
        sb=tk.Frame(self,bg=C_NAVY,height=26); sb.pack(fill="x",side="bottom"); sb.pack_propagate(False)
        self.lbl_count=tk.Label(sb,text="Nessun dato caricato",font=FONT_SMALL,bg=C_NAVY,fg=C_SKY)
        self.lbl_count.pack(side="left",padx=12)
        self.lbl_sel=tk.Label(sb,text="",font=FONT_SMALL,bg=C_NAVY,fg=C_ORANGE)
        self.lbl_sel.pack(side="right",padx=12)

    # ── Connessione SQL ───────────────────────────────────────────────────────
    def _open_connection_dialog(self, initial_params=None):
        dlg=ConnectionDialog(self,initial_params=initial_params)
        self.wait_window(dlg)
        if dlg.result:
            self._conn_info=dlg.result
            self._load_schema_async()

    def _load_schema_async(self):
        self.lbl_db.config(text="⏳ Connessione…",fg="#FFC107")
        self.lbl_count.config(text="Caricamento schema…")
        threading.Thread(target=self._load_schema_thread,daemon=True).start()

    def _load_schema_thread(self):
        try:
            conn=get_sql_connection(self._conn_info)
            rows=fetch_schema(conn); conn.close()
            self.after(0,lambda:self._on_schema_loaded(rows,
                self._conn_info["database"],self._conn_info["server"]))
        except Exception as ex:
            self.after(0,lambda:self._on_schema_error(str(ex)))

    def _on_schema_loaded(self, rows, database, server):
        overlay=getattr(self,"_pending_overlay",None); self._pending_overlay=None
        self._grid_data=[{**r,"include":True} for r in rows]
        self._table_meta={}
        for r in self._grid_data:
            tk=self._tkey(r["schema"],r["table_name"])
            self._table_meta.setdefault(tk,{"load_mode":"FULL","incr_field":""})
        if overlay:
            for r in self._grid_data:
                tk=self._tkey(r["schema"],r["table_name"])
                if tk in overlay:
                    r["include"]=overlay[tk].get("columns",{}).get(r["column_name"],True)
            for tk,tm in overlay.items():
                if tk in self._table_meta:
                    self._table_meta[tk]["load_mode"]=tm.get("load_mode","FULL")
                    self._table_meta[tk]["incr_field"]=tm.get("incr_field","")
        self.lbl_db.config(text=f"● {server}  ›  {database}  ({len(rows):,} campi)",fg="#6EE7B7")
        self._apply_filter()

    def _on_schema_error(self,msg):
        self.lbl_db.config(text="● Errore connessione",fg="#FF6B6B")
        self.lbl_count.config(text="Errore")
        messagebox.showerror("Errore di Connessione",
            f"Impossibile connettersi:\n\n{msg}\n\nVerificare driver ODBC 18.")

    # ── Configurazione ADLS ───────────────────────────────────────────────────
    def _open_adls_dialog(self):
        dlg=AdlsDialog(self,initial=self._adls_cfg)
        self.wait_window(dlg)
        if dlg.result:
            self._adls_cfg=dlg.result
            acc  = self._adls_cfg.get("account_name","")
            con  = self._adls_cfg.get("container","")
            tmpl = self._adls_cfg.get("path_template","")
            # Mostra account/container e template abbreviato
            short_tmpl = tmpl[:38]+"…" if len(tmpl)>38 else tmpl
            lbl_txt = f"{acc}/{con}  [{short_tmpl}]" if acc else "(non configurato)"
            self._adls_lbl.config(text=lbl_txt,
                                   fg=C_TEAL if acc else C_TEXT_MUT)

    # ── Struttura dati ────────────────────────────────────────────────────────
    def _grouped(self,rows):
        groups,order={},[]
        for r in rows:
            key=(r["schema"],r["table_name"],r["object_type"])
            if key not in groups: groups[key]=[]; order.append(key)
            groups[key].append(r)
        return order,groups

    def _table_inc_state(self,fields):
        inc=sum(1 for f in fields if f["include"])
        return "none" if inc==0 else "all" if inc==len(fields) else "partial"

    def _tkey(self,schema,table): return f"{schema}.{table}"

    # ── Griglia ───────────────────────────────────────────────────────────────
    def _apply_filter(self,*_):
        if not hasattr(self, "tree"):
            return   # UI non ancora costruita
        raw=self._filter_var.get().strip().lower()
        filt="" if raw.startswith("filtra") else raw
        tf=self._type_filter.get()
        visible=[r for r in self._grid_data
                 if (not filt or filt in r["table_name"].lower() or filt in r["column_name"].lower())
                 and (tf=="Tutti" or r["object_type"]==tf)]
        self._refresh_tree(visible)

    def _refresh_tree(self,data):
        exp={iid for iid in self.tree.get_children() if self.tree.item(iid,"open")}
        self.tree.delete(*self.tree.get_children())
        order,groups=self._grouped(data)
        for key in order:
            schema,tbl,obj=key; fields=groups[key]
            state=self._table_inc_state(fields)
            tk_=self._tkey(schema,tbl)
            meta=self._table_meta.get(tk_,{})
            mode=meta.get("load_mode","FULL")
            incr_field=meta.get("incr_field","")
            chk="☑" if state=="all" else ("▣" if state=="partial" else "☐")
            ico="📋" if obj=="TABLE" else "👁"
            tag={"all":"tbl_all","none":"tbl_none","partial":"tbl_part"}[state]
            mode_lbl="⟳ INCR" if mode=="INCREMENTALE" else "⊞ FULL"
            # campo data sul nodo tabella (solo se INCR)
            if mode=="INCREMENTALE":
                dt_lbl=f"📅 {incr_field}" if incr_field else "📅 — da selezionare —"
            else:
                dt_lbl=""
            tiid=f"tbl:{tk_}"
            self.tree.insert("","end",iid=tiid,text=f"  {ico}  {schema}.{tbl}",
                values=(chk,mode_lbl,dt_lbl,obj,""),tags=(tag,),
                open=(tiid in exp or not exp))
            for j,r in enumerate(fields):
                inc=r["include"]; ch="☑" if inc else "☐"; nl="✓" if r["is_nullable"] else "✗"
                is_dt=(mode=="INCREMENTALE" and r["column_name"]==incr_field and incr_field)
                if is_dt:
                    radio="◉"; tg="col_dt" if j%2==0 else "col_dt2"
                else:
                    radio="○" if mode=="INCREMENTALE" else ""
                    even=j%2==0; tg=("col_inc" if even else "col_inc2") if inc else ("col_exc" if even else "col_exc2")
                self.tree.insert(tiid,"end",iid=str(id(r)),
                    text=f"    {r['column_name']}",
                    values=(ch,"",radio,r["data_type"],nl),tags=(tg,))
        total=len(self._grid_data); ns=len(data)
        sc=sum(1 for r in self._grid_data if r["include"])
        ni=sum(1 for m in self._table_meta.values() if m["load_mode"]=="INCREMENTALE")
        nf=len(self._table_meta)-ni
        # tabelle INCR senza campo data configurato
        n_warn=sum(1 for tk_,m in self._table_meta.items()
                   if m["load_mode"]=="INCREMENTALE" and not m.get("incr_field",""))
        warn_lbl=f"  ⚠ {n_warn} INCR senza campo data" if n_warn else ""
        self.lbl_count.config(text=f"Mostrati {ns:,} campi in {len(order)} oggetti  ·  totale {total:,}")
        self.lbl_sel.config(text=f"Inclusi: {sc:,}/{total:,}  ·  INCR:{ni}  FULL:{nf}{warn_lbl}")

    # ── Interazione griglia ───────────────────────────────────────────────────
    def _is_tbl(self,iid): return iid.startswith("tbl:")
    def _find_col(self,iid):
        t=int(iid)
        for r in self._grid_data:
            if id(r)==t: return r
        return None
    def _cols_for_tbl(self,tiid):
        return [self._find_col(c) for c in self.tree.get_children(tiid) if not self._is_tbl(c)]

    def _on_click(self,event):
        region=self.tree.identify_region(event.x,event.y)
        col=self.tree.identify_column(event.x)
        iid=self.tree.identify_row(event.y)
        if region!="cell" or not iid: return
        if col=="#1": self._tog_include(iid)
        elif col=="#2" and self._is_tbl(iid): self._tog_mode(iid)
        elif col=="#3" and not self._is_tbl(iid): self._tog_incr_field(iid)

    def _on_space(self,event):
        for iid in self.tree.selection(): self._tog_include(iid)

    def _tog_include(self,iid):
        if self._is_tbl(iid):
            cols=[c for c in self._cols_for_tbl(iid) if c]
            if not cols: return
            all_inc=all(c["include"] for c in cols)
            for c in cols: c["include"]=not all_inc
        else:
            r=self._find_col(iid)
            if r: r["include"]=not r["include"]
        self._apply_filter()

    def _tog_mode(self,tiid):
        tk_=tiid[4:]
        self._table_meta.setdefault(tk_,{"load_mode":"FULL"})
        cur=self._table_meta[tk_]["load_mode"]
        self._table_meta[tk_]["load_mode"]="INCREMENTALE" if cur=="FULL" else "FULL"
        # se torniamo a FULL, azzera il campo data
        if self._table_meta[tk_]["load_mode"]=="FULL":
            self._table_meta[tk_]["incr_field"]=""
        self._apply_filter()

    def _tog_incr_field(self,col_iid):
        """Imposta / deimposta il campo data per l'incrementale del nodo padre."""
        parent_iid=self.tree.parent(col_iid)
        if not parent_iid or not self._is_tbl(parent_iid): return
        tk_=parent_iid[4:]
        meta=self._table_meta.get(tk_,{})
        if meta.get("load_mode","FULL")!="INCREMENTALE": return   # solo per INCR
        r=self._find_col(col_iid)
        if not r: return
        col_name=r["column_name"]
        current=meta.get("incr_field","")
        # toggle: se già selezionato deseleziona, altrimenti seleziona
        self._table_meta[tk_]["incr_field"]="" if current==col_name else col_name
        self._apply_filter()

    # ── Azioni batch ──────────────────────────────────────────────────────────
    def _select_all(self):
        for r in self._grid_data: r["include"]=True
        self._apply_filter()
    def _deselect_all(self):
        for r in self._grid_data: r["include"]=False
        self._apply_filter()
    def _invert(self):
        for r in self._grid_data: r["include"]=not r["include"]
        self._apply_filter()

    # ── ESEGUI COPIA → ADLS ──────────────────────────────────────────────────
    def _run_copy(self):
        if not self._conn_info:
            messagebox.showerror("Errore","Nessuna connessione SQL Server configurata."); return
        if not self._adls_cfg.get("account_name"):
            messagebox.showerror("Errore","Configurazione ADLS non presente.\nUsa il pulsante '☁ ADLS Config'."); return
        if not self._grid_data:
            messagebox.showwarning("Attenzione","Nessun schema caricato."); return

        # Costruisci piano di esecuzione
        plan={}
        for r in self._grid_data:
            if not r["include"]: continue
            tk_=self._tkey(r["schema"],r["table_name"])
            if tk_ not in plan:
                meta_=self._table_meta.get(tk_,{})
                plan[tk_]={"schema":r["schema"],"table_name":r["table_name"],
                            "object_type":r["object_type"],
                            "load_mode":meta_.get("load_mode","FULL"),
                            "incr_field":meta_.get("incr_field",""),
                            "columns":[]}
            plan[tk_]["columns"].append(r["column_name"])

        if not plan:
            messagebox.showwarning("Attenzione","Nessuna tabella con campi inclusi da copiare."); return

        n_tbl=len(plan)
        n_full=sum(1 for t in plan.values() if t["load_mode"]=="FULL")
        n_incr=n_tbl-n_full
        # tabelle INCR senza campo data
        incr_no_field=[t["table_name"] for t in plan.values()
                       if t["load_mode"]=="INCREMENTALE" and not t.get("incr_field","")]
        acc=self._adls_cfg["account_name"]; con=self._adls_cfg["container"]
        warn_txt=""
        if incr_no_field:
            nomi=", ".join(incr_no_field[:5])+("…" if len(incr_no_field)>5 else "")
            warn_txt=(f"\n\n⚠  {len(incr_no_field)} tabella/e INCREMENTALE senza campo data:\n"
                      f"  {nomi}\n  → verranno copiate SENZA filtro data (tutte le righe).")
        if not messagebox.askyesno("Conferma esecuzione",
            f"Stai per copiare {n_tbl} tabelle su ADLS Gen2:\n\n"
            f"  Account:    {acc}\n  Container:  {con}\n"
            f"  FULL:  {n_full}   INCREMENTALE: {n_incr}"
            f"{warn_txt}\n\n"
            "Le tabelle FULL verranno cancellate e riscritte.\nProcedere?"):
            return

        dlg=RunDialog(self)
        dlg._running=True
        q=dlg._q
        t=threading.Thread(target=run_copy_worker,
                           args=(self._conn_info,self._adls_cfg,list(plan.values()),q),
                           daemon=True)
        t.start()
        dlg.after(100,dlg.poll)

    # ── Salva JSON ────────────────────────────────────────────────────────────
    def _save_json(self):
        if not self._grid_data:
            messagebox.showwarning("Salva","Nessun dato da salvare."); return
        path=filedialog.asksaveasfilename(defaultextension=".json",
            filetypes=[("JSON","*.json"),("Tutti i file","*.*")],
            title="Salva configurazione schema")
        if not path: return

        tables={}
        for r in self._grid_data:
            tk_=self._tkey(r["schema"],r["table_name"])
            tables.setdefault(tk_,{
                "schema":r["schema"],"table_name":r["table_name"],
                "object_type":r["object_type"],
                "load_mode":self._table_meta.get(tk_,{}).get("load_mode","FULL"),
                "incr_field":self._table_meta.get(tk_,{}).get("incr_field",""),
                "columns":{}})
            tables[tk_]["columns"][r["column_name"]]={
                "include":r["include"],"data_type":r["data_type"],
                "is_nullable":bool(r["is_nullable"])}

        # Gestione cifratura password
        conn_data = dict(self._conn_info or {})
        raw_pwd   = conn_data.get("password", "")
        enc_pwd   = ""
        if raw_pwd:
            passphrase = _ask_passphrase(self, mode="save")
            if passphrase is None:
                return  # utente ha annullato
            try:
                enc_pwd = encrypt_password(raw_pwd, passphrase)
            except RuntimeError as ex:
                messagebox.showerror("Errore cifratura", str(ex)); return
            conn_data["password_enc"] = enc_pwd
            conn_data.pop("password", None)   # rimuovi testo in chiaro
        else:
            # Nessuna password: salva il blocco senza campo password
            conn_data.pop("password", None)

        out={
            "_meta":{"saved_at":datetime.now().isoformat(timespec="seconds"),
                     "tool":"SQL Schema Explorer — Regolo Farm",
                     "total_tables":len(tables),"total_columns":len(self._grid_data),
                     "password_encrypted": bool(enc_pwd)},
            "connection": conn_data,
            "adls":self._adls_cfg,
            "tables":tables,
        }
        with open(path,"w",encoding="utf-8") as f:
            json.dump(out,f,indent=2,ensure_ascii=False)
        pwd_note = "  ·  password cifrata con Fernet" if enc_pwd else "  ·  nessuna password"
        messagebox.showinfo("Salvataggio completato",
            f"Salvato:\n{path}\n\n{len(tables)} tabelle · {len(self._grid_data)} campi\n"
            f"Configurazione ADLS inclusa: {'sì' if self._adls_cfg else 'no'}{pwd_note}")

    # ── Carica JSON ───────────────────────────────────────────────────────────
    def _load_json(self):
        path=filedialog.askopenfilename(
            filetypes=[("JSON","*.json"),("Tutti i file","*.*")],
            title="Carica configurazione schema")
        if not path: return
        try:
            with open(path,"r",encoding="utf-8") as f: data=json.load(f)
        except Exception as ex:
            messagebox.showerror("Errore",f"Impossibile leggere il file:\n{ex}"); return

        conn_params=data.get("connection")
        adls_params=data.get("adls",{})
        tables_cfg =data.get("tables",{})
        if not tables_cfg:
            messagebox.showerror("Errore","File senza dati schema validi."); return

        # Decifratura password se presente
        if conn_params and conn_params.get("password_enc"):
            passphrase = _ask_passphrase(self, mode="load")
            if passphrase is None:
                return  # utente ha annullato
            try:
                conn_params["password"] = decrypt_password(
                    conn_params["password_enc"], passphrase)
                conn_params.pop("password_enc", None)
            except ValueError as ex:
                messagebox.showerror("Errore decifratura", str(ex)); return
            except RuntimeError as ex:
                messagebox.showerror("Errore", str(ex)); return
        elif conn_params:
            # Retrocompatibilità: file vecchio senza cifratura
            conn_params.setdefault("password", "")

        # Carica configurazione ADLS se presente
        if adls_params and adls_params.get("account_name"):
            self._adls_cfg=adls_params
            acc=adls_params.get("account_name",""); con=adls_params.get("container","")
            self._adls_lbl.config(text=f"{acc}/{con}",fg=C_TEAL)

        overlay={tk_:{"load_mode":td.get("load_mode","FULL"),
                      "incr_field":td.get("incr_field",""),
                      "columns":{c:cd.get("include",True) for c,cd in td.get("columns",{}).items()}}
                 for tk_,td in tables_cfg.items()}

        meta=data.get("_meta",{})
        info=(f"File: {path}\nSalvato: {meta.get('saved_at','n/d')}\n"
              f"Tabelle: {meta.get('total_tables','?')}  Campi: {meta.get('total_columns','?')}\n"
              f"ADLS: {'✓ inclusa' if adls_params.get('account_name') else 'non presente'}\n\n")

        if self._grid_data:
            if messagebox.askyesno("Carica JSON",
                info+"Schema già in memoria.\n\nApplico solo i flag senza riconnettermi?\n(No = riconnetti con parametri del JSON)"):
                self._apply_overlay(overlay); return

        if conn_params:
            if messagebox.askyesno("Carica JSON",
                info+"Apro la dialog di connessione con i parametri del file?"):
                self._pending_overlay=overlay
                self._open_connection_dialog(initial_params=conn_params); return
        else:
            messagebox.showinfo("Carica JSON",info+"Nessuna connessione nel file.\nUsa 'Connetti' per caricare lo schema.")

    def _apply_overlay(self,overlay):
        for r in self._grid_data:
            tk_=self._tkey(r["schema"],r["table_name"])
            if tk_ in overlay:
                r["include"]=overlay[tk_].get("columns",{}).get(r["column_name"],r["include"])
        for tk_,tm in overlay.items():
            if tk_ in self._table_meta:
                self._table_meta[tk_]["load_mode"]=tm.get("load_mode","FULL")
                self._table_meta[tk_]["incr_field"]=tm.get("incr_field","")
        self._apply_filter()
        messagebox.showinfo("Flag applicati",f"Flag applicati su {len(overlay)} tabelle.")

    # ── Esporta CSV / JSON schema ─────────────────────────────────────────────
    def _export_csv(self):
        included=[r for r in self._grid_data if r["include"]]
        if not included: messagebox.showwarning("Esporta","Nessun campo selezionato."); return
        path=filedialog.asksaveasfilename(defaultextension=".csv",
            filetypes=[("CSV","*.csv"),("Tutti i file","*.*")],title="Esporta CSV")
        if not path: return
        flds=["object_type","schema","table_name","load_mode","column_name","data_type","is_nullable"]
        with open(path,"w",newline="",encoding="utf-8-sig") as f:
            w=csv.DictWriter(f,fieldnames=flds); w.writeheader()
            for r in included:
                row={k:r.get(k,"") for k in flds}
                row["load_mode"]=self._table_meta.get(self._tkey(r["schema"],r["table_name"]),{}).get("load_mode","FULL")
                w.writerow(row)
        messagebox.showinfo("CSV esportato",f"Esportati {len(included):,} campi in:\n{path}")

    def _export_json_schema(self):
        included=[r for r in self._grid_data if r["include"]]
        if not included: messagebox.showwarning("Esporta","Nessun campo selezionato."); return
        path=filedialog.asksaveasfilename(defaultextension=".json",
            filetypes=[("JSON","*.json"),("Tutti i file","*.*")],title="Esporta JSON schema")
        if not path: return
        export={}
        for r in included:
            tk_=self._tkey(r["schema"],r["table_name"])
            export.setdefault(tk_,{"type":r["object_type"],
                "load_mode":self._table_meta.get(tk_,{}).get("load_mode","FULL"),"columns":[]})
            export[tk_]["columns"].append({"column":r["column_name"],
                "data_type":r["data_type"],"nullable":bool(r["is_nullable"])})
        with open(path,"w",encoding="utf-8") as f:
            json.dump(export,f,indent=2,ensure_ascii=False)
        messagebox.showinfo("JSON esportato",f"Esportati {len(included):,} campi ({len(export)} oggetti) in:\n{path}")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        import pyodbc  # noqa
    except ImportError:
        root=tk.Tk(); root.withdraw()
        messagebox.showerror("Dipendenza mancante",
            "pyodbc non installato.\n\npip install pyodbc\n\n"
            "Serve anche 'ODBC Driver 18 for SQL Server'.")
        root.destroy(); raise SystemExit(1)
    SchemaExplorer().mainloop()
