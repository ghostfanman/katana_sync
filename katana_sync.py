"""
KATANA KRONOS – Enterprise Backup Solution
Version 7.0
"""

import hashlib
import json
import os
import queue
import re
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import tkinter as tk
from datetime import datetime, timedelta
from tkinter import filedialog, messagebox, scrolledtext, ttk

# ---------------------------------------------------------------------------
# Globale Konfiguration
# ---------------------------------------------------------------------------
CONFIG_FILE            = os.path.expanduser("~/.katana_sync_config.json")
LOG_FILE               = os.path.expanduser("~/katana_backup.log")
AUTOSTART_DIR          = os.path.expanduser("~/.config/autostart")
APP_SCRIPT_PATH        = os.path.abspath(__file__)
MAX_LOG_SIZE_BYTES     = 10 * 1024 * 1024   # 10 MB pro Log-Datei
MAX_LOG_BACKUPS        = 5                   # Anzahl rotierter Log-Dateien
MAX_BACKUP_HISTORY     = 20                  # Backup-Einträge in der Historie
MIN_FREE_SPACE_WARN_GB = 0.5                 # GB – absolutes Minimum auf Ziel
SIGTERM_WAIT_MS        = 5_000              # ms bis SIGKILL nach SIGTERM
SCHEDULER_INTERVAL_MS  = 10_000            # ms zwischen Scheduler-Checks
QUEUE_POLL_MS          = 50                 # ms zwischen Queue-Polls
# Regex für rsync --info=progress2 Ausgabe
_RE_PCT = re.compile(r"(\d+)%")
_RE_SPD = re.compile(r"(\d+\.\d+[kMGT]?B/s)")
_RE_ETA = re.compile(r"(\d+:\d+:\d+)")
# Erkennt reine Progress-Zeilen (beginnen mit Leerzeichen + Zahl)
_RE_PROGRESS_LINE = re.compile(r"^\s+[\d,]")


# ---------------------------------------------------------------------------
# Hauptklasse
# ---------------------------------------------------------------------------
class KatanaBackupMaster:

    # -----------------------------------------------------------------------
    # Initialisierung
    # -----------------------------------------------------------------------
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("KATANA KRONOS | Enterprise Backup Solution")
        self.root.geometry("1150x950")
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)

        # Theme-Farben
        self.colors: dict[str, str] = {
            "bg":           "#0b0b0b",
            "panel":        "#141414",
            "fg":           "#e0e0e0",
            "accent":       "#d32f2f",
            "success":      "#2e7d32",
            "warning":      "#f57c00",
            "info":         "#0288d1",
            "tab_bg":       "#1e1e1e",
            "tab_fg":       "#aaaaaa",
            "help":         "#546e7a",
            "list_bg":      "#111111",
            "list_select":  "#333333",
            "restore":      "#4a148c",
        }
        self.root.configure(bg=self.colors["bg"])
        self._setup_styles()

        # Pfad-Variablen
        self.src_path = tk.StringVar()
        self.dst_path = tk.StringVar()

        # Backup-Strategie
        self.dry_run          = tk.BooleanVar(value=False)
        self.verify_mode      = tk.BooleanVar(value=False)
        self.auto_suspend     = tk.BooleanVar(value=False)
        self.versioning       = tk.BooleanVar(value=True)
        self.smart_exclude    = tk.BooleanVar(value=True)
        self.mirror_mode      = tk.BooleanVar(value=False)
        self.generate_manifest = tk.BooleanVar(value=True)

        # Scheduler
        self.sched_enabled = tk.BooleanVar(value=False)
        self.sched_type    = tk.StringVar(value="daily")
        self.sched_time    = tk.StringVar(value="18:00")
        self.sched_days: dict[str, tk.BooleanVar] = {
            d: tk.BooleanVar()
            for d in ("Mo", "Di", "Mi", "Do", "Fr", "Sa", "So")
        }
        self.retention_enabled = tk.BooleanVar(value=False)
        self.retention_days    = tk.IntVar(value=30)
        self.run_missed        = tk.BooleanVar(value=True)

        # Laufzeit-Zustand
        self.process: subprocess.Popen | None = None
        self._process_lock = threading.Lock()  # Schutz für self.process
        self.msg_queue: queue.Queue = queue.Queue()
        self.is_running        = False
        self._backup_start_time: float | None = None
        self.last_run: str | None = None
        self.backup_history: list[dict] = []

        # UI aufbauen, Config laden, Loops starten
        self._setup_ui()
        self._load_config()
        self._rotate_log_if_needed()
        self._monitor_queue()
        self._scheduler_loop()

        self.log("Katana Kronos v7.0 (Enterprise Edition) gestartet.")
        # Verpasste Backups erst nach vollständigem Init prüfen
        self.root.after(5_000, self._check_missed_backup)

    # -----------------------------------------------------------------------
    # Theme / Styles
    # -----------------------------------------------------------------------
    def _setup_styles(self) -> None:
        style = ttk.Style()
        style.theme_use("clam")

        style.configure("TFrame",  background=self.colors["bg"])
        style.configure("TLabel",  background=self.colors["bg"],
                        foreground=self.colors["fg"], font=("Segoe UI", 10))
        style.configure("Header.TLabel", font=("Impact", 32),
                        foreground=self.colors["accent"],
                        background=self.colors["panel"])

        style.configure("TNotebook",     background=self.colors["bg"], borderwidth=0)
        style.configure("TNotebook.Tab", background=self.colors["tab_bg"],
                        foreground=self.colors["tab_fg"],
                        font=("Segoe UI", 11, "bold"), padding=[20, 10])
        style.map("TNotebook.Tab",
                  background=[("selected", self.colors["accent"])],
                  foreground=[("selected", "white")])

        style.configure("Red.Horizontal.TProgressbar",
                        troughcolor="#222", background=self.colors["accent"],
                        thickness=20)

        style.configure("Treeview",
                        background=self.colors["list_bg"], foreground="white",
                        fieldbackground=self.colors["list_bg"],
                        font=("Consolas", 10), rowheight=35)
        style.configure("Treeview.Heading",
                        background="#333", foreground="white",
                        font=("Segoe UI", 10, "bold"))
        style.map("Treeview", background=[("selected", self.colors["accent"])])

    # -----------------------------------------------------------------------
    # UI-Aufbau
    # -----------------------------------------------------------------------
    def _setup_ui(self) -> None:
        # Header
        header = tk.Frame(self.root, bg=self.colors["panel"], height=100)
        header.pack(fill=tk.X)
        tk.Label(header, text="KATANA KRONOS", font=("Impact", 32),
                 fg=self.colors["accent"], bg=self.colors["panel"]).pack(pady=(15, 0))
        tk.Label(header, text="ENTERPRISE STORAGE & BACKUP OPS",
                 font=("Consolas", 10), fg="#666",
                 bg=self.colors["panel"]).pack(pady=(0, 15))

        # Tabs
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)

        self.tab_cockpit = tk.Frame(self.notebook, bg=self.colors["bg"])
        self.notebook.add(self.tab_cockpit, text="⚡ COCKPIT")
        self._build_cockpit(self.tab_cockpit)

        self.tab_sched = tk.Frame(self.notebook, bg=self.colors["bg"])
        self.notebook.add(self.tab_sched, text="🕒 AUTOPILOT")
        self._build_scheduler(self.tab_sched)

        self.tab_restore = tk.Frame(self.notebook, bg=self.colors["bg"])
        self.notebook.add(self.tab_restore, text="🔄 RESTORE")
        self._build_restore_tab(self.tab_restore)

        self.tab_log = tk.Frame(self.notebook, bg=self.colors["bg"])
        self.notebook.add(self.tab_log, text="📜 LOGBUCH")
        self._build_log_tab(self.tab_log)

    # --- Cockpit -----------------------------------------------------------
    def _build_cockpit(self, parent: tk.Frame) -> None:
        io_frame = tk.LabelFrame(parent, text=" STORAGE COMMANDER ",
                                 bg=self.colors["bg"], fg="#888",
                                 font=("Segoe UI", 9, "bold"))
        io_frame.pack(fill=tk.X, padx=20, pady=20)
        self._create_io_box(io_frame, "QUELLE (Von wo?)", self.src_path, "📂", is_source=True)
        self._create_io_box(io_frame, "ZIEL (Wohin?)",   self.dst_path, "💾", is_source=False)

        # Dashboard
        dash = tk.Frame(parent, bg=self.colors["bg"])
        dash.pack(fill=tk.X, padx=20, pady=10)
        self.lbl_speed = self._stat_label(dash, "SPEED",    "0.0 MB/s", tk.LEFT)
        self.lbl_files = self._stat_label(dash, "PROGRESS", "0%",       tk.LEFT)
        self.lbl_eta   = self._stat_label(dash, "ETA",      "--:--",    tk.RIGHT)

        # Fortschritt
        self.progress_var = tk.DoubleVar()
        ttk.Progressbar(parent, variable=self.progress_var, maximum=100,
                        style="Red.Horizontal.TProgressbar").pack(
            fill=tk.X, padx=20, pady=(20, 5))
        self.lbl_status = tk.Label(parent, text="System bereit.",
                                   bg=self.colors["bg"], fg="#666",
                                   font=("Consolas", 9))
        self.lbl_status.pack(fill=tk.X, padx=20)

        self.lbl_last_backup = tk.Label(parent, text="Letztes Backup: --",
                                        bg=self.colors["bg"],
                                        fg=self.colors["info"],
                                        font=("Consolas", 9))
        self.lbl_last_backup.pack(fill=tk.X, padx=20)

        # Strategie
        strat = tk.LabelFrame(parent, text=" BACKUP STRATEGIE ",
                              bg=self.colors["bg"], fg=self.colors["warning"])
        strat.pack(fill=tk.X, padx=20, pady=20)
        c1 = tk.Frame(strat, bg=self.colors["bg"])
        c1.pack(side=tk.LEFT, padx=20, pady=10)
        c2 = tk.Frame(strat, bg=self.colors["bg"])
        c2.pack(side=tk.LEFT, padx=20, pady=10)

        self._cb(c1, "🪞 Mirroring (Ziel exakt spiegeln - Löscht Fehlendes!)", self.mirror_mode,      "red")
        self._cb(c1, "🛡️ Versionierung (Gelöschtes -> _Archiv)",               self.versioning,       "white")
        self._cb(c1, "🧹 Smart Clean (Papierkörbe ignorieren)",                 self.smart_exclude,    "white")
        self._cb(c2, "🔍 Integrity Check (Inhalt prüfen)",                     self.verify_mode,      "white")
        self._cb(c2, "🌙 Auto-Suspend nach Abschluss",                         self.auto_suspend,     "white")
        self._cb(c2, "🧪 Dry-Run (Nur simulieren)",                            self.dry_run,          "cyan")
        self._cb(c2, "📋 Manifest erstellen (SHA256-Prüfsummen)",              self.generate_manifest, "white")

        # Steuerung
        btn_frame = tk.Frame(parent, bg=self.colors["bg"])
        btn_frame.pack(pady=20)
        self.btn_start = tk.Button(
            btn_frame, text="JETZT SICHERN",
            bg=self.colors["accent"], fg="white",
            font=("Segoe UI", 12, "bold"), width=18, pady=10,
            command=self.start_thread, relief="flat")
        self.btn_start.pack(side=tk.LEFT, padx=10)
        self.btn_stop = tk.Button(
            btn_frame, text="STOP",
            bg="#333", fg="white", font=("Segoe UI", 12, "bold"),
            width=8, pady=10, command=self.stop_process,
            relief="flat", state=tk.DISABLED)
        self.btn_stop.pack(side=tk.LEFT, padx=10)
        tk.Button(btn_frame, text="❓ HILFE",
                  bg=self.colors["help"], fg="white",
                  font=("Segoe UI", 11, "bold"), width=12, pady=10,
                  command=self._show_help, relief="flat").pack(side=tk.LEFT, padx=10)

    # --- Scheduler ---------------------------------------------------------
    def _build_scheduler(self, parent: tk.Frame) -> None:
        top_f = tk.Frame(parent, bg=self.colors["bg"], pady=20)
        top_f.pack(fill=tk.X, padx=20)
        tk.Checkbutton(
            top_f, text="AUTOMATISCHEN ZEITPLAN AKTIVIEREN",
            variable=self.sched_enabled,
            bg=self.colors["bg"], fg=self.colors["success"],
            font=("Segoe UI", 14, "bold"), selectcolor="#222",
            activebackground=self.colors["bg"],
            activeforeground=self.colors["success"]).pack()

        time_f = tk.LabelFrame(parent, text=" ZEITPLANUNG ",
                               bg=self.colors["bg"], fg="#888",
                               font=("Segoe UI", 10, "bold"))
        time_f.pack(fill=tk.X, padx=20, pady=10)

        row1 = tk.Frame(time_f, bg=self.colors["bg"])
        row1.pack(fill=tk.X, padx=20, pady=10)
        tk.Label(row1, text="Startzeit (HH:MM):",
                 bg=self.colors["bg"], fg="white").pack(side=tk.LEFT)
        tk.Entry(row1, textvariable=self.sched_time, width=10,
                 bg="#222", fg="white", insertbackground="white",
                 font=("Consolas", 12)).pack(side=tk.LEFT, padx=10)

        row2 = tk.Frame(time_f, bg=self.colors["bg"])
        row2.pack(fill=tk.X, padx=20, pady=10)
        for text, value in (("Täglich", "daily"), ("Wöchentlich:", "weekly")):
            tk.Radiobutton(row2, text=text, variable=self.sched_type,
                           value=value, bg=self.colors["bg"], fg="white",
                           selectcolor="#222",
                           activebackground=self.colors["bg"]).pack(
                side=tk.LEFT, padx=(0, 20))

        row3 = tk.Frame(time_f, bg=self.colors["bg"])
        row3.pack(fill=tk.X, padx=20, pady=5)
        for d in ("Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"):
            tk.Checkbutton(row3, text=d, variable=self.sched_days[d],
                           bg=self.colors["bg"], fg="#aaa",
                           selectcolor="#222",
                           activebackground=self.colors["bg"]).pack(
                side=tk.LEFT, padx=5)

        tk.Checkbutton(time_f, text="Verpasste Backups beim Start nachholen",
                       variable=self.run_missed,
                       bg=self.colors["bg"], fg="orange", selectcolor="#222",
                       activebackground=self.colors["bg"]).pack(
            anchor="w", padx=20, pady=10)

        # Retention
        ret_f = tk.LabelFrame(parent, text=" RETENTION (Auto-Löschen) ",
                              bg=self.colors["bg"], fg="#888",
                              font=("Segoe UI", 10, "bold"))
        ret_f.pack(fill=tk.X, padx=20, pady=10)
        r_row = tk.Frame(ret_f, bg=self.colors["bg"])
        r_row.pack(fill=tk.X, padx=20, pady=10)
        tk.Checkbutton(r_row, text="Archive löschen älter als:",
                       variable=self.retention_enabled,
                       bg=self.colors["bg"], fg="white", selectcolor="#222",
                       activebackground=self.colors["bg"]).pack(side=tk.LEFT)
        tk.Entry(r_row, textvariable=self.retention_days, width=5,
                 bg="#222", fg="white",
                 font=("Consolas", 11)).pack(side=tk.LEFT, padx=5)
        tk.Label(r_row, text="Tage",
                 bg=self.colors["bg"], fg="#aaa").pack(side=tk.LEFT)

        # Autostart & Next-Run-Label
        sys_f = tk.Frame(parent, bg=self.colors["bg"])
        sys_f.pack(fill=tk.X, padx=20, pady=20)
        tk.Button(sys_f, text="LINUX AUTOSTART EINRICHTEN",
                  bg="#37474f", fg="white",
                  command=self._setup_autostart, relief="flat").pack(side=tk.LEFT)
        self.lbl_next_run = tk.Label(sys_f, text="...",
                                     bg=self.colors["bg"],
                                     fg=self.colors["info"],
                                     font=("Consolas", 11, "bold"))
        self.lbl_next_run.pack(side=tk.RIGHT)

        # Backup-Historie
        hist_f = tk.LabelFrame(parent, text=" BACKUP-HISTORIE ",
                               bg=self.colors["bg"], fg="#888",
                               font=("Segoe UI", 10, "bold"))
        hist_f.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)
        cols = ("ts", "status", "dauer", "src", "dst")
        self.hist_tree = ttk.Treeview(hist_f, columns=cols,
                                      show="headings", selectmode="browse")
        self.hist_tree.heading("ts",     text="Zeitstempel")
        self.hist_tree.heading("status", text="Status")
        self.hist_tree.heading("dauer",  text="Dauer")
        self.hist_tree.heading("src",    text="Quelle")
        self.hist_tree.heading("dst",    text="Ziel")
        self.hist_tree.column("ts",     width=160)
        self.hist_tree.column("status", width=90)
        self.hist_tree.column("dauer",  width=80)
        self.hist_tree.column("src",    width=250)
        self.hist_tree.column("dst",    width=250)
        self.hist_tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        tk.Button(hist_f, text="🔄 Aktualisieren", bg="#333", fg="white",
                  relief="flat",
                  command=self._refresh_history_view).pack(
            anchor="e", padx=10, pady=(0, 10))

    # --- Restore -----------------------------------------------------------
    def _build_restore_tab(self, parent: tk.Frame) -> None:
        tk.Label(parent, text="BACKUP WIEDERHERSTELLEN",
                 font=("Impact", 20), fg=self.colors["accent"],
                 bg=self.colors["bg"]).pack(pady=15)
        tk.Label(parent,
                 text="Wählen Sie Ziel-Verzeichnis und anschließend einen"
                      " Archiv-Snapshot zur Wiederherstellung.",
                 bg=self.colors["bg"], fg="#888").pack()

        ctrl_f = tk.Frame(parent, bg=self.colors["bg"])
        ctrl_f.pack(fill=tk.X, padx=20, pady=10)
        tk.Label(ctrl_f, text="Backup-Ziel (enthält _Archiv):",
                 bg=self.colors["bg"], fg="white").pack(side=tk.LEFT)
        self.restore_dst_var = tk.StringVar()
        tk.Entry(ctrl_f, textvariable=self.restore_dst_var,
                 bg="#1a1a1a", fg="white", insertbackground="white",
                 relief="flat", font=("Consolas", 11),
                 width=50).pack(side=tk.LEFT, padx=10, ipady=5)
        tk.Button(ctrl_f, text="AUSWÄHLEN", bg="#333", fg="white",
                  relief="flat",
                  command=lambda: self.restore_dst_var.set(
                      filedialog.askdirectory())).pack(side=tk.LEFT)
        tk.Button(ctrl_f, text="🔍 SNAPSHOTS LADEN",
                  bg=self.colors["info"], fg="white", relief="flat",
                  command=self._load_restore_snapshots,
                  padx=10).pack(side=tk.LEFT, padx=10)

        snap_f = tk.LabelFrame(parent, text=" VERFÜGBARE SNAPSHOTS ",
                               bg=self.colors["bg"], fg="#888",
                               font=("Segoe UI", 10, "bold"))
        snap_f.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)
        snap_cols = ("name", "timestamp", "size", "path")
        self.snap_tree = ttk.Treeview(snap_f, columns=snap_cols,
                                      show="headings", selectmode="browse")
        self.snap_tree.heading("name",      text="Snapshot-Name")
        self.snap_tree.heading("timestamp", text="Zeitpunkt")
        self.snap_tree.heading("size",      text="Größe")
        self.snap_tree.heading("path",      text="Pfad")
        self.snap_tree.column("name",      width=220)
        self.snap_tree.column("timestamp", width=180)
        self.snap_tree.column("size",      width=100)
        self.snap_tree.column("path",      width=400)
        snap_scroll = ttk.Scrollbar(snap_f, orient="vertical",
                                    command=self.snap_tree.yview)
        self.snap_tree.configure(yscrollcommand=snap_scroll.set)
        self.snap_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True,
                            padx=(10, 0), pady=10)
        snap_scroll.pack(side=tk.RIGHT, fill=tk.Y, pady=10, padx=(0, 10))

        restore_ctrl = tk.Frame(parent, bg=self.colors["bg"])
        restore_ctrl.pack(fill=tk.X, padx=20, pady=10)
        tgt_f = tk.Frame(restore_ctrl, bg=self.colors["bg"])
        tgt_f.pack(fill=tk.X, pady=5)
        tk.Label(tgt_f, text="Wiederherstellen nach:",
                 bg=self.colors["bg"], fg="white").pack(side=tk.LEFT)
        self.restore_target_var = tk.StringVar()
        tk.Entry(tgt_f, textvariable=self.restore_target_var,
                 bg="#1a1a1a", fg="white", insertbackground="white",
                 relief="flat", font=("Consolas", 11),
                 width=50).pack(side=tk.LEFT, padx=10, ipady=5)
        tk.Button(tgt_f, text="AUSWÄHLEN", bg="#333", fg="white",
                  relief="flat",
                  command=lambda: self.restore_target_var.set(
                      filedialog.askdirectory())).pack(side=tk.LEFT)

        btn_f = tk.Frame(restore_ctrl, bg=self.colors["bg"])
        btn_f.pack(pady=10)
        tk.Button(btn_f, text="🧪 DRY-RUN (Simulation)",
                  bg="#37474f", fg="white",
                  font=("Segoe UI", 11, "bold"), relief="flat",
                  padx=15, pady=8,
                  command=lambda: self._start_restore(dry_run=True)).pack(
            side=tk.LEFT, padx=10)
        self.btn_restore = tk.Button(
            btn_f, text="🔄 JETZT WIEDERHERSTELLEN",
            bg=self.colors["restore"], fg="white",
            font=("Segoe UI", 11, "bold"), relief="flat",
            padx=15, pady=8,
            command=lambda: self._start_restore(dry_run=False))
        self.btn_restore.pack(side=tk.LEFT, padx=10)

        self.lbl_restore_status = tk.Label(parent, text="",
                                           bg=self.colors["bg"], fg="#aaa",
                                           font=("Consolas", 9))
        self.lbl_restore_status.pack(fill=tk.X, padx=20)

    # --- Log-Tab -----------------------------------------------------------
    def _build_log_tab(self, parent: tk.Frame) -> None:
        self.console = scrolledtext.ScrolledText(
            parent, bg="#000", fg="#0f0",
            font=("Consolas", 10), bd=0)
        self.console.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)

        btn_row = tk.Frame(parent, bg=self.colors["bg"])
        btn_row.pack(fill=tk.X, padx=20, pady=(0, 10))
        tk.Button(btn_row, text="🗑️ LOG LEEREN", bg="#333", fg="white",
                  relief="flat", command=self._clear_log).pack(
            side=tk.LEFT, padx=5)
        tk.Button(btn_row, text="📂 LOG ÖFFNEN", bg="#333", fg="white",
                  relief="flat", command=self._open_log_file).pack(
            side=tk.LEFT, padx=5)

    # -----------------------------------------------------------------------
    # Widget-Hilfsmethoden
    # -----------------------------------------------------------------------
    def _create_io_box(self, parent: tk.Frame, title: str,
                       var: tk.StringVar, icon: str,
                       is_source: bool) -> None:
        f = tk.Frame(parent, bg=self.colors["bg"])
        f.pack(fill=tk.X, pady=5)
        tk.Label(f, text=f"{icon} {title}", width=18, anchor="w",
                 bg=self.colors["bg"], fg="#888",
                 font=("Segoe UI", 9, "bold")).pack(side=tk.LEFT)
        tk.Entry(f, textvariable=var, bg="#1a1a1a", fg="white",
                 insertbackground="white", relief="flat",
                 font=("Consolas", 11)).pack(
            side=tk.LEFT, fill=tk.X, expand=True, ipady=6, padx=10)
        tk.Button(f, text="AUSWÄHLEN", bg="#333", fg="white", relief="flat",
                  command=lambda: self._open_drive_selector(
                      var, is_source)).pack(side=tk.LEFT)

    def _stat_label(self, parent: tk.Frame, title: str,
                    default: str, align: str) -> tk.Label:
        f = tk.Frame(parent, bg=self.colors["bg"])
        f.pack(side=align, padx=30, expand=True)
        lbl = tk.Label(f, text=default, font=("Consolas", 24, "bold"),
                       fg=self.colors["fg"], bg=self.colors["bg"])
        lbl.pack()
        tk.Label(f, text=title, font=("Arial", 8, "bold"),
                 fg="#555", bg=self.colors["bg"]).pack()
        return lbl

    def _cb(self, parent: tk.Frame, text: str,
            var: tk.BooleanVar, color: str) -> None:
        fg_col = self.colors["fg"] if color == "white" else color
        tk.Checkbutton(parent, text=text, variable=var,
                       bg=self.colors["bg"], fg=fg_col,
                       selectcolor="#222",
                       activebackground=self.colors["bg"],
                       activeforeground=fg_col).pack(anchor="w", pady=2)

    # -----------------------------------------------------------------------
    # Drive Selector
    # -----------------------------------------------------------------------
    def _open_drive_selector(self, target_var: tk.StringVar,
                             is_source: bool) -> None:
        dlg = tk.Toplevel(self.root)
        title = "QUELLE WÄHLEN" if is_source else "ZIEL WÄHLEN"
        dlg.title(f"Storage Commander – {title}")
        dlg.geometry("1000x650")
        dlg.configure(bg=self.colors["bg"])

        tk.Label(dlg, text=title, font=("Impact", 18),
                 bg=self.colors["bg"], fg=self.colors["fg"]).pack(pady=10)
        tk.Label(dlg, text="Wählen Sie ein Laufwerk aus der Liste:",
                 bg=self.colors["bg"], fg="#888").pack()

        columns = ("name", "path", "size", "fs", "type")
        tree = ttk.Treeview(dlg, columns=columns,
                            show="headings", selectmode="browse")
        tree.heading("name", text="Label / Bezeichnung")
        tree.heading("path", text="Pfad (Mountpoint)")
        tree.heading("size", text="Belegung")
        tree.heading("fs",   text="Dateisystem")
        tree.heading("type", text="Anschluss")
        tree.column("name", width=250)
        tree.column("path", width=300)
        tree.column("size", width=180)
        tree.column("fs",   width=80)
        tree.column("type", width=100)
        tree.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)

        self._populate_drives(tree, is_source)

        btn_frame = tk.Frame(dlg, bg=self.colors["bg"])
        btn_frame.pack(fill=tk.X, padx=20, pady=20)

        def on_select() -> None:
            sel = tree.selection()
            if sel:
                target_var.set(tree.item(sel[0])["values"][1])
                dlg.destroy()

        def on_subfolder() -> None:
            sel = tree.selection()
            if not sel:
                messagebox.showwarning(
                    "Hinweis",
                    "Bitte zuerst ein Laufwerk aus der Liste markieren.")
                return
            root_path = tree.item(sel[0])["values"][1]
            chosen = filedialog.askdirectory(
                initialdir=root_path, title="Unterordner wählen")
            if chosen:
                target_var.set(chosen)
                dlg.destroy()

        tk.Button(btn_frame, text="✅ LAUFWERK ÜBERNEHMEN",
                  bg=self.colors["success"], fg="white",
                  font=("Segoe UI", 11, "bold"), command=on_select,
                  relief="flat", padx=20, pady=10).pack(side=tk.RIGHT)
        tk.Button(btn_frame, text="📂 ORDNER WÄHLEN",
                  bg=self.colors["info"], fg="white",
                  font=("Segoe UI", 11, "bold"), command=on_subfolder,
                  relief="flat", padx=20, pady=10).pack(
            side=tk.RIGHT, padx=10)
        tk.Button(btn_frame, text="Manuell suchen...",
                  bg="#333", fg="white", relief="flat",
                  padx=10, pady=10,
                  command=lambda: [
                      target_var.set(filedialog.askdirectory()),
                      dlg.destroy()]).pack(side=tk.LEFT)
        tk.Button(btn_frame, text="🔄 Aktualisieren",
                  bg="#333", fg="white", relief="flat",
                  padx=10, pady=10,
                  command=lambda: self._populate_drives(
                      tree, is_source)).pack(side=tk.LEFT, padx=10)

    def _populate_drives(self, tree: ttk.Treeview, is_source: bool) -> None:
        """Füllt den Drive-Selector mit Shortcuts und lsblk-Geräten."""
        for item in tree.get_children():
            tree.delete(item)

        # Benutzer-Shortcuts (nur für Quelle)
        if is_source:
            user = os.path.expanduser("~")
            shortcuts = [
                ("🏠 Home",        user),
                ("📄 Dokumente",   os.path.join(user, "Dokumente")),
                ("📷 Bilder",      os.path.join(user, "Bilder")),
                ("🎥 Videos",      os.path.join(user, "Videos")),
            ]
            for name, path in shortcuts:
                if os.path.exists(path):
                    fs = self._detect_fs_for_path(path)
                    tree.insert("", tk.END,
                                values=(name, path, "Verzeichnis", fs, "USER"))

        # Hardware-Scan via lsblk
        try:
            result = subprocess.run(
                ["lsblk", "-J", "-l", "-o",
                 "NAME,LABEL,MOUNTPOINT,SIZE,FSTYPE,TRAN"],
                capture_output=True, text=True, timeout=10)
            data = json.loads(result.stdout)

            skip_mounts = {"/boot/efi", "/boot", "/snap"}
            for device in data.get("blockdevices", []):
                mount = device.get("mountpoint")
                if not mount or mount in skip_mounts:
                    continue

                label    = device.get("label") or device.get("name") or "Unbekannt"
                tran     = (device.get("tran") or "SATA").upper()
                fstype   = (device.get("fstype") or "UNKNOWN").upper()

                try:
                    usage     = shutil.disk_usage(mount)
                    usage_str = (
                        f"{usage.free / 1024**3:.1f} GB frei"
                        f" / {usage.total / 1024**3:.1f} GB"
                    )
                except OSError:
                    usage_str = device.get("size", "?")

                if "USB" in tran:
                    icon, type_desc = "🔌", "EXTERN (USB)"
                elif "NVME" in tran:
                    icon, type_desc = "🚀", "NVMe SSD"
                else:
                    icon, type_desc = "💽", "INTERN"

                display_name = f"{icon} {label}"
                if not is_source and "USB" in tran:
                    display_name = f"⭐ {display_name}"

                tree.insert("", tk.END,
                            values=(display_name, mount,
                                    usage_str, fstype, type_desc))

        except Exception as exc:
            tree.insert("", tk.END,
                        values=("SCAN FEHLER", str(exc), "", "", ""))

    def _detect_fs_for_path(self, path: str) -> str:
        """Ermittelt das Dateisystem eines Pfads via df."""
        try:
            result = subprocess.run(
                ["df", "--output=fstype", path],
                capture_output=True, text=True, timeout=5)
            lines = result.stdout.strip().splitlines()
            if len(lines) >= 2:
                return lines[1].strip().upper()
        except Exception:
            pass
        return "UNKNOWN"

    # -----------------------------------------------------------------------
    # Hilfe-Dialog
    # -----------------------------------------------------------------------
    def _show_help(self) -> None:
        hw = tk.Toplevel(self.root)
        hw.title("Katana Kronos – Benutzerhandbuch")
        hw.geometry("950x850")
        hw.configure(bg=self.colors["bg"])

        txt = scrolledtext.ScrolledText(
            hw, bg=self.colors["panel"], fg=self.colors["fg"],
            font=("Segoe UI", 11), padx=30, pady=30, relief="flat")
        txt.pack(fill=tk.BOTH, expand=True)

        txt.tag_config("H1", font=("Impact", 24),
                       foreground=self.colors["accent"],
                       spacing1=20, spacing3=10)
        txt.tag_config("H2", font=("Segoe UI", 16, "bold"),
                       foreground=self.colors["info"],
                       spacing1=15, spacing3=5)
        txt.tag_config("WARN", foreground="#ff5555",
                       font=("Segoe UI", 11, "bold"))

        help_text = (
            "\nKATANA KRONOS – BENUTZERHANDBUCH\n"
            "================================\n\n"
            "Willkommen bei Katana Kronos. Dieses Handbuch erklärt jeden"
            " Schalter und Knopf.\n\n"
            "TAB 1: DAS COCKPIT\n"
            "------------------\n\n"
            "QUELLE & ZIEL\n"
            "  Klick auf AUSWÄHLEN öffnet den Storage Commander mit allen"
            " angeschlossenen Laufwerken.\n"
            "  Mit ORDNER WÄHLEN können Sie einen Unterordner (z.B. Fotos)"
            " direkt auswählen.\n\n"
            "DASHBOARD\n"
            "  SPEED: Aktuelle Übertragungsrate.\n"
            "  PROGRESS: Gesamtfortschritt in Prozent.\n"
            "  ETA: Geschätzte Restzeit.\n\n"
            "BACKUP STRATEGIE\n\n"
            "  [ ] Mirroring – WARNUNG: Macht das Ziel exakt gleich wie die"
            " Quelle.\n"
            "      Gelöschte Dateien auf der Quelle werden auch auf dem Ziel"
            " gelöscht!\n\n"
            "  [ ] Versionierung – Empfohlen. Vor dem Überschreiben oder"
            " Löschen wird die alte\n"
            "      Version in _Archiv gesichert.\n\n"
            "  [ ] Smart Clean – Ignoriert Papierkörbe, .tmp, Thumbs.db etc.\n\n"
            "  [ ] Integrity Check – Prüft den Dateiinhalt (langsamer, sicherer).\n\n"
            "  [ ] Auto-Suspend – PC geht nach Abschluss in den Schlafmodus.\n\n"
            "  [ ] Dry-Run – Simulation ohne Datenänderungen.\n\n"
            "  [ ] Manifest erstellen – SHA256-Prüfsummendatei im Zielordner.\n\n"
            "  HINWEIS: Katana kopiert immer den INHALT des Quell-Ordners"
            " ins Ziel.\n\n"
            "STEUERUNG\n"
            "  JETZT SICHERN – Startet sofort.\n"
            "  STOP – Erst SIGTERM, nach 5 Sekunden SIGKILL.\n\n\n"
            "TAB 2: DER AUTOPILOT\n"
            "--------------------\n\n"
            "  Startzeit, täglich oder wöchentlich konfigurierbar.\n"
            "  'Verpasste nachholen' holt das Backup beim nächsten Start nach.\n"
            "  Retention löscht alte _Archiv-Snapshots automatisch.\n"
            "  Backup-Historie zeigt die letzten 20 Läufe.\n\n\n"
            "TAB 3: RESTORE\n"
            "--------------\n\n"
            "  Zielverzeichnis (mit _Archiv) wählen → Snapshots laden →\n"
            "  Snapshot auswählen → Zielordner festlegen →\n"
            "  Erst DRY-RUN testen, dann echten Restore starten.\n\n\n"
            "TAB 4: LOGBUCH\n"
            "--------------\n\n"
            "  Alle Aktionen werden hier protokolliert.\n"
            "  Rote Einträge weisen auf Fehler hin.\n"
        )

        txt.insert(tk.END, help_text)
        content = txt.get("1.0", tk.END)

        # Titel
        txt.tag_add("H1", "2.0", "3.0")

        # Tab-Überschriften
        for header in ("TAB 1:", "TAB 2:", "TAB 3:", "TAB 4:"):
            start = content.find(header)
            if start != -1:
                line_idx = content[:start].count("\n") + 1
                txt.tag_add("H2", f"{line_idx}.0", f"{line_idx}.end")

        # Warnungen hervorheben
        pos = "1.0"
        while True:
            pos = txt.search("WARNUNG:", pos, stopindex=tk.END)
            if not pos:
                break
            end = f"{pos} lineend"
            txt.tag_add("WARN", pos, end)
            pos = end

        txt.config(state=tk.DISABLED)

    # -----------------------------------------------------------------------
    # Backup – Kernlogik
    # -----------------------------------------------------------------------
    def start_thread(self, automated: bool = False) -> None:
        """Validiert Eingaben, führt Preflight-Check durch und startet Backup-Thread."""
        src = self.src_path.get().strip()
        dst = self.dst_path.get().strip()

        if not src or not dst:
            msg = "Bitte Quelle und Ziel wählen!"
            if not automated:
                messagebox.showerror("Fehler", msg)
            else:
                self.log(f"FEHLER: {msg}")
            return

        if not os.access(src, os.R_OK):
            msg = f"Kein Lesezugriff auf Quelle:\n{src}"
            if not automated:
                messagebox.showerror("Fehler", msg)
            else:
                self.log(f"FEHLER: {msg}")
            return

        if not os.access(dst, os.W_OK):
            msg = f"Kein Schreibzugriff auf Ziel:\n{dst}"
            if not automated:
                messagebox.showerror("Fehler", msg)
            else:
                self.log(f"FEHLER: {msg}")
            return

        # FIX P1: Preflight-Rückgabewert auswerten – Abbruch durch User respektieren
        if not self._preflight_diskspace_check(src, dst, automated):
            return

        self._save_config()
        self.is_running = True
        self.btn_start.config(state=tk.DISABLED, bg="#222")
        self.btn_stop.config(state=tk.NORMAL, bg=self.colors["accent"])
        self.lbl_files.config(text="Scan...")

        t = threading.Thread(target=self._run_rsync,
                             args=(src, dst), daemon=True)
        t.start()

    def _preflight_diskspace_check(self, src: str, dst: str,
                                   automated: bool = False) -> bool:
        """
        Warnt bei möglicherweise unzureichendem Speicherplatz.

        FIX P2: Quellgröße wird über _get_dir_size() ermittelt, nicht über
        shutil.disk_usage(), das die gesamte Partition misst.

        Returns:
            True  → Backup darf fortfahren
            False → Benutzer hat abgebrochen (nur im manuellen Modus möglich)
        """
        try:
            # Tatsächliche Größe des Quell-Verzeichnisses
            src_size_bytes  = self._get_dir_size(src)
            dst_usage       = shutil.disk_usage(dst)
            available_bytes = dst_usage.free

            self.log(
                f"Pre-flight: Quelle ca. {self._format_bytes(src_size_bytes)}"
                f", Ziel frei: {self._format_bytes(available_bytes)}"
            )

            min_free = max(
                src_size_bytes * 0.10,
                MIN_FREE_SPACE_WARN_GB * 1024 ** 3
            )

            if available_bytes < min_free:
                warn_msg = (
                    f"⚠️ Speicherplatz-Warnung!\n\n"
                    f"Quelle belegt ca.:   {self._format_bytes(src_size_bytes)}\n"
                    f"Ziel verfügbar:      {self._format_bytes(available_bytes)}\n\n"
                    f"Möglicherweise nicht genug Platz für Backup + Archiv.\n"
                    f"Trotzdem fortfahren?"
                )
                if not automated:
                    if not messagebox.askyesno("Speicherplatz-Warnung", warn_msg):
                        return False  # Benutzer hat abgebrochen
                else:
                    self.log(warn_msg.replace("\n", " "))

        except Exception as exc:
            self.log(f"WARN: Speicherplatz-Check fehlgeschlagen: {exc}")

        return True

    def _run_rsync(self, src: str, dst: str) -> None:
        """
        Baut den rsync-Befehl zusammen und führt ihn aus.

        FIX P3: src wird immer mit Trailing-Slash übergeben → rsync kopiert
        den INHALT des Quell-Ordners, nicht das Verzeichnis selbst.
        """
        self._backup_start_time = time.monotonic()

        # Trailing-Slash normalisieren: Inhalt kopieren, nicht Verzeichnis selbst
        src_arg = src.rstrip("/") + "/"

        cmd = ["rsync", "-a", "--info=progress2", "--no-inc-recursive"]

        if not self.verify_mode.get():
            cmd.extend(["-W", "--no-compress", "--preallocate"])
        else:
            cmd.append("--checksum")

        if self.mirror_mode.get():
            cmd.append("--delete")

        if self.versioning.get():
            ts         = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            backup_dir = os.path.join(dst, "_Archiv", ts)
            cmd.extend(["--backup", f"--backup-dir={backup_dir}", "--suffix="])

        if self.smart_exclude.get():
            for pattern in (
                ".Trash-*", ".trash", "lost+found",
                "*.tmp", "*.bak", "Thumbs.db", ".DS_Store",
                ".cache", "katana_manifest.sha256"
            ):
                cmd.extend(["--exclude", pattern])

        if self.dry_run.get():
            cmd.append("--dry-run")

        cmd.extend([src_arg, dst])
        self.log(f"Starte Backup: {src_arg} -> {dst}")
        self.log(f"rsync-Befehl: {' '.join(cmd)}")

        try:
            with self._process_lock:
                self.process = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    universal_newlines=True, bufsize=1)

            for line in self.process.stdout:
                line = line.strip()
                if not line:
                    continue
                mp = _RE_PCT.search(line)
                ms = _RE_SPD.search(line)
                me = _RE_ETA.search(line)
                if mp:
                    self.msg_queue.put((
                        "STATS",
                        int(mp.group(1)),
                        ms.group(1) if ms else "",
                        me.group(1) if me else "",
                        line,
                    ))
                elif "error" in line.lower():
                    self.msg_queue.put(("LOG", f"ERR: {line}"))

            self.process.wait()
            elapsed = time.monotonic() - self._backup_start_time

            if self.process.returncode == 0:
                self.msg_queue.put(("DONE", "Backup erfolgreich.",
                                    elapsed, src, dst))
            else:
                self.msg_queue.put(("ERROR",
                                    f"rsync Exit-Code {self.process.returncode}",
                                    elapsed, src, dst))

        except Exception as exc:
            elapsed = time.monotonic() - (self._backup_start_time or 0.0)
            self.msg_queue.put(("ERROR", str(exc), elapsed, src, dst))

    # -----------------------------------------------------------------------
    # Prozess stoppen
    # -----------------------------------------------------------------------
    def stop_process(self) -> None:
        """Graceful Shutdown: SIGTERM, dann nach Timeout SIGKILL."""
        # FIX P5: Lokale Kopie erstellen, um Race Condition zu vermeiden
        with self._process_lock:
            proc = self.process

        if proc is None:
            return

        try:
            os.kill(proc.pid, signal.SIGTERM)
            self.msg_queue.put(
                ("LOG", "⏹ SIGTERM gesendet – warte auf sauberes Beenden..."))
            self.root.after(SIGTERM_WAIT_MS,
                            lambda: self._force_kill_process(proc))
        except ProcessLookupError:
            self.msg_queue.put(("LOG", "Prozess bereits beendet."))
        except Exception as exc:
            self.msg_queue.put(("LOG", f"Stop-Fehler: {exc}"))

    def _force_kill_process(self, proc: subprocess.Popen) -> None:
        """Sendet SIGKILL, falls der Prozess nach SIGTERM noch läuft."""
        if proc.poll() is None:
            try:
                os.kill(proc.pid, signal.SIGKILL)
                self.msg_queue.put(
                    ("LOG", "⛔ SIGKILL gesendet (Abbruch durch Benutzer)."))
            except ProcessLookupError:
                pass
            except Exception as exc:
                self.msg_queue.put(("LOG", f"Force-Kill-Fehler: {exc}"))

    # -----------------------------------------------------------------------
    # Queue-Monitor (Haupt-Thread)
    # -----------------------------------------------------------------------
    def _monitor_queue(self) -> None:
        try:
            while True:
                msg = self.msg_queue.get_nowait()
                tag = msg[0]

                if tag == "LOG":
                    self.log(msg[1])

                elif tag == "STATS":
                    _, pct, spd, eta, status_line = msg
                    self.progress_var.set(pct)
                    self.lbl_files.config(text=f"{pct}%")
                    if spd:
                        self.lbl_speed.config(text=spd)
                    if eta:
                        self.lbl_eta.config(text=eta)
                    self.lbl_status.config(text=status_line)

                elif tag == "DONE":
                    _, result_msg, elapsed, src, dst = msg
                    # FIX P9: Variablen-Shadowing vermieden
                    total_secs = int(elapsed)
                    hrs, remainder = divmod(total_secs, 3600)
                    mins, secs    = divmod(remainder, 60)
                    duration_str = (
                        f"{hrs:02d}:{mins:02d}:{secs:02d}"
                        if hrs > 0 else f"{mins:02d}:{secs:02d}"
                    )
                    self.log(f"✅ {result_msg} | Dauer: {duration_str}")
                    self.progress_var.set(100)
                    self.lbl_speed.config(text="FERTIG",
                                          fg=self.colors["success"])
                    self._record_last_run(
                        status="OK", elapsed=elapsed, src=src, dst=dst)

                    if self.generate_manifest.get() and not self.dry_run.get():
                        threading.Thread(
                            target=self._generate_manifest,
                            args=(dst,), daemon=True).start()

                    # FIX P4: Retention im eigenen Thread – kein GUI-Freeze
                    threading.Thread(
                        target=self._retention_cleanup,
                        daemon=True).start()

                    if self.auto_suspend.get():
                        self.log("🌙 System wird in 30 Sekunden suspended...")
                        self.root.after(30_000, lambda: subprocess.run(
                            ["systemctl", "suspend"], check=False))

                    self._reset_ui()

                elif tag == "ERROR":
                    _, err_msg, elapsed, src, dst = msg
                    self.log(f"❌ Fehler: {err_msg}")
                    self.lbl_speed.config(text="ERROR", fg="red")
                    self._record_last_run(
                        status="ERR", elapsed=elapsed, src=src, dst=dst)
                    self._reset_ui()

                elif tag == "RESTORE_DONE":
                    self.btn_restore.config(state=tk.NORMAL,
                                           bg=self.colors["restore"])
                    ok = msg[1]
                    self.lbl_restore_status.config(
                        text="✅ Restore abgeschlossen."
                             if ok else "⚠️ Restore mit Fehler beendet.",
                        fg=self.colors["success"] if ok else "red")

        except queue.Empty:
            pass

        self.root.after(QUEUE_POLL_MS, self._monitor_queue)

    def _reset_ui(self) -> None:
        self.is_running = False
        with self._process_lock:
            self.process = None
        self._backup_start_time = None
        self.btn_start.config(state=tk.NORMAL, bg=self.colors["accent"])
        self.btn_stop.config(state=tk.DISABLED, bg="#333")

    # -----------------------------------------------------------------------
    # Manifest-Generierung (in separatem Thread)
    # -----------------------------------------------------------------------
    def _generate_manifest(self, dst: str) -> None:
        """Erstellt SHA256-Prüfsummendatei aller Dateien im Ziel (ohne _Archiv)."""
        manifest_path = os.path.join(dst, "katana_manifest.sha256")
        self.msg_queue.put(("LOG", "📋 Manifest-Generierung gestartet..."))
        try:
            count = 0
            with open(manifest_path, "w", encoding="utf-8") as mf:
                mf.write(f"# Katana Kronos Manifest – {datetime.now().isoformat()}\n")
                mf.write(f"# Quelle: {self.src_path.get()}\n")
                mf.write(f"# Ziel:   {dst}\n\n")
                for dirpath, dirnames, filenames in os.walk(dst):
                    dirnames[:] = [d for d in dirnames if d != "_Archiv"]
                    for fname in filenames:
                        if fname == "katana_manifest.sha256":
                            continue
                        fpath = os.path.join(dirpath, fname)
                        try:
                            sha256 = self._sha256_file(fpath)
                            rel    = os.path.relpath(fpath, dst)
                            mf.write(f"{sha256}  {rel}\n")
                            count += 1
                        except (IOError, OSError) as exc:
                            mf.write(
                                f"ERROR  {os.path.relpath(fpath, dst)}"
                                f"  ({exc})\n")
            self.msg_queue.put(
                ("LOG", f"✅ Manifest erstellt: {count} Dateien → {manifest_path}"))
        except Exception as exc:
            self.msg_queue.put(("LOG", f"❌ Manifest-Fehler: {exc}"))

    def _sha256_file(self, path: str, chunk_size: int = 1024 * 1024) -> str:
        """Berechnet SHA256-Prüfsumme einer Datei blockweise."""
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(chunk_size):
                h.update(chunk)
        return h.hexdigest()

    # -----------------------------------------------------------------------
    # Retention-Cleanup (in separatem Thread – FIX P4)
    # -----------------------------------------------------------------------
    def _retention_cleanup(self) -> None:
        """Löscht alte Archiv-Snapshots anhand des Verzeichnisnamens."""
        if not self.retention_enabled.get():
            return
        dst    = self.dst_path.get()
        archiv = os.path.join(dst, "_Archiv")
        if not os.path.exists(archiv):
            return

        limit         = datetime.now() - timedelta(days=self.retention_days.get())
        deleted_count = 0

        self.msg_queue.put((
            "LOG",
            f"♻️ Retention-Prüfung: Archive älter als"
            f" {self.retention_days.get()} Tage werden gelöscht..."
        ))

        for entry in os.scandir(archiv):
            if not entry.is_dir():
                continue
            # Timestamp aus Verzeichnisnamen parsen (zuverlässiger als ctime)
            try:
                dir_dt = datetime.strptime(entry.name, "%Y-%m-%d_%H-%M-%S")
            except ValueError:
                try:
                    dir_dt = datetime.fromtimestamp(
                        os.path.getctime(entry.path))
                except OSError as err:
                    self.msg_queue.put((
                        "LOG",
                        f"WARN: Zeitstempel für {entry.name} nicht"
                        f" ermittelbar: {err}"))
                    continue

            if dir_dt < limit:
                try:
                    shutil.rmtree(entry.path)
                    self.msg_queue.put(("LOG", f"♻️ Gelöscht: {entry.name}"))
                    deleted_count += 1
                except OSError as err:
                    self.msg_queue.put((
                        "LOG", f"ERR: Konnte {entry.name} nicht löschen: {err}"))

        self.msg_queue.put((
            "LOG",
            f"♻️ Retention abgeschlossen: {deleted_count} Snapshot(s) gelöscht."
        ))

    # -----------------------------------------------------------------------
    # Restore
    # -----------------------------------------------------------------------
    def _load_restore_snapshots(self) -> None:
        """Lädt verfügbare Archiv-Snapshots aus dem _Archiv-Verzeichnis."""
        dst = self.restore_dst_var.get().strip()
        if not dst:
            dst = self.dst_path.get().strip()
            self.restore_dst_var.set(dst)

        archiv_dir = os.path.join(dst, "_Archiv")
        for item in self.snap_tree.get_children():
            self.snap_tree.delete(item)

        if not os.path.isdir(archiv_dir):
            messagebox.showwarning(
                "Hinweis",
                f"Kein _Archiv-Verzeichnis gefunden:\n{archiv_dir}")
            return

        snapshots: list[tuple] = []
        try:
            for entry in os.scandir(archiv_dir):
                if not entry.is_dir():
                    continue
                try:
                    dt = datetime.strptime(entry.name, "%Y-%m-%d_%H-%M-%S")
                except ValueError:
                    dt = None
                size_str = self._format_bytes(self._get_dir_size(entry.path))
                snapshots.append((entry.name, dt, size_str, entry.path))
        except Exception as exc:
            messagebox.showerror(
                "Fehler", f"Snapshots konnten nicht geladen werden:\n{exc}")
            return

        # Neueste zuerst
        snapshots.sort(
            key=lambda x: x[1] if x[1] else datetime.min, reverse=True)
        for name, dt, size_str, path in snapshots:
            ts_str = dt.strftime("%d.%m.%Y %H:%M:%S") if dt else "Unbekannt"
            self.snap_tree.insert("", tk.END,
                                  values=(name, ts_str, size_str, path))

        self.lbl_restore_status.config(
            text=f"{len(snapshots)} Snapshot(s) gefunden.")

    def _start_restore(self, dry_run: bool = False) -> None:
        """Startet den Restore-Vorgang aus einem gewählten Snapshot."""
        selected = self.snap_tree.selection()
        if not selected:
            messagebox.showwarning("Hinweis",
                                   "Bitte zuerst einen Snapshot auswählen.")
            return

        snap_path = self.snap_tree.item(selected[0])["values"][3]
        target    = self.restore_target_var.get().strip()

        if not target:
            messagebox.showerror(
                "Fehler",
                "Bitte Ziel-Verzeichnis für die Wiederherstellung angeben.")
            return
        if not os.path.isdir(target):
            messagebox.showerror(
                "Fehler", f"Ziel-Verzeichnis existiert nicht:\n{target}")
            return
        if not os.path.isdir(snap_path):
            messagebox.showerror(
                "Fehler",
                f"Snapshot-Verzeichnis nicht gefunden:\n{snap_path}")
            return

        mode = "DRY-RUN SIMULATION" if dry_run else "WIEDERHERSTELLUNG"
        if not dry_run:
            if not messagebox.askyesno(
                    "Bestätigung",
                    f"⚠️ Dateien werden aus:\n{snap_path}\n\nnach:\n{target}\n\n"
                    f"wiederhergestellt!\nVorhandene Dateien werden überschrieben.\n\n"
                    f"Jetzt fortfahren?"):
                return

        self.btn_restore.config(state=tk.DISABLED, bg="#555")
        self.lbl_restore_status.config(
            text=f"🔄 {mode} läuft...", fg="orange")
        self.notebook.select(self.tab_log)

        threading.Thread(
            target=self._run_restore_thread,
            args=(snap_path, target, dry_run), daemon=True).start()

    def _run_restore_thread(self, snap_path: str, target: str,
                            dry_run: bool) -> None:
        """
        rsync-basierter Restore in separatem Thread.

        FIX P7: Reine Progress-Zeilen (beginnen mit Leerzeichen + Zahl)
        werden herausgefiltert, um das Log nicht zu fluten.
        """
        # Trailing-Slash: Inhalt des Snapshots ins Ziel kopieren
        src_arg  = snap_path.rstrip("/") + "/"
        cmd      = ["rsync", "-a", "--info=progress2", "--no-inc-recursive"]
        if dry_run:
            cmd.append("--dry-run")
        cmd.extend([src_arg, target])

        mode_str = "Restore DRY-RUN" if dry_run else "RESTORE"
        self.msg_queue.put(
            ("LOG", f"🔄 {mode_str}: {snap_path} -> {target}"))

        try:
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True, bufsize=1)

            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                # Redundante Progress-Zeilen unterdrücken
                if _RE_PROGRESS_LINE.match(line) and "error" not in line.lower():
                    continue
                self.msg_queue.put(("LOG", f"  {line}"))

            proc.wait()
            if proc.returncode == 0:
                self.msg_queue.put(
                    ("LOG", f"✅ {mode_str} abgeschlossen (Exit 0)."))
                self.msg_queue.put(("RESTORE_DONE", True))
            else:
                self.msg_queue.put((
                    "LOG",
                    f"⚠️ {mode_str} mit Fehler beendet (Code {proc.returncode})."))
                self.msg_queue.put(("RESTORE_DONE", False))

        except Exception as exc:
            self.msg_queue.put(("LOG", f"❌ Restore-Fehler: {exc}"))
            self.msg_queue.put(("RESTORE_DONE", False))

    # -----------------------------------------------------------------------
    # Scheduler
    # -----------------------------------------------------------------------
    def _scheduler_loop(self) -> None:
        """
        Prüft alle SCHEDULER_INTERVAL_MS, ob ein Backup fällig ist.

        Hinweis: Bei 10-Sekunden-Intervall ist das Trigger-Fenster von 60 s
        ausreichend. Unter extremer GUI-Last könnte ein Tick überspringen –
        _check_missed_backup fängt solche Fälle beim nächsten Start ab.
        """
        if self.sched_enabled.get() and not self.is_running:
            now = datetime.now()
            try:
                sh, sm = map(int, self.sched_time.get().split(":"))
                is_due = False

                if self.sched_type.get() == "daily":
                    is_due = (now.hour == sh and now.minute == sm)

                elif self.sched_type.get() == "weekly":
                    wday_map = {
                        0: "Mo", 1: "Di", 2: "Mi", 3: "Do",
                        4: "Fr", 5: "Sa", 6: "So"
                    }
                    today_code = wday_map[now.weekday()]
                    is_due = (
                        self.sched_days[today_code].get()
                        and now.hour == sh
                        and now.minute == sm
                    )

                if is_due:
                    self.log("🕒 Zeitplan ausgelöst!")
                    self.start_thread(automated=True)
                    # 61 Sekunden warten – verhindert Doppel-Trigger in derselben Minute
                    self.root.after(61_000, self._scheduler_loop)
                    return

            except (ValueError, KeyError):
                pass

        self.lbl_next_run.config(
            text=f"Check: {datetime.now().strftime('%H:%M')}")
        self.root.after(SCHEDULER_INTERVAL_MS, self._scheduler_loop)

    def _check_missed_backup(self) -> None:
        """
        Prüft beim Programmstart, ob ein geplantes Backup verpasst wurde.
        Wird 5 Sekunden nach Init aufgerufen (nach _load_config).
        """
        if not self.sched_enabled.get() or not self.run_missed.get() \
                or self.is_running:
            return

        try:
            sh, sm = map(int, self.sched_time.get().split(":"))
        except ValueError:
            return

        now  = datetime.now()
        last = datetime.fromisoformat(self.last_run) if self.last_run else None

        if self.sched_type.get() == "daily":
            scheduled_today = now.replace(
                hour=sh, minute=sm, second=0, microsecond=0)
            if now >= scheduled_today:
                if last is None or last < scheduled_today:
                    self.log("🕒 Verpasstes tägliches Backup wird nachgeholt...")
                    self.start_thread(automated=True)

        elif self.sched_type.get() == "weekly":
            wday_map = {
                0: "Mo", 1: "Di", 2: "Mi", 3: "Do",
                4: "Fr", 5: "Sa", 6: "So"
            }
            # Rückblick maximal 7 Tage
            for days_back in range(7):
                check_day  = now - timedelta(days=days_back)
                day_code   = wday_map[check_day.weekday()]
                if not self.sched_days[day_code].get():
                    continue
                scheduled = check_day.replace(
                    hour=sh, minute=sm, second=0, microsecond=0)
                if now >= scheduled:
                    if last is None or last < scheduled:
                        self.log(
                            f"🕒 Verpasstes wöchentliches Backup"
                            f" ({day_code}) wird nachgeholt...")
                        self.start_thread(automated=True)
                    # Ob nachgeholt oder nicht: neuester Plantermin wurde geprüft
                    return

    # -----------------------------------------------------------------------
    # Autostart
    # -----------------------------------------------------------------------
    def _setup_autostart(self) -> None:
        """
        Legt eine .desktop-Datei für den Linux-Autostart an.
        FIX P10: Hidden=false hinzugefügt für maximale DE-Kompatibilität.
        """
        os.makedirs(AUTOSTART_DIR, exist_ok=True)
        entry = (
            "[Desktop Entry]\n"
            "Type=Application\n"
            "Name=Katana Kronos\n"
            f"Exec=python3 \"{APP_SCRIPT_PATH}\"\n"
            "Hidden=false\n"
            "X-GNOME-Autostart-enabled=true\n"
            "Comment=Katana Kronos Backup – Autostart\n"
            "StartupNotify=false\n"
        )
        autostart_path = os.path.join(AUTOSTART_DIR, "katana_backup.desktop")
        try:
            with open(autostart_path, "w", encoding="utf-8") as f:
                f.write(entry)
            messagebox.showinfo(
                "Autostart",
                "Erfolgreich installiert!\n"
                "Das Programm startet nun automatisch beim Login.")
        except Exception as exc:
            messagebox.showerror("Fehler", str(exc))

    # -----------------------------------------------------------------------
    # Backup-Historie
    # -----------------------------------------------------------------------
    def _record_last_run(self, status: str = "OK",
                         elapsed: float = 0.0,
                         src: str = "", dst: str = "") -> None:
        """Speichert Zeitstempel und Eintrag des letzten Backups."""
        self.last_run = datetime.now().isoformat()
        total_secs    = int(elapsed)
        hrs, remainder = divmod(total_secs, 3600)
        mins, secs     = divmod(remainder, 60)
        duration_str   = (
            f"{hrs:02d}:{mins:02d}:{secs:02d}"
            if hrs > 0 else f"{mins:02d}:{secs:02d}"
        )
        entry = {
            "ts":       self.last_run,
            "status":   status,
            "duration": duration_str,
            "src":      src,
            "dst":      dst,
        }
        self.backup_history.insert(0, entry)
        if len(self.backup_history) > MAX_BACKUP_HISTORY:
            self.backup_history = self.backup_history[:MAX_BACKUP_HISTORY]
        self._save_config()
        self._update_last_backup_label()
        self._refresh_history_view()

    def _update_last_backup_label(self) -> None:
        if self.last_run:
            try:
                dt = datetime.fromisoformat(self.last_run)
                self.lbl_last_backup.config(
                    text=f"Letztes Backup: {dt.strftime('%d.%m.%Y %H:%M:%S')}",
                    fg=self.colors["success"])
            except ValueError:
                pass

    def _refresh_history_view(self) -> None:
        """Aktualisiert die Backup-Historie-Tabelle im Autopilot-Tab."""
        try:
            for item in self.hist_tree.get_children():
                self.hist_tree.delete(item)
            for entry in self.backup_history:
                try:
                    ts_fmt = datetime.fromisoformat(
                        entry.get("ts", "")).strftime("%d.%m.%Y %H:%M")
                except (ValueError, TypeError):
                    ts_fmt = entry.get("ts", "")
                status     = entry.get("status", "?")
                status_disp = f"✅ {status}" if status == "OK" else f"❌ {status}"
                self.hist_tree.insert("", tk.END, values=(
                    ts_fmt,
                    status_disp,
                    entry.get("duration", "--"),
                    entry.get("src", ""),
                    entry.get("dst", ""),
                ))
        except Exception:
            pass

    # -----------------------------------------------------------------------
    # Log
    # -----------------------------------------------------------------------
    def _rotate_log_if_needed(self) -> None:
        """Rotiert die Log-Datei wenn sie MAX_LOG_SIZE_BYTES überschreitet."""
        try:
            if (os.path.exists(LOG_FILE)
                    and os.path.getsize(LOG_FILE) > MAX_LOG_SIZE_BYTES):
                for i in range(MAX_LOG_BACKUPS - 1, 0, -1):
                    src_log = f"{LOG_FILE}.{i}"
                    dst_log = f"{LOG_FILE}.{i + 1}"
                    if os.path.exists(src_log):
                        os.rename(src_log, dst_log)
                os.rename(LOG_FILE, f"{LOG_FILE}.1")
                # FIX P8: Context Manager statt offenem open()-Aufruf
                with open(LOG_FILE, "w", encoding="utf-8"):
                    pass
                print(f"Log rotiert: {LOG_FILE}")
        except Exception as exc:
            print(f"Log-Rotation fehlgeschlagen: {exc}")

    def log(self, msg: str) -> None:
        ts        = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted = f"[{ts}] {msg}\n"
        self.console.insert(tk.END, formatted)
        self.console.see(tk.END)
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(formatted)
        except OSError as exc:
            print(f"Log-Schreibfehler: {exc}")

    def _clear_log(self) -> None:
        self.console.config(state=tk.NORMAL)
        self.console.delete("1.0", tk.END)

    def _open_log_file(self) -> None:
        try:
            subprocess.Popen(["xdg-open", LOG_FILE])
        except Exception as exc:
            messagebox.showwarning(
                "Hinweis",
                f"Log-Datei konnte nicht geöffnet werden:\n{exc}\n"
                f"Pfad: {LOG_FILE}")

    # -----------------------------------------------------------------------
    # Konfiguration (atomares Speichern)
    # -----------------------------------------------------------------------
    def _on_closing(self) -> None:
        self._save_config()
        self.root.destroy()

    def _load_config(self) -> None:
        if not os.path.exists(CONFIG_FILE):
            self._update_last_backup_label()
            self.root.after(100, self._refresh_history_view)
            return

        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                data: dict = json.load(f)

            self.src_path.set(data.get("src", ""))
            self.dst_path.set(data.get("dst", ""))

            # Strategie
            self.mirror_mode.set(data.get("mirror",           False))
            self.versioning.set(data.get("versioning",        True))
            self.smart_exclude.set(data.get("smart_exclude",  True))
            self.verify_mode.set(data.get("verify_mode",      False))
            self.auto_suspend.set(data.get("auto_suspend",    False))
            self.dry_run.set(data.get("dry_run",              False))
            self.generate_manifest.set(
                data.get("generate_manifest", True))

            # Scheduler
            self.sched_enabled.set(data.get("sched_en",   False))
            self.sched_time.set(data.get("sched_time",    "18:00"))
            self.sched_type.set(data.get("sched_type",    "daily"))
            self.run_missed.set(data.get("run_missed",    True))

            saved_days = data.get("sched_days", {})
            for day, var in self.sched_days.items():
                if day in saved_days:
                    var.set(saved_days[day])

            # Retention
            self.retention_enabled.set(
                data.get("retention_enabled", False))
            self.retention_days.set(data.get("retention_days", 30))

            # Letzter Lauf & Historie
            self.last_run       = data.get("last_run", None)
            self.backup_history = data.get("backup_history", [])

        except Exception as exc:
            print(f"Config Load Error: {exc}")
            self.log(
                f"WARN: Konfiguration konnte nicht geladen werden: {exc}")

        self._update_last_backup_label()
        self.root.after(100, self._refresh_history_view)

    def _save_config(self) -> None:
        """Speichert die Konfiguration atomar via Temp-Datei + os.replace()."""
        data = {
            "src":               self.src_path.get(),
            "dst":               self.dst_path.get(),
            # Strategie
            "mirror":            self.mirror_mode.get(),
            "versioning":        self.versioning.get(),
            "smart_exclude":     self.smart_exclude.get(),
            "verify_mode":       self.verify_mode.get(),
            "auto_suspend":      self.auto_suspend.get(),
            "dry_run":           self.dry_run.get(),
            "generate_manifest": self.generate_manifest.get(),
            # Scheduler
            "sched_en":          self.sched_enabled.get(),
            "sched_time":        self.sched_time.get(),
            "sched_type":        self.sched_type.get(),
            "sched_days":        {k: v.get() for k, v in self.sched_days.items()},
            "run_missed":        self.run_missed.get(),
            # Retention
            "retention_enabled": self.retention_enabled.get(),
            "retention_days":    self.retention_days.get(),
            # Letzter Lauf & Historie
            "last_run":          self.last_run,
            "backup_history":    self.backup_history,
        }

        config_dir = os.path.dirname(CONFIG_FILE)
        tmp_path   = None
        try:
            with tempfile.NamedTemporaryFile(
                    "w", dir=config_dir, delete=False,
                    suffix=".tmp", encoding="utf-8") as tf:
                json.dump(data, tf, indent=4, ensure_ascii=False)
                tmp_path = tf.name
            os.replace(tmp_path, CONFIG_FILE)
        except Exception as exc:
            print(f"Config Save Error: {exc}")
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

    # -----------------------------------------------------------------------
    # Hilfsmethoden
    # -----------------------------------------------------------------------
    def _get_dir_size(self, path: str) -> int:
        """Berechnet die Gesamtgröße eines Verzeichnisses in Bytes."""
        total = 0
        try:
            for dirpath, _, filenames in os.walk(path):
                for fname in filenames:
                    try:
                        total += os.path.getsize(os.path.join(dirpath, fname))
                    except OSError:
                        pass
        except Exception:
            pass
        return total

    @staticmethod
    def _format_bytes(size_bytes: int) -> str:
        """Formatiert Byte-Anzahl in lesbare Einheit."""
        if size_bytes < 1_024:
            return f"{size_bytes} B"
        if size_bytes < 1_024 ** 2:
            return f"{size_bytes / 1_024:.1f} KB"
        if size_bytes < 1_024 ** 3:
            return f"{size_bytes / 1_024 ** 2:.1f} MB"
        return f"{size_bytes / 1_024 ** 3:.2f} GB"


# ---------------------------------------------------------------------------
# Einstiegspunkt
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    root = tk.Tk()
    app  = KatanaBackupMaster(root)
    root.mainloop()
