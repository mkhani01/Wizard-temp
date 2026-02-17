#!/usr/bin/env python3
"""
AOS System Migration Wizard
Cross-platform (Mac, Windows, Linux) wizard UI for migrating data to the new AOS system.
Uses existing migration modules; sets env and copies files to expected paths before running.
"""

import json
import os
import sys
import shutil
import logging
import threading
import webbrowser
from pathlib import Path
from datetime import datetime

try:
    from tkinter import (
        Tk, ttk, Frame, Label, Button, Entry, Checkbutton, BooleanVar,
        StringVar, messagebox, scrolledtext, N, S, E, W, HORIZONTAL,
        Canvas, BOTH, RIGHT, Y, ALL, NW,
    )
    from tkinter import filedialog
except ModuleNotFoundError as e:
    if "_tkinter" in str(e) or "tkinter" in str(e).lower():
        ver = f"{sys.version_info.major}.{sys.version_info.minor}"
        print("The wizard needs Tkinter (GUI library), which is not available in this Python.", file=sys.stderr)
        print("", file=sys.stderr)
        if sys.platform == "darwin":
            print("  macOS (Homebrew):  brew install python-tk@{}".format(ver), file=sys.stderr)
            print("  Then recreate your venv with that Python, or run: python{} wizard.py".format(ver), file=sys.stderr)
        elif sys.platform == "linux":
            print("  Debian/Ubuntu:  sudo apt install python3-tk", file=sys.stderr)
            print("  Fedora:         sudo dnf install python3-tkinter", file=sys.stderr)
        else:
            print("  Install Tcl/Tk for your OS and use a Python build that includes tkinter.", file=sys.stderr)
        print("  See README.md 'Tkinter (GUI)' for details.", file=sys.stderr)
        sys.exit(1)
    raise

# Project root (parent of this file; when frozen by PyInstaller, use executable dir)
if getattr(sys, "frozen", False):
    PROJECT_ROOT = Path(sys.executable).resolve().parent
else:
    PROJECT_ROOT = Path(__file__).resolve().parent
ASSETS = PROJECT_ROOT / "assets"

# Step indices
STEP_WELCOME = 0
STEP_DB = 1
STEP_CHECKBOXES = 2
STEP_FILES = 3
STEP_SUMMARY = 4
STEP_RUN = 5
TOTAL_STEPS = 6

# Migration option keys (must match checkbox keys and file keys)
OPT_CAREGIVERS = "caregivers"
OPT_AVAILABILITY_TYPES = "availability_types"
OPT_CAREGIVERS_AVAILABILITY = "caregivers_availability"
OPT_CLIENTS = "clients"
OPT_CLIENTS_AVAILABILITY = "clients_availability"
OPT_GEOCODE_API = "geocode_api"
OPT_GEOCODE_CLIENT_FILE = "geocode_client_file"
OPT_GEOCODE_CAREGIVER_FILE = "geocode_caregiver_file"
OPT_CALCULATE_DISTANCES = "calculate_distances"
OPT_FVISIT_HISTORY = "fvisit_history"

# Options that require a file in step 4 (excluding geocode_api and calculate_distances)
FILE_OPTIONS = [
    OPT_CAREGIVERS,
    OPT_AVAILABILITY_TYPES,
    OPT_CAREGIVERS_AVAILABILITY,
    OPT_CLIENTS,
    OPT_CLIENTS_AVAILABILITY,
    OPT_GEOCODE_CLIENT_FILE,
    OPT_GEOCODE_CAREGIVER_FILE,
    OPT_FVISIT_HISTORY,
]
# Geocode API needs IE.txt (and optional API key in env)
GEOCODE_API_FILES = ["geocode_ie_txt", "geocode_api_key"]

# File type per option: "file", "folder", or "file_or_folder"
OPT_FILE_TYPE = {
    OPT_CAREGIVERS: "file",
    OPT_AVAILABILITY_TYPES: "file",
    OPT_CAREGIVERS_AVAILABILITY: "file",
    OPT_CLIENTS: "file",
    OPT_CLIENTS_AVAILABILITY: "file",
    OPT_GEOCODE_CLIENT_FILE: "file",
    OPT_GEOCODE_CAREGIVER_FILE: "file",
    OPT_FVISIT_HISTORY: "file",
}

# Where to copy each option's file(s) (relative to ASSETS)
OPT_ASSET_PATH = {
    OPT_CAREGIVERS: "CareAssistantExport.csv",
    OPT_AVAILABILITY_TYPES: "availabilitytypes/availabilityTypes.csv",
    OPT_CAREGIVERS_AVAILABILITY: "userAvailabilities/userAvailabilities.xlsx",
    OPT_CLIENTS: "CustomerExport.csv",
    OPT_CLIENTS_AVAILABILITY: "clientAvailability/ClientHoursWithServiceType.xlsx",
    OPT_GEOCODE_CLIENT_FILE: "clientbackup.json",
    OPT_GEOCODE_CAREGIVER_FILE: "usersBackup.json",
    OPT_FVISIT_HISTORY: "visit_data.csv",
}

AOS_URL = "https://aossystem.com/"
PRIVACY_URL = "https://aossystem.com/"

# Required keys in client/user location JSON backup files
LOCATION_JSON_REQUIRED_KEYS = ("latitude", "longitude", "name", "lastname")


def validate_location_json_file(file_path, root_key, label="File"):
    """
    Validate that a JSON backup file has the required structure for location import.
    root_key: "client" or "user"
    Each record must have keys: latitude, longitude, name, lastname (values may be null).
    Returns (True, None) if valid, else (False, error_message).
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        return False, f"{label}: Invalid JSON — %s" % e
    except OSError as e:
        return False, f"{label}: Cannot read file — %s" % e
    records = data.get(root_key)
    if records is None:
        return False, f"{label}: Missing root key '%s'. Expected a JSON object with key '%s' containing an array." % (root_key, root_key)
    if not isinstance(records, list):
        return False, f"{label}: Root key '%s' must be an array of records." % root_key
    for i, rec in enumerate(records):
        if not isinstance(rec, dict):
            return False, f"{label}: Record at index %d is not an object." % i
        for key in LOCATION_JSON_REQUIRED_KEYS:
            if key not in rec:
                return False, f"{label}: Record at index %d is missing required key '%s'. Each record must have: %s." % (i, key, ", ".join(LOCATION_JSON_REQUIRED_KEYS))
    return True, None


def try_load_logo(root, path, size=(64, 64)):
    """Load favicon for display. Returns PhotoImage or None if Pillow is not installed."""
    try:
        from PIL import Image, ImageTk
        img = Image.open(path).convert("RGBA")
        img.thumbnail(size, Image.Resampling.LANCZOS)
        return ImageTk.PhotoImage(img)
    except Exception:
        return None


class MigrationWizard:
    def __init__(self):
        self.root = Tk()
        self.root.title("AOS System – Migration Wizard")
        self.root.minsize(640, 520)
        self.root.geometry("720x580")

        # Window icon (favicon) – keep reference so it persists, especially on Mac
        self.icon_photo = None
        self.logo_photo = None
        favicon = PROJECT_ROOT / "favicon.png"
        if favicon.exists():
            try:
                from PIL import Image, ImageTk
                icon_img = Image.open(favicon).convert("RGBA")
                self.icon_photo = ImageTk.PhotoImage(icon_img)
                self.root.iconphoto(True, self.icon_photo)
                # Logo for top of each step (slightly larger)
                logo_img = Image.open(favicon).convert("RGBA")
                logo_img.thumbnail((56, 56), Image.Resampling.LANCZOS)
                self.logo_photo = ImageTk.PhotoImage(logo_img)
            except Exception:
                pass

        self.current_step = 0
        self.frames = []
        self.db_config = {
            "host": StringVar(value=os.getenv("DB_HOST", "localhost")),
            "port": StringVar(value=os.getenv("DB_PORT", "5432")),
            "database": StringVar(value=os.getenv("DB_NAME", "appDB")),
            "user": StringVar(value=os.getenv("DB_USER", "root")),
            "password": StringVar(value=os.getenv("DB_PASSWORD", "root")),
        }
        self.check_vars = {key: BooleanVar(value=False) for key in [
            OPT_CAREGIVERS, OPT_AVAILABILITY_TYPES, OPT_CAREGIVERS_AVAILABILITY,
            OPT_CLIENTS, OPT_CLIENTS_AVAILABILITY, OPT_GEOCODE_API,
            OPT_GEOCODE_CLIENT_FILE, OPT_GEOCODE_CAREGIVER_FILE,
            OPT_CALCULATE_DISTANCES, OPT_FVISIT_HISTORY,
        ]}
        self.file_paths = {}  # option -> path string (file or folder)
        self.geocode_api_key = StringVar(value=os.getenv("GOOGLE_MAPS_API_KEY", ""))
        self.geocode_ie_txt_path = StringVar(value="")
        self.privacy_accepted = BooleanVar(value=False)

        self._setup_styles()
        self._build_ui()
        self._show_step(STEP_WELCOME)

    def _setup_styles(self):
        """Apply consistent padding and fonts for better readability (especially on Mac)."""
        style = ttk.Style()
        try:
            # Prefer a readable system font on Mac (e.g. San Francisco / Helvetica)
            default_font = ("Helvetica", 11) if sys.platform == "darwin" else ("", 10)
        except Exception:
            default_font = ("", 10)
        style.configure("TLabel", padding=(0, 4), font=default_font)
        style.configure("TButton", padding=(10, 6), font=default_font)
        style.configure("TCheckbutton", padding=(0, 6), font=default_font)
        style.configure("TEntry", padding=4)

    def _build_ui(self):
        main = ttk.Frame(self.root, padding=16)
        main.grid(row=0, column=0, sticky=(N, S, E, W))
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main.columnconfigure(0, weight=1)
        main.rowconfigure(1, weight=1)

        # Progress
        self.step_label = ttk.Label(main, text="Step 1 of 6 – Welcome", font=("", 10, "bold"))
        self.step_label.grid(row=0, column=0, columnspan=2, sticky=W, pady=(0, 8))

        # Content area
        self.content = ttk.Frame(main)
        self.content.grid(row=1, column=0, columnspan=2, sticky=(N, S, E, W))
        self.content.columnconfigure(0, weight=1)
        self.content.rowconfigure(0, weight=1)

        for i in range(TOTAL_STEPS):
            f = ttk.Frame(self.content)
            f.grid(row=0, column=0, sticky=(N, S, E, W))
            f.columnconfigure(0, weight=1)
            self.frames.append(f)

        self._build_step_welcome(self.frames[STEP_WELCOME])
        self._build_step_db(self.frames[STEP_DB])
        self._build_step_checkboxes(self.frames[STEP_CHECKBOXES])
        self._build_step_files(self.frames[STEP_FILES])
        self._build_step_summary(self.frames[STEP_SUMMARY])
        self._build_step_run(self.frames[STEP_RUN])

        # Buttons
        btn_frame = ttk.Frame(main)
        btn_frame.grid(row=2, column=0, columnspan=2, sticky=E, pady=(12, 0))
        self.btn_cancel = ttk.Button(btn_frame, text="Cancel", command=self._on_cancel)
        self.btn_cancel.pack(side="right", padx=4)
        self.btn_back = ttk.Button(btn_frame, text="Back", command=self._on_back)
        self.btn_back.pack(side="right", padx=4)
        self.btn_continue = ttk.Button(btn_frame, text="Continue", command=self._on_continue)
        self.btn_continue.pack(side="right", padx=4)

    def _add_step_header(self, parent, start_row=0):
        """Add logo at top of step. Returns next row index. Keeps logo reference on parent."""
        if self.logo_photo:
            logo_lbl = ttk.Label(parent, image=self.logo_photo)
            logo_lbl.grid(row=start_row, column=0, pady=(0, 10))
            logo_lbl.image = self.logo_photo
            return start_row + 1
        return start_row

    def _build_step_welcome(self, parent):
        parent.columnconfigure(0, weight=1)
        row = self._add_step_header(parent, 0)
        ttk.Label(parent, text="Migration Wizard", font=("", 16, "bold")).grid(row=row, column=0, sticky=W, pady=(0, 12))
        row += 1
        hint = (
            "This wizard guides you through migrating your existing data (caregivers, clients, availability, etc.) "
            "into the new AOS system.\n\n"
            "What to do:\n"
            "• Enter your PostgreSQL database details in the next step.\n"
            "• Choose which data to migrate (e.g. Caregivers, Clients, Availability types).\n"
            "• Select the CSV or Excel files that contain your export data.\n"
            "• Run the migration; a detailed log file will be saved so you can review any warnings or errors.\n\n"
            "Important: Do not close this window until the migration has finished. "
            "If you use Google Maps geocoding, ensure you have a stable internet connection."
        )
        ttk.Label(parent, text=hint, justify="left", wraplength=580, padding=(0, 8)).grid(row=row, column=0, sticky=W, pady=(0, 12))
        row += 1
        ttk.Label(parent, text="More information:", font=("", 10, "bold")).grid(row=row, column=0, sticky=W, pady=(8, 2))
        row += 1
        link = ttk.Label(parent, text=AOS_URL, foreground="blue", cursor="hand2")
        link.grid(row=row, column=0, sticky=W, pady=(0, 8))
        link.bind("<Button-1>", lambda e: webbrowser.open(AOS_URL))
        parent.rowconfigure(row, weight=0)

    def _build_step_db(self, parent):
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)
        row = self._add_step_header(parent, 0)
        ttk.Label(parent, text="Database connection (PostgreSQL)", font=("", 13, "bold")).grid(row=row, column=0, columnspan=2, sticky=W, pady=(0, 6))
        row += 1
        ttk.Label(
            parent,
            text="Enter the connection details for the PostgreSQL database where AOS data will be stored. "
                 "These settings are used for every migration step (caregivers, clients, availability, etc.).",
            wraplength=560, padding=(0, 4)
        ).grid(row=row, column=0, columnspan=2, sticky=W, pady=(0, 14))
        row += 1
        hints = {
            "host": "Usually localhost or 127.0.0.1 if the database is on this computer.",
            "port": "PostgreSQL default is 5432. Use your actual port if different.",
            "database": "Name of the existing database (e.g. appDB).",
            "user": "PostgreSQL username with write access to the database.",
            "password": "Password for the user above.",
        }
        for label, key in [("Host", "host"), ("Port", "port"), ("Database", "database"), ("User", "user"), ("Password", "password")]:
            ttk.Label(parent, text=label + ":").grid(row=row, column=0, sticky=W, padx=(0, 12), pady=(6, 2))
            w = Entry(parent, textvariable=self.db_config[key], width=38, show="*" if key == "password" else None)
            w.grid(row=row, column=1, sticky=(E, W), pady=(6, 2), padx=(0, 8))
            row += 1
            ttk.Label(parent, text=hints[key], wraplength=520, padding=(0, 0, 0, 8)).grid(row=row, column=1, sticky=W, padx=(0, 8))
            row += 1
        parent.columnconfigure(1, weight=1)

    def _build_step_checkboxes(self, parent):
        parent.columnconfigure(0, weight=1)
        row = self._add_step_header(parent, 0)
        ttk.Label(parent, text="Select data to migrate", font=("", 13, "bold")).grid(row=row, column=0, sticky=W, pady=(0, 6))
        row += 1
        intro = (
            "Tick each type of data you want to import. In the next step you will choose the file (or folder) for each option.\n\n"
            "Hint: If you want to migrate Caregivers Availability or Clients Availability, you must tick \"Availability types\" as well, "
            "and run that step first (the wizard runs steps in the correct order)."
        )
        ttk.Label(parent, text=intro, wraplength=560, padding=(0, 6)).grid(row=row, column=0, sticky=W, pady=(0, 10))
        row += 1
        row_for_scroll = row

        # Scrollable area: canvas + scrollbar + inner frame
        canvas = Canvas(parent, highlightthickness=0)
        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)

        scroll_frame = ttk.Frame(canvas)
        scroll_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox(ALL))
        )
        canvas_window = canvas.create_window((0, 0), window=scroll_frame, anchor=NW)
        canvas.configure(yscrollcommand=scrollbar.set)

        def _on_canvas_configure(event):
            canvas.itemconfig(canvas_window, width=event.width)

        def _on_mousewheel(event):
            # Windows: delta is ±120 per notch; Mac: often ±1 or similar small value
            d = getattr(event, "delta", 0)
            if abs(d) >= 100:
                units = int(-d / 120)
            else:
                units = -1 if d > 0 else 1
            canvas.yview_scroll(units, "units")

        def _on_mousewheel_linux(event):
            if event.num == 5:
                canvas.yview_scroll(1, "units")
            elif event.num == 4:
                canvas.yview_scroll(-1, "units")

        def _focus_canvas(_event):
            canvas.focus_set()

        canvas.bind("<Configure>", _on_canvas_configure)
        canvas.bind("<MouseWheel>", _on_mousewheel)
        canvas.bind("<Button-4>", _on_mousewheel_linux)
        canvas.bind("<Button-5>", _on_mousewheel_linux)
        canvas.bind("<Enter>", _focus_canvas)
        scroll_frame.bind("<MouseWheel>", _on_mousewheel)
        scroll_frame.bind("<Button-4>", _on_mousewheel_linux)
        scroll_frame.bind("<Button-5>", _on_mousewheel_linux)
        scroll_frame.bind("<Enter>", _focus_canvas)

        canvas.grid(row=row_for_scroll, column=0, sticky=(N, S, E, W))
        scrollbar.grid(row=row_for_scroll, column=1, sticky=(N, S))
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(row_for_scroll, weight=1)

        # Content inside scroll_frame
        inner = scroll_frame
        inner.columnconfigure(0, weight=1)
        wrap = 540

        opts = [
            (OPT_CAREGIVERS, "Caregivers", "Import care assistants (caregivers) from a CSV export. Hint: Use a file like CareAssistantExport.csv with columns such as First Name, Last Name, Email, Mobile. You will pick the file in the next step."),
            (OPT_AVAILABILITY_TYPES, "Availability types", "Import availability type definitions (e.g. name, type, category, description) from CSV. Required before Caregivers Availability or Clients Availability. Hint: Add this if you use availability or shifts."),
            (OPT_CAREGIVERS_AVAILABILITY, "Caregivers Availability", "Import each caregiver’s availability from an Excel workbook. Hint: Requires Availability types. File is usually userAvailabilities.xlsx. You will select it in the next step."),
            (OPT_CLIENTS, "Clients", "Import clients from a CSV export. Hint: Use a file like CustomerExport.csv. You will choose the file in the next step."),
            (OPT_CLIENTS_AVAILABILITY, "Clients Availability", "Import client availability from Excel. Hint: Requires Availability types. You can select one file or a folder of workbooks (e.g. Client Hours with Service Type)."),
            (OPT_GEOCODE_CLIENT_FILE, "Get clients location from file", "Update client latitude/longitude from a JSON backup file (e.g. clientbackup.json). File must contain \"latitude\", \"longitude\", \"name\", \"lastname\" for each record. This is useful for manually seeding known coordinates before using the Google API."),
            (OPT_GEOCODE_CAREGIVER_FILE, "Get users location from file", "Update user (caregiver) latitude/longitude from a JSON backup file (e.g. usersBackup.json). File must contain \"latitude\", \"longitude\", \"name\", \"lastname\" for each record. This is useful for manually seeding known coordinates before using the Google API."),
            (OPT_GEOCODE_API, "Calculated Geocode (Google API)", "Use Google Maps API to fill in latitude/longitude from postcodes for users and clients that have NULL coordinates. This runs AFTER file-based location imports (if selected), only geocoding records with postcodes but missing lat/long. You need a Google Maps API key and the Irish cities file (IE.txt). Can be combined with file-based options."),
            (OPT_CALCULATE_DISTANCES, "Calculate distances", "Compute travel distances between caregivers and clients using OSRM. Reads user and client lat/long from the database, calls OSRM for each pair and travel method (driving, cycling, walking), then inserts or updates the travel_distances table. Runs a verification step when done. Requires network access to OSRM."),
            (OPT_FVISIT_HISTORY, "Feasible pairs (visit history)", "Seed feasible_pairs from visit data CSV (Assignee = caregiver, Customer = client). Pick a CSV with columns Assignee and Customer in the next step."),
        ]
        row = 0
        for key, title, hint in opts:
            # Card-like block: checkbox + description
            block = ttk.Frame(inner, padding=(0, 6, 0, 10))
            block.grid(row=row, column=0, sticky=(E, W), pady=2)
            block.columnconfigure(0, weight=1)
            cb = ttk.Checkbutton(block, text=title, variable=self.check_vars[key], command=self._sync_checkbox_dependencies)
            cb.grid(row=0, column=0, sticky=W)
            if key in (OPT_GEOCODE_CLIENT_FILE, OPT_GEOCODE_CAREGIVER_FILE):
                setattr(self, "_cb_%s" % key, cb)
            if key in (OPT_CAREGIVERS_AVAILABILITY, OPT_CLIENTS_AVAILABILITY):
                setattr(self, "_cb_%s" % key, cb)
            desc = ttk.Label(block, text=hint, wraplength=wrap, padding=(28, 4, 8, 4))
            desc.grid(row=1, column=0, sticky=W)
            row += 1
        # More info button in its own row
        dist_frame = ttk.Frame(inner)
        dist_frame.grid(row=row, column=0, sticky=W, pady=(12, 8))
        ttk.Button(dist_frame, text="More info (Extra Cost) – Calculate distances", command=self._show_distance_info).pack(side="left", padx=(0, 8))
        self._sync_checkbox_dependencies()

    def _show_distance_info(self):
        messagebox.showinfo(
            "Calculate distances",
            "This step loads all caregivers (users) and clients that have latitude/longitude from your database, "
            "calls the OSRM service (e.g. https://osrm.caspianbmp.ie) to get driving, cycling, and walking distances and durations, "
            "then inserts or updates the travel_distances table. A verification run checks that all expected distances were stored. "
            "Ensure the database has user and client locations filled (e.g. via Geocode or location file steps) and that the OSRM endpoint is reachable."
        )

    def _sync_checkbox_dependencies(self):
        """Apply dependencies: Caregivers/Clients availability only when Availability types selected."""
        # Availability: enable Caregivers/Clients availability only when Availability types is selected
        has_availability_types = self.check_vars[OPT_AVAILABILITY_TYPES].get()
        for opt in (OPT_CAREGIVERS_AVAILABILITY, OPT_CLIENTS_AVAILABILITY):
            cb = getattr(self, "_cb_%s" % opt, None)
            if cb is not None:
                cb.config(state="normal" if has_availability_types else "disabled")
                if not has_availability_types:
                    self.check_vars[opt].set(False)

    def _build_step_files(self, parent):
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(2, weight=1)
        row = self._add_step_header(parent, 0)
        ttk.Label(parent, text="Select files for each migration", font=("", 13, "bold")).grid(row=row, column=0, columnspan=3, sticky=W, pady=(0, 6))
        row += 1
        ttk.Label(
            parent,
            text="For each option you ticked above, choose the corresponding file (or folder). "
                 "Hint: \"Calculated Geocode\" and \"Calculate distances\" use their own settings and do not need a file here.",
            wraplength=560, padding=(0, 4)
        ).grid(row=row, column=0, columnspan=3, sticky=W, pady=(0, 12))
        row += 1
        self._files_start_row = row
        self.file_rows = []
        self.file_widgets = []
        parent.columnconfigure(1, weight=1)

    def _refresh_file_step(self):
        for w in self.file_rows:
            w.destroy()
        self.file_rows.clear()
        self.file_widgets.clear()
        parent = self.frames[STEP_FILES]
        row = getattr(self, "_files_start_row", 2)
        selected = [k for k, v in self.check_vars.items() if v.get()]
        # Geocode API: show IE.txt and optional API key
        if OPT_GEOCODE_API in selected:
            ttk.Label(parent, text="Irish cities file (IE.txt) for Calculated Geocode:").grid(row=row, column=0, sticky=W, padx=(0, 8), pady=4)
            e = Entry(parent, textvariable=self.geocode_ie_txt_path, width=40)
            e.grid(row=row, column=1, sticky=(E, W), pady=4)
            b = ttk.Button(parent, text="Browse…", command=lambda: self._browse_file(self.geocode_ie_txt_path, parent))
            b.grid(row=row, column=2, padx=4, pady=4)
            self.file_rows.extend([e, b])
            row += 1
            ttk.Label(parent, text="Google Maps API key (optional if in .env):").grid(row=row, column=0, sticky=W, padx=(0, 8), pady=4)
            e2 = Entry(parent, textvariable=self.geocode_api_key, width=40, show="*")
            e2.grid(row=row, column=1, sticky=(E, W), pady=4)
            self.file_rows.append(e2)
            row += 1
        for key in FILE_OPTIONS:
            if key not in selected:
                continue
            label = {
                OPT_CAREGIVERS: "Caregivers CSV:",
                OPT_AVAILABILITY_TYPES: "Availability types CSV:",
                OPT_CAREGIVERS_AVAILABILITY: "Caregivers availability XLSX:",
                OPT_CLIENTS: "Clients CSV:",
                OPT_CLIENTS_AVAILABILITY: "Client Hours with Service Type XLSX:",
                OPT_GEOCODE_CLIENT_FILE: "Client geocode JSON:",
                OPT_GEOCODE_CAREGIVER_FILE: "Caregiver geocode JSON:",
                OPT_FVISIT_HISTORY: "Feasible pairs (visit data) CSV:",
            }.get(key, key)
            existing = self.file_paths.get(key)
            if hasattr(existing, "get"):
                existing = existing.get()
            var = StringVar(value=(existing or "").strip())
            self.file_paths[key] = var  # keep ref
            ttk.Label(parent, text=label).grid(row=row, column=0, sticky=W, padx=(0, 8), pady=4)
            e = Entry(parent, textvariable=var, width=40)
            e.grid(row=row, column=1, sticky=(E, W), pady=4)
            if OPT_FILE_TYPE.get(key) == "folder":
                cmd = lambda v=var, p=parent: self._browse_folder(v, p)
            else:
                cmd = lambda v=var, p=parent: self._browse_file(v, p)
            b = ttk.Button(parent, text="Browse…", command=cmd)
            b.grid(row=row, column=2, padx=4, pady=4)
            self.file_rows.extend([e, b])
            self.file_widgets.append((key, var, row))
            row += 1

    def _browse_file(self, var: StringVar, parent):
        path = filedialog.askopenfilename(parent=parent, title="Select file")
        if path:
            var.set(path)

    def _browse_folder(self, var: StringVar, parent):
        path = filedialog.askdirectory(parent=parent, title="Select folder")
        if path:
            var.set(path)

    def _build_step_summary(self, parent):
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(3, weight=1)
        row = self._add_step_header(parent, 0)
        ttk.Label(parent, text="Review and confirm", font=("", 13, "bold")).grid(row=row, column=0, sticky=W, pady=(0, 6))
        row += 1
        ttk.Label(
            parent,
            text="Check the summary below. When you click \"Start migration\", the wizard will run each step in order and write a detailed log file. You must accept the privacy policy to continue.",
            wraplength=560, padding=(0, 4)
        ).grid(row=row, column=0, sticky=W, pady=(0, 10))
        row += 1
        self.summary_text = scrolledtext.ScrolledText(parent, height=12, width=72, wrap="word", state="disabled")
        self.summary_text.grid(row=row, column=0, sticky=(N, S, E, W), pady=(4, 8))
        row += 1
        cb = ttk.Checkbutton(parent, text="I accept the privacy policy of AOS system", variable=self.privacy_accepted)
        cb.grid(row=row, column=0, sticky=W, pady=(4, 4))
        row += 1
        link = ttk.Label(parent, text=PRIVACY_URL, foreground="blue", cursor="hand2")
        link.grid(row=row, column=0, sticky=W, pady=(0, 4))
        link.bind("<Button-1>", lambda e: webbrowser.open(PRIVACY_URL))

    def _refresh_summary(self):
        self.summary_text.config(state="normal")
        self.summary_text.delete("1.0", "end")
        lines = ["Database: " + self.db_config["database"].get() + " @ " + self.db_config["host"].get() + "\n"]
        for key, var in self.check_vars.items():
            if not var.get():
                continue
            name = key.replace("_", " ").title()
            lines.append("• " + name)
            if key in self.file_paths and hasattr(self.file_paths[key], "get"):
                path = self.file_paths[key].get()
                if path:
                    lines.append("  File: " + path)
            if key == OPT_GEOCODE_API:
                ie = self.geocode_ie_txt_path.get()
                if ie:
                    lines.append("  IE.txt: " + ie)
        self.summary_text.insert("1.0", "\n".join(lines))
        self.summary_text.config(state="disabled")

    def _build_step_run(self, parent):
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(3, weight=1)
        row = self._add_step_header(parent, 0)
        ttk.Label(parent, text="Migration in progress", font=("", 13, "bold")).grid(row=row, column=0, sticky=W, pady=(0, 6))
        row += 1
        ttk.Label(
            parent,
            text="Do not close this window until the migration finishes. The log below shows progress; a full detailed log file (with every step and any warnings) will be saved to the project folder when done. If any step fails, check that log file for the cause.",
            wraplength=560, padding=(0, 4)
        ).grid(row=row, column=0, sticky=W, pady=(0, 10))
        row += 1
        self.run_log = scrolledtext.ScrolledText(parent, height=16, width=72, wrap="word", state="disabled")
        self.run_log.grid(row=row, column=0, sticky=(N, S, E, W), pady=(4, 8))
        row += 1
        self.run_progress = ttk.Progressbar(parent, mode="indeterminate")
        self.run_progress.grid(row=row, column=0, sticky=(E, W), pady=(0, 4))
        parent.columnconfigure(0, weight=1)

    def _show_step(self, step: int):
        self.current_step = step
        for i, f in enumerate(self.frames):
            f.grid_remove() if i != step else f.grid()
        titles = [
            "Step 1 of 6 – Welcome",
            "Step 2 of 6 – Database connection",
            "Step 3 of 6 – Select data to migrate",
            "Step 4 of 6 – Select files",
            "Step 5 of 6 – Review and confirm",
            "Step 6 of 6 – Running migration",
        ]
        self.step_label.config(text=titles[step])
        self.btn_back.config(state="normal" if step > 0 else "disabled")
        if step == STEP_FILES:
            self._refresh_file_step()
        elif step == STEP_SUMMARY:
            self._refresh_summary()
        if step == STEP_RUN:
            self.btn_continue.config(state="disabled")
            self.btn_back.config(state="disabled")
            self.btn_cancel.config(state="disabled")
        else:
            self.btn_continue.config(state="normal")
            self.btn_cancel.config(state="normal")
            if step == STEP_SUMMARY:
                self.btn_continue.config(text="Start migration")
            else:
                self.btn_continue.config(text="Continue")

    def _on_cancel(self):
        if messagebox.askyesno("Cancel", "Are you sure you want to cancel the wizard?"):
            self.root.quit()
            self.root.destroy()

    def _on_back(self):
        if self.current_step > 0:
            self._show_step(self.current_step - 1)

    def _on_continue(self):
        if self.current_step == STEP_WELCOME:
            self._show_step(STEP_DB)
        elif self.current_step == STEP_DB:
            if not self._validate_db():
                return
            self._show_step(STEP_CHECKBOXES)
        elif self.current_step == STEP_CHECKBOXES:
            if not self._validate_checkboxes():
                return
            self._show_step(STEP_FILES)
        elif self.current_step == STEP_FILES:
            if not self._validate_files():
                return
            self._show_step(STEP_SUMMARY)
        elif self.current_step == STEP_SUMMARY:
            if not self.privacy_accepted.get():
                messagebox.showwarning("Privacy policy", "Please accept the privacy policy to continue.")
                return
            self._show_step(STEP_RUN)
            self._run_migrations()

    def _validate_db(self):
        if not self.db_config["database"].get().strip():
            messagebox.showwarning("Database", "Please enter the database name.")
            return False
        if not self.db_config["user"].get().strip():
            messagebox.showwarning("Database", "Please enter the database user.")
            return False
        try:
            port = int(self.db_config["port"].get().strip())
            if port <= 0 or port > 65535:
                raise ValueError("Invalid port")
        except ValueError:
            messagebox.showwarning("Database", "Please enter a valid port (1–65535).")
            return False
        return True

    def _validate_checkboxes(self):
        selected = [k for k, v in self.check_vars.items() if v.get()]
        if not selected:
            messagebox.showwarning(
                "Select data to migrate",
                "Please select at least one option to migrate before continuing."
            )
            return False
        need_avail = self.check_vars[OPT_CAREGIVERS_AVAILABILITY].get() or self.check_vars[OPT_CLIENTS_AVAILABILITY].get()
        if need_avail and not self.check_vars[OPT_AVAILABILITY_TYPES].get():
            if not messagebox.askyesno("Availability types", "Caregivers Availability and Clients Availability require Availability types to be migrated first. Add 'Availability types' to your selection?"):
                return False
            self.check_vars[OPT_AVAILABILITY_TYPES].set(True)
        return True

    def _validate_files(self):
        selected = [k for k, v in self.check_vars.items() if v.get()]
        if OPT_GEOCODE_API in selected:
            if not self.geocode_ie_txt_path.get().strip():
                messagebox.showwarning("Files", "Please select the Irish cities file (IE.txt) for Calculated Geocode.")
                return False
            api_key = self.geocode_api_key.get().strip()
            if not api_key and not os.getenv("GOOGLE_MAPS_API_KEY"):
                if not messagebox.askyesno("API key", "No Google Maps API key entered. It may be in your .env file. Continue anyway?"):
                    return False
        for key in FILE_OPTIONS:
            if key not in selected:
                continue
            path = self.file_paths.get(key)
            if path is None:
                continue
            p = path.get() if hasattr(path, "get") else path
            if not (p and str(p).strip()):
                messagebox.showwarning("Files", f"Please select a file or folder for: {key.replace('_', ' ').title()}")
                return False
            if Path(p).exists() is False:
                messagebox.showwarning("Files", f"Path does not exist: {p}")
                return False
            # Validate client/user location JSON files: must have latitude, longitude, name, lastname
            if key == OPT_GEOCODE_CLIENT_FILE:
                ok, err = validate_location_json_file(p, "client", "Client location file")
                if not ok:
                    messagebox.showwarning("Files", err)
                    return False
            if key == OPT_GEOCODE_CAREGIVER_FILE:
                ok, err = validate_location_json_file(p, "user", "User (caregiver) location file")
                if not ok:
                    messagebox.showwarning("Files", err)
                    return False
        return True

    def _run_migrations(self):
        self.run_progress.start(10)
        log_path = PROJECT_ROOT / ("migration_wizard_%s.log" % datetime.now().strftime("%Y%m%d_%H%M%S"))
        self._append_log("Log file: %s\n" % log_path)
        self._append_log("Setting environment and copying files...\n")
        self.root.update()
        thread = threading.Thread(target=self._do_run, args=(log_path,), daemon=True)
        thread.start()

    def _append_log(self, msg: str):
        self.run_log.config(state="normal")
        self.run_log.insert("end", msg)
        self.run_log.see("end")
        self.run_log.config(state="disabled")
        self.root.update_idletasks()

    def _do_run(self, log_path: Path):
        import io
        os.chdir(PROJECT_ROOT)
        if str(PROJECT_ROOT) not in sys.path:
            sys.path.insert(0, str(PROJECT_ROOT))
        buf = io.StringIO()
        log_fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler = logging.StreamHandler(buf)
        handler.setFormatter(log_fmt)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(log_fmt)
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(handler)
        root_logger.addHandler(console_handler)
        try:
            self.root.after(0, lambda: self._append_log("Running pre-run checks...\n"))
            try:
                from tests.test_before_run import run_all as run_pre_run_checks
                old_stdout, sys.stdout = sys.stdout, buf
                try:
                    checks_ok = run_pre_run_checks()
                finally:
                    sys.stdout = old_stdout
                check_output = buf.getvalue()
                if check_output:
                    print(check_output, end="", flush=True)
                for line in check_output.splitlines():
                    self.root.after(0, lambda l=line: self._append_log(l + "\n"))
                if not checks_ok:
                    self.root.after(0, lambda: self._append_log("Pre-run checks failed. Fix the issues above and try again.\n"))
                    log_content = buf.getvalue()
                    log_path.write_text(log_content, encoding="utf-8")
                    self.run_progress.stop()
                    self.root.after(0, lambda: self._run_finished(log_path, log_content))
                    return
            except Exception as e:
                self.root.after(0, lambda err=str(e): self._append_log("Pre-run checks error: %s\n" % err))
                logging.exception("Pre-run checks failed")
                log_content = buf.getvalue()
                log_path.write_text(log_content, encoding="utf-8")
                self.run_progress.stop()
                self.root.after(0, lambda: self._run_finished(log_path, log_content))
                return
            self._apply_env()
            self._copy_files()
            order = self._migration_order()
            for i, (name, fn) in enumerate(order):
                self.root.after(0, lambda n=name: self._append_log("Running: %s\n" % n))
                try:
                    success = fn()
                    self.root.after(0, lambda n=name, s=success: self._append_log("  %s: %s\n" % (n, "OK" if s else "FAILED")))
                    if not success:
                        self.root.after(0, lambda: self._append_log("\nMigration stopped due to failure.\n"))
                        break
                except (ImportError, ModuleNotFoundError, AttributeError) as e:
                    self.root.after(0, lambda n=name: self._append_log("  %s: Not available yet (add your migration script later).\n" % n))
                    logging.info("Migration %s not available: %s", name, e)
                except Exception as e:
                    self.root.after(0, lambda n=name, err=str(e): self._append_log("  %s: ERROR %s\n" % (n, err)))
                    logging.exception("Migration %s failed", name)
                    break
            else:
                self.root.after(0, lambda: self._append_log("\nAll selected steps completed.\n"))
        finally:
            root_logger.removeHandler(handler)
            root_logger.removeHandler(console_handler)
            log_content = buf.getvalue()
            try:
                log_path.write_text(log_content, encoding="utf-8")
            except Exception:
                pass
            self.run_progress.stop()
            self.root.after(0, lambda: self._run_finished(log_path, log_content))

    def _run_finished(self, log_path: Path, log_content: str):
        self._append_log("\nDetailed log saved to: %s\n" % log_path)
        if "ERROR" in log_content or "FAILED" in log_content:
            self._append_log("\nMigration finished with errors. Please check the log file.\n")
            messagebox.showerror("Migration", "Migration finished with errors. See the log: %s" % log_path)
        else:
            self._append_log("\nDone successfully.\n")
            messagebox.showinfo("Migration", "Migration completed successfully.\n\nLog saved to:\n%s" % log_path)
        self.btn_cancel.config(state="normal")
        self.btn_cancel.config(text="Close")

    def _apply_env(self):
        os.environ["DB_HOST"] = self.db_config["host"].get().strip()
        os.environ["DB_PORT"] = self.db_config["port"].get().strip()
        os.environ["DB_NAME"] = self.db_config["database"].get().strip()
        os.environ["DB_USER"] = self.db_config["user"].get().strip()
        os.environ["DB_PASSWORD"] = self.db_config["password"].get().strip()
        api_key = self.geocode_api_key.get().strip()
        if api_key:
            os.environ["GOOGLE_MAPS_API_KEY"] = api_key

    def _copy_files(self):
        ASSETS.mkdir(parents=True, exist_ok=True)
        (ASSETS / "userAvailabilities").mkdir(parents=True, exist_ok=True)
        (ASSETS / "availabilitytypes").mkdir(parents=True, exist_ok=True)
        (ASSETS / "clientsAvailabilities").mkdir(parents=True, exist_ok=True)

        if self.check_vars[OPT_GEOCODE_API].get():
            ie_src = self.geocode_ie_txt_path.get().strip()
            if ie_src:
                shutil.copy2(ie_src, ASSETS / "IE.txt")

        for key in FILE_OPTIONS:
            if not self.check_vars[key].get():
                continue
            path_var = self.file_paths.get(key)
            if not path_var or not hasattr(path_var, "get"):
                continue
            src = Path(path_var.get().strip())
            if not src.exists():
                continue
            dest_rel = OPT_ASSET_PATH.get(key)
            if not dest_rel:
                continue
            dest = ASSETS / dest_rel
            if OPT_FILE_TYPE.get(key) == "folder":
                for f in src.iterdir():
                    if f.is_file():
                        shutil.copy2(f, dest / f.name)
            else:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dest)

    def _migration_order(self):
        order = []
        if self.check_vars[OPT_CAREGIVERS].get():
            order.append(("Users (Caregivers)", self._run_users))
        if self.check_vars[OPT_AVAILABILITY_TYPES].get():
            order.append(("Availability types", self._run_availability_types))
        if self.check_vars[OPT_CAREGIVERS_AVAILABILITY].get():
            order.append(("Caregivers Availability", self._run_user_availability))
        if self.check_vars[OPT_CLIENTS].get():
            order.append(("Clients", self._run_clients))
        if self.check_vars[OPT_CLIENTS_AVAILABILITY].get():
            order.append(("Clients Availability", self._run_client_availability))
        # File-based geocoding runs first (manual seeding)
        if self.check_vars[OPT_GEOCODE_CLIENT_FILE].get():
            order.append(("Client Geocode (file)", self._run_client_locations))
        if self.check_vars[OPT_GEOCODE_CAREGIVER_FILE].get():
            order.append(("Caregiver Geocode (file)", self._run_user_locations))
        # Google API geocoding runs after file-based (fills in remaining nulls)
        if self.check_vars[OPT_GEOCODE_API].get():
            order.append(("Calculated Geocode (API)", self._run_geocode_api))
        if self.check_vars[OPT_CALCULATE_DISTANCES].get():
            order.append(("Calculate distances", self._run_travel_distances))
        if self.check_vars[OPT_FVISIT_HISTORY].get():
            order.append(("Feasible pairs (visit history)", self._run_feasible_pairs))
        return order

    def _run_users(self):
        sys.path.insert(0, str(PROJECT_ROOT))
        from usersMigration.main import run
        return run()

    def _run_availability_types(self):
        sys.path.insert(0, str(PROJECT_ROOT))
        from availabilityTypeMigration.main import run
        return run()

    def _run_user_availability(self):
        sys.path.insert(0, str(PROJECT_ROOT))
        from userAvailabilityMigration.main import run
        return run()

    def _run_clients(self):
        sys.path.insert(0, str(PROJECT_ROOT))
        from clientsMigration.main import run
        return run()

    def _run_client_availability(self):
        sys.path.insert(0, str(PROJECT_ROOT))
        from clientAvailabilityMigration.main import run
        return run()

    def _run_geocode_api(self):
        sys.path.insert(0, str(PROJECT_ROOT))
        from geocodeCalculation.main import run
        return run()

    def _run_client_locations(self):
        sys.path.insert(0, str(PROJECT_ROOT))
        from clientLocationsMigration.main import run
        return run()

    def _run_user_locations(self):
        sys.path.insert(0, str(PROJECT_ROOT))
        from userLocationsMigration.main import run
        return run()

    def _run_travel_distances(self):
        sys.path.insert(0, str(PROJECT_ROOT))
        from distance_migration.travel_distances_migration import run
        return run()

    def _run_feasible_pairs(self):
        sys.path.insert(0, str(PROJECT_ROOT))
        from feasible_pairs_migration.feasible_pairs_migration import run as run_feasible_pairs
        csv_path = ASSETS / "visit_data.csv"
        return run_feasible_pairs(csv_path=str(csv_path))

    def run(self):
        self.root.mainloop()


def main():
    wizard = MigrationWizard()
    wizard.run()


if __name__ == "__main__":
    main()
