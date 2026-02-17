# AOS System Migration Tool

Migration tool for moving existing data (caregivers, clients, availability, geocode, travel distances, feasible pairs) into the AOS system. Includes a **graphical wizard** (Mac, Windows, Linux) and a **CLI**.

---

## Project structure

```
Migration/
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── main.py                   # CLI entry (python main.py <command>)
├── wizard.py                 # GUI wizard entry (python wizard.py)
├── assets/                   # Input files (CSV, JSON, XLSX) – wizard copies here
├── tests/
│   ├── test_before_run.py    # Pre-run checks (deps, imports)
│   ├── test_distance.py      # Optional OSRM distance test
│   └── run_tests.py          # Run all tests
├── distance_migration/      # OSRM client + travel_distances migration
│   ├── osrm.py               # OSRM table API, get_distance_matrix, load_json_data
│   └── travel_distances_migration.py  # DB migration (user↔client distances)
├── areaMigration/
├── availabilityTypeMigration/
├── clientAvailabilityMigration/
├── clientLocationsMigration/
├── clientsMigration/
├── feasible_pairs_migration/
├── geocodeCalculation/
├── userAvailabilityMigration/
├── userLocationsMigration/
└── usersMigration/
```

---

## Setup

1. **Python 3.8+** and pip.

2. **Tkinter (GUI)** — required only for the wizard (`python wizard.py`). If you get `ModuleNotFoundError: No module named '_tkinter'`:

   - **macOS (Homebrew):**  
     `brew install python-tk@3.12` (use your Python minor version, e.g. `3.11` or `3.12`). Then use that same Python to create/use your venv or run `python3 wizard.py`.
   - **Linux (Debian/Ubuntu):**  
     `sudo apt install python3-tk`
   - **Linux (Fedora):**  
     `sudo dnf install python3-tkinter`  
   - **Windows:**  
     Use the official python.org installer and ensure "tcl/tk and IDLE" is selected.

3. **Create a virtual environment and install dependencies:**

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

4. **Environment variables** (e.g. in `.env` in the project root):

   - `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` for PostgreSQL.

   Optional: `GOOGLE_MAPS_API_KEY` for geocode step; OSRM base URL is in `distance_migration/osrm.py`.

---

## Run tests before migrating

Run the pre-run checks (and optional distance test) so the wizard or CLI don’t fail midway:

```bash
python main.py test
# or
python tests/run_tests.py
# or
python -m tests.run_tests
```

The **wizard runs these checks automatically** when you click “Start migration”.

---

## Run the wizard (GUI)

```bash
python wizard.py
```

Steps:

1. **Welcome** – Intro and link to AOS.
2. **Database** – PostgreSQL connection (Host, Port, Database, User, Password).
3. **Select data** – Choose what to migrate (Caregivers, Clients, Availability types, etc.). Select **Availability types** if you use Caregivers/Clients Availability.
4. **Files** – For each option, choose the input file or folder. Not needed for “Calculated Geocode” or “Calculate distances” (except IE.txt for API geocode).
5. **Review** – Summary; accept the privacy policy to continue.
6. **Run** – Pre-run checks run first; then migrations run in order. A log file is saved in the project root.

Do not close the window until the run finishes.

---

## CLI commands

```bash
python main.py <command> [options]
```

| Command | Description |
|--------|-------------|
| `areas` | Migrate areas/teams to `users_group` table |
| `users` | Migrate users (caregivers) |
| `clients` | Migrate clients |
| `availability-types [path]` | Migrate availability types from CSV (default: `assets/availabilitytypes`) |
| `user-availabilities` | Migrate user availability |
| `availabilities [xlsx] [out_dir]` | Migrate client availabilities (Client Hours with Service Type) |
| `geocode-calculation` | Run geocode calculation (Google API) |
| `userlocations` | Update user lat/lng from JSON backup |
| `clientlocations` | Update client lat/lng from JSON backup |
| `travel-distances` | Compute user↔client distances via OSRM, upsert `travel_distances`, verify |
| `feasible-pairs [path]` | Seed feasible_pairs from visit data CSV (default: `assets/visit_data.csv`). Supports columns **Assignee** / **Customer** or **VisitExport** style: **Customer Name**, **Actual Employee Name** / **Planned Employee Name** (e.g. from sheet "All Visit Details" exported to CSV). |
| `test` | Run pre-run checks and optional distance test |

---

## CI/CD: Build exe, dmg, and Linux binary (free)

**GitHub Actions** build all three platforms on every push to `main`/`master` and on pull requests. You can also run the workflow manually from the **Actions** tab → **Build executables** → **Run workflow**.

1. Push this repo to **GitHub** (or use your fork).
2. Open the repo → **Actions** → select the latest **Build executables** run.
3. At the bottom of the run, under **Artifacts**, download:
   - **AOS-Migration-Wizard-windows** (contains `.exe`)
   - **AOS-Migration-Wizard-macos** (contains `.app` and `.dmg`)
   - **AOS-Migration-Wizard-linux** (contains the Linux binary)

No paid plan required: public repos get unlimited Actions minutes; private repos get a free allowance.

---

## Building executables locally (exe, dmg, pkg)

Use **PyInstaller** to build a standalone executable. You must build on the target OS (Windows for `.exe`, macOS for `.app`/`.dmg`/`.pkg`, Linux for a binary).

**On macOS:** you can build the **.app**, **DMG**, and **.pkg** in one go:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt pyinstaller
./build_mac.sh
```

Optional: `brew install create-dmg` so the script can also create a `.dmg` installer.  
This produces `dist/AOS-Migration-Wizard.app`, and (if create-dmg is installed) a `.dmg`. **You cannot build a Windows `.exe` on a Mac**—use a Windows machine or CI (e.g. GitHub Actions) to build the exe.

### 1. Install PyInstaller

```bash
pip install pyinstaller
```

### 2. One-file executable (wizard)

From the project root:

```bash
# All platforms: one-file bundle (wizard only)
pyinstaller --onefile --name "AOS-Migration-Wizard" wizard.py
```

Output:

- **Windows:** `dist/AOS-Migration-Wizard.exe`
- **macOS:** `dist/AOS-Migration-Wizard` (no .app; see below for .app/.dmg)
- **Linux:** `dist/AOS-Migration-Wizard`

For the wizard, **one-folder** is often more reliable (tkinter, paths):

```bash
pyinstaller --onedir --name "AOS-Migration-Wizard" wizard.py
```

Then run the executable inside `dist/AOS-Migration-Wizard/`.

### 3. Windows: EXE

```bash
# From project root on Windows
pyinstaller --onefile --name "AOS-Migration-Wizard" wizard.py
```

Copy `dist/AOS-Migration-Wizard.exe` and (if you use `--onedir`) the whole `dist/AOS-Migration-Wizard` folder. Users need the same `assets` layout if the app expects paths relative to the exe; you can add `assets` next to the exe or document where to put input files.

### 4. macOS: .app and DMG

**Create .app (PyInstaller):**

```bash
# On macOS
pyinstaller --onedir --name "AOS-Migration-Wizard" --windowed wizard.py
```

Then turn the `dist/AOS-Migration-Wizard` bundle into a proper .app (e.g. rename and move `AOS-Migration-Wizard` to `AOS-Migration-Wizard.app/Contents/MacOS/` with a minimal `Info.plist`), or use:

```bash
pyinstaller --onefile --name "AOS-Migration-Wizard" --windowed wizard.py
```

**Create DMG (installer image):**

- Install [create-dmg](https://github.com/create-dmg/create-dmg) (e.g. `brew install create-dmg`), or use Disk Utility.
- Put the `.app` (or the `AOS-Migration-Wizard` folder) in a folder, then create a read-only DMG that opens that folder when mounted.

Example with `create-dmg` (after you have `AOS-Migration-Wizard.app`):

```bash
create-dmg --volname "AOS Migration Wizard" --window-size 500 300 dist/AOS-Migration-Wizard.app dist/
```

**Create .pkg (installer package):**

- Use Apple’s `pkgbuild` and `productbuild` to wrap the `.app` (or the application folder) into a `.pkg` that users can double-click to install.

```bash
# Example: build a .pkg that installs the app to /Applications
pkgbuild --identifier com.aos.migration-wizard --root dist/AOS-Migration-Wizard.app --install-location /Applications/AOS-Migration-Wizard.app AOS-Migration-Wizard.pkg
```

### 5. Trusting the app on Windows (Defender / SmartScreen / other AV)

Windows Defender and other security software often block or quarantine PyInstaller-built executables (false positives). To run the migration wizard without it being stopped:

**Option A – Add a folder exclusion in Windows Defender**

1. Open **Windows Security** (search “Windows Security” in Start).
2. Go to **Virus & threat protection** → **Manage settings** (under “Virus & threat protection settings”).
3. Scroll to **Exclusions** → **Add or remove exclusions** → **Add an exclusion** → **Folder**.
4. Add one or both of:
   - The **project folder** (e.g. `C:\Users\YourName\...\Migration`), so scripts and venv are not scanned.
   - The **output folder** where the exe lives (e.g. `C:\...\Migration\dist` or the folder where you copied `AOS-Migration-Wizard.exe`).

**Option B – Restore a quarantined file**

If Defender already removed the exe:

1. **Windows Security** → **Virus & threat protection** → **Protection history**.
2. Find the entry for `AOS-Migration-Wizard.exe` (or your migration exe).
3. Click it → **Actions** → **Restore**. The file will be restored and often allowed after you choose “Restore”.

**Option C – SmartScreen “Windows protected your PC”**

If you see “Windows protected your PC” when running the exe:

1. Click **More info**.
2. Click **Run anyway**.

**Other antivirus software**

- Add the same folder (project or `dist`) as an **exclusion** or **trusted path** in your AV’s settings (e.g. Norton, McAfee, Kaspersky). The exact menu name varies (e.g. “Exclusions”, “Trusted files”, “Allow list”).

**If you don’t want to add exclusions**

Run the wizard without building an exe: from the project folder run `python wizard.py` (with `.venv` activated). No standalone exe is involved, so AV is less likely to block it.

---

### 6. Linux: binary and (optional) AppImage

**Binary:**

```bash
# On Linux
pyinstaller --onefile --name "AOS-Migration-Wizard" wizard.py
# Run: ./dist/AOS-Migration-Wizard
```

**AppImage (optional):** use [python-appimage](https://github.com/niess/python-appimage) or [appimage-builder](https://appimage-builder.readthedocs.io/) to wrap the PyInstaller output into an AppImage.

---

## Summary

| Goal | Command / action |
|------|-------------------|
| Install deps | `pip install -r requirements.txt` |
| Run tests | `python main.py test` or `python tests/run_tests.py` |
| GUI wizard | `python wizard.py` |
| CLI | `python main.py <command>` |
| Windows exe | `pyinstaller --onefile wizard.py` on **Windows** only |
| macOS .app / dmg / pkg | `./build_mac.sh` on macOS (or PyInstaller + create-dmg / pkgbuild) |
| Linux binary | `pyinstaller --onefile wizard.py` on Linux |

Log files (e.g. `migration_wizard_*.log`) are written in the project root; run the wizard from the project directory so paths and assets resolve correctly.
