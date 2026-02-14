#!/usr/bin/env bash
# Build AOS Migration Wizard for macOS: .app, .dmg, and .pkg
# Run from project root: ./build_mac.sh
# Requires: Python 3.8+, pip. Optional: create-dmg (brew install create-dmg) for DMG.

set -e
cd "$(dirname "$0")"

# Only run on macOS
if [[ "$(uname)" != Darwin ]]; then
  echo "This script is for macOS only. On Windows use PyInstaller to build .exe; on Linux to build the binary."
  exit 1
fi

# Use project venv if present (recommended: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt pyinstaller)
if [[ -d .venv ]]; then
  source .venv/bin/activate
fi

echo "=== 1. PyInstaller ==="
if ! python3 -c "import PyInstaller" 2>/dev/null; then
  echo "Installing PyInstaller (use a venv if your system Python is externally managed)..."
  pip3 install pyinstaller
fi
rm -rf build dist
pyinstaller --noconfirm --clean AOS-Migration-Wizard-mac.spec

APP_PATH="dist/AOS-Migration-Wizard.app"
if [[ ! -d "$APP_PATH" ]]; then
  echo "Build failed: $APP_PATH not found"
  exit 1
fi
echo "Built: $APP_PATH"

echo ""
echo "=== 2. DMG (installer disk image) ==="
if command -v create-dmg &>/dev/null; then
  DMG_NAME="AOS-Migration-Wizard-1.0.0.dmg"
  # Remove previous DMG so create-dmg can create a new one
  rm -f "dist/$DMG_NAME"
  create-dmg \
    --volname "AOS Migration Wizard" \
    --window-size 500 320 \
    --icon "AOS-Migration-Wizard.app" 180 150 \
    --app-drop-link 320 150 \
    "dist/$DMG_NAME" \
    "dist/"
  echo "Built: dist/$DMG_NAME"
else
  echo "create-dmg not found. Install with: brew install create-dmg"
  echo "Skipping DMG. You can create it manually with Disk Utility or install create-dmg and re-run."
fi

echo ""
echo "=== 3. PKG (installer package) ==="
PKG_PATH="dist/AOS-Migration-Wizard-1.0.0.pkg"
if pkgbuild \
  --identifier com.aos.migration-wizard \
  --root "$APP_PATH" \
  --install-location /Applications/AOS-Migration-Wizard.app \
  "$PKG_PATH" 2>/dev/null; then
  echo "Built: $PKG_PATH"
else
  echo "PKG build skipped. To install: drag $APP_PATH to Applications."
fi

echo ""
echo "Done. Outputs:"
echo "  - $APP_PATH (double-click to run)"
echo "  - dist/AOS-Migration-Wizard-1.0.0.dmg (if create-dmg was installed)"
echo "  - dist/AOS-Migration-Wizard-1.0.0.pkg (if pkgbuild succeeded)"
echo ""
echo "Note: Windows .exe cannot be built on Mac. Build it on Windows with:"
echo "  pyinstaller --onefile --name AOS-Migration-Wizard wizard.py"
echo "  or use GitHub Actions with a Windows runner."
