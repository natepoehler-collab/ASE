#!/bin/bash
cd "$(dirname "$0")" || exit 1
APP="$(pwd)"
TITLE="Asset Selection Engine Update"
note() { osascript -e "display dialog \"$1\" buttons {\"OK\"} default button 1 with title \"$TITLE\"" >/dev/null 2>&1; }
ask()  { osascript -e "display dialog \"$1\" buttons {\"Cancel\", \"Update\"} default button 2 with title \"$TITLE\"" >/dev/null 2>&1; }

# Newest update in Downloads -- a .zip OR an already-extracted ASE-update folder (Safari's "open safe files after
# downloading" auto-unzips it). One sorted list, newest first; works if only one of the two patterns matches.
SRC="$(ls -dt "$HOME/Downloads/"ASE-update*.zip "$HOME/Downloads/"ASE-update*/ 2>/dev/null | head -n 1)"
if [ -z "$SRC" ]; then
  note "I could not find an update in your Downloads folder. Open Discord, click the newest update link to download it, then double-click this Update file again."
  exit 0
fi

if ! ask "Found your update. Please make sure the app is closed, then click Update."; then
  exit 0
fi

# Stage the new files in a temp folder -- copy the already-extracted folder, or unzip the .zip.
TMP="$(mktemp -d)"
if [ -d "$SRC" ]; then
  cp -R "${SRC%/}/." "$TMP/" 2>/dev/null
elif ! unzip -o -q "$SRC" -d "$TMP"; then
  note "That update file would not open. Please download it again from Discord, then try once more."
  rm -rf "$TMP"; exit 1
fi

# Copy each updated code file into the app by basename. Never touch the launchers, node_modules, the extracted
# runtime, or the bundled runtime zips -- updates are code-only.
( cd "$TMP" && find . -type f -print0 | while IFS= read -r -d "" f; do
    base="$(basename "$f")"
    case "$f" in *node_modules*|*/runtime/*) continue;; esac
    case "$base" in start.command|Update.command|.DS_Store) continue;; esac
    case "$base" in electron-v*-darwin-*.zip) continue;; esac
    cp -f "$f" "$APP/$base"
  done )
rm -rf "$TMP"

# Newly-copied files arrive quarantined -> clear so the relaunch isn't Gatekeeper-blocked.
xattr -dr com.apple.quarantine "$APP" 2>/dev/null

# Relaunch via the already-unpacked runtime (no Node, no reinstall).
case "$(uname -m)" in arm64) Z="electron-v42.4.0-darwin-arm64" ;; *) Z="electron-v42.4.0-darwin-x64" ;; esac
EXE="$APP/runtime/$Z/Electron.app/Contents/MacOS/Electron"
if [ ! -x "$EXE" ]; then
  note "Update installed. Double-click start.command to open the app."
  exit 0
fi
note "Update installed! The app will open now."
nohup "$EXE" "$APP" >"$APP/launch.log" 2>&1 &
exit 0
