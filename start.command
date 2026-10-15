#!/bin/bash
cd "$(dirname "$0")" || exit 1
APP="$(pwd)"
TITLE="Asset Selection Engine"

err() { osascript -e "display dialog \"$1\" buttons {\"OK\"} default button 1 with title \"$TITLE\"" >/dev/null 2>&1; }

# One-time right-click -> Open gets past Gatekeeper (this app isn't code-signed). This then de-quarantines the whole
# folder so every future double-click just works. Idempotent + silent if there's nothing to clear.
xattr -dr com.apple.quarantine "$APP" 2>/dev/null

# Pick the runtime for this Mac's chip (Apple Silicon = arm64, Intel = x86_64).
case "$(uname -m)" in
  arm64) Z="electron-v42.4.0-darwin-arm64" ;;
  *)     Z="electron-v42.4.0-darwin-x64" ;;
esac
EXE="$APP/runtime/$Z/Electron.app/Contents/MacOS/Electron"

# First run only: unpack THIS Mac's runtime from the bundled zip. 'ditto' restores the app's symlinks + permissions
# (a Windows-built outer zip cannot). Quick local unpack (~10-20s) -- no download, no Node, no install wait.
if [ ! -x "$EXE" ]; then
  if [ ! -f "$APP/$Z.zip" ]; then err "This Mac's app runtime is missing from the download. Please re-download the app folder and try again."; exit 1; fi
  rm -rf "$APP/runtime/$Z"; mkdir -p "$APP/runtime/$Z"
  if ! ditto -x -k "$APP/$Z.zip" "$APP/runtime/$Z" 2>/dev/null; then err "Could not unpack the app runtime. Please re-download the app folder and try again."; exit 1; fi
  xattr -dr com.apple.quarantine "$APP/runtime" 2>/dev/null
fi
if [ ! -x "$EXE" ]; then err "App runtime wasn't found after unpacking. Please re-download the app folder."; exit 1; fi

# Launch detached so closing this small Terminal window does NOT close the app. The app's own output goes to
# launch.log so that if it ever fails to appear (Gatekeeper kill / crash), there's a diagnostic to send Nate
# instead of a silent "nothing happened".
nohup "$EXE" "$APP" >"$APP/launch.log" 2>&1 &
exit 0
