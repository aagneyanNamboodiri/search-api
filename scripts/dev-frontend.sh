#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../frontend"

export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
if [[ -s "$NVM_DIR/nvm.sh" ]]; then
  # shellcheck source=/dev/null
  source "$NVM_DIR/nvm.sh"
  nvm use 22
fi

exec pnpm dev
