#!/usr/bin/env bash
# Sync the engine journal with the `algo-state` branch (used by GitHub Actions).
#   deploy/state.sh pull   -> ./state contains the latest nifty.db / btc.db / status.json
#   deploy/state.sh push   -> commit ./state back to algo-state (only if something changed)
set -euo pipefail
BR="${STATE_BRANCH:-algo-state}"
DIR="${NIFTY_ALGO_STATE_DIR:-state}"
case "${1:-}" in
  pull)
    rm -rf "$DIR"; git worktree prune
    if git ls-remote --exit-code --heads origin "$BR" >/dev/null 2>&1; then
      git fetch -q --depth=1 origin "$BR"
      git worktree add -q -B "$BR" "$DIR" "origin/$BR"
    else
      git worktree add -q --detach "$DIR"
      (cd "$DIR" && git checkout -q --orphan "$BR" && git rm -rqf . >/dev/null 2>&1 || true)
      (cd "$DIR" && find . -mindepth 1 -maxdepth 1 ! -name .git -exec rm -rf {} +)
    fi
    printf 'engine.lock\n*-wal\n*-shm\n' > "$DIR/.gitignore"
    ls -la "$DIR"
    ;;
  push)
    cd "$DIR"
    git add -A
    if git rev-parse -q --verify HEAD >/dev/null && git diff --cached --quiet; then echo "state unchanged"; exit 0; fi
    # single-commit snapshot (the journal itself holds the full history) -> repo never bloats
    export GIT_AUTHOR_NAME="algo-engine-bot" GIT_COMMITTER_NAME="algo-engine-bot"
    export GIT_AUTHOR_EMAIL="41898282+github-actions[bot]@users.noreply.github.com"
    export GIT_COMMITTER_EMAIL="$GIT_AUTHOR_EMAIL"
    C=$(git commit-tree "$(git write-tree)" -m "state $(date -u +%Y-%m-%dT%H:%MZ)")
    for i in 1 2 3 4; do
      git push -q -f origin "$C:refs/heads/$BR" && { echo "state pushed ($C)"; exit 0; }
      sleep $((2 ** i))
    done
    echo "state push failed" >&2; exit 1
    ;;
  *) echo "usage: $0 pull|push" >&2; exit 2 ;;
esac
