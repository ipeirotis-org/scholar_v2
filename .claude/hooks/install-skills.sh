#!/bin/bash
# Installs the paper-writing skills into Claude Code cloud sessions:
#   blue-pencil        revises prose; also registers the /paper:* commands and
#                      the paper-reviser subagent
#   facts-and-figures  re-runs the analysis pipeline behind manuscript numbers
#                      and figures; also registers its isolated subagent
#   citation-needed    checks citations and novelty claims against sources
#
# Each skill is a public repo cloned into ~/.claude/skills/<name>. A fresh
# session start also pulls its latest commit; resume, clear and compact only
# fill in what is missing, so they need no network. Outside cloud sessions the
# script does nothing, leaving a local install alone. Failures print a warning
# and continue: a missing skill should not stop the session from starting.
set -uo pipefail

if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

SKILLS=(blue-pencil facts-and-figures citation-needed)
GITHUB_OWNER=ipeirotis-org
SKILLS_DIR="$HOME/.claude/skills"
GIT_TIMEOUT=60

# The hook's stdin JSON says why the session started.
event=
if [ ! -t 0 ]; then
  event=$(jq -r '.source // empty' 2>/dev/null)
fi
event=${event:-startup}

# Printed to stdout so the warning reaches the session's context.
warn() { echo "install-skills: $*"; }

install_or_update() {
  local name=$1
  local url="https://github.com/$GITHUB_OWNER/$name"
  local dest="$SKILLS_DIR/$name"
  local origin

  if [ ! -e "$dest" ] && [ ! -L "$dest" ]; then
    mkdir -p "$SKILLS_DIR"
    if ! timeout "$GIT_TIMEOUT" git clone --quiet --depth 1 "$url" "$dest"; then
      rm -rf "$dest"
      warn "could not clone $url; the $name skill is not installed"
    fi
    return 0
  fi

  # Leave anything that is not our checkout alone (an account-synced or
  # hand-made copy, say).
  origin=$(git -C "$dest" remote get-url origin 2>/dev/null) || return 0
  case "${origin%.git}" in
    "$url"|"https://github.com/ipeirotis/$name") ;;
    *) return 0 ;;
  esac

  [ "$event" = startup ] || return 0
  if [ -n "$(git -C "$dest" status --porcelain 2>/dev/null)" ]; then
    warn "$dest has local changes; not updating it"
    return 0
  fi
  if ! timeout "$GIT_TIMEOUT" git -C "$dest" fetch --quiet --depth 1 origin HEAD \
    || ! git -C "$dest" reset --quiet --hard FETCH_HEAD; then
    warn "could not update $name; using the copy already installed"
  fi
}

# Copies blue-pencil's /paper:* commands and subagents into ~/.claude, as its
# own `install.sh --commands` does, and keeps that installer's manifest so its
# --update and --uninstall still work here.
register_blue_pencil() {
  local src="$SKILLS_DIR/blue-pencil/.claude"
  local manifest="$HOME/.claude/.blue-pencil-manifest"
  local f rel
  [ -d "$src/commands/paper" ] || return 0

  # Remove files an earlier version placed that this one no longer ships.
  if [ -f "$manifest" ]; then
    while IFS= read -r rel; do
      if [ -n "$rel" ] && [ ! -e "$src/$rel" ]; then
        rm -f "$HOME/.claude/$rel"
      fi
    done < "$manifest"
  fi

  mkdir -p "$HOME/.claude/commands/paper" "$HOME/.claude/agents"
  : > "$manifest.tmp"
  for f in "$src"/commands/paper/*.md "$src"/agents/*.md; do
    [ -e "$f" ] || continue
    rel=${f#"$src"/}
    cp "$f" "$HOME/.claude/$rel" && echo "$rel" >> "$manifest.tmp"
  done
  mv "$manifest.tmp" "$manifest"
}

register_facts_and_figures() {
  local f="$SKILLS_DIR/facts-and-figures/agents/claude-code/facts-and-figures.md"
  [ -f "$f" ] || return 0
  mkdir -p "$HOME/.claude/agents"
  cp "$f" "$HOME/.claude/agents/"
}

for name in "${SKILLS[@]}"; do
  install_or_update "$name"
done
register_blue_pencil
register_facts_and_figures
exit 0
