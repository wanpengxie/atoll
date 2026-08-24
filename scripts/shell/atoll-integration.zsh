# Atoll shell integration (zsh).
#
# Source this from ~/.zshrc. It emits OSC 133 semantic-prompt marks so the
# terminal line can tell where one command ends and the next begins — WITHOUT
# anyone parsing your prompt out of the byte stream, which is fragile and which
# Atoll deliberately does not do.
#
#   [ -f /path/to/atoll-integration.zsh ] && . /path/to/atoll-integration.zsh
#
# What lands on the channel ledger, one row per command: the command text, its
# exit code, how long it took, and a bounded tail of its output. Keystrokes and
# scrolling output NEVER land — they are a live stream, not a record.
#
# This is cooperative, not enforced: `exec bash`, a subshell or an ssh out will
# stop producing marks, and those commands simply are not recorded. The
# terminal is a door that keeps an honest log, not a door that judges.
#
# The same marks are what VS Code, iTerm2, kitty, WezTerm and Ghostty use, so
# installing this improves those terminals too.

# Only inside an Atoll terminal that asked for it.
[[ -n "$ATOLL_SHELL_INTEGRATION" ]] || return 0
# Guard against double-sourcing (a re-sourced rc would double every mark).
[[ -n "$__ATOLL_INTEGRATION_LOADED" ]] && return 0
__ATOLL_INTEGRATION_LOADED=1

autoload -Uz add-zsh-hook

# OSC 133;C — the command is about to run; output starts after this.
# OSC 1337;AtollCmd=… — the command text, taken verbatim from zsh rather than
# recovered from the screen. ${(q)1} quotes it so newlines and spaces survive.
__atoll_preexec() {
  printf '\e]133;C\e\\'
  printf '\e]1337;AtollCmd=%s\e\\' "${(q)1}"
  # Where it ran matters as much as what ran: the same command means
  # different things in different trees.
  printf '\e]1337;AtollCwd=%s\e\\' "${(q)PWD}"
}

# OSC 133;D;<code> — the command finished, with its status.
# OSC 133;A — a new prompt begins.
# $? must be read first: anything else here would clobber it.
__atoll_precmd() {
  local code=$?
  printf '\e]133;D;%s\e\\' "$code"
  printf '\e]133;A\e\\'
}

add-zsh-hook preexec __atoll_preexec
add-zsh-hook precmd __atoll_precmd
