# Atoll shell integration (bash). See atoll-integration.zsh for what this does
# and what it deliberately does not do.
#
# bash has no preexec hook, so this uses the DEBUG trap — the standard approach
# (bash-preexec uses the same one). It is more fragile than zsh's: a command
# substitution or a PROMPT_COMMAND can fire it too, so the guard below keeps a
# single mark per real command.

[[ -n "$ATOLL_SHELL_INTEGRATION" ]] || return 0
[[ -n "$__ATOLL_INTEGRATION_LOADED" ]] && return 0
__ATOLL_INTEGRATION_LOADED=1

__atoll_preexec() {
  [[ -n "$COMP_LINE" ]] && return          # completion, not a command
  [[ "$BASH_COMMAND" == "$PROMPT_COMMAND" ]] && return
  [[ -n "$__atoll_in_cmd" ]] && return
  __atoll_in_cmd=1
  printf '\e]133;C\e\\'
  printf '\e]1337;AtollCmd=%s\e\\' "$(printf %q "$BASH_COMMAND")"
}

__atoll_precmd() {
  local code=$?
  if [[ -n "$__atoll_in_cmd" ]]; then
    printf '\e]133;D;%s\e\\' "$code"
    unset __atoll_in_cmd
  fi
  printf '\e]133;A\e\\'
}

trap '__atoll_preexec' DEBUG
PROMPT_COMMAND="__atoll_precmd${PROMPT_COMMAND:+; $PROMPT_COMMAND}"
