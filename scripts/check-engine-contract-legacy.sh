#!/usr/bin/env bash
set -euo pipefail

# Migration guard (NOT an invariant wall). The C1 contract cutover deleted
# every gin.H{...} error literal and every gin JSON binding on the contract
# face; this grep only stops a parallel branch from merging the old forms back
# during the migration window. The invariant itself (one error shape + strict
# fail-closed decode, K7/D7) is enforced positively by app/contract's golden
# schema + decoder tests — not by this script.
#
# REMOVAL CONDITION: delete this script and its Makefile hook once the C1
# cutover commit is merged and no pre-cutover branch remains open.
if rg -n 'ShouldBindJSON|BindJSON|gin\.H\{"error"|gin\.H\{"code"' \
  app platform/subjectgate drivers/gateway \
  --glob '*.go' --glob '!*_test.go'; then
  echo "legacy engine API decode/error path remains" >&2
  exit 1
fi
