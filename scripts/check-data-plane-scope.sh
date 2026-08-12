#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"
export GOCACHE=/tmp/atoll-go-build

production=(
  runtime/accessdoor runtime/resourcespec runtime/internal/store/resources*.go
  runtime/internal/store/schema.go platform/dataplane platform/home/storagehost.go
  platform/internal/link drivers/devicehost/internal/storagehost
  drivers/tools/device
)

retired='GenerateCoord|placement_coord|ReserveCreate|CommitReservation|Committed|ReservationID|resource_reservations|resource_tombstones|TombstoneID|Scrubber|choosePlacement|is_dir|json:"dir"|\bDir[[:space:]]+(bool|\*)|\bDir:|(?i:\bstaging\b|\bcoord\b|/live/|"live")'
if rg -n "$retired" "${production[@]}" --glob '*.go'; then
  echo "[data-plane-scope] retired data-plane mechanism remains" >&2
  exit 1
fi

deferred='presign|Content-Range|Accept-Ranges|RangeRequest|byte[_ -]?range|cache[_ -]?warm|replica(tion)?|quota|file lock|s3 backend'
if rg -ni "$deferred" "${production[@]}" --glob '*.go'; then
  echo "[data-plane-scope] deferred data-plane feature found" >&2
  exit 1
fi

go test ./runtime/accessdoor ./drivers/devicehost/internal/storagehost ./platform/dataplane -count=1
echo "[data-plane-scope] PASS"
