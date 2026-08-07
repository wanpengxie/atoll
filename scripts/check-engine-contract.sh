#!/usr/bin/env bash
set -euo pipefail

go generate ./app/contract
git diff --exit-code -- app/contract/testdata/engine-api.schema.json
go test ./app/contract -run '^(TestGoldenSchemaIsCurrent|TestRegistryMethodsCarrySince)$'
