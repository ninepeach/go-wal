#!/usr/bin/env bash

set -e

echo "==> Running benchmarks..."

go test -bench=. -benchmem ./

echo "✅ Done"

