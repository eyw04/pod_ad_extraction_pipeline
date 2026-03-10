#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

LOGFILE="$LOG_DIR/pipeline_$(date +%Y-%m-%d).log"
PYTHON="/opt/anaconda/bin/python3"

echo "=== Pipeline run started at $(date) ===" >> "$LOGFILE"
$PYTHON "$SCRIPT_DIR/run_pipeline.py" >> "$LOGFILE" 2>&1
EXIT_CODE=$?
echo "=== Pipeline run finished at $(date) with exit code $EXIT_CODE ===" >> "$LOGFILE"

exit $EXIT_CODE
