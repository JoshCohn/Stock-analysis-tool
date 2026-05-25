#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "================================================"
echo "  Top Analyst Picks — Stock Analysis Tool"
echo "================================================"

# Create venv if missing
if [ ! -d ".venv" ]; then
  echo "Creating virtual environment..."
  python3 -m venv .venv
fi

source .venv/bin/activate

echo "Installing / updating dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

echo ""
echo "Starting server → http://localhost:8000"
echo ""
echo "  Dashboard   : http://localhost:8000/"
echo "  API docs    : http://localhost:8000/docs"
echo ""
echo "  NOTE: First data load for ALL S&P 500 stocks takes"
echo "  3-5 minutes while analyst data is fetched."
echo "  Sector views are faster (~1 min for ~30 stocks)."
echo "  All data is cached for 1 hour."
echo ""
echo "  Press Ctrl+C to stop."
echo ""

uvicorn main:app --host 0.0.0.0 --port 8000 --reload
