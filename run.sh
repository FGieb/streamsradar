#!/bin/bash
# StreamsRadar - Quick start script
echo "🚀 Starting StreamsRadar..."
echo "   Open http://localhost:8000 in your browser"
echo ""
python -m uvicorn app:app --reload --host 0.0.0.0 --port 8000
