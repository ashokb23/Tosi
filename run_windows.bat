@echo off
cd /d %~dp0
pip install -q yfinance pandas numpy
start "" http://localhost:8765
python server.py
pause
