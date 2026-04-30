@echo off
cd /d C:\Users\PJ\python\venv\chuppy_ondemand_converter
echo add venv...
call .\Scripts\activate.bat
echo Flask start...
python app.py
pause