@echo off
REM Navigate to your project folder
cd /d "C:\Users\hp\Downloads\Amdari\Data Scrapping"

REM Activate the virtual environment (adjust if yours is named differently)
call venv\Scripts\activate

REM Run the ETL script
python etl.py

REM Pause to see output (optional)
pause
