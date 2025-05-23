@echo off
REM Navigate to your project folder
cd /d "C:\Users\hp\Downloads\Amdari\Data Scrapping"

REM Activate the virtual environment 
call venv\Scripts\activate

REM Run the ETL script
python etl.py

REM Pause to see output 
pause
