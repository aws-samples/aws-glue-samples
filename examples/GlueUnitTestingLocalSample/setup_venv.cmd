@echo off
call python --version
IF %ERRORLEVEL% NEQ 0 (
    echo This script requires a version of Python3 to be installed and included in the PATH
    exit /b 1
)

call mvn --version
IF %ERRORLEVEL% NEQ 0 (
    echo This script requires Maven to be installed and included in the PATH
    exit /b 1
)

call git --version
IF %ERRORLEVEL% NEQ 0 (
    echo This script requires Git to be installed and included in the PATH
    exit /b 1
)    

echo Creating Python virtual environment
python -m venv venv
IF %ERRORLEVEL% NEQ 0 (
    echo Failed to create Python virtual environment, cannot continue
    exit /b 1
)    
echo Created venv
call venv/Scripts/activate.bat

echo Installing Python modules
pip install pyspark==3.3.0 pytest pytest-cov || exit /b
pip install git+https://github.com/awslabs/aws-glue-libs.git || exit /b

set TMP_PY_FILE=tmp_get_path.py
echo import pyspark > %TMP_PY_FILE%
echo import os >> %TMP_PY_FILE%
echo print(os.path.dirname(os.path.realpath(pyspark.__file__))) >> %TMP_PY_FILE%
for /f %%i in ('python %TMP_PY_FILE%') do set PYSPARK_PATH=%%i
del %TMP_PY_FILE%
echo "Installed PySpark under: %PYSPARK_PATH%"

echo Replacing jars with the ones from Glue 4
set JARS_PATH=%PYSPARK_PATH%\jars
move %JARS_PATH% %JARS_PATH%_bak
call mvn -f configuration/pom.xml dependency:copy-dependencies -DoutputDirectory=%JARS_PATH%

echo ---------------------------------------------------------------------------------------------------------------------
echo Done, to run the test you can run 'pytest' with virtual environment activated,
echo  if you need to reactivate it later you can run 'call venv/Scripts/activate'
echo ---------------------------------------------------------------------------------------------------------------------

