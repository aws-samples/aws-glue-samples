In a system with a version of Python3 and Maven installed, run the script setup_venv.sh from the same directory
If you don't have Apache Maven, you can download it from https://maven.apache.org/download.cgi, extract it and put the bin directory in the PATH
The script will setup a venv to run tests and in it will put:
- packages from pip
- the awsglue module from Github
- Glue libraries from Maven repositories on s3

If the script fails, you migth have to wipe the venv directory to rerun after you solve the issue. 
Once the setup in complete, you can activate the venv created using "source venv/bin/activate" and then 
run "pytest", which will find the tests using naming and conventions conventions, you can also generate a junit report.
In the build directory it will produce reports for the tests and coverage, that other tools can use.
