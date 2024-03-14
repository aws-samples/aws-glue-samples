## GlueUnitTestingLocalSample
This project can be used as a template for a Python Glue for Spark 4.0 project with pytest unit tests

### Prerequisites
To do the setup correctly, the script needs several tools to be installed and available in the system path during the setup, afterwards you just need Python and Java.
- Python3 https://www.python.org/downloads/
- Java JRE with the JAVA_HOME environment variable set. https://docs.aws.amazon.com/corretto/latest/corretto-8-ug/downloads-list.html. Java 8 is recommended, since is what Glue 4.0 uses, but Java 11 also works since is backwards compatible with 8.
- Apache Maven https://maven.apache.org/download.cgi
- Git https://git-scm.com/downloads

### Setup
Execute the setup script provided on the base directory.   
On Linux/Mac: 

    sh setup_venv.sh

On Microsoft Windows:  

    setup_venv.cmd

The script will create a Python virtual environment for the project and in it install PySpark and the Glue libraries required.  
If the script fails, you migth have to wipe the *venv* directory to rerun after you solve the issue.   

Once the setup in complete, you will get a message indicating that the setup is done.  
Then in Linux/Mac you can activate the virtual environment created running **source venv/bin/activate** so the prompt starts with *(venv)*.  
On Windows it is already activated by the script. 

With the environment activated, simply run the **pytest** command which will locate and run the sample unit test provided that tests the Glue script under the *src* folder.
If all goes as expected, pytest will report that the test has passed and store the test report and coverage files under the *build* directory.  
