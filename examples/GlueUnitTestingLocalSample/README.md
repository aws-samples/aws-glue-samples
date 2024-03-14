## GlueUnitTestingLocalSample
This project can be used as a template for a Python Glue for Spark project with pytest based unit tests

### Prerequisites
To do the setup correctly, the script needs several tools to be installed and available in the system path.
- Python3 https://www.python.org/downloads/
- Git (only needed during the initial setup) https://git-scm.com/downloads
- Apache Maven (only needed during the initial setup) https://maven.apache.org/download.cgi

### Setup
Execute the setup script provided on the base directory. 
On Linux/Mac: 

    sh setup_venv.sh

On Microsoft Windows:  

    setup_venv.cmd

The script will create a Python virtual environment for the project and in it install PySpark and the Glue libraries required.  
If the script fails, you migth have to wipe the venv directory to rerun after you solve the issue.   

Once the setup in complete, you will get a message indicating that the setup is done.  
Then in Linux/Mac you can activate the virtual environment created using "source venv/bin/activate" so the prompt start with *(venv)*.
On Windows it is already activated by the script. 

With the environment activated, simply run the **pytest** command which will locate and run the sample unit test provided that tests the Glue script under the *src* folder.
If all goes as expected, pytest will report that the test has passed and store the test report and coverage files under the *build* directory.  
