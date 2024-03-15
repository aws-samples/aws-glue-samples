## GlueUnitTestingLocalSample
This project can be used as a template for a AWS Glue version 4.0 (PySpark) project with pytest unit tests

### Prerequisites
To do the setup correctly, the script needs several tools to be installed and available in the system path during the setup, afterwards you just need Python and Java.
- Python3 https://www.python.org/downloads/
- Java JRE with the JAVA_HOME environment variable set. https://docs.aws.amazon.com/corretto/latest/corretto-8-ug/downloads-list.html. Java 8 is recommended, since is what Glue 4.0 uses, but Java 11 also works since is backwards compatible with 8.
- Apache Maven https://maven.apache.org/download.cgi
- Git https://git-scm.com/downloads

### Setup
Execute the setup script provided on the base directory.   
On a Linux/Mac shell: 

    sh setup_venv.sh

On a Microsoft Windows Command Prompt:  

    setup_venv.cmd

The script will create a Python virtual environment for the project and in it install PySpark and the Glue libraries required.  
If the script fails, you migth have to wipe the *venv* directory to rerun after you solve the issue.   
Once the setup in complete, you will get a message indicating that the setup is done.  

### Run unit tests
On Linux/Mac, activate the virtual environment created running:

    source venv/bin/activate

On Windows, it is already activated by the setup script, if you need to reactivate it later run: 

    call venv/Scripts/activate
  
Once the environment is activated and the prompt starts with *(venv)*, simply run the **pytest** command which will locate and run the sample unit test in the *test* directory that tests the Glue script under the *src* folder.
If all goes as expected, pytest will report that the test has passed and store the test report and coverage files under the *build* directory.  

### Sample unit test provided
The project includes a sample test, when you run pytest, it will find the find *test_glue_script.py* in the *test* directory, load the test suite and run the test *test_glue_script*.
It finds the files and the test configuration based on pytest naming conventions.    
The test first mocks a catalog source and a Postgres sink, since unit tests shouldn't make external connections.   
Then it loads the Glue script in the *src* directory and validates that the data produced is the result of reading and transforming as expected.   
The result of running the test suite looks like this (using the flag *--disable-warnings* for simplicity): 

    ===================================================================================== test session starts =====================================================================================
    platform linux -- Python 3.7.16, pytest-7.4.4, pluggy-1.2.0
    rootdir: /tmp/aws-glue-samples/examples/GlueUnitTestingLocalSample
    configfile: pytest.ini
    plugins: cov-4.1.0
    collected 1 item
    
    test/test_glue_script.py .                                                                                                                                                              [100%]
    
    ------------------------------------------- generated xml file: /tmp/aws-glue-samples/examples/GlueUnitTestingLocalSample/build/gluetest-report.xml -------------------------------------------
    
    ---------- coverage: platform linux, python 3.7.16-final-0 -----------
    Coverage XML written to file build/cov.xml
    
    =============================================================================== 1 passed, 2 warnings in 12.46s ================================================================================
