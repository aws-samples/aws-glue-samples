# Glue Python Dependency Packager

This command line utility packages Python dependencies into a single wheel file for use with AWS Glue jobs. The tool creates an "uber wheel" containing all your dependencies and their transitive dependencies, ensuring consistent and reliable deployments across AWS Glue environments.

## Features

- Resolves all transitive dependencies for target Glue environments
- Creates a single wheel file containing all dependencies
- Automatically selects correct Python version and platform tags based on Glue version
- Provides runtime installation utilities for easy dependency loading in Glue jobs
- Validates glibc compatibility for native dependencies

## Supported Glue Versions

- AWS Glue 5.0 (Python 3.11, glibc 2.34)
- AWS Glue 4.0 (Python 3.10, glibc 2.26)
- AWS Glue 3.0 (Python 3.7, glibc 2.26)
- AWS Glue 2.0 (Python 3.7, glibc 2.17)
- AWS Glue 1.0 (Python 3.6, glibc 2.17)
- AWS Glue 0.9 (Python 2.7, glibc 2.17)

## How to use it

You can run this utility in any location where you have Python and the following environment.

### Pre-requisite

- Python 3.6+ (matching or compatible with your target Glue version)
- pip, build, wheel, setuptools, and pip-tools
- A `requirements.txt` file with your dependencies

### Command line Syntax

```bash
./wheel_packager.sh -g GLUE_VERSION [OPTIONS]
```

**Required Arguments:**

- `-g, --glue-version VERSION` - AWS Glue version (required)

**Optional Arguments:**

- `-r, --requirements FILE` - Path to requirements.txt file (default: requirements.txt)
- `-o, --wheel-output DIR` - Output directory for final wheel (default: current directory)
- `-n, --name NAME` - Package name (default: current directory name)
- `-v, --version VERSION` - Package version (default: 0.1.0)
- `-h, --help` - Show help message

## Examples

### Example 1. Basic usage with requirements.txt

```bash
./wheel_packager.sh -g 4.0 -r path/to/requirements.txt
```

### Example 2. Custom output directory

```bash
./wheel_packager.sh -g 4.0 -r path/to/requirements.txt -o dist
```

### Example 3. Full configuration

```bash
./wheel_packager.sh -g 4.0 -r path/to/requirements.txt -n my_glue_dependencies -v 1.0.0 -o dist
```

### Example of command output

```
=========================================
Building wheel for my_glue_dependencies with all dependencies from requirements.txt
=========================================
Using Glue version 4.0
Using Glue python version 3.10
Using Glue glibc version 2.26

Step 1/5: Installing build tools...
✓ Build tools installed successfully

Step 2/5: Creating build environment...
✓ Build environment created successfully

Step 3/5: Resolving all dependencies...
✓ Dependencies resolved successfully

Step 4/5: Downloading all dependency wheels...
✓ Downloaded 15 dependency wheels successfully

Step 5/5: Creating uber wheel with all dependencies included...
✓ Uber wheel created successfully!

=========================================
BUILD COMPLETED SUCCESSFULLY!
=========================================
Final wheel: ./my_glue_dependencies-1.0.0-py3-none-any.whl
Wheel size: 25M
Dependencies included: 15 packages

To install the bundle, run:
  pip install ./my_glue_dependencies-1.0.0-py3-none-any.whl
```

## Using the Generated Wheel

### 1. Upload to S3

```bash
aws s3 cp my_glue_dependencies-1.0.0-py3-none-any.whl s3://your-bucket/glue-dependencies/
```

### 2. Configure your Glue job

Add the following job parameter:
```
--additional-python-modules s3://your-bucket/glue-dependencies/my_glue_dependencies-1.0.0-py3-none-any.whl
```

### 3. Use in your Glue script

```python
# Option 1: Automatic installation (recommended)
import my_glue_dependencies.auto

# Option 2: Manual installation
from my_glue_dependencies import load_wheels
load_wheels()

# Your dependencies are now available
import pandas as pd
import numpy as np
```

## Troubleshooting

### Error: "No Python executable found"
Ensure Python 3 is installed and available in your PATH.

### Error: "Failed to resolve dependencies"
Check for conflicting version requirements in your requirements.txt. Ensure all packages are available for your target platform.

### Error: "Unsupported glue version"
Verify you're using a supported Glue version (0.9, 1.0, 2.0, 3.0, 4.0, 5.0).

### Warning: "Package name contains dashes"
The script automatically converts dashes to underscores for Python compatibility.

## Limitations

- Must be run in an environment compatible with your target Glue version
- Requires network access to download dependencies
- Large wheels may impact Glue job startup time
- Native dependencies must be compatible with the target Glue environment