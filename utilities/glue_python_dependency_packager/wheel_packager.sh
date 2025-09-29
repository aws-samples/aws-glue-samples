#!/bin/bash
set -e
REQUIREMENTS_FILE="requirements.txt"
FINAL_WHEEL_OUTPUT_DIRECTORY="."
PACKAGE_NAME=$(basename "$(pwd)")
PACKAGE_VERSION="0.1.0"
# Help message
show_help() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -r, --requirements FILE   Path to requirements.txt file (default: requirements.txt)"
    echo "  -o, --wheel-output DIR    Output directory for final wheel (default: current directory)"
    echo "  -n, --name NAME           Package name (default: current directory name)"
    echo "  -v, --version VERSION     Package version (default: 0.1.0)"
    echo "  -h, --help                Show this help message"
    echo "  -g, --glue-version        Glue version (required)"
    echo ""
    echo "Example:"
    echo "  $0 -r custom-requirements.txt -o dist -n my_package -v 1.2.3 -g 4.0"
}
# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
    -r | --requirements)
        REQUIREMENTS_FILE="$2"
        shift 2
        ;;
    -o | --wheel-output)
        FINAL_WHEEL_OUTPUT_DIRECTORY="$2"
        shift 2
        ;;
    -n | --name)
        PACKAGE_NAME="$2"
        shift 2
        ;;
    -v | --version)
        PACKAGE_VERSION="$2"
        shift 2
        ;;
    -g | --glue-version)
        GLUE_VERSION="$2"
        shift 2
        ;;
    -h | --help)
        show_help
        exit 0
        ;;
    *)
        echo "Unknown option: $1"
        show_help
        exit 1
        ;;
    esac
done
# If package name has dashes, convert to underscores and notify user. We need to check this since we cant import a package with dashes.
if [[ "$PACKAGE_NAME" =~ "-" ]]; then
    echo "Warning: Package name '$PACKAGE_NAME' contains dashes. Converting to underscores."
    PACKAGE_NAME=$(echo "$PACKAGE_NAME" | tr '-' '_')
fi
UBER_WHEEL_NAME="${PACKAGE_NAME}-${PACKAGE_VERSION}-py3-none-any.whl"
# Check if glue version is provided
if [ -z "$GLUE_VERSION" ]; then
    echo "Error: Glue version is required."
    exit 1
fi
# Validate version format (basic check)
if [[ ! "$PACKAGE_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] && [[ ! "$PACKAGE_VERSION" =~ ^[0-9]+\.[0-9]+$ ]]; then
    echo "Warning: Version '$PACKAGE_VERSION' doesn't follow semantic versioning (x.y.z or x.y)"
fi
# Check if requirements file exists
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    echo "Error: Requirements file '$REQUIREMENTS_FILE' not found."
    exit 1
fi
# Get relevant platform tags/python versions based on glue version
if [[ "$GLUE_VERSION" == "5.0" ]]; then
    PYTHON_VERSION="3.11"
    GLIBC_VERSION="2.34"
elif [[ "$GLUE_VERSION" == "4.0" ]]; then
    PYTHON_VERSION="3.10"
    GLIBC_VERSION="2.26"
elif [[ "$GLUE_VERSION" == "3.0" ]]; then
    PYTHON_VERSION="3.7"
    GLIBC_VERSION="2.26"
elif [[ "$GLUE_VERSION" == "2.0" ]]; then
    PYTHON_VERSION="3.7"
    GLIBC_VERSION="2.17"
elif [[ "$GLUE_VERSION" == "1.0" ]]; then
    PYTHON_VERSION="3.6"
    GLIBC_VERSION="2.17"
elif [[ "$GLUE_VERSION" == "0.9" ]]; then
    PYTHON_VERSION="2.7"
    GLIBC_VERSION="2.17"
else
    echo "Error: Unsupported glue version '$GLUE_VERSION'."
    exit 1
fi
echo "Using Glue version $GLUE_VERSION"
echo "Using Glue python version $PYTHON_VERSION"
echo "Using Glue glibc version $GLIBC_VERSION"
PIP_PLATFORM_FLAG=""
is_glibc_compatible() {
    # assumes glibc version in the form of major.minor (ex: 2.17)
    # glue glibc must be >= platform glibc
    local glue_glibc_version="$GLIBC_VERSION"
    local platform_glibc_version="$1"
    # 2.27 (platform) can run on 2.27 (glue)
    if [[ "$platform_glibc_version" == "$glue_glibc_version" ]]; then
        return 0
    fi
    local glue_glibc_major="${glue_glibc_version%%.*}"
    local glue_glibc_minor="${glue_glibc_version#*.}"
    local platform_glibc_major="${platform_glibc_version%%.*}"
    local platform_glibc_minor="${platform_glibc_version#*.}"
    # 3.27 (platform) cannot run on 2.27 (glue)
    if [[ "$platform_glibc_major" -gt "$glue_glibc_major" ]]; then
        return 1
    fi
    # 2.34 (platform) cannot run on 2.27 (glue)
    if [[ "$platform_glibc_major" -eq "$glue_glibc_major" ]] && [[ "$platform_glibc_minor" -gt "$glue_glibc_minor" ]]; then
        return 1
    fi
    # 2.17 (platform) can run on 2.27 (glue)
    return 0
}
PIP_PLATFORM_FLAG=""
if is_glibc_compatible "2.17"; then
    PIP_PLATFORM_FLAG="${PIP_PLATFORM_FLAG} --platform manylinux2014_x86_64"
fi
if is_glibc_compatible "2.28"; then
    PIP_PLATFORM_FLAG="${PIP_PLATFORM_FLAG} --platform manylinux_2_28_x86_64"
fi
if is_glibc_compatible "2.34"; then
    PIP_PLATFORM_FLAG="${PIP_PLATFORM_FLAG} --platform manylinux_2_34_x86_64"
fi
if is_glibc_compatible "2.39"; then
    PIP_PLATFORM_FLAG="${PIP_PLATFORM_FLAG} --platform manylinux_2_39_x86_64"
fi
echo "Using pip platform flags: $PIP_PLATFORM_FLAG"
# Convert to absolute paths
REQUIREMENTS_FILE=$(realpath "$REQUIREMENTS_FILE")
FINAL_WHEEL_OUTPUT_DIRECTORY=$(realpath "$FINAL_WHEEL_OUTPUT_DIRECTORY")
TEMP_WORKING_DIR=$(mktemp -d)
VENV_DIR="${TEMP_WORKING_DIR}/.build_venv"
WHEEL_OUTPUT_DIRECTORY="${TEMP_WORKING_DIR}/wheelhouse"
# Cleanup function
cleanup() {
    echo "Cleaning up temporary files..."
    rm -rf "$TEMP_WORKING_DIR"
}
trap cleanup EXIT
echo "========================================="
echo "Building wheel for $PACKAGE_NAME with all dependencies from $REQUIREMENTS_FILE"
echo "========================================="
# Determine Python executable to use consistently
PYTHON_EXEC=$(which python3 2>/dev/null || which python 2>/dev/null)
if [ -z "$PYTHON_EXEC" ]; then
    echo "Error: No Python executable found"
    exit 1
fi
echo "Using Python: $PYTHON_EXEC"
echo ""
# Install build requirements
echo "Step 1/5: Installing build tools..."
echo "----------------------------------------"
"$PYTHON_EXEC" -m pip install --upgrade pip build wheel setuptools
echo "✓ Build tools installed successfully"
echo ""
# Create a virtual environment for building
echo "Step 2/5: Creating build environment..."
echo "----------------------------------------"
"$PYTHON_EXEC" -m venv "$VENV_DIR"
# Check if virtual environment was created successfully
if [ ! -f "$VENV_DIR/bin/activate" ]; then
    echo "Error: Failed to create virtual environment"
    exit 1
fi
source "$VENV_DIR/bin/activate"
# Install pip-tools for dependency resolution
"$VENV_DIR/bin/pip" install pip-tools
echo "✓ Build environment created successfully"
echo ""
# Compile requirements to get all transitive dependencies
GLUE_PIP_ARGS="$PIP_PLATFORM_FLAG --python-version $PYTHON_VERSION --only-binary=:all:"
echo "Step 3/5: Resolving all dependencies..."
echo "----------------------------------------"
if ! "$VENV_DIR/bin/pip-compile" --pip-args "$GLUE_PIP_ARGS" --no-emit-index-url --output-file "$TEMP_WORKING_DIR/.compiled_requirements.txt" "$REQUIREMENTS_FILE"; then
    echo "Error: Failed to resolve dependencies. Check for conflicts in $REQUIREMENTS_FILE"
    exit 1
fi
echo "✓ Dependencies resolved successfully"
echo ""
# Download all wheels for dependencies
echo "Step 4/5: Downloading all dependency wheels..."
echo "----------------------------------------"
"$VENV_DIR/bin/pip" download -r "$TEMP_WORKING_DIR/.compiled_requirements.txt" -d "$WHEEL_OUTPUT_DIRECTORY" $GLUE_PIP_ARGS
# Check if any wheels were downloaded
if [ ! "$(ls -A "$WHEEL_OUTPUT_DIRECTORY")" ]; then
    echo "Error: No wheels were downloaded. Check your requirements file."
    exit 1
fi
# Count downloaded wheels (using find instead of ls for better handling)
WHEEL_COUNT=$(find "$WHEEL_OUTPUT_DIRECTORY" -name "*.whl" -type f | wc -l | tr -d ' ')
echo "✓ Downloaded $WHEEL_COUNT dependency wheels successfully"
echo ""
# Create a single uber wheel with all dependencies
echo "Step 5/5: Creating uber wheel with all dependencies included..."
echo "----------------------------------------"
# Create a temporary directory for the uber wheel
UBER_WHEEL_DIR="$TEMP_WORKING_DIR/uber"
mkdir -p "$UBER_WHEEL_DIR"
# Create the setup.py file with custom install command
cat >"$UBER_WHEEL_DIR/setup.py" <<EOF
from setuptools import setup, find_packages
import setuptools.command.install
import os
import glob
import subprocess
import sys
setup(
    name='${PACKAGE_NAME}',
    version='${PACKAGE_VERSION}',
    description='Bundle containing dependencies for ${PACKAGE_NAME}',
    author='Package Builder',
    author_email='builder@example.com',
    packages=['${PACKAGE_NAME}'],  # Include the package directory to hold wheels
    include_package_data=True,
    package_data={
        '${PACKAGE_NAME}': ['wheels/*.whl'],  # Include wheels in the package directory
    }
)
EOF
# Create a MANIFEST.in file to include all wheels
cat >"$UBER_WHEEL_DIR/MANIFEST.in" <<EOF
recursive-include ${PACKAGE_NAME}/wheels *.whl
EOF
# Create an __init__.py file that imports all the bundled wheel files (no auto-install logic)
mkdir -p "$UBER_WHEEL_DIR/${PACKAGE_NAME}"
cat >"$UBER_WHEEL_DIR/${PACKAGE_NAME}/__init__.py" <<EOF
"""
${PACKAGE_NAME} - dependencies can be installed at runtime using the $(load_wheels) function
"""
from pathlib import Path
import logging
import subprocess
import sys
__version__ = "${PACKAGE_VERSION}"

def load_wheels(log_level=logging.INFO):
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("[Glue Python Wheel Installer] %(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(log_level)
    logger.info("Starting wheel installation process")
    package_dir = Path(__file__).parent.absolute()
    wheels_dir = package_dir / "wheels"
    logger.debug(f"Package directory: {package_dir}")
    logger.debug(f"Looking for wheels in: {wheels_dir}")
    if not wheels_dir.exists():
        logger.error(f"Wheels directory not found: {wheels_dir}")
        return False
    wheel_files = list(wheels_dir.glob("*.whl"))
    if not wheel_files:
        logger.warning(f"No wheels found in: {wheels_dir}")
        return False
    logger.info(f"Found {len(wheel_files)} wheels")
    wheel_file_paths = [str(wheel_file) for wheel_file in wheel_files]
    logger.info(f"Installing {wheel_file_paths}...")
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", *wheel_file_paths], check=True, capture_output=True, text=True
        )
        logger.info(f"✓ Successfully installed wheel files")
        logger.debug(f"pip output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        error_msg = f"Failed to install wheel files"
        logger.error(f"✗ {error_msg}: {e}")
        if e.stderr:
            logger.error(f"Error details: {e.stderr}")
        return False
    logger.info("All wheels installed successfully")
    return True
EOF
cat >"$UBER_WHEEL_DIR/${PACKAGE_NAME}/auto.py" <<EOF
"""
${PACKAGE_NAME} - utility module that allows users to automatically install modules by adding $(import ${PACKAGE_NAME}.auto) to the top of their script
"""
from ${PACKAGE_NAME} import load_wheels
load_wheels()
EOF
# Copy all wheels to the uber wheel directory
mkdir -p "$UBER_WHEEL_DIR/${PACKAGE_NAME}/wheels"
cp "$WHEEL_OUTPUT_DIRECTORY"/*.whl "$UBER_WHEEL_DIR/${PACKAGE_NAME}/wheels/"
# Build the uber wheel
echo "Building uber wheel package..."
# Install build tools in the current environment
"$VENV_DIR/bin/pip" install build
if ! (cd "$UBER_WHEEL_DIR" && "$VENV_DIR/bin/python" -m build --skip-dependency-check --wheel --outdir .); then
    echo "Error: Failed to build uber wheel"
    exit 1
fi
# Ensure output directory exists
mkdir -p "$FINAL_WHEEL_OUTPUT_DIRECTORY"
# Copy the uber wheel to the output directory
FINAL_WHEEL_OUTPUT_PATH="$FINAL_WHEEL_OUTPUT_DIRECTORY/$UBER_WHEEL_NAME"
# Find the generated wheel (should be only one in the root directory)
GENERATED_WHEEL=$(find "$UBER_WHEEL_DIR" -maxdepth 1 -name "*.whl" -type f | head -1)
if [ -z "$GENERATED_WHEEL" ]; then
    echo "Error: No uber wheel was generated"
    exit 1
fi
cp "$GENERATED_WHEEL" "$FINAL_WHEEL_OUTPUT_PATH"
# Get final wheel size for user feedback
WHEEL_SIZE=$(du -h "$FINAL_WHEEL_OUTPUT_PATH" | cut -f1)
echo "✓ Uber wheel created successfully!"
echo ""
echo "========================================="
echo "BUILD COMPLETED SUCCESSFULLY!"
echo "========================================="
echo "Final wheel: $FINAL_WHEEL_OUTPUT_PATH"
echo "Wheel size: $WHEEL_SIZE"
echo "Dependencies included: $WHEEL_COUNT packages"
echo ""
echo "To install the bundle, run:"
echo "  pip install $FINAL_WHEEL_OUTPUT_PATH"
echo ""
echo "After installation, you can verify that the bundle works by running:"
echo "  python -c \"import ${PACKAGE_NAME}; ${PACKAGE_NAME}.load_wheels()\""
echo "  or "
echo "  python -c \"import ${PACKAGE_NAME}.auto\""
echo "========================================="
