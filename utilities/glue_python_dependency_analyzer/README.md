# Glue Python Dependency Analyzer

This command line utility analyzes Python package dependencies in AWS Glue jobs to identify version conflicts, unpinned dependencies, and other issues that could cause runtime problems in Glue environments. The tool creates isolated virtual environments matching target Glue versions to ensure accurate dependency resolution.

## Features

- Analyzes single or multiple Glue jobs in batch
- Supports both requirements.txt files and comma-separated module lists
- Handles wheel files from S3 locations
- Creates isolated virtual environments matching target Glue versions
- Provides detailed, human-readable analysis reports
- Identifies unpinned packages, version conflicts, and properly pinned dependencies

## Supported Glue Versions

- AWS Glue 4.0 (Python 3.10, glibc 2.26)
- AWS Glue 5.0 (Python 3.11, glibc 2.34)

## How to use it

You can run this utility in any location where you have Python and the following environment.

### Pre-requisite

- Python 3.8+ with venv module
- AWS credentials configured (CLI, environment variables, or IAM roles)
- Required AWS permissions: `glue:GetJob`, `s3:GetObject`
- Dependencies: `boto3`, `packaging`, `pkginfo`

### Command line Syntax

```
python cli.py -j JOB_NAME [OPTIONS]
```

**Required Arguments:**

- `-j, --job JOB_NAME` - Glue job name to analyze (can be specified multiple times)

**Optional Arguments:**

- `--aws-profile PROFILE` - AWS profile to use for authentication
- `--aws-region REGION` - AWS region (e.g., us-east-1, us-west-2)
- `-v, --verbose` - Enable verbose logging (use -vv for debug level)
- `-h, --help` - Show help message and exit

## Examples

### Example 1. Analyze a single Glue job

```
python cli.py -j my-glue-job
```

### Example 2. Analyze multiple jobs with specific AWS profile and region

```
python cli.py -j job1 -j job2 --aws-profile production --aws-region us-west-2
```

### Example 3. Analyze with verbose logging

```
python cli.py -j my-job --verbose
```

### Example 4. Debug mode with maximum logging detail

```
python cli.py -j my-job -vv
```

### Example of command output

```
🔍 AWS Glue Python Dependency Analyzer
==================================================

📊 Package Analysis Results  (Target Glue Job: my-job)
============================================================

⚠️  UNPINNED (3 packages)
----------------------------------------
  requests     │ version not pinned in requirements, would install 2.31.0
  urllib3      │ not specified in requirements, would install 2.0.4
  certifi      │ not specified in requirements, would install 2023.7.22

🔄 VERSION_OVERWRITTEN (1 packages)
----------------------------------------
  numpy        │ pinned to 1.24.0, but would install 1.24.3

✅ VERSION_PINNED (2 packages)
----------------------------------------
  pandas       │ pinned to 2.0.3
  boto3        │ pinned to 1.28.17

📋 Summary:
   Total packages analyzed: 6
   Packages with issues: 4
   Packages properly pinned: 2

💡 Recommendation: Consider pinning versions for unpinned packages
   to ensure consistent deployments across environments.
```

## Output Details

The tool provides a formatted report showing:

- **Unpinned packages (⚠️)**: Dependencies without version constraints that could cause inconsistent behavior
- **Version conflicts (🔄)**: Mismatches between specified and resolved versions
- **Properly pinned packages (✅)**: Dependencies with correct version constraints
- **Summary statistics**: Total packages analyzed, issues found, and recommendations

### Analysis Categories

1. **Unpinned Dependencies**: Packages specified without version constraints or transitive dependencies not mentioned in requirements
2. **Version Overwritten**: Packages where the specified version differs from what would actually be installed
3. **Version Pinned**: Packages correctly pinned to specific versions that match the resolved versions

## Limitations

- Only analyzes Python dependencies specified in Glue job configurations
- Requires network access to download S3-hosted wheel files and requirements.txt files
- Virtual environment creation requires sufficient disk space in `/tmp` directory
- Glue environment replication via venv is best effort. There may be some discrepancies between environments.
- Analysis is based on pip's dependency resolution and may not catch all runtime issues

## Troubleshooting

### AttributeError: module 'pkgutil' has no attribute 'ImpImporter'. Did you mean: 'zipimporter'?

If you encounter the following error:

```
AttributeError: module 'pkgutil' has no attribute 'ImpImporter'. Did you mean: 'zipimporter'?
```

In python 3.12+, `pkgutl.ImpImporter` has been deprecated. As a workaround, try using an older python version like 3.10.