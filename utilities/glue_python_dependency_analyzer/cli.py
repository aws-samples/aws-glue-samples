#!/usr/bin/env python3
"""
AWS Glue Python Dependency Analyzer - Command Line Interface

This module provides a command-line interface for analyzing Python package dependencies
in AWS Glue jobs. It identifies version conflicts, unpinned dependencies, and other
issues that could cause runtime problems in Glue environments.

Features:
    - Analyzes single or multiple Glue jobs in batch
    - Supports both requirements.txt files and comma-separated module lists
    - Handles wheel files from S3 locations
    - Creates isolated virtual environments matching target Glue versions
    - Provides detailed, human-readable analysis reports
    - Supports custom AWS profiles and regions

Supported Glue Versions:
    - AWS Glue 4.0 (Python 3.10, glibc 2.26)
    - AWS Glue 5.0 (Python 3.11, glibc 2.34)

Prerequisites:
    - Python 3.8+ with venv module
    - AWS credentials configured (CLI, environment variables, or IAM roles)
    - Required permissions: glue:GetJob, s3:GetObject
    - Dependencies: boto3, packaging, pkginfo

Usage:
    python cli.py -j JOB_NAME [OPTIONS]

Required Arguments:
    -j, --job JOB_NAME          Glue job name to analyze (can be specified multiple times)

Optional Arguments:
    --aws-profile PROFILE       AWS profile to use for authentication
    --aws-region REGION         AWS region (e.g., us-east-1, us-west-2)
    -v, --verbose               Enable verbose logging (use -vv for debug level)
    -h, --help                  Show help message and exit

Examples:
    # Analyze a single Glue job
    python cli.py -j my-glue-job

    # Analyze multiple jobs with specific AWS profile and region
    python cli.py -j job1 -j job2 --aws-profile production --aws-region us-west-2

    # Analyze with verbose logging to see detailed progress
    python cli.py -j my-job --verbose

    # Debug mode with maximum logging detail
    python cli.py -j my-job -vv

Output:
    The tool provides a formatted report showing:
    - Unpinned packages (⚠️): Dependencies without version constraints
    - Version conflicts (🔄): Mismatches between specified and resolved versions
    - Properly pinned packages (✅): Dependencies with correct version constraints
    - Summary statistics and recommendations

Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
"""

import argparse
import logging
import sys
from typing import List, Dict, Optional, Tuple

import boto3

from dependency_analyzer import (
    AnalysisResult,
    AnalysisStatus,
    GluePythonDependencyAnalyzer,
    logger,
)


def print_formatted_results(results: List[AnalysisResult], source: str):
    """Print analysis results in a formatted way"""
    if not results:
        print(f"\n✅ No package issues found ({source})")
        return

    status_groups: Dict[AnalysisStatus, List[AnalysisResult]] = {}
    for result in results:
        status = result.status
        if status not in status_groups:
            status_groups[status] = []
        status_groups[status].append(result)

    print(f"\n📊 Package Analysis Results  ({source})")
    print("=" * 60)

    status_icons = {
        AnalysisStatus.UNPINNED: "⚠️ ",
        AnalysisStatus.VERSION_OVERWRITTEN: "🔄",
        AnalysisStatus.VERSION_PINNED: "✅",
    }

    for status, items in status_groups.items():
        icon = status_icons.get(status, "📦")
        print(f"\n{icon} {status.value.upper()} ({len(items)} packages)")
        print("-" * 40)

        # Find max package name length for alignment
        max_name_len = max(len(item.dependency.name) for item in items)

        for item in items:
            package = item.dependency.name.ljust(max_name_len)
            details = item.to_human_readable_str()
            print(f"  {package} │ {details}")

    total_issues = sum(len(items) for status, items in status_groups.items() if status != AnalysisStatus.VERSION_PINNED)

    print("\n📋 Summary:")
    print(f"   Total packages analyzed: {len(results)}")
    print(f"   Packages with issues: {total_issues}")
    print(f"   Packages properly pinned: {len(status_groups.get(AnalysisStatus.VERSION_PINNED, []))}")

    if total_issues > 0:
        print("\n💡 Recommendation: Consider pinning versions for unpinned packages")
        print("   to ensure consistent deployments across environments.")


def analyze_specified_job(job_name: str, analyzer: GluePythonDependencyAnalyzer) -> List[AnalysisResult]:
    """Analyze a specific Glue job using the analyzer"""
    try:
        results = analyzer.analyze_job(job_name)
        if results:
            print_formatted_results(results, f"Target Glue Job: {job_name}")
        else:
            print(f"No additional Python modules configured in job '{job_name}' - nothing to analyze")
        return results
    except Exception as e:
        logger.exception(f"Error analyzing job '{job_name}': {e}")
        raise


def initialize_aws_clients(profile_name: Optional[str] = None, region_name: Optional[str] = None) -> Tuple:
    """Initialize AWS clients with optional profile and region"""
    session_kwargs = {}
    if profile_name:
        session_kwargs["profile_name"] = profile_name
    if region_name:
        session_kwargs["region_name"] = region_name

    session = boto3.Session(**session_kwargs)
    glue_client = session.client("glue")
    s3_client = session.client("s3")

    logger.info(
        f"Initialized AWS clients with profile: {profile_name or 'default'}, region: {region_name or 'default'}"
    )
    return glue_client, s3_client


def analyze_jobs(job_names: List[str], glue_client, s3_client) -> List[str]:
    """Analyze multiple Glue jobs and return list of failed job names"""
    analysis_failure_job_names = []

    # Create analyzer with virtual environments for dependency analysis
    # This ensures user's dependencies don't conflict with the analysis
    with GluePythonDependencyAnalyzer(glue_client, s3_client) as analyzer:
        for job_name in job_names:
            logger.info(f"Target job specified: {job_name}")
            try:
                analyze_specified_job(job_name, analyzer)
            except Exception as e:
                logger.exception(f"Error analyzing job '{job_name}': {e}")
                analysis_failure_job_names.append(job_name)

    return analysis_failure_job_names


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="AWS Glue Python Dependency Analyzer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cli.py -j my-job
  python cli.py -j job1 -j job2 --aws-profile myprofile
  python cli.py -j my-job --aws-region us-west-2
        """,
    )

    parser.add_argument(
        "-j",
        "--job",
        action="append",
        dest="glue_jobs",
        required=True,
        help="Glue job name to analyze (can be specified multiple times)",
    )

    parser.add_argument("--aws-profile", help="AWS profile to use for authentication")

    parser.add_argument("--aws-region", help="AWS region to use")

    parser.add_argument("--verbose", "-v", action="count", default=0, help="Enable verbose logging")

    return parser.parse_args()


def main():
    """Main CLI entry point"""
    args = parse_args()

    if args.verbose == 1:
        logger.setLevel(logging.INFO)
    elif args.verbose == 2:
        logger.setLevel(logging.DEBUG)

    print("🔍 AWS Glue Python Dependency Analyzer")
    print("=" * 50)

    try:
        # Initialize AWS clients
        glue_client, s3_client = initialize_aws_clients(args.aws_profile, args.aws_region)

        analysis_failure_job_names = analyze_jobs(args.glue_jobs, glue_client, s3_client)

        if analysis_failure_job_names:
            print(
                f"❌ Failed to analyze dependencies for the following jobs: {analysis_failure_job_names}. "
                f"Please check the logs above for error details."
            )
            sys.exit(1)

    except Exception as e:
        logger.error(f"Failed to run analysis: {e}")
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
