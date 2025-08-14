"""
AWS Glue Python Dependency Analyzer - Core Library

Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
"""

import logging
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Dict, Tuple, Optional

import pkginfo
from packaging.requirements import Requirement
from packaging.version import parse as parse_version

logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Constants
MIN_PIP_VERSION = "23.3"
ADDITIONAL_MODULES_ARG = "--additional-python-modules"
INSTALLER_OPTION_ARG = "--python-modules-installer-option"
REQUIREMENTS_FLAGS = ["--requirements", "-r"]


class UnsupportedGlueVersionError(Exception):
    """Raised when an unsupported Glue version is encountered"""

    pass


class GlueVersion(Enum):
    V4_0 = "4.0"
    V5_0 = "5.0"

    @property
    def python_version(self) -> str:
        if self == GlueVersion.V4_0:
            return "3.10"
        elif self == GlueVersion.V5_0:
            return "3.11"
        else:
            raise UnsupportedGlueVersionError(f"Unsupported Glue version: {self.value}")

    @property
    def compatible_platforms(self) -> List[str]:
        if self == GlueVersion.V4_0:
            return ["manylinux2014_x86_64"]
        elif self == GlueVersion.V5_0:
            return ["manylinux_2_34_x86_64", "manylinux_2_28_x86_64", "manylinux2014_x86_64"]
        else:
            raise UnsupportedGlueVersionError(f"Unsupported Glue version: {self.value}")

    @property
    def platform_tags(self) -> List[str]:
        platform_tags = []

        for platform in self.compatible_platforms:
            platform_tags.extend(["--platform", platform])

        platform_tags.extend(["--python-version", self.python_version])
        platform_tags.append("--only-binary=:all:")

        return platform_tags


@dataclass
class GlueVirtualEnvironment:
    pip_path: str
    temp_dir: tempfile.TemporaryDirectory

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.temp_dir.__exit__(exc_type, exc_value, traceback)
        logger.info(f"Cleaned up temporary directory: {self.temp_dir.name}")

    @classmethod
    def create(cls, glue_version: GlueVersion) -> "GlueVirtualEnvironment":
        """Create a new virtual environment for the specified Glue version"""
        tmp_dir = tempfile.TemporaryDirectory()
        venv_path = f"{tmp_dir.name}/venv"
        pip_path = f"{venv_path}/bin/pip" if os.name != "nt" else f"{venv_path}/Scripts/pip.exe"

        try:
            # Create virtual environment
            logger.debug(f"Creating venv at {venv_path}")
            subprocess.run([sys.executable, "-m", "venv", venv_path], check=True, capture_output=True)

            # Install Glue requirements
            current_dir = os.path.dirname(os.path.abspath(__file__))
            requirements_file = (
                f"{current_dir}/requirements/glue_{glue_version.value.replace('.', '_')}_requirements.txt"
            )

            if not os.path.exists(requirements_file):
                raise FileNotFoundError(f"Requirements file not found: {requirements_file}")

            logger.debug(f"Installing Glue requirements from {requirements_file}")
            subprocess.run([pip_path, "install", "-r", requirements_file], text=True, check=True, capture_output=True)

            # Update pip for platform flag support
            logger.debug("Updating venv pip + setuptools")
            subprocess.run(
                [pip_path, "install", "--upgrade", f"pip>={MIN_PIP_VERSION}", "setuptools"],
                text=True,
                check=True,
                capture_output=True,
            )

            return cls(pip_path=pip_path, temp_dir=tmp_dir)

        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            if isinstance(e, subprocess.CalledProcessError):
                logger.error(f"Error during virtual environment creation when running the following command: {e.cmd}")
                logger.error(f"Stdout:\n{e.stdout}")
                logger.error(f"Stderr:\n{e.stderr}")

            tmp_dir.cleanup()
            raise e


@dataclass
class GlueJobInformation:
    additional_python_modules: Optional[str]
    installer_option: Optional[str]
    glue_version: GlueVersion


@dataclass
class Dependency:
    """Base class for Python package dependencies"""

    name: str
    source: str

    def __post_init__(self):
        """Normalize the package name after initialization"""
        self.name = re.sub(r"[-_.]+", "-", self.name.lower())

    @property
    def normalized_name(self) -> str:
        return self.name

    @property
    def version(self) -> Optional[str]:
        """to be overridden by subclasses"""
        raise NotImplementedError("Subclasses must implement this property")

    def get_pip_argument(self) -> str:
        """Get the argument to pass to pip for this dependency"""
        return str(self)

    def compare_version(self, other_version: str) -> bool:
        if not self.version:
            return False
        return parse_version(self.version) == parse_version(other_version)

    def __str__(self) -> str:
        """String representation of the dependency"""
        if self.version:
            return f"{self.name}=={self.version}"
        return self.name


@dataclass
class UnconstrainedDependency(Dependency):
    """Dependency with no version constraints"""

    @property
    def version(self) -> Optional[str]:
        return None

    @classmethod
    def from_name(cls, name: str, source: str = "requirements") -> "UnconstrainedDependency":
        return cls(name=name, source=source)


@dataclass
class ConstrainedDependency(Dependency):
    """Dependency with version constraints (but not necessarily pinned)"""

    constraint: str = ""

    @property
    def version(self) -> Optional[str]:
        return None

    def get_pip_argument(self) -> str:
        return f"{self.name}{self.constraint}"

    @classmethod
    def from_constraint(cls, name: str, constraint: str, source: str = "requirements") -> "ConstrainedDependency":
        return cls(name=name, source=source, constraint=constraint)

    def __str__(self) -> str:
        return f"{self.name}{self.constraint}"


@dataclass
class PinnedDependency(Dependency):
    """Dependency pinned to a specific version"""

    _version: str = ""

    @property
    def version(self) -> str:
        return self._version

    @classmethod
    def from_pip_output(cls, name: str, version: str) -> "PinnedDependency":
        return cls(name=name, source="pip", _version=version)

    @classmethod
    def from_pinned_requirement(cls, name: str, version: str, source: str = "requirements") -> "PinnedDependency":
        return cls(name=name, source=source, _version=version)


@dataclass
class WheelDependency(PinnedDependency):
    """Dependency from a wheel file (implicitly pinned)"""

    local_path: str = ""
    s3_path: str = ""

    def get_pip_argument(self) -> str:
        return self.local_path if self.local_path else self.s3_path

    @classmethod
    def from_wheel_file(cls, name: str, version: str, s3_path: str, local_path: str = "") -> "WheelDependency":
        return cls(name=name, source=s3_path, _version=version, local_path=local_path, s3_path=s3_path)


class AnalysisStatus(Enum):
    UNPINNED = "unpinned"
    VERSION_OVERWRITTEN = "version overwritten"
    VERSION_PINNED = "version pinned"


@dataclass
class AnalysisResult:
    dependency: Dependency
    status: AnalysisStatus
    expected_version: Optional[str] = None
    actual_version: Optional[str] = None
    source_name: Optional[str] = None
    is_transitive: bool = False  # False if package was mentioned in requirements

    def to_human_readable_str(self) -> str:
        """Format analysis result details for human-readable display"""
        if self.status == AnalysisStatus.UNPINNED:
            if self.is_transitive:
                # Transitive dependency not mentioned in requirements
                return f"transitive dependency not specified in {self.source_name or 'requirements'}, would install {self.actual_version}"
            else:
                # Package was specified but without version constraint
                return (
                    f"version not pinned in {self.source_name or 'requirements'}, would install {self.actual_version}"
                )

        elif self.status == AnalysisStatus.VERSION_OVERWRITTEN:
            if isinstance(self.dependency, WheelDependency):
                return f"whl specifies {self.expected_version}, but would install {self.actual_version}"
            else:
                return f"pinned to {self.expected_version}, but would install {self.actual_version}"

        elif self.status == AnalysisStatus.VERSION_PINNED:
            if isinstance(self.dependency, WheelDependency):
                return f"pinned to {self.expected_version} via whl file"
            else:
                return f"pinned to {self.actual_version}"

        return f"unknown status: {self.status}"


def create_dependency_from_requirement(requirement_str: str, source: str = "requirements") -> Dependency:
    """Create appropriate dependency type from a requirement string"""
    try:
        req = Requirement(requirement_str.strip())

        if not req.specifier:
            return UnconstrainedDependency.from_name(req.name, source)
        elif len(req.specifier) == 1 and list(req.specifier)[0].operator == "==":
            spec = list(req.specifier)[0]
            return PinnedDependency.from_pinned_requirement(req.name, spec.version, source)
        else:
            return ConstrainedDependency.from_constraint(req.name, str(req.specifier), source)
    except Exception:
        # Fallback for malformed requirements
        if "==" in requirement_str:
            name, version = requirement_str.split("==", 1)
            return PinnedDependency.from_pinned_requirement(name.strip(), version.strip(), source)
        return UnconstrainedDependency.from_name(requirement_str.strip(), source)


class GluePythonDependencyAnalyzer:
    def __init__(self, glue_client: Any, s3_client: Any):
        self.venvs: Dict[GlueVersion, GlueVirtualEnvironment] = {}
        self._glue_client = glue_client
        self._s3_client = s3_client

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Cleanup all venvs
        logger.debug("Cleaning up temporary directories")
        for glue_version, venv in self.venvs.items():
            venv.__exit__(exc_type, exc_value, traceback)

    def _get_glue_venv(self, glue_version: GlueVersion) -> GlueVirtualEnvironment:
        """Get or create a virtual environment for the specified Glue version"""
        if glue_version in self.venvs:
            logger.debug(f"Using cached venv for Glue {glue_version.value}")
            return self.venvs[glue_version]

        logger.info(f"Creating venv for Glue {glue_version.value}")
        venv = GlueVirtualEnvironment.create(glue_version)
        self.venvs[glue_version] = venv
        return venv

    def get_glue_job_info(self, job_name: str) -> GlueJobInformation:
        """Retrieve Glue job configuration information"""
        response = self._glue_client.get_job(JobName=job_name)
        job = response["Job"]

        glue_version_str = job.get("GlueVersion")
        if not glue_version_str:
            raise RuntimeError(f"No Glue version found for job '{job_name}'")

        try:
            glue_version = GlueVersion(glue_version_str)
        except ValueError:
            raise UnsupportedGlueVersionError(f"Unsupported Glue version: {glue_version_str}")

        default_args = job.get("DefaultArguments", {})
        additional_python_modules = default_args.get(ADDITIONAL_MODULES_ARG)
        installer_option = default_args.get(INSTALLER_OPTION_ARG)

        return GlueJobInformation(
            additional_python_modules=additional_python_modules,
            installer_option=installer_option,
            glue_version=glue_version,
        )

    @staticmethod
    def _parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
        """Parse S3 URI into bucket and key components"""
        s3_path = s3_uri.replace("s3://", "")
        if "/" not in s3_path:
            raise ValueError(f"Invalid S3 URI format: {s3_uri}. Expected format: s3://bucket/key")

        parts = s3_path.split("/", 1)
        return parts[0], parts[1]

    def _download_s3_file(self, s3_uri: str, local_dir: str) -> str:
        """Download a file from S3 to local directory"""
        bucket_name, key = self._parse_s3_uri(s3_uri)
        filename = key.split("/")[-1]
        local_path = f"{local_dir}/{filename}"

        logger.info(f"Downloading {s3_uri} to {local_path}")

        try:
            self._s3_client.download_file(bucket_name, key, local_path)
            return local_path
        except Exception as e:
            logger.error(f"Failed to download {s3_uri}: {e}")
            raise RuntimeError(f"S3 download failed for {s3_uri}: {e}")

    @staticmethod
    def _parse_requirements_txt(filepath: str) -> List[Dependency]:
        """Parse requirements.txt file and return list of Dependency objects"""
        requirements = []
        with open(filepath, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                requirements.append(create_dependency_from_requirement(line, source=filepath))
        return requirements

    @staticmethod
    def _get_package_info_from_whl(whl_file_path: str) -> Tuple[str, str]:
        """Extract package name and version from wheel file"""
        try:
            wheel_info = pkginfo.Wheel(whl_file_path)
            if wheel_info.name and wheel_info.version:
                return wheel_info.name, wheel_info.version
        except Exception as e:
            logger.warning(f"pkginfo failed to parse wheel file {whl_file_path}: {e}")
        raise ValueError(f"Could not extract package info from {whl_file_path}")

    @staticmethod
    def _parse_pip_dry_run_stdout(stdout: str) -> List[Dependency]:
        """Parse pip dry-run output to extract package dependencies"""
        installed: List[Dependency] = []
        match = re.search(r"Would install (.+)", stdout)
        if match:
            packages_line = match.group(1)
            for entry in packages_line.strip().split():
                if "-" in entry:
                    parts = entry.rsplit("-", 1)
                    if len(parts) == 2:
                        name, version_str = parts
                        try:
                            parsed_version = parse_version(version_str)
                            installed.append(PinnedDependency.from_pip_output(name, str(parsed_version)))
                        except Exception as e:
                            logger.warning(f"Failed to parse version '{version_str}' for package '{name}': {e}")
                            installed.append(PinnedDependency.from_pip_output(name, version_str))
        return installed

    @staticmethod
    def _compare_installed_to_requirements(
        installed_packages: List[Dependency],
        requirements_dependencies: List[Dependency],
        source_name: str,
    ) -> List[AnalysisResult]:
        """Compare installed packages against requirements to identify issues"""
        required_modules = {req.normalized_name: req for req in requirements_dependencies}
        results = []

        for installed_pkg in installed_packages:
            requirement_dep = required_modules.get(installed_pkg.normalized_name)

            if not requirement_dep:
                # Package not in requirements (transitive dependency)
                results.append(
                    AnalysisResult(
                        dependency=installed_pkg,
                        status=AnalysisStatus.UNPINNED,
                        expected_version=None,
                        actual_version=installed_pkg.version,
                        source_name=source_name,
                        is_transitive=True,
                    )
                )
            elif isinstance(requirement_dep, WheelDependency):
                # Wheel file dependency - preserve the WheelDependency type for proper display
                if requirement_dep.version and not installed_pkg.compare_version(requirement_dep.version):
                    results.append(
                        AnalysisResult(
                            dependency=requirement_dep,
                            status=AnalysisStatus.VERSION_OVERWRITTEN,
                            expected_version=requirement_dep.version,
                            actual_version=installed_pkg.version,
                            source_name=source_name,
                            is_transitive=False,
                        )
                    )
                else:
                    results.append(
                        AnalysisResult(
                            dependency=requirement_dep,
                            status=AnalysisStatus.VERSION_PINNED,
                            expected_version=requirement_dep.version,
                            actual_version=installed_pkg.version,
                            source_name=source_name,
                            is_transitive=False,
                        )
                    )
            elif not requirement_dep.version:
                # Unpinned requirement (specified but no version)
                results.append(
                    AnalysisResult(
                        dependency=installed_pkg,
                        status=AnalysisStatus.UNPINNED,
                        expected_version=None,
                        actual_version=installed_pkg.version,
                        source_name=source_name,
                        is_transitive=False,
                    )
                )
            elif not installed_pkg.compare_version(requirement_dep.version):
                # Version mismatch
                results.append(
                    AnalysisResult(
                        dependency=installed_pkg,
                        status=AnalysisStatus.VERSION_OVERWRITTEN,
                        expected_version=requirement_dep.version,
                        actual_version=installed_pkg.version,
                        source_name=source_name,
                        is_transitive=False,
                    )
                )
            else:
                # Correctly pinned
                results.append(
                    AnalysisResult(
                        dependency=installed_pkg,
                        status=AnalysisStatus.VERSION_PINNED,
                        expected_version=requirement_dep.version,
                        actual_version=installed_pkg.version,
                        source_name=source_name,
                        is_transitive=False,
                    )
                )

        return results

    def _run_pip_dry_run(
        self,
        dependencies: List[Dependency],
        installer_options: Optional[List[str]] = None,
        source_name: str = ADDITIONAL_MODULES_ARG,
        pip_command: Optional[List[str]] = None,
        glue_version: Optional[GlueVersion] = None,
    ) -> List[AnalysisResult]:
        """Run pip install dry-run and analyze results"""
        logger.info("Running pip install dry-run to check package dependencies")

        # Build pip command
        pip_cmd = pip_command or [sys.executable, "-m", "pip"]
        platform_tags = glue_version.platform_tags if glue_version else []
        pip_args = [dep.get_pip_argument() for dep in dependencies]

        full_cmd = [*pip_cmd, "install", *pip_args, "--dry-run", *platform_tags, *(installer_options or [])]

        try:
            result = subprocess.run(full_cmd, capture_output=True, text=True, check=True)
            installed = self._parse_pip_dry_run_stdout(result.stdout)
            logger.info(f"Found {len(installed)} packages that would be installed")

            analysis_results = self._compare_installed_to_requirements(installed, dependencies, source_name)
            logger.info(f"Analysis complete. Found {len(analysis_results)} flagged packages")
            return analysis_results

        except subprocess.CalledProcessError as e:
            logger.error(f"Pip dry-run failed with return code {e.returncode}")
            logger.error(f"Error output: {e.stderr}")
            raise RuntimeError("Pip dry-run failed")

    def _parse_all_dependencies(
        self, glue_job_info: GlueJobInformation, s3_download_dir: str
    ) -> Tuple[List[Dependency], str, List[str]]:
        """Parse all dependencies from Glue job (either requirements file OR comma-separated modules)"""
        installer_options = glue_job_info.installer_option.split() if glue_job_info.installer_option else []

        if not glue_job_info.additional_python_modules:
            return [], ADDITIONAL_MODULES_ARG, installer_options

        additional_python_modules = glue_job_info.additional_python_modules.split(",")

        logger.info(f"Additional Python modules: {glue_job_info.additional_python_modules}")
        logger.info(f"Installer options: {installer_options}")

        all_dependencies = []
        source_name = ADDITIONAL_MODULES_ARG

        # Check if using requirements file
        requirements_flag = None
        for flag in REQUIREMENTS_FLAGS:
            if flag in installer_options:
                requirements_flag = flag
                installer_options.remove(flag)
                break

        if requirements_flag:
            # Find the requirements file S3 path
            requirements_file_s3_path = next(
                (entry for entry in additional_python_modules if entry.startswith("s3://")), None
            )

            if not requirements_file_s3_path:
                raise RuntimeError("Requirements flag set but could not find S3 path for requirements file")

            # Download and parse requirements file
            requirements_file = self._download_s3_file(requirements_file_s3_path, s3_download_dir)
            logger.info(f"Reading requirements from {requirements_file_s3_path}")
            all_dependencies = self._parse_requirements_txt(requirements_file)
            source_name = f"requirements file ({requirements_file_s3_path})"

        else:
            # Process comma-separated modules
            for pkg in additional_python_modules:
                normalized_pkg = pkg.strip()

                if normalized_pkg.startswith("s3://"):
                    # Handle wheel files
                    try:
                        dependency_path = self._download_s3_file(normalized_pkg, s3_download_dir)
                        whl_name, whl_version = self._get_package_info_from_whl(dependency_path)
                        wheel_dep = WheelDependency.from_wheel_file(
                            whl_name, whl_version, normalized_pkg, dependency_path
                        )
                        all_dependencies.append(wheel_dep)
                    except Exception as e:
                        filename = normalized_pkg.split("/")[-1]
                        logger.warning(f"Failed to process wheel file {filename}: {e}")
                        # Continue processing other dependencies rather than failing completely

                elif not normalized_pkg.startswith("/") and not normalized_pkg.endswith(".whl"):
                    # Handle regular package requirements
                    dependency = create_dependency_from_requirement(normalized_pkg, source=ADDITIONAL_MODULES_ARG)
                    all_dependencies.append(dependency)
                else:
                    logger.warning(f"Skipping invalid dependency: {normalized_pkg}")

        return all_dependencies, source_name, installer_options

    def analyze_job(self, job_name: str) -> List[AnalysisResult]:
        """Analyze dependencies for a specific Glue job"""
        logger.info(f"Analyzing dependencies for specified job: {job_name}")

        # Step 1: Get job configuration
        glue_job_info = self.get_glue_job_info(job_name)

        if not glue_job_info.additional_python_modules:
            logger.warning(f"No additional Python modules found in job '{job_name}'")
            return []

        venv = self._get_glue_venv(glue_job_info.glue_version)

        # Step 2: Parse all dependencies (either requirements file OR comma-separated modules)
        all_dependencies, source_name, installer_options = self._parse_all_dependencies(
            glue_job_info, s3_download_dir=venv.temp_dir.name
        )

        # Step 3: Run pip analysis
        return self._run_pip_dry_run(
            all_dependencies,
            installer_options=installer_options,
            source_name=source_name,
            pip_command=[venv.pip_path],
            glue_version=glue_job_info.glue_version,
        )
