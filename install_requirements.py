#!/usr/bin/env python3
"""
Installation script for NYC Taxi Data Pipeline
Automatically installs all required Python packages.

Usage:
    python install_requirements.py

    or make it executable and run:
    chmod +x install_requirements.py
    ./install_requirements.py
"""

import subprocess
import sys
from typing import List, Tuple

# Color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


# List of required packages with versions (optional)
REQUIRED_PACKAGES = [
    # Core data processing with PyArrow and Pandas
    "pyarrow",

    # Data analysis and manipulation
    "pandas",

    # PostgreSQL support
    "psycopg2-binary",  # PostgreSQL adapter for Python
    "sqlalchemy",      # SQL toolkit and ORM

    # HTTP requests (for Discord webhook and downloads)
    "requests",

    # Optional but recommended packages
    "python-dotenv",  # For .env file support (optional)
]

# Optional packages that enhance functionality but aren't strictly required
OPTIONAL_PACKAGES = [
    "tabulate",       # Pretty print tables
]


def print_header(message: str):
    """Print a formatted header."""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 70}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{message:^70}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 70}{Colors.ENDC}\n")


def print_success(message: str):
    """Print a success message."""
    print(f"{Colors.OKGREEN}✓ {message}{Colors.ENDC}")


def print_error(message: str):
    """Print an error message."""
    print(f"{Colors.FAIL}✗ {message}{Colors.ENDC}")


def print_info(message: str):
    """Print an info message."""
    print(f"{Colors.OKCYAN}ℹ {message}{Colors.ENDC}")


def print_warning(message: str):
    """Print a warning message."""
    print(f"{Colors.WARNING}⚠ {message}{Colors.ENDC}")


def check_pip():
    """Check if pip is available."""
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "--version"],
            check=True,
            capture_output=True,
            text=True
        )
        print_success("pip is available")
        return True
    except subprocess.CalledProcessError:
        print_error("pip is not available")
        return False


def upgrade_pip():
    """Upgrade pip to the latest version."""
    print_info("Upgrading pip to latest version...")
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "--upgrade", "pip"],
            check=True,
            capture_output=True,
            text=True
        )
        print_success("pip upgraded successfully")
        return True
    except subprocess.CalledProcessError as e:
        print_warning(f"Could not upgrade pip: {e}")
        return False


def install_package(package: str) -> Tuple[bool, str]:
    """
    Install a single package using pip.

    Args:
        package: Package name to install

    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        print(f"Installing {Colors.BOLD}{package}{Colors.ENDC}...", end=" ")
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", package],
            check=True,
            capture_output=True,
            text=True
        )
        print_success(f"{package} installed")
        return True, f"{package} installed successfully"
    except subprocess.CalledProcessError as e:
        print_error(f"Failed to install {package}")
        error_msg = e.stderr if e.stderr else str(e)
        return False, f"Failed to install {package}: {error_msg}"


def check_package_installed(package: str) -> bool:
    """Check if a package is already installed."""
    try:
        # Extract package name without version specifier
        package_name = package.split('==')[0].split('>=')[0].split('<=')[0]
        subprocess.run(
            [sys.executable, "-m", "pip", "show", package_name],
            check=True,
            capture_output=True,
            text=True
        )
        return True
    except subprocess.CalledProcessError:
        return False


def install_all_packages(packages: List[str], skip_installed: bool = True) -> Tuple[int, int]:
    """
    Install all packages from the list.

    Args:
        packages: List of package names to install
        skip_installed: Skip packages that are already installed

    Returns:
        Tuple of (successful_count, failed_count)
    """
    successful = 0
    failed = 0
    failed_packages = []

    for package in packages:
        # Check if already installed
        if skip_installed and check_package_installed(package):
            print_info(f"{package} is already installed (skipping)")
            successful += 1
            continue

        # Install package
        success, message = install_package(package)
        if success:
            successful += 1
        else:
            failed += 1
            failed_packages.append((package, message))

    return successful, failed, failed_packages


def generate_requirements_txt():
    """Generate requirements.txt file for future use."""
    print_info("Generating requirements.txt file...")
    try:
        with open("requirements.txt", "w") as f:
            f.write("# NYC Taxi Data Pipeline - Python Dependencies\n")
            f.write("# Auto-generated by install_requirements.py\n\n")
            f.write("# Core dependencies\n")
            for package in REQUIRED_PACKAGES:
                f.write(f"{package}\n")
            if OPTIONAL_PACKAGES:
                f.write("\n# Optional dependencies\n")
                for package in OPTIONAL_PACKAGES:
                    f.write(f"# {package}\n")
        print_success("requirements.txt generated successfully")
    except Exception as e:
        print_warning(f"Could not generate requirements.txt: {e}")


def main():
    """Main installation function."""
    print_header("NYC Taxi Data Pipeline - Dependency Installer")

    print(f"{Colors.BOLD}Python version:{Colors.ENDC} {sys.version}")
    print(f"{Colors.BOLD}Python executable:{Colors.ENDC} {sys.executable}\n")

    # Step 1: Check pip
    print_header("Step 1: Checking pip")
    if not check_pip():
        print_error("pip is required but not available. Please install pip first.")
        sys.exit(1)

    # Step 2: Upgrade pip
    print_header("Step 2: Upgrading pip")
    upgrade_pip()

    # Step 3: Install required packages
    print_header("Step 3: Installing Required Packages")
    print(f"Installing {len(REQUIRED_PACKAGES)} required packages...\n")

    successful, failed, failed_packages = install_all_packages(REQUIRED_PACKAGES)

    # Step 4: Install optional packages
    print_header("Step 4: Installing Optional Packages")
    print(f"Installing {len(OPTIONAL_PACKAGES)} optional packages...\n")

    opt_successful, opt_failed, opt_failed_packages = install_all_packages(OPTIONAL_PACKAGES)

    # Step 5: Generate requirements.txt
    print_header("Step 5: Generating requirements.txt")
    generate_requirements_txt()

    # Summary
    print_header("Installation Summary")

    total_required = len(REQUIRED_PACKAGES)
    total_optional = len(OPTIONAL_PACKAGES)

    print(f"{Colors.BOLD}Required Packages:{Colors.ENDC}")
    print(f"  {Colors.OKGREEN}✓ Successful: {successful}/{total_required}{Colors.ENDC}")
    if failed > 0:
        print(f"  {Colors.FAIL}✗ Failed: {failed}/{total_required}{Colors.ENDC}")

    print(f"\n{Colors.BOLD}Optional Packages:{Colors.ENDC}")
    print(f"  {Colors.OKGREEN}✓ Successful: {opt_successful}/{total_optional}{Colors.ENDC}")
    if opt_failed > 0:
        print(f"  {Colors.WARNING}⚠ Failed: {opt_failed}/{total_optional} (non-critical){Colors.ENDC}")

    # Show failed packages if any
    if failed_packages:
        print(f"\n{Colors.FAIL}{Colors.BOLD}Failed Required Packages:{Colors.ENDC}")
        for pkg, msg in failed_packages:
            print(f"  • {pkg}")
            print(f"    Reason: {msg}\n")

    if opt_failed_packages:
        print(f"\n{Colors.WARNING}{Colors.BOLD}Failed Optional Packages:{Colors.ENDC}")
        for pkg, msg in opt_failed_packages:
            print(f"  • {pkg}")
            print(f"    Reason: {msg}\n")

    # Final status
    print_header("Installation Complete")

    if failed == 0:
        print_success("All required packages installed successfully!")
        print_info("You can now run the NYC Taxi Data Pipeline scripts.")
        print(f"\n{Colors.BOLD}Next steps:{Colors.ENDC}")
        print("  1. Configure your .env file with database credentials")
        print("  2. Run data extraction: python data_extraction.py --mode daily --date 2025-09-01")
        print("  3. Run data pipeline: python data_pipeline.py --table week_1_september_2025")
        print("  4. Send reports: python send_weekly_report.py --week week_1_september_2025")
        sys.exit(0)
    else:
        print_error(f"{failed} required package(s) failed to install.")
        print_info("Please check the error messages above and install them manually:")
        for pkg, _ in failed_packages:
            print(f"  pip install {pkg}")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n{Colors.WARNING}Installation cancelled by user{Colors.ENDC}")
        sys.exit(1)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        sys.exit(1)
