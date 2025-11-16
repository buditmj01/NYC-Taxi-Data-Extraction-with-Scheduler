"""
Configuration loader for NYC Taxi Data Pipeline.
Loads environment variables from .env file.
"""
import os
from pathlib import Path


def load_env_file(env_path: Path = None) -> dict:
    """Load .env file and return as dictionary."""
    if env_path is None:
        env_path = Path(__file__).parent / ".env"

    env_vars = {}

    if not env_path.exists():
        return env_vars

    with open(env_path, "r") as f:
        for line in f:
            line = line.strip()
            # Skip comments and empty lines
            if not line or line.startswith("#"):
                continue

            # Parse KEY=VALUE
            if "=" in line:
                key, value = line.split("=", 1)
                env_vars[key.strip()] = value.strip()

    return env_vars


# Auto-load .env file when this module is imported
env_file = Path(__file__).parent / ".env"
if env_file.exists():
    env_vars = load_env_file(env_file)
    for key, value in env_vars.items():
        os.environ[key] = value
