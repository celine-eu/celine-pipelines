#!/usr/bin/env python3
"""
Simple version management for monorepo apps.
Usage:
  python scripts/bump_version.py owm patch
  python scripts/bump_version.py copernicus minor --commit
"""
import argparse
import subprocess
from pathlib import Path
import semver


def get_version_file(app: str) -> Path:
    path = Path(__file__).resolve().parents[1] / "apps" / app / "version.txt"

    if app == "base":
        path = Path(__file__).resolve().parents[1] / "version-base.txt"

    if not path.exists():
        path.write_text("0.0.0")

    return path


def read_version(app: str) -> str:
    version_file = get_version_file(app)
    return version_file.read_text(encoding="utf-8").strip()


def write_version(app: str, version: str) -> None:
    version_file = get_version_file(app)
    version_file.write_text(f"{version}\n", encoding="utf-8")


def bump_version(version: str, level: str) -> str:
    """Bump semantic version."""
    if level == "major":
        return str(semver.VersionInfo.parse(version).bump_major())
    elif level == "minor":
        return str(semver.VersionInfo.parse(version).bump_minor())
    elif level == "patch":
        return str(semver.VersionInfo.parse(version).bump_patch())
    else:
        raise ValueError(f"Invalid bump level: {level}")


def commit_version(app: str, version: str):
    version_file = get_version_file(app)
    subprocess.run(["git", "add", version_file], check=True)
    subprocess.run(
        ["git", "commit", "-m", f"chore({app}): bump to {version}"], check=True
    )


def main():
    parser = argparse.ArgumentParser(description="Manage per-app semantic versions.")
    parser.add_argument("app", help="App name (e.g., owm, copernicus, osm)")
    parser.add_argument(
        "bump",
        choices=["major", "minor", "patch", "show"],
        help="Bump type or 'show' to print current version",
    )
    parser.add_argument("--commit", action="store_true", help="Commit the version bump")
    args = parser.parse_args()

    current = read_version(args.app)
    if args.bump == "show":
        print(current)
        return

    new_version = bump_version(current, args.bump)
    write_version(args.app, new_version)

    print(f"{args.app}: {current} → {new_version}")

    if args.commit:
        commit_version(args.app, new_version)


if __name__ == "__main__":
    main()
