#!/usr/bin/env python3
"""Check that every tested Rust workspace package has a CI task assignment."""

import json
import subprocess
from pathlib import Path


TASK_PACKAGES = {
    "test-tree-ffi": {"crowdb-tree-ffi"},
    "test-rpc-ffi": {"crowdb-rpc-ffi"},
    "test-common": {"crowdb-common"},
    "test-protocol": {"crowdb-protocol"},
    "test-kv-core": {"crowdb-kv"},
    "test-kv-client": {"crowdb-kv-client"},
    "test-chunkdb-client": {"crowdb-chunkdb-client"},
    "test-kv-server": {"crowdb-kv-server"},
    "test-diskdb": {"crowdb-diskdb"},
    "test-diskdb-client": {"crowdb-diskdb-client"},
    "test-chunkdb": {"crowdb-chunkdb"},
    "test-chunk-client": {"crowdb-chunk-client"},
    "test-diskio-client": {"crowdb-diskio-client"},
    "test-console-shared": {"crowdb-console-shared"},
    "test-console-cli": {"crowdb-cli"},
    "test-console-server": {"crowdb-web"},
}

SUPPORT_PACKAGES = {
    "crowdb-test-harness": "test support library; covered by dependent package tests",
    "crowdb-port-alloc": "E2E support binary; exercised by test-console-ui",
}


def workspace_packages() -> set[str]:
    result = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        check=True,
        capture_output=True,
        text=True,
    )
    metadata = json.loads(result.stdout)
    return {package["name"] for package in metadata["packages"]}


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    packages = workspace_packages()
    assignments: dict[str, str] = {}
    for task, task_packages in TASK_PACKAGES.items():
        for package in task_packages:
            previous = assignments.setdefault(package, task)
            if previous != task:
                raise SystemExit(f"package {package} is assigned to both {previous} and {task}")

    missing = sorted(packages - set(assignments) - set(SUPPORT_PACKAGES))
    unknown_support = sorted(set(SUPPORT_PACKAGES) - packages)
    if missing:
        print("Rust workspace packages missing from CI test-task coverage:")
        for package in missing:
            print(f"  {package}")
        print("Add the package to TASK_PACKAGES in tools/check-test-task-coverage.py")
        return 1
    if unknown_support:
        print("Support-package allowlist contains packages not in the workspace:")
        for package in unknown_support:
            print(f"  {package}")
        return 1

    pixi_text = (root / "pixi.toml").read_text(encoding="utf-8")
    pixi_tasks = {
        line.split(" =", 1)[0]: line
        for line in pixi_text.splitlines()
        if " =" in line and not line.startswith(" ")
    }
    missing_tasks = [task for task in TASK_PACKAGES if task not in pixi_tasks]
    if missing_tasks:
        print("Coverage map references missing Pixi tasks:")
        for task in missing_tasks:
            print(f"  {task}")
        return 1
    missing_commands = [
        (task, package)
        for task, task_packages in TASK_PACKAGES.items()
        for package in task_packages
        if f"-p {package}" not in pixi_tasks[task]
    ]
    if missing_commands:
        print("Coverage map packages are not targeted by their Pixi tasks:")
        for task, package in missing_commands:
            print(f"  {task}: {package}")
        return 1

    print(f"Test-task coverage verified for {len(packages)} workspace packages")
    for package in sorted(assignments):
        print(f"  {package}: {assignments[package]}")
    for package, reason in sorted(SUPPORT_PACKAGES.items()):
        print(f"  {package}: support ({reason})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
