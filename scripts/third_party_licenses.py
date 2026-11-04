"""Write the THIRD_PARTY_LICENSES.md of each wheel: the Rust crates compiled into its extension
module, with their licenses and copyright lines, and every distinct license text once.

    uv run python scripts/third_party_licenses.py            # regenerate every wheel's file
    uv run python scripts/third_party_licenses.py --check    # exit 1 if a file is out of date

Run it after adding or updating a Rust dependency, and commit the result. The crates are read
from `cargo metadata`: everything the wheel's root crate reaches through normal dependencies, on
every target platform (so `windows-sys` and friends are listed even on macOS). Build scripts,
build dependencies, dev dependencies and procedural macros (with their own dependencies) run on
the build machine only, so they are left out. Workspace crates are porto's own code, under
porto's license, and are left out too. The license files (LICENSE*, LICENCE*, COPYING*, NOTICE*,
UNLICENSE*) come from each crate's source in the cargo registry.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict, cast

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
LICENSE_FILE_PREFIXES = ("LICENSE", "LICENCE", "COPYING", "NOTICE", "UNLICENSE")
REGENERATE_COMMAND = "uv run python scripts/third_party_licenses.py"

#: A copyright notice line ("Copyright (c) 2016 Name", "Copyright The Rust Project ...",
#: "(C) 2020 ...", "© 2020 ..."), not the license's own sentences that mention copyright.
COPYRIGHT_LINE = re.compile(
    r"^[ \t]*(?:"
    r"(?:Copyright|COPYRIGHT)[ \t]*(?:\(c\)|\(C\)|©)?[ \t]*(?:\d{4}|[A-Z][a-z])"
    r"|(?:\(c\)|\(C\)|©)[ \t]*\d{4}"
    r").*$",
    re.MULTILINE,
)
#: Copyright lines that are a license's fill-in-the-blanks template, not a real notice.
TEMPLATE_PLACEHOLDERS = ("[yyyy]", "<year>", "{yyyy}", "<copyright holders>")


@dataclass(frozen=True)
class Wheel:
    """A wheel, the crate built as its extension module, and where its license file goes."""

    name: str
    root_crate: str
    output: Path


#: One entry per wheel. douro-py and porto-console join this list when their wheels exist.
WHEELS = (
    Wheel(
        name="porto",
        root_crate="porto-py",
        output=REPOSITORY_ROOT / "packaging" / "porto" / "THIRD_PARTY_LICENSES.md",
    ),
)


class CargoTarget(TypedDict):
    kind: list[str]


class CargoPackage(TypedDict):
    id: str
    name: str
    version: str
    license: str | None
    license_file: str | None
    repository: str | None
    manifest_path: str
    targets: list[CargoTarget]


class CargoDependencyKind(TypedDict):
    kind: str | None


class CargoDependency(TypedDict):
    pkg: str
    dep_kinds: list[CargoDependencyKind]


class CargoNode(TypedDict):
    id: str
    deps: list[CargoDependency]


class CargoResolve(TypedDict):
    nodes: list[CargoNode]


class CargoMetadata(TypedDict):
    packages: list[CargoPackage]
    workspace_members: list[str]
    resolve: CargoResolve


@dataclass(frozen=True)
class LicenseFile:
    """A license or notice file a crate ships, with its copyright lines split out."""

    file_name: str
    text_without_copyright: str
    copyright_lines: tuple[str, ...]


@dataclass(frozen=True)
class CrateLicenses:
    package: CargoPackage
    files: tuple[LicenseFile, ...]


class ScriptError(Exception):
    """A failure the user can fix; the message says how."""


def load_cargo_metadata() -> CargoMetadata:
    cargo = shutil.which("cargo")
    if cargo is None:
        raise ScriptError(
            "cargo is not on PATH. Install Rust with rustup (rust-toolchain.toml pins the "
            "version), then rerun."
        )
    command = [cargo, "metadata", "--format-version", "1", "--locked"]
    result = subprocess.run(
        command, cwd=REPOSITORY_ROOT, capture_output=True, text=True, encoding="utf-8"
    )
    if result.returncode != 0:
        raise ScriptError(
            f"`cargo metadata --locked` failed:\n{result.stderr.strip()}\n"
            "If Cargo.lock is out of date, run `cargo check --workspace` to update it, then rerun."
        )
    return cast(CargoMetadata, json.loads(result.stdout))


def is_proc_macro(package: CargoPackage) -> bool:
    return any("proc-macro" in target["kind"] for target in package["targets"])


def is_normal_dependency(dependency: CargoDependency) -> bool:
    """True when the dependency is linked into its dependent (not a build or dev dependency)."""
    return any(kind["kind"] is None for kind in dependency["dep_kinds"])


def find_workspace_crate(metadata: CargoMetadata, crate_name: str) -> str:
    packages = {package["id"]: package for package in metadata["packages"]}
    for member_id in metadata["workspace_members"]:
        if packages[member_id]["name"] == crate_name:
            return member_id
    members = sorted(packages[member_id]["name"] for member_id in metadata["workspace_members"])
    raise ScriptError(
        f"no workspace crate is named {crate_name!r} (members: {', '.join(members)}). "
        "Fix the root_crate of its entry in WHEELS in scripts/third_party_licenses.py."
    )


def crates_in_binary(metadata: CargoMetadata, root_crate: str) -> list[CargoPackage]:
    """The third-party crates compiled into the root crate's binary, sorted by name and version."""
    packages = {package["id"]: package for package in metadata["packages"]}
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    members = set(metadata["workspace_members"])
    reached: set[str] = set()
    pending = [find_workspace_crate(metadata, root_crate)]
    while pending:
        package_id = pending.pop()
        if package_id in reached or is_proc_macro(packages[package_id]):
            continue
        reached.add(package_id)
        pending.extend(
            dependency["pkg"]
            for dependency in nodes[package_id]["deps"]
            if is_normal_dependency(dependency)
        )
    third_party = [packages[package_id] for package_id in reached - members]
    return sorted(third_party, key=lambda package: (package["name"], package["version"]))


def normalize_text(text: str) -> str:
    """LF line endings, no trailing whitespace, no leading or trailing blank lines."""
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    return "\n".join(line.rstrip() for line in lines).strip("\n")


def read_license_file(path: Path) -> LicenseFile:
    """The file's text with its copyright lines split out (they are listed with the crate).

    A NOTICE file keeps its copyright lines too: Apache-2.0 section 4(d) asks for its text as is.
    """
    text = normalize_text(path.read_text(encoding="utf-8", errors="replace"))
    copyright_lines: list[str] = []
    for match in COPYRIGHT_LINE.finditer(text):
        line = match.group(0).strip()
        is_template = any(marker in line for marker in TEMPLATE_PLACEHOLDERS)
        if line not in copyright_lines and not is_template:
            copyright_lines.append(line)
    if not path.name.upper().startswith("NOTICE"):
        text = re.sub(r"\n{3,}", "\n\n", normalize_text(COPYRIGHT_LINE.sub("", text)))
    return LicenseFile(path.name, text, tuple(copyright_lines))


def license_file_paths(package: CargoPackage) -> list[Path]:
    crate_folder = Path(package["manifest_path"]).parent
    if not crate_folder.is_dir():
        raise ScriptError(
            f"the source of {package['name']} {package['version']} is missing at {crate_folder}. "
            "Run `cargo fetch`, then rerun."
        )
    paths = {
        path
        for path in crate_folder.iterdir()
        if path.is_file() and path.name.upper().startswith(LICENSE_FILE_PREFIXES)
    }
    declared = package["license_file"]
    if declared is not None and (crate_folder / declared).is_file():
        paths.add(crate_folder / declared)
    return sorted(paths, key=lambda path: path.name)


def read_crate_licenses(package: CargoPackage) -> CrateLicenses:
    files = tuple(read_license_file(path) for path in license_file_paths(package))
    return CrateLicenses(package, files)


def dedupe_key(text: str) -> str:
    """Texts that differ only in whitespace are the same license."""
    return re.sub(r"\s+", " ", text).strip()


def number_distinct_texts(crates: list[CrateLicenses]) -> dict[str, int]:
    """Number each distinct license text from 1, in the order the sorted crates first use it."""
    numbers: dict[str, int] = {}
    for crate in crates:
        for license_file in crate.files:
            numbers.setdefault(dedupe_key(license_file.text_without_copyright), len(numbers) + 1)
    return numbers


def text_number(license_file: LicenseFile, numbers: dict[str, int]) -> int:
    return numbers[dedupe_key(license_file.text_without_copyright)]


def render_crate(crate: CrateLicenses, numbers: dict[str, int]) -> list[str]:
    package = crate.package
    lines = [
        f"### {package['name']} {package['version']}",
        "",
        f"- License: `{package['license'] or 'none declared'}`",
        f"- Repository: {package['repository'] or 'not declared'}",
    ]
    copyright_lines = dict.fromkeys(line for f in crate.files for line in f.copyright_lines)
    if copyright_lines:
        lines.append("- Copyright:")
        lines.extend(f"  - {line}" for line in copyright_lines)
    if crate.files:
        references = [
            f"{f.file_name} ([text {text_number(f, numbers)}](#text-{text_number(f, numbers)}))"
            for f in crate.files
        ]
        lines.append(f"- License files: {', '.join(references)}")
    else:
        lines.append(
            "- License files: **none shipped in the crate.** The license is the SPDX expression "
            "above; its standard texts are at https://spdx.org/licenses/."
        )
    return [*lines, ""]


def render_texts(crates: list[CrateLicenses], numbers: dict[str, int]) -> list[str]:
    texts: dict[int, str] = {}
    users: dict[int, list[str]] = {}
    for crate in crates:
        for license_file in crate.files:
            number = text_number(license_file, numbers)
            texts.setdefault(number, license_file.text_without_copyright)
            user = f"{crate.package['name']} {crate.package['version']} ({license_file.file_name})"
            users.setdefault(number, []).append(user)
    lines: list[str] = []
    for number in sorted(texts):
        lines += [
            f'<a id="text-{number}"></a>',
            "",
            f"### Text {number}",
            "",
            f"Used by: {', '.join(users[number])}.",
            "",
            "```text",
            texts[number],
            "```",
            "",
        ]
    return lines


def render(wheel: Wheel, crates: list[CrateLicenses]) -> str:
    numbers = number_distinct_texts(crates)
    missing = [crate.package["name"] for crate in crates if not crate.files]
    lines = [
        "# Third-party licenses",
        "",
        f"The `{wheel.name}` wheel contains compiled code from the Rust crates below.",
        "Each crate is listed with its license, repository and copyright lines; the license",
        "and notice texts follow, each distinct text once (copyright lines are listed with the",
        "crates instead).",
        "",
        "Generated by `scripts/third_party_licenses.py` from the dependencies of",
        f"`{wheel.root_crate}` in Cargo.lock; do not edit by hand.",
        "",
        "## Crates",
        "",
    ]
    if missing:
        lines += [
            f"These crates ship no license file: {', '.join(missing)}. Their entries give the",
            "SPDX expression to follow instead.",
            "",
        ]
    for crate in crates:
        lines += render_crate(crate, numbers)
    lines += ["## License texts", "", *render_texts(crates, numbers)]
    return "\n".join(lines).rstrip("\n") + "\n"


def generate(metadata: CargoMetadata, wheel: Wheel) -> tuple[str, int, int]:
    """The file's content, the number of crates and the number of distinct texts."""
    crates = [read_crate_licenses(p) for p in crates_in_binary(metadata, wheel.root_crate)]
    return render(wheel, crates), len(crates), len(number_distinct_texts(crates))


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write the THIRD_PARTY_LICENSES.md of each wheel from `cargo metadata`."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="write nothing; exit 1 if a THIRD_PARTY_LICENSES.md is missing or out of date",
    )
    return parser.parse_args(argv)


def write_or_check(wheel: Wheel, content: str, *, check: bool) -> bool:
    """Write the file (unless checking); return whether it was up to date."""
    current = wheel.output.read_text(encoding="utf-8") if wheel.output.exists() else None
    if current != content and not check:
        wheel.output.write_text(content, encoding="utf-8", newline="\n")
    return current == content


def main(argv: list[str]) -> int:
    check = bool(parse_arguments(argv).check)
    outdated: list[str] = []
    try:
        metadata = load_cargo_metadata()
        for wheel in WHEELS:
            content, crate_count, text_count = generate(metadata, wheel)
            relative = wheel.output.relative_to(REPOSITORY_ROOT).as_posix()
            if not write_or_check(wheel, content, check=check):
                outdated.append(relative)
            print(f"{relative}: {crate_count} crates, {text_count} distinct license texts")
    except ScriptError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    if check and outdated:
        print(
            f"out of date: {', '.join(outdated)}. "
            f"Run `{REGENERATE_COMMAND}` and commit the result.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
