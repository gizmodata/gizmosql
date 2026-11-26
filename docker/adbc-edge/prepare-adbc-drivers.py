#!/usr/bin/env python3
"""Stage pinned native drivers, manifests and notices for one or both Linux arches.

Used for the GizmoSQL edge channel's -adbc images (a copy of Hardcharger's
docker/prepare-adbc-drivers.py, without the Hardcharger driver). Requires gh and Python 3.11.
GitHub asset digests are checked before unpacking, and each library must be an ELF
shared object for the architecture being staged that exports the entrypoint its
manifest names. No credentials are passed into Docker.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re
import struct
import subprocess
import tarfile
import tempfile
import tomllib


# The same pins as Hardcharger's ADBC images (both run DuckDB 2.0). dbc supplies the other ten.
# Every release attaches one archive per platform holding libadbc_driver_<name>.so and
# its license files. GizmoSQL's also holds the manifest template
# (<name>.toml.in). Oracle's, Db2's and Quack's hold none, so their manifest is written
# here from the entrypoint their changelogs name and the license their archive carries.
DRIVERS = {
    "gizmosql": {"repo": "gizmosql-adbc", "tag": "v2.0.18",
                 "asset": "libadbc_driver_gizmosql-{tag}-linux_{arch}.tar.gz"},
    "oracle": {"repo": "adbc-driver-oracle", "tag": "v1.1.0",
               "asset": "libadbc_driver_oracle-linux-{arch}.tar.gz",
               "entrypoint": "AdbcDriverOracleInit", "license": "MIT"},
    "db2": {"repo": "adbc-driver-db2", "tag": "v0.3.0",
            "asset": "libadbc_driver_db2-linux-{arch}.tar.gz",
            "entrypoint": "AdbcDriverDb2Init", "license": "MIT"},
    # 1.x speaks Quack protocol 3 (DuckDB 2.0 servers); 0.1.0-alpha.11 spoke DuckDB 1.5's.
    "quack": {"repo": "adbc-driver-quack", "tag": "v1.1.0",
              "asset": "libadbc_driver_quack-linux-{arch}.tar.gz",
              "entrypoint": "AdbcDriverQuackInit", "license": "MIT"},
}
# What a LICENSE file must start with before a manifest written here may name the license.
LICENSE_TITLES = {"MIT": "MIT License"}
ELF_MACHINES = {"amd64": 62, "arm64": 183}  # e_machine: EM_X86_64, EM_AARCH64


def gh(*args):
    return subprocess.check_output(args=["gh", *args])


def exported_functions(*, data, arch, label):
    """Check that data is a 64-bit ELF shared object for arch; return the functions it exports."""
    if data[:6] != b"\x7fELF\x02\x01":
        raise RuntimeError(f"Not a 64-bit little-endian ELF file: {label}")
    kind, machine = struct.unpack_from("<HH", buffer=data, offset=16)
    if kind != 3 or machine != ELF_MACHINES[arch]:  # ET_DYN
        raise RuntimeError(f"Not a linux/{arch} shared object (type {kind}, machine {machine}): {label}")
    (section_offset,) = struct.unpack_from("<Q", buffer=data, offset=40)
    section_size, section_count = struct.unpack_from("<HH", buffer=data, offset=58)
    sections = [struct.unpack_from("<IIQQQQIIQQ", buffer=data, offset=section_offset + index * section_size)
                for index in range(section_count)]
    names = set()
    for _, section_type, _, _, offset, size, link, _, _, entry_size in sections:
        if section_type != 11:  # SHT_DYNSYM
            continue
        strings = sections[link][4]
        for position in range(offset, offset + size, entry_size):
            name, info, _, section = struct.unpack_from("<IBBH", buffer=data, offset=position)
            # Defined here, GLOBAL or WEAK binding, a function.
            if section and info >> 4 in (1, 2) and info & 0xF == 2:
                names.add(data[strings + name:data.index(b"\0", strings + name)].decode())
    if not names:
        raise RuntimeError(f"No exported functions found: {label}")
    return names


def prepare(*, output, arch):
    root = output / f"linux_{arch}"
    for directory in ("lib", "manifests", "notices"):
        (root / directory).mkdir(parents=True, exist_ok=True)
    inventory = {}
    for name, driver in DRIVERS.items():
        repo = driver["repo"]
        version = driver["tag"]
        release = json.loads(gh("release", "view", version, "--repo", f"gizmodata/{repo}",
                                "--json", "assets"))
        expected = driver["asset"].format(tag=version, arch=arch)
        assets = [asset for asset in release["assets"] if asset["name"] == expected]
        if len(assets) != 1:
            raise RuntimeError(f"Expected one {name} {arch} archive named {expected}, found {len(assets)}")
        asset = assets[0]
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", asset.get("digest") or ""):
            raise RuntimeError(f"Missing SHA-256 digest: {asset['name']}")
        with tempfile.TemporaryDirectory() as temporary:
            gh("release", "download", version, "--repo", f"gizmodata/{repo}",
               "--pattern", asset["name"], "--dir", temporary)
            payload = Path(temporary, asset["name"]).read_bytes()
            digest = "sha256:" + hashlib.sha256(payload).hexdigest()
            if digest != asset["digest"]:
                raise RuntimeError(f"Digest mismatch: {asset['name']}")
            notices = root / "notices" / name
            notices.mkdir(exist_ok=True)
            library = root / "lib" / f"libadbc_driver_{name}.so"
            with tarfile.open(name=Path(temporary, asset["name"]), mode="r:gz") as archive:
                members = [member for member in archive.getmembers() if member.isfile()]
                libraries = [member for member in members if Path(member.name).name == library.name]
                if len(libraries) != 1:
                    raise RuntimeError(f"Expected one library inside {asset['name']}")
                binary = archive.extractfile(member=libraries[0]).read()
                staged = set()
                for member in members:
                    basename = Path(member.name).name
                    if basename.startswith(("LICENSE", "NOTICE", "THIRD_PARTY")):
                        if basename in staged:
                            raise RuntimeError(f"Two files named {basename} inside {asset['name']}")
                        staged.add(basename)
                        (notices / basename).write_bytes(archive.extractfile(member=member).read())
                if "LICENSE" not in staged:
                    raise RuntimeError(f"Missing LICENSE inside {asset['name']}")
                templates = [member for member in members if Path(member.name).name == f"{name}.toml.in"]
                if len(templates) > 1:
                    raise RuntimeError(f"Expected one {name} manifest template inside {asset['name']}")
                if templates:
                    manifest = archive.extractfile(member=templates[0]).read().decode()
                    manifest = manifest.replace("@PREFIX@", "/usr/local/lib").replace("@VERSION@", version.removeprefix("v"))
                elif "entrypoint" in driver:
                    # The archive names no license in a form a manifest can quote; check the text.
                    title = (notices / "LICENSE").read_text().lstrip().splitlines()[0].strip()
                    if title != LICENSE_TITLES[driver["license"]]:
                        raise RuntimeError(f"{name} LICENSE is not {driver['license']}: {title!r}")
                    manifest = f"""manifest_version = 1
name = '{name}'
version = '{version.removeprefix('v')}'
publisher = 'GizmoData'
license = '{driver['license']}'
url = 'https://github.com/gizmodata/{repo}'
[ADBC]
version = '1.1.0'
[Driver]
entrypoint = '{driver['entrypoint']}'
[Driver.shared]
linux_amd64 = '/usr/local/lib/{library.name}'
linux_arm64 = '/usr/local/lib/{library.name}'
"""
                else:
                    raise RuntimeError(f"Missing {name} manifest template")
            # The manifest must describe what was staged: this version, the image's path
            # for this architecture, and an entrypoint this library exports.
            parsed = tomllib.loads(manifest)
            if parsed["version"] != version.removeprefix("v"):
                raise RuntimeError(f"{name} manifest names version {parsed['version']!r}, not {version}")
            if parsed["Driver"]["shared"][f"linux_{arch}"] != f"/usr/local/lib/{library.name}":
                raise RuntimeError(f"{name} manifest does not point at /usr/local/lib/{library.name}")
            entrypoint = parsed["Driver"]["entrypoint"]
            if entrypoint not in exported_functions(data=binary, arch=arch, label=f"{library.name} in {asset['name']}"):
                raise RuntimeError(f"{library.name} in {asset['name']} does not export {entrypoint}")
            library.write_bytes(binary)
            library.chmod(mode=0o755)
            (root / "manifests" / f"{name}.toml").write_text(data=manifest)
            inventory[name] = {"repository": f"gizmodata/{repo}", "tag": version,
                               "asset": asset["name"], "digest": digest, "entrypoint": entrypoint}
            print(f"Verified {name} {version} linux/{arch}: {digest} ({entrypoint})", flush=True)
    (root / "notices" / "native-drivers.json").write_text(data=json.dumps(obj=inventory, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("drivers"))
    parser.add_argument("--arch", choices=("amd64", "arm64"), action="append")
    options = parser.parse_args()
    for architecture in options.arch or ("amd64", "arm64"):
        prepare(output=options.output, arch=architecture)
