#!/usr/bin/env python3
"""Retag Mach-O object files from macOS to iOS in place.

Some of GizmoSQL's iOS dependencies (Arrow, gRPC, DuckDB, OpenSSL,
abseil, etc.) are built via CMake ExternalProject_Add for the HOST
toolchain (macOS arm64) instead of being cross-compiled for iOS.
The resulting .o files are binary-compatible with iOS arm64 (same
instruction set), but their Mach-O LC_BUILD_VERSION load command
says platform 1 (macOS). The iOS app linker refuses to link them:

  Building for 'iOS', but linking in object file ... built for 'macOS'

vtool can't retag .o files because they have no padding to grow load
commands. This script patches the LC_BUILD_VERSION's `platform`,
`minos`, and `sdk` fields in place — the load command size doesn't
change, so no padding is needed.

Usage: retag_objects_to_ios.py <object_file> [<object_file>...]
"""
import struct
import sys

LC_BUILD_VERSION = 0x32
MH_MAGIC_64 = 0xfeedfacf

PLATFORM_IOS = 2
# Packed version: (major << 16) | (minor << 8) | patch
MIN_OS_IOS = (17 << 16)             # 17.0.0
SDK_IOS = (26 << 16) | (4 << 8)     # 26.4.0


def patch_file(path: str) -> bool:
    """Patch the LC_BUILD_VERSION command in a Mach-O 64-bit object file.
    Returns True if a patch was applied, False otherwise."""
    with open(path, "rb") as f:
        data = bytearray(f.read())
    if len(data) < 32:
        return False
    magic = struct.unpack_from("<I", data, 0)[0]
    if magic != MH_MAGIC_64:
        return False
    ncmds = struct.unpack_from("<I", data, 16)[0]
    offset = 32  # mach_header_64 size
    patched = False
    for _ in range(ncmds):
        cmd, cmdsize = struct.unpack_from("<II", data, offset)
        if cmd == LC_BUILD_VERSION and cmdsize >= 24:
            # struct build_version_command:
            #   uint32_t cmd, cmdsize, platform, minos, sdk, ntools
            struct.pack_into("<I", data, offset + 8, PLATFORM_IOS)
            struct.pack_into("<I", data, offset + 12, MIN_OS_IOS)
            struct.pack_into("<I", data, offset + 16, SDK_IOS)
            patched = True
        offset += cmdsize
    if patched:
        with open(path, "wb") as f:
            f.write(data)
    return patched


if __name__ == "__main__":
    for path in sys.argv[1:]:
        try:
            patch_file(path)
        except Exception:
            pass
