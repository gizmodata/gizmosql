#!/usr/bin/env python3
"""Retag Mach-O object files (and ar archives full of them) from
macOS to iOS in place.

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

For .a archives, we MUST patch in place without extracting, because
ar archives can contain duplicate filenames (e.g. libgrpc.a has
several files named `status.upb.c.o` from different source dirs).
`ar -x` overwrites duplicates on extraction, losing objects.

Usage: retag_objects_to_ios.py <file> [<file>...]
       Files can be .o (Mach-O) or .a (BSD ar archive).
"""
import struct
import sys

LC_BUILD_VERSION = 0x32
MH_MAGIC_64 = 0xfeedfacf

PLATFORM_IOS = 2
# Packed version: (major << 16) | (minor << 8) | patch
MIN_OS_IOS = (17 << 16)             # 17.0.0
SDK_IOS = (26 << 16) | (4 << 8)     # 26.4.0


def patch_macho_buffer(data: bytearray, offset: int, length: int) -> bool:
    """Patch LC_BUILD_VERSION in a Mach-O 64 file embedded at
    data[offset:offset+length]. Returns True if any patch applied."""
    if length < 32:
        return False
    magic = struct.unpack_from("<I", data, offset)[0]
    if magic != MH_MAGIC_64:
        return False
    ncmds = struct.unpack_from("<I", data, offset + 16)[0]
    cmd_offset = offset + 32  # mach_header_64 size
    end = offset + length
    patched = False
    for _ in range(ncmds):
        if cmd_offset + 8 > end:
            break
        cmd, cmdsize = struct.unpack_from("<II", data, cmd_offset)
        if cmd == LC_BUILD_VERSION and cmdsize >= 24 and cmd_offset + 24 <= end:
            struct.pack_into("<I", data, cmd_offset + 8, PLATFORM_IOS)
            struct.pack_into("<I", data, cmd_offset + 12, MIN_OS_IOS)
            struct.pack_into("<I", data, cmd_offset + 16, SDK_IOS)
            patched = True
        cmd_offset += cmdsize
    return patched


def patch_object_file(path: str) -> None:
    """Patch a standalone .o file in place."""
    with open(path, "rb") as f:
        data = bytearray(f.read())
    if patch_macho_buffer(data, 0, len(data)):
        with open(path, "wb") as f:
            f.write(data)


def patch_archive(path: str) -> None:
    """Patch every Mach-O object inside a BSD ar archive in place.

    BSD ar format:
      magic: 8 bytes "!<arch>\\n"
      member:
        header (60 bytes): name(16) mtime(12) uid(6) gid(6) mode(8) size(10) magic(2)
        if name starts with "#1/<n>": next n bytes after header are the real name
        file data: <size> bytes (size includes the long-name suffix if present)
        padding: pad to 2-byte boundary
    """
    with open(path, "rb") as f:
        data = bytearray(f.read())
    if data[:8] != b"!<arch>\n":
        return

    pos = 8
    end = len(data)
    while pos + 60 <= end:
        header = bytes(data[pos:pos + 60])
        size_str = header[48:58].rstrip(b" \x00").decode("ascii", "replace")
        try:
            member_size = int(size_str)
        except ValueError:
            break
        member_start = pos + 60
        member_end = member_start + member_size

        # Long-name extension: name field starts with "#1/<n>"
        name_field = header[0:16].rstrip(b" \x00").decode("ascii", "replace")
        long_name_len = 0
        if name_field.startswith("#1/"):
            try:
                long_name_len = int(name_field[3:])
            except ValueError:
                long_name_len = 0

        # The actual Mach-O object starts AFTER the long name (if any)
        obj_start = member_start + long_name_len
        obj_len = member_size - long_name_len
        if obj_start + obj_len <= end:
            patch_macho_buffer(data, obj_start, obj_len)

        # Advance past member, with 2-byte alignment padding
        pos = member_end
        if pos % 2 == 1:
            pos += 1

    with open(path, "wb") as f:
        f.write(data)


def patch_path(path: str) -> None:
    if path.endswith(".a"):
        patch_archive(path)
    else:
        patch_object_file(path)


if __name__ == "__main__":
    for path in sys.argv[1:]:
        try:
            patch_path(path)
        except Exception as e:
            print(f"warning: failed to patch {path}: {e}", file=sys.stderr)
