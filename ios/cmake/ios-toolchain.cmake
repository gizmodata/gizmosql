# iOS cross-compilation toolchain for GizmoSQL
#
# Usage:
#   cmake -DCMAKE_TOOLCHAIN_FILE=ios/cmake/ios-toolchain.cmake ..

set(CMAKE_SYSTEM_NAME iOS)
set(CMAKE_SYSTEM_PROCESSOR aarch64)
set(CMAKE_OSX_ARCHITECTURES arm64)
set(CMAKE_OSX_DEPLOYMENT_TARGET "17.0" CACHE STRING "Minimum iOS version")

# Use the iphoneos SDK
set(CMAKE_OSX_SYSROOT iphoneos)

# Standard C++20
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Static libraries only — iOS requires static linking
set(BUILD_SHARED_LIBS OFF)

# Position-independent code (required for iOS static libs linked into app)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Suppress bitcode (deprecated since Xcode 14, removed in Xcode 15)
set(CMAKE_XCODE_ATTRIBUTE_ENABLE_BITCODE NO)

# Don't look for host programs in the iOS SDK
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY BOTH)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE BOTH)
# BOTH for packages — our deps (Arrow, DuckDB) are built into ${CMAKE_BINARY_DIR}/third_party
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE BOTH)
