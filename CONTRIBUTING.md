# Contributing to GizmoSQL

Thank you for your interest in contributing to GizmoSQL! This guide will help you get started with development.

## Prerequisites

- CMake 3.20 or higher
- C++17 compatible compiler (Clang, GCC, or MSVC)
- Boost libraries (program_options)
- OpenSSL 3.x

On macOS with Homebrew:
```bash
brew install cmake boost openssl@3
```

On Ubuntu/Debian:
```bash
sudo apt-get install cmake libboost-program-options-dev libssl-dev
```

## Building

```bash
# Clone the repository
git clone https://github.com/gizmodata/gizmosql.git
cd gizmosql

# Configure the build
cmake -B build -DCMAKE_BUILD_TYPE=Release

# Build (use -j to parallelize)
cmake --build build -j12
```

The first build will download and compile dependencies (Arrow, DuckDB, gRPC, etc.) which may take some time.

## Running the Server

```bash
# Start with an in-memory database
./build/gizmosql_server --password mypassword

# Start with a persistent database
./build/gizmosql_server --database-filename mydb.db --password mypassword

# See all options
./build/gizmosql_server --help
```

## Running Tests

### Integration Tests

The integration tests start actual GizmoSQL server instances, so ensure ports 31337-31342 are available.

```bash
# Run all integration tests
./build/tests/gizmosql_integration_tests

# Run a specific test suite
./build/tests/gizmosql_integration_tests --gtest_filter="InstrumentationServerFixture.*"

# Run a specific test
./build/tests/gizmosql_integration_tests --gtest_filter="InstrumentationServerFixture.InstrumentationRecordsCreated"

# List all available tests
./build/tests/gizmosql_integration_tests --gtest_list_tests

# Run with verbose output
./build/tests/gizmosql_integration_tests --gtest_print_time=1
```

### Test Suites

| Suite | Description |
|-------|-------------|
| `BulkIngestServerFixture` | Tests for bulk data ingestion |
| `GeoArrowServerFixture` | Tests for GeoArrow/GEOMETRY type support |
| `InstrumentationServerFixture` | Tests for session instrumentation |
| `InstrumentationManagerTest` | Tests for instrumentation manager internals |
| `KillSessionServerFixture` | Tests for KILL SESSION functionality |

### Python Tests

Python tests verify end-to-end functionality using ADBC (Arrow Database Connectivity).

```bash
# Install Python test dependencies
pip install adbc-driver-flightsql pyarrow geopandas shapely duckdb

# Start the server (in a separate terminal)
./build/gizmosql_server --password gizmosql_password

# Run all Python tests
python tests/test_geoarrow.py
python tests/test_bulk_ingest.py

# Run with custom connection settings
GIZMOSQL_HOST=localhost \
GIZMOSQL_PORT=31337 \
GIZMOSQL_PASSWORD=gizmosql_password \
python tests/test_geoarrow.py

# Run with TLS enabled
TLS_ENABLED=1 python tests/test_geoarrow.py
```

| Test | Description |
|------|-------------|
| `test_geoarrow.py` | Tests GeoArrow/GEOMETRY export and GeoPandas integration |
| `test_bulk_ingest.py` | Tests ADBC bulk ingestion with TPC-H data |

## Code Style

- Use `clang-format` for C++ code formatting
- Follow existing code conventions in the repository
- Add tests for new functionality

## Pull Requests

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Run the tests to ensure they pass
5. Commit your changes with a descriptive message
6. Push to your fork and open a pull request

## Reporting Issues

Please report issues on the [GitHub issue tracker](https://github.com/gizmodata/gizmosql/issues) with:
- A clear description of the problem
- Steps to reproduce
- Expected vs actual behavior
- GizmoSQL version and environment details
