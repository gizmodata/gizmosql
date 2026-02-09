# Claude Code Guidelines for GizmoSQL

This file provides context and reminders for Claude Code sessions working on the GizmoSQL codebase.

## Pre-Commit Checklist

Before committing any change, ensure you have completed ALL of the following:

### 1. Tests
- [ ] Add integration tests for new features in `tests/integration/`
- [ ] Follow existing test patterns - see `test_server_fixture.h` for the CRTP-based fixture pattern
- [ ] Register new test files in `tests/CMakeLists.txt`
- [ ] Run tests with: `cd build && ninja gizmosql_integration_tests && ./tests/gizmosql_integration_tests --gtest_filter="*YourTestName*"`

### 2. Documentation
- [ ] Update relevant docs in `docs/` directory
- [ ] For new CLI flags/options, document in both:
  - The relevant feature doc (e.g., `docs/token_authentication.md`)
  - The CLI help text in `src/gizmosql_server.cpp`
- [ ] For new or changed environment variables, keep these in sync:
  - `src/gizmosql_server.cpp` — native env var handling (`std::getenv` calls)
  - `scripts/start_gizmosql.sh` and `scripts/start_gizmosql_slim.sh` — header comment env var tables
  - The relevant feature doc in `docs/` (e.g., `docs/token_authentication.md`)
- [ ] For library API changes, update docstrings in `src/common/include/gizmosql_library.h`

### 3. Changelog
- [ ] **ALWAYS** update `CHANGELOG.md` for every user-facing change
- [ ] Add entries under `## [Unreleased]` section
- [ ] Follow [Keep a Changelog](https://keepachangelog.com/) format:
  - `### Added` - new features
  - `### Changed` - changes in existing functionality
  - `### Deprecated` - soon-to-be removed features
  - `### Removed` - removed features
  - `### Fixed` - bug fixes
  - `### Security` - vulnerability fixes

## Project Architecture

### Key Directories
- `src/common/` - Shared code (security, logging, library API)
- `src/duckdb/` - DuckDB backend implementation
- `src/sqlite/` - SQLite backend implementation
- `src/enterprise/` - Enterprise-only features (instrumentation, kill session, catalog permissions)
- `tests/integration/` - Integration tests
- `docs/` - Documentation (served via Docsify)

### Important Files
- `src/common/include/gizmosql_library.h` - Public C API for the library
- `src/common/gizmosql_library.cpp` - Library implementation with `RunFlightSQLServer` and `CreateFlightSQLServer`
- `src/gizmosql_server.cpp` - CLI entry point with Boost.ProgramOptions
- `src/common/include/detail/gizmosql_security.h` - Authentication middleware
- `tests/integration/test_server_fixture.h` - Test fixture template for integration tests

### Code Flow for New Parameters
When adding a new configuration parameter, update these locations in order:
1. `src/common/include/gizmosql_library.h` - Add to `RunFlightSQLServer()` signature and docstring
2. `src/gizmosql_server.cpp` - Add CLI option and env var handling
3. `src/common/gizmosql_library.cpp` - Update:
   - `FlightSQLServerBuilder()` signature and implementation
   - `CreateFlightSQLServer()` signature and call to builder
   - `RunFlightSQLServer()` call to `CreateFlightSQLServer()`
4. `tests/integration/test_server_fixture.h` - Add to `TestServerConfig` and forward declaration

### Build Commands
```bash
# Configure (from repo root)
mkdir -p build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Debug

# Build everything
ninja

# Build just the tests
ninja gizmosql_integration_tests

# Run specific tests
./tests/gizmosql_integration_tests --gtest_filter="*TestName*"

# Run all tests
./tests/gizmosql_integration_tests
```

### Enterprise vs Core
- Enterprise features are guarded by `#ifdef GIZMOSQL_ENTERPRISE`
- Enterprise source is in `src/enterprise/`
- Tests that need enterprise features should check license availability

## Coding Conventions

### Naming
- Classes: `PascalCase`
- Functions/Methods: `PascalCase` for public API, `snake_case` for internal
- Variables: `snake_case`
- Constants: `kPascalCase` or `SCREAMING_SNAKE_CASE`
- Member variables: `trailing_underscore_`

### Logging
- Use `GIZMOSQL_LOG(level)` for simple messages
- Use `GIZMOSQL_LOGKV(level, message, ...)` for structured key-value logging
- Levels: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `FATAL`

### Error Handling
- Use Arrow's `Status` and `Result<T>` types
- Use `ARROW_RETURN_NOT_OK()` and `ARROW_ASSIGN_OR_RAISE()` macros
- For Flight errors, use `MakeFlightError(FlightStatusCode, message)`

## Common Patterns

### Adding a New CLI Flag
```cpp
// In gizmosql_server.cpp, add to options:
("my-new-flag", po::value<bool>()->default_value(false),
  "Description of what this flag does. "
  "If not set, uses env var MY_NEW_FLAG (1/true to enable).")

// Parse with env var fallback:
bool my_flag = vm["my-new-flag"].as<bool>();
if (!my_flag) {
  if (const char* env_val = std::getenv("MY_NEW_FLAG")) {
    std::string val(env_val);
    my_flag = (val == "1" || val == "true" || val == "TRUE" || val == "True");
  }
}
```

### Adding a Test Fixture
```cpp
class MyFeatureFixture
    : public gizmosql::testing::ServerTestFixture<MyFeatureFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "my_feature_test.db",
        .port = 31XXX,  // Use unique port
        .health_port = 31XXY,
        .username = "testuser",
        .password = "testpassword",
        // ... other config options
    };
  }
};

// Required static member definitions:
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<MyFeatureFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<MyFeatureFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<MyFeatureFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<MyFeatureFixture>::config_{};
```

## Reminders

- The test server uses `"test_secret_key_for_testing"` as the secret key
- Always use unique ports for test fixtures to avoid conflicts
- Clean up database files in test teardown
- JWT tokens use HS256 algorithm with the server's secret key
- Instance IDs are UUIDs generated at server startup
