# Installation

This guide covers multiple ways to install and deploy GizmoSQL.

---

## Prerequisites

Before you begin, ensure you have one of the following:

- Docker (for containerized deployment)
- Python 3.9+ (for building from source)
- CMake and Ninja (for manual builds)

---

## Option 1: Docker (Recommended)

The easiest way to run GizmoSQL is using Docker.

### Basic Docker Run

```bash
docker run --name gizmosql \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --env TLS_ENABLED="1" \
           --env GIZMOSQL_PASSWORD="gizmosql_password" \
           --env PRINT_QUERIES="1" \
           --pull always \
           gizmodata/gizmosql:latest
```

?> **Note**: You can disable TLS by setting `TLS_ENABLED="0"`, though this is not recommended for production.

### Mount Your Own Database

You can mount your own DuckDB database file into the container:

<!-- tabs:start -->

#### **Create TPC-H Database**

```bash
# Generate a TPC-H Scale Factor 1 database
duckdb ./tpch_sf1.duckdb << EOF
.bail on
.echo on
SELECT VERSION();
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=1);
EOF
```

#### **Run with Mounted Database**

```bash
docker run --name gizmosql \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --env TLS_ENABLED="1" \
           --env GIZMOSQL_PASSWORD="gizmosql_password" \
           --pull always \
           --mount type=bind,source=$(pwd),target=/opt/gizmosql/data \
           --env DATABASE_FILENAME="data/tpch_sf1.duckdb" \
           gizmodata/gizmosql:latest
```

<!-- tabs:end -->

### Stop the Container

```bash
docker stop gizmosql
```

---

## Option 2: CLI Executable

Download the pre-built CLI executable for your platform:

| Platform | Download Link |
|----------|---------------|
| Linux x86-64 | [gizmosql_cli_linux_amd64.zip](https://github.com/gizmodata/gizmosql/releases/latest/download/gizmosql_cli_linux_amd64.zip) |
| Linux arm64 | [gizmosql_cli_linux_arm64.zip](https://github.com/gizmodata/gizmosql/releases/latest/download/gizmosql_cli_linux_arm64.zip) |
| MacOS x86-64 | [gizmosql_cli_macos_amd64.zip](https://github.com/gizmodata/gizmosql/releases/latest/download/gizmosql_cli_macos_amd64.zip) |
| MacOS arm64 | [gizmosql_cli_macos_arm64.zip](https://github.com/gizmodata/gizmosql/releases/latest/download/gizmosql_cli_macos_arm64.zip) |

### Run the CLI Server

```bash
# Extract the downloaded file
unzip gizmosql_cli_*.zip

# Run the server
GIZMOSQL_PASSWORD="gizmosql_password" \
  gizmosql_server --database-filename data/some_db.duckdb --print-queries
```

### View All Options

```bash
gizmosql_server --help
```

---

## Option 3: Build from Source

For developers who want to build GizmoSQL from source:

### Step 1: Clone the Repository

```bash
git clone https://github.com/gizmodata/gizmosql --recurse-submodules
cd gizmosql
```
### Step 2: Install Dependencies

- Mac OS
```bash
brew install automake boost gflags
```
- Linux
```bash
sudo apt-get install -y \
            build-essential \
            ninja-build \
            automake \
            cmake \
            gcc \
            git \
            libboost-all-dev \
            libgflags-dev \
            libssl-dev
```
### Step 3: Build with CMake

```bash
cmake -S . -B build -G Ninja -DCMAKE_INSTALL_PREFIX=/usr/local
cmake --build build --target install
```

### Step 4: Install Python Requirements

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install --requirement ./requirements.txt
```

<!-- tabs:start -->

#### **SQLite Sample**

```bash
wget https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H-small.db \
     -O ./data/TPC-H-small.sqlite
```

#### **DuckDB Sample**

```bash
python "scripts/create_duckdb_database_file.py" \
       --file-name="TPC-H-small.duckdb" \
       --file-path="data" \
       --overwrite-file=true \
       --scale-factor=0.01
```

<!-- tabs:end -->

### Step 5: Generate TLS Certificates (Optional)

```bash
pushd tls
./gen-certs.sh
popd
```

### Step 6: Start the Server

```bash
GIZMOSQL_PASSWORD="gizmosql_password" \
  gizmosql_server --database-filename data/TPC-H-small.duckdb --print-queries
```

---

## Slim Docker Image

A minimal Docker image is available without Python, sample databases, or TLS certificate generation utilities.

### Required Environment Variables

- `DATABASE_FILENAME` - Path to the database file
- `GIZMOSQL_PASSWORD` - Server password

### Optional Environment Variables

- `TLS_ENABLED` - Set to "1" to enable TLS (default: "0")
- `TLS_CERT` - Path to TLS certificate file (if TLS is enabled)
- `TLS_KEY` - Path to TLS key file (if TLS is enabled)

### Run Slim Image

```bash
docker run --name gizmosql-slim \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --env DATABASE_FILENAME="data/some_database.duckdb" \
           --env TLS_ENABLED="0" \
           --env GIZMOSQL_PASSWORD="gizmosql_password" \
           --env PRINT_QUERIES="1" \
           --pull always \
           gizmodata/gizmosql:latest-slim
```

---

## Next Steps

After installation, learn how to:

- [Configure GizmoSQL](configuration.md)
- [Connect clients](clients.md)
- [Secure your deployment](security.md)

---

## Platform-Specific Notes

### macOS

?> On macOS, you may need to install additional dependencies via Homebrew:

```bash
brew install cmake ninja
```

### Linux

?> Ensure you have the necessary build tools:

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install build-essential cmake ninja-build

# RHEL/CentOS/Fedora
sudo dnf install gcc-c++ cmake ninja-build
```

---

## Troubleshooting

### Port Already in Use

If port 31337 is already in use, you can change it:

```bash
docker run ... --publish 31338:31337 ...
```

### Database File Not Found

Ensure the database file exists and the path is correct. Use absolute paths when mounting volumes.

### TLS Certificate Issues

If you encounter TLS certificate verification issues when connecting:

- Use `disableCertificateVerification=true` in JDBC connection strings
- Use `DatabaseOptions.TLS_SKIP_VERIFY.value: "true"` in Python ADBC connections
- Use `--tls-skip-verify` in CLI client connections

---

## Need Help?

📧 Email: info@gizmodata.com  
🌐 Website: [https://gizmodata.com](https://gizmodata.com)  
📚 GitHub Issues: [https://github.com/gizmodata/gizmosql/issues](https://github.com/gizmodata/gizmosql/issues)
