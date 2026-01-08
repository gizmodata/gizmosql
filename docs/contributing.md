# Contributing

Thank you for your interest in contributing to GizmoSQL!

---

## Ways to Contribute

- 🐛 Report bugs and issues
- 💡 Suggest new features
- 📝 Improve documentation
- 🔧 Submit pull requests
- 🧪 Write tests
- 🌟 Star the repository

---

## Getting Started

### 1. Fork the Repository

```bash
# Fork on GitHub, then clone
git clone https://github.com/YOUR_USERNAME/gizmosql.git
cd gizmosql
```

### 2. Set Up Development Environment

```bash
# Clone with submodules
git clone --recurse-submodules https://github.com/gizmodata/gizmosql.git

# Build
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug
cmake --build build

# Install Python dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Create a Branch

```bash
git checkout -b feature/my-new-feature
```

---

## Development Guidelines

### Code Style

**C++:**
- Follow existing code style
- Use `.clang-format` for formatting
- Document public APIs

```bash
# Format code
clang-format -i src/**/*.cpp src/**/*.h
```

**Python:**
- Follow PEP 8
- Use type hints
- Write docstrings

```bash
# Format code
black .
isort .

# Lint
pylint src/
```

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Code style (formatting)
- `refactor`: Code refactoring
- `test`: Tests
- `chore`: Maintenance

**Examples:**
```
feat(server): add query timeout support

fix(client): handle connection errors gracefully

docs: update installation instructions
```

---

## Pull Request Process

### 1. Make Your Changes

- Write clean, documented code
- Add tests for new functionality
- Update documentation
- Follow code style guidelines

### 2. Test Your Changes

```bash
# Run tests
cd tests
pytest

# Test build
cmake --build build --target test
```

### 3. Update Documentation

- Update README.md if needed
- Add/update docs/ files
- Update CHANGELOG.md

### 4. Submit Pull Request

1. Push to your fork
2. Open PR on GitHub
3. Fill out PR template
4. Wait for review

**PR Template:**
```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Documentation
- [ ] Other

## Testing
How was this tested?

## Checklist
- [ ] Code follows style guidelines
- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] Commits follow convention
```

---

## Reporting Issues

### Bug Reports

Use the bug report template:

```markdown
**Describe the bug**
Clear description of the bug

**To Reproduce**
Steps to reproduce:
1. Start server with...
2. Run query...
3. See error...

**Expected behavior**
What should happen

**Environment:**
- OS: [e.g., Ubuntu 22.04]
- GizmoSQL version: [e.g., v1.4.3]
- Backend: [e.g., DuckDB]

**Additional context**
Logs, screenshots, etc.
```

### Feature Requests

Use the feature request template:

```markdown
**Is your feature request related to a problem?**
Description of the problem

**Describe the solution you'd like**
Your proposed solution

**Describe alternatives you've considered**
Other approaches

**Additional context**
Any other context
```

---

## Development Workflow

### Local Testing

```bash
# Start server
GIZMOSQL_PASSWORD="test_pass" \
  ./build/gizmosql_server \
  --database-filename data/test.duckdb \
  --print-queries

# Test client
./build/gizmosql_client \
  --command Execute \
  --query "SELECT version()" \
  --username gizmosql_username \
  --password test_pass
```

### Docker Testing

```bash
# Build Docker image
docker build -t gizmosql-dev .

# Run container
docker run --rm -p 31337:31337 \
  --env GIZMOSQL_PASSWORD="test_pass" \
  gizmosql-dev
```

---

## Code Review

### Review Guidelines

**For Reviewers:**
- Be constructive and respectful
- Focus on code quality and maintainability
- Test the changes
- Approve when ready

**For Contributors:**
- Address all review comments
- Be open to feedback
- Update PR as needed
- Squash commits if requested

---

## Community Guidelines

### Code of Conduct

- Be respectful and inclusive
- Welcome newcomers
- Focus on constructive feedback
- No harassment or discrimination
- Follow GitHub's Community Guidelines

### Communication

- **GitHub Issues**: Bug reports, feature requests
- **Pull Requests**: Code contributions
- **Email**: info@gizmodata.com

---

## Documentation

### Writing Documentation

- Use clear, concise language
- Include code examples
- Add screenshots where helpful
- Keep docs up to date

### Documentation Structure

```
docs/
├── README.md              # Home page
├── installation.md        # Installation guide
├── configuration.md       # Configuration reference
├── clients.md            # Client connections
├── security.md           # Security guide
├── performance.md        # Performance tuning
├── integrations.md       # Integrations
├── troubleshooting.md    # Troubleshooting
├── api-reference.md      # API reference
└── contributing.md       # This file
```

---

## Testing

### Unit Tests

```bash
cd tests
pytest test_unit.py -v
```

### Integration Tests

```bash
pytest test_integration.py -v
```

### Performance Tests

```bash
pytest test_performance.py -v --benchmark
```

---

## Release Process

1. Update version in CMakeLists.txt
2. Update CHANGELOG.md
3. Create git tag
4. Build binaries
5. Create GitHub release
6. Build and push Docker images

---

## Resources

- [GitHub Repository](https://github.com/gizmodata/gizmosql)
- [Issue Tracker](https://github.com/gizmodata/gizmosql/issues)
- [Apache Arrow](https://arrow.apache.org/)
- [DuckDB](https://duckdb.org/)
- [SQLite](https://sqlite.org/)

---

## Recognition

Contributors are recognized in:
- GitHub contributors list
- CHANGELOG.md
- Release notes

---

## Questions?

- Open a [GitHub Discussion](https://github.com/gizmodata/gizmosql/discussions)
- Email: info@gizmodata.com

---

Thank you for contributing to GizmoSQL! 🎉
