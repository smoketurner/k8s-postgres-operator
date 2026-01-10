# Contributing to PostgreSQL Kubernetes Operator

Thank you for your interest in contributing to the PostgreSQL Kubernetes Operator! This document provides guidelines and instructions for contributing.

## Code of Conduct

This project adheres to a [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## How to Contribute

### Reporting Bugs

Before creating a bug report:

1. Check the [existing issues](https://github.com/smoketurner/k8s-postgres-operator/issues) to avoid duplicates
2. Collect relevant information:
   - Kubernetes version (`kubectl version`)
   - Operator version
   - PostgresCluster manifest (sanitized)
   - Operator logs
   - Relevant events (`kubectl describe pgc <name>`)

Create a [new issue](https://github.com/smoketurner/k8s-postgres-operator/issues/new?template=bug_report.md) with:
- A clear, descriptive title
- Steps to reproduce
- Expected vs. actual behavior
- Environment details

### Suggesting Features

Feature requests are welcome! Please:

1. Check [existing issues](https://github.com/smoketurner/k8s-postgres-operator/issues) for similar requests
2. Create a [new issue](https://github.com/smoketurner/k8s-postgres-operator/issues/new?template=feature_request.md) describing:
   - The problem you're trying to solve
   - Your proposed solution
   - Alternatives you've considered

### Pull Requests

#### Before You Start

1. **Open an issue first** for significant changes to discuss the approach
2. **Fork the repository** and create a feature branch
3. **Read the [development guide](docs/development.md)** for setup instructions

#### Development Process

1. Create a feature branch:
   ```bash
   git checkout -b feature/my-feature
   ```

2. Make your changes following our [coding standards](#coding-standards)

3. Add or update tests as needed

4. Run all checks:
   ```bash
   make fmt
   make lint
   make test
   ```

5. Commit with a descriptive message:
   ```bash
   git commit -m "Add support for feature X

   - Implemented X in src/foo.rs
   - Added tests for X
   - Updated documentation"
   ```

6. Push and create a pull request:
   ```bash
   git push origin feature/my-feature
   ```

#### Pull Request Guidelines

- **Title**: Use a clear, descriptive title
- **Description**: Explain what the PR does and why
- **Testing**: Describe how you tested the changes
- **Documentation**: Update docs if behavior changes
- **Size**: Keep PRs focused and reasonably sized

## Coding Standards

### Rust Style

- Follow [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- Use `rustfmt` for formatting (enforced by CI)
- Address all `clippy` warnings
- Write doc comments for public APIs

### Code Organization

```rust
// Imports: std, external crates, internal modules
use std::sync::Arc;

use kube::Client;

use crate::controller::Context;

// Constants
const MAX_RETRIES: u32 = 5;

// Types, then implementations
pub struct MyStruct {
    field: String,
}

impl MyStruct {
    pub fn new() -> Self { ... }
}
```

### Error Handling

- Use `Result<T, Error>` for fallible operations
- Create specific error types with `thiserror`
- Provide context in error messages
- Log errors at appropriate levels

### Testing

- Write unit tests for new functionality
- Add integration tests for controller behavior
- Use property-based testing where appropriate
- Aim for meaningful test coverage, not 100%

### Documentation

- Add doc comments (`///`) to public items
- Include examples in doc comments
- Update README.md for user-facing changes
- Update API reference for CRD changes

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
type(scope): description

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Formatting, no code change
- `refactor`: Code change without feature/fix
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

Examples:
```
feat(pgbouncer): add support for replica pooler

fix(reconciler): handle deleted resources gracefully

docs(readme): add troubleshooting section
```

## Review Process

1. **Automated checks**: CI must pass (fmt, lint, tests)
2. **Code review**: At least one maintainer approval required
3. **Testing**: Manual testing for significant changes
4. **Documentation**: Verify docs are updated if needed

### Review Criteria

- Code correctness and safety
- Test coverage
- Documentation quality
- Performance implications
- Backward compatibility

## Release Process

Releases are automated via GitHub Actions:

1. Version bump in `Cargo.toml` and `Chart.yaml`
2. Update `CHANGELOG.md`
3. Create and push a version tag:
   ```bash
   git tag v0.2.0
   git push origin v0.2.0
   ```
4. GitHub Actions builds and publishes:
   - Container image to ghcr.io
   - Helm chart to OCI registry
   - GitHub release with artifacts

## Getting Help

- **Questions**: Open a [discussion](https://github.com/smoketurner/k8s-postgres-operator/discussions)
- **Bugs**: Open an [issue](https://github.com/smoketurner/k8s-postgres-operator/issues)
- **Security**: See [SECURITY.md](SECURITY.md) for reporting vulnerabilities

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
