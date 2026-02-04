# Contributing to smf

Thank you for your interest in contributing to **smf**! We welcome contributions from everyone.

This project is licensed under the [Apache License 2.0](LICENSE). By contributing, you agree that your contributions will be licensed under its terms.

## Table of Contents

- [Prerequisites](#prerequisites)
- [How to Contribute](#how-to-contribute)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Git Commit Messages](#git-commit-messages)

## Prerequisites

To build and test the project, you will need:

- **Go**: Version 1.25.6 or later (as specified in `go.mod`).
- **golangci-lint**: The project uses `golangci-lint` for linting. You can find installation instructions [here](https://golangci-lint.run/docs/welcome/install/local/).

## How to Contribute

1.  **Fork the repository** on GitHub.
2.  **Clone your fork** to your local machine.
3.  **Create a new branch** for your feature or bug fix:
    ```bash
    git checkout -b feat/your-feature-name
    ```
4.  **Make your changes**. Ensure your code follows the coding standards and passes all tests.
5.  **Commit your changes** using [Conventional Commits](#git-commit-messages).
6.  **Push to your fork**:
    ```bash
    git push origin feat/your-feature-name
    ```
7.  **Open a Pull Request** against the `main` branch of the original repository.

## Coding Standards

- **Formatting**: Use `go fmt` format your code.
- **Linting**: Before submitting a PR, run the linter to ensure there are no issues:
  ```bash
  golangci-lint run
  ```
- **Documentation**: Provide comments for public functions and types.

## Testing

We take testing seriously. Please ensure that your changes are covered by tests.

To run all tests in the project:

```bash
go test ./...
```

For more detailed output or running specific tests:

```bash
go test -v ./...
# Run tests in a specific package
go test ./internal/dialect/mysql
# Run tests with coverage
go test -cover ./...
```

## Git Commit Messages

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification for commit messages. This helps in generating clear changelogs and managing versions.

Structure of a commit message:

```text
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### Types:

- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `style`: Changes that do not affect the meaning of the code (white-space, formatting, etc.)
- `refactor`: A code change that neither fixes a bug nor adds a feature
- `perf`: A code change that improves performance
- `test`: Adding missing tests or correcting existing tests
- `chore`: Changes to the build process or auxiliary tools and libraries

### Example:

```text
feat(mysql): add support for new TiDB-specific column types
```
