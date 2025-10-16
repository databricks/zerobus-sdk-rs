# Contributing to Zerobus SDK for Rust

We happily welcome contributions to the Zerobus SDK for Rust. We use [GitHub Issues](https://github.com/databricks/zerobus-sdk-rs/issues) to track community reported issues and [GitHub Pull Requests](https://github.com/databricks/zerobus-sdk-rs/pulls) for accepting changes.

Contributions are licensed on a license-in/license-out basis.

## Communication

Before starting work on a major feature, please open a GitHub issue. We will make sure no one else is already working on it and that it is aligned with the goals of the project.

A "major feature" is defined as any change that is > 100 LOC altered (not including tests), or changes any user-facing behavior.

We will use the GitHub issue to discuss the feature and come to agreement. This is to prevent your time being wasted, as well as ours. The GitHub review process for major features is also important so that organizations with commit access can come to agreement on design.

If it is appropriate to write a design document, the document must be hosted either in the GitHub tracking issue, or linked to from the issue and hosted in a world-readable location.

Small patches and bug fixes don't need prior communication.

## Development Setup

### Prerequisites

- Git
- cargo

### Setting Up Your Development Environment

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/databricks/zerobus-sdk-rs.git
    cd zerobus-sdk-rs
    ```

2. **Build the project:**
   ```bash
   make build
   ```

   This will:
   - Download all dependencies
   - Build the project in debug mode

## Coding Style

Code style is enforced by a formatter check in your pull request. We use `rustfmt` to format our code. Run `make fmt` to ensure your code is properly formatted prior to raising a pull request.

### Running the Formatter

Format your code before committing:

```bash
make fmt
```

This runs `cargo fmt --all` to format all crates in the workspace.

### Running Linters

Check your code for issues:

```bash
make lint
```

This runs `cargo clippy` to catch common mistakes and improve your code.

### Running Tests

Run the test suite to ensure your changes don't break existing functionality:

```bash
make test
```

This runs `cargo test` for all crates in the workspace.

## Pull Request Process

1. **Create a feature branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes:**
   - Write clear, concise commit messages
   - Follow existing code style
   - Update documentation as needed

3. **Format and test your code:**
   ```bash
   make fmt
   make test
   ```

4. **Commit your changes:**
   ```bash
   git add .
   git commit -m "Add feature: description of your changes"
   ```

5. **Push to your fork:**
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Create a Pull Request:**
   - Provide a clear description of changes
   - Reference any related issues
   - Ensure all CI checks pass

## Signed Commits

This repo requires all contributors to sign their commits. To configure this, you can follow [Github's documentation](https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits) to create a GPG key, upload it to your Github account, and configure your git client to sign commits.

## Developer Certificate of Origin

To contribute to this repository, you must sign off your commits to certify that you have the right to contribute the code and that it complies with the open source license. The rules are pretty simple, if you can certify the content of [DCO](./DCO), then simply add a "Signed-off-by" line to your commit message to certify your compliance. Please use your real name as pseudonymous/anonymous contributions are not accepted.

```
Signed-off-by: Joe Smith <joe.smith@email.com>
```

If you set your `user.name` and `user.email` git configs, you can sign your commit automatically with `git commit -s`:

```bash
git commit -s -m "Your commit message"
```

## Code Review Guidelines

When reviewing code:

- Check for adherence to code style
- Look for potential edge cases
- Consider performance implications
- Ensure documentation is updated

## Commit Message Guidelines

Follow these conventions for commit messages:

- Use present tense: "Add feature" not "Added feature"
- Use imperative mood: "Fix bug" not "Fixes bug"
- First line should be 50 characters or less
- Reference issues: "Fix #123: Description of fix"

Example:
```
Add async stream creation example

- Add async_example.py demonstrating non-blocking ingestion
- Update README with async API documentation

Fixes #42
```

## Documentation

### Updating Documentation

- Update docstrings for all public APIs
- Use Google-style docstrings
- Include examples in docstrings where helpful
- Update README.md for user-facing changes
- Update examples/ for new features

Example docstring:
```python
def ingest_record(self, record) -> RecordAcknowledgment:
    """
    Submits a single record for ingestion into the stream.

    This method may block if the maximum number of in-flight records
    has been reached.

    Args:
        record: The Protobuf message object to be ingested.

    Returns:
        RecordAcknowledgment: An object to wait on for the server's acknowledgment.

    Raises:
        ZerobusException: If the stream is not in a valid state for ingestion.

    Example:
        >>> record = AirQuality(device_name="sensor-1", temp=25)
        >>> ack = stream.ingest_record(record)
        >>> ack.wait_for_ack()
    """
```

## Continuous Integration

All pull requests must pass CI checks:

- **fmt**: Runs formatting checks (`cargo fmt`)
- **lint**: Runs linting checks (`cargo clippy`)
- **tests**: Runs tests on Ubuntu and Windows for the stable Rust toolchain.

You can view CI results in the GitHub Actions tab of the pull request.

## Makefile Targets

Available make targets:

- `make build` - Build the project for debugging
- `make build-release` - Build the project for release
- `make fmt` - Format code with `rustfmt`
- `make lint` - Run linting with `clippy`
- `make check` - Run all checks (fmt and lint)
- `make test` - Run unit tests with `cargo test`
- `make clean` - Remove build artifacts
- `make help` - Show available targets

## Versioning

We follow [Semantic Versioning](https://semver.org/):

- **MAJOR**: Incompatible API changes
- **MINOR**: Backwards-compatible functionality additions
- **PATCH**: Backwards-compatible bug fixes

## Getting Help

- **Issues**: Open an issue on GitHub for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions
- **Documentation**: Check the README and examples/

## Package Name

The package is published on [crates.io](https://crates.io/) as `zerobus`.

## Code of Conduct

- Be respectful and inclusive
- Welcome newcomers
- Focus on constructive feedback
- Follow the [Python Community Code of Conduct](https://www.python.org/psf/conduct/)