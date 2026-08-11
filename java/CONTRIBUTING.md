# Contributing to the Zerobus Java SDK

Please read the [top-level CONTRIBUTING.md](https://github.com/databricks/zerobus-sdk/blob/main/CONTRIBUTING.md) first for general contribution guidelines, pull request process, and commit requirements.

This document covers Java-specific development setup and workflow.

## Development Setup

### Prerequisites

- Git
- Java 8 or higher - [Download Java](https://adoptium.net/)
- Maven 3.6 or higher - [Download Maven](https://maven.apache.org/download.cgi)
- Protocol Buffers compiler (`protoc`) 33.0 - [Download protoc](https://github.com/protocolbuffers/protobuf/releases/tag/v33.0)

### Setting Up Your Development Environment

1. **Clone the repository:**
   ```bash
   git clone https://github.com/databricks/zerobus-sdk.git
   cd zerobus-sdk/java
   ```

2. **Build the project:**
   ```bash
   mvn clean install -Dzerobus.skipNativeLibCheck=true
   ```

   This will generate protobuf Java classes, compile the source code, run tests, and install the artifact to your local Maven repository. Omit `-Dzerobus.skipNativeLibCheck=true` only when you have staged release JNI libraries under `src/main/resources/native/` and want to build a redistributable SDK JAR.

3. **Run tests:**
   ```bash
   mvn test
   ```

## Coding Style

Code style is enforced by [Spotless](https://github.com/diffplug/spotless) in your pull request. We use Google Java Format for code formatting.

### Running the Formatter

Format your code before committing:

```bash
mvn spotless:apply
```

This will format:
- **Java code**: Using Google Java Format
- **Imports**: Organized and unused imports removed
- **pom.xml**: Sorted dependencies and plugins

### Checking Formatting

```bash
mvn spotless:check
```

### Running Tests

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=ZerobusSdkTest

# Run specific test method
mvn test -Dtest=ZerobusSdkTest#testSingleRecordIngestAndAcknowledgment
```

## Continuous Integration

All pull requests must pass CI checks:

- **fmt**: Runs formatting checks (`mvn spotless:check`)
- **test**: Runs `mvn clean compile` and `mvn test` on Java 11, 17, and 21 across Ubuntu and Windows runners.

You can view CI results in the GitHub Actions tab of the pull request.

## Maven Commands

```bash
# Clean build
mvn clean

# Compile code
mvn compile

# Run tests
mvn test

# Format code
mvn spotless:apply

# Check formatting
mvn spotless:check

# Create JARs (regular + fat JAR)
mvn package -Dzerobus.skipNativeLibCheck=true

# Install to local Maven repo
mvn install -Dzerobus.skipNativeLibCheck=true

# Generate protobuf classes
mvn protobuf:compile
```
