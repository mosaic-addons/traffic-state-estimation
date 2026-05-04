# AGENTS Playbook

Purpose-built for autonomous coding agents working in `traffic-state-estimation`.

## Repository Orientation

This repository provides **Traffic State Estimation (TSE)** applications for [Eclipse MOSAIC](https://eclipse.dev/mosaic), enabling FCD-based traffic analysis in co-simulation scenarios.

### Project Overview

- **Artifact**: `com.dcaiti.mosaic.app:traffic-state-estimation:26.0-SNAPSHOT`
- **Purpose**: Application suite to evaluate traffic state estimation approaches based on Floating Car Data (FCD)
- **License**: Eclipse Public License 2.0 (EPL-2.0)
- **Organization**: Fraunhofer FOKUS / DCAITI
- **Inception**: 2023

### Dependency on Eclipse MOSAIC

- Requires `org.eclipse.mosaic:mosaic-application:26.0-SNAPSHOT` from `repo.eclipse.org`
- Built as an extension application, NOT part of core Eclipse MOSAIC
- Applications are deployed as JAR files to MOSAIC scenario `application/` directories
- See parent [mosaic/AGENTS.md](../mosaic/AGENTS.md) for Eclipse MOSAIC core platform guidelines

### Key Dependencies

- Java 17+ (source/target 17)
- Maven 3.1+
- Eclipse MOSAIC 26.0-SNAPSHOT
- Apache Commons Math3 (3.6.1) - statistical computations
- Apache Parquet (1.15.2) + Hadoop (3.4.0) - efficient FCD storage
- JTS (1.19.0) - geometry/spatial operations
- Python 3.x (for evaluation utilities)

## Module Entry Points

### Main Applications

- **`TseServerApp`** (`com.dcaiti.mosaic.app.tse.TseServerApp`) - Server application receiving and processing FCD messages
- **`FxdReceiverApp`** (`com.dcaiti.mosaic.app.tse.FxdReceiverApp`) - Alternative receiver for FxD (Floating x Data) messages
- **`FcdTransmitterApp`** (`com.dcaiti.mosaic.app.fxd.FcdTransmitterApp`) - Vehicle application periodically transmitting FCD
- **`FxdTransmitterApp`** (`com.dcaiti.mosaic.app.fxd.FxdTransmitterApp`) - Extended FxD transmitter

### Package Structure

```
com.dcaiti.mosaic.app.fxd/              # FCD/FxD message transmission & reception
├── messages/                           # Message DTOs (FcdUpdateMessage, FxdUpdateMessage)
├── data/                               # Data models (FcdRecord, FxdRecord, FcdTraversal, FxdTraversal)
└── config/                             # Configuration classes (CFcdTransmitterApp, CFxdTransmitterApp)

com.dcaiti.mosaic.app.tse/              # Traffic state estimation logic
├── processors/                         # FCD processing strategies
│   ├── TimeBasedProcessor              # Time-interval based processing
│   ├── TraversalBasedProcessor         # Edge-traversal based processing
│   ├── MessageBasedProcessor           # Per-message processing
│   ├── ThresholdProcessor              # Threshold calculation
│   ├── AbstractSpatioTemporalProcessor # Shared spatial-temporal logic
│   ├── SpatioTemporalProcessor         # Spatial-temporal metrics
│   ├── AggregatedSpatioTemporalProcessor # Aggregated metrics
│   └── FcdWriterProcessor              # Database writer
├── persistence/                        # Database access layer
│   ├── FcdDataStorage                  # Storage interface
│   ├── FcdDatabaseHelper               # SQLite implementation
│   ├── FcdParquetStorage               # Parquet implementation
│   └── ScenarioDatabaseHelper          # Scenario metadata
├── parquet/                            # Parquet/Avro serialization
├── data/                               # Data models & interfaces
├── config/                             # Configuration (CTseServerApp, CFxdReceiverApp)
├── gson/                               # GSON type adapters
└── events/                             # Custom events (ExpiredUnitRemovalEvent)
```

### Configuration Files

Applications are configured via JSON files in scenario `application/` directories:

- **`FcdTransmitterApp.json`** - Vehicle FCD transmission config (intervals, receiver ID)
- **`TseServerApp.json`** - Server processing config (storage type, processors, thresholds)

See [README.md](README.md) for detailed configuration examples.

## Build & Package Commands

### Maven Build Commands
```bash
# Full build with tests (recommended before commits)
mvn clean install

# Package JAR only (faster iteration)
mvn package

# Build without tests (bootstrap only)
mvn clean install -DskipTests

# Generate JAR with dependencies for MOSAIC deployment
mvn clean package
# Output: target/traffic-state-estimation-26.0-SNAPSHOT.jar

# Generate standalone JAR with all dependencies (shaded)
mvn clean package -Pfcd-shaded-jar
# Output: target/traffic-state-estimation-26.0-SNAPSHOT-jar-with-dependencies.jar

# Run all tests
mvn test

# Run single test class
mvn test -Dtest=ClassName

# Run single test method
mvn test -Dtest=ClassName#methodName
```

### Python Evaluation Commands
```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Install dependencies
python -m pip install -r evaluation/requirements.txt

# Launch Jupyter notebooks
jupyter notebook evaluation/notebooks/

# Example: usage_example.ipynb demonstrates data loading/preprocessing
```

## Runtime & Deployment

### Deploying to MOSAIC Scenarios

1. **Build the JAR**: `mvn clean package`
2. **Copy to scenario**: `cp target/traffic-state-estimation-26.0-SNAPSHOT.jar /path/to/scenario/application/`
3. **Configure mapping**: Update `scenario/mapping/mapping_config.json` (see [README](README.md#usage))
4. **Configure cell network**: Update `scenario/cell/network.json` if using servers
5. **Configure applications**: Create `FcdTransmitterApp.json` and `TseServerApp.json` in `scenario/application/`
6. **Run MOSAIC**: `mosaic.sh -s YourScenario` (from MOSAIC bundle)

### Example Scenarios

- **BeST Scenario integration**: Pre-configured files in `configs/best-scenario/`
  - `configs/best-scenario/mapping/mapping_config.json`
  - `configs/best-scenario/cell/network.json`
- Reference: [BeST Scenario](https://github.com/mosaic-addons/best-scenario) (large-scale Berlin traffic)

### IDE Execution

- Main class: `org.eclipse.mosaic.starter.MosaicStarter` (from MOSAIC core)
- VM args: Point to scenario directory and MOSAIC config
- Ensure `SUMO_HOME` is set if using SUMO federates

## Test Strategy & Commands

### Unit Testing

```bash
# Run all tests
mvn test

# Test specific class
mvn test -Dtest=FcdDataStorageTest

# Test specific method
mvn test -Dtest=FcdDataStorageTest#testInsertAndRetrieve

# Skip tests temporarily (document in commits)
mvn install -DskipTests
```

### Testing Patterns

- Use JUnit 4 patterns (matches Eclipse MOSAIC core)
- Mock external dependencies with Mockito
- Test resources go in `src/test/resources/`
- Prefer descriptive test names: `testMethodName_ExpectedBehavior`

## Quality Gates & Static Analysis

### Checkstyle (Google Java Style)

```bash
# Run Checkstyle verification
mvn checkstyle:check

# Generate Checkstyle report
mvn checkstyle:checkstyle
# Report: target/site/checkstyle.html
```

### SpotBugs (Static Analysis)

```bash
# Run SpotBugs
mvn com.github.spotbugs:spotbugs-maven-plugin:check

# Generate SpotBugs report
mvn com.github.spotbugs:spotbugs-maven-plugin:spotbugs
# Report: target/spotbugsXml.xml
```

### Pre-Commit Checklist

Before every commit:

1. `mvn clean install` passes without errors
2. `mvn checkstyle:check` passes (no style violations)
3. No new SpotBugs warnings introduced
4. No new `FIXME`/`TODO` comments without issue tracking
5. New public APIs have Javadoc
6. Tests cover new functionality

## Code Style Guidelines

### Java Code Style (Google Java Style)

**Indentation & Formatting:**
- 4-space indentation (NO tabs)
- One statement per line
- Column limit: 140 characters
- UTF-8 file encoding (all files)

**Braces (K&R Style):**
```java
if (condition) {
    doSomething();
} else {
    doOther();
}
```
- No line break before opening brace
- Line break after opening brace
- Line break before closing brace
- `else`/`catch` hugs closing brace

**Whitespace:**
- Space after reserved words: `if (`, `for (`, `catch (`
- Space before/after operators: `a == 1 && b == 3`
- Space after closing paren of cast: `((Target) object)`
- Space around ternary: `value = condition ? trueVal : falseVal`

### Imports
```java
// 1. org.eclipse.mosaic imports
import org.eclipse.mosaic.lib.database.Database;
import org.eclipse.mosaic.lib.util.gson.TimeFieldAdapter;

// 2. Third-party libraries (alphabetical)
import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.parquet.hadoop.ParquetWriter;

// 3. Standard Java libraries (alphabetical)
import java.util.List;
import java.util.Map;
```

- **NO wildcard imports** (e.g., `import java.util.*`)
- Blank line between import groups
- Static imports after regular imports

### Naming Conventions

| Element        | Pattern        | Example               |
|----------------|----------------|-----------------------|
| Class          | UpperCamelCase | TseServerApp          |
| Interface      | UpperCamelCase | DatabaseAccess        |
| Enum           | UpperCamelCase | ProcessorType         |
| Enum value     | UPPER_CASE     | TIME_BASED            |
| Field          | lowerCamelCase | fcdDataStorage        |
| Final field    | lowerCamelCase | maximumItems          |
| Static const   | UPPER_CASE     | MIN_SAMPLES           |
| Variable       | lowerCamelCase | listOfValues          |
| Method         | lowerCamelCase | processTraversal      |
| Package        | lowercase      | com.dcaiti.mosaic.app |
| Parameter      | lowerCamelCase | typeName              |
| Type parameter | UPPER_CASE     | T, E                  |
| Config class   | C + ClassName  | CTseServerApp         |

**Rules:**

- Interfaces **NO** 'I' prefix (use `DatabaseAccess`, NOT `IDatabaseAccess`)
- Abbreviations: only first letter capitalized (`V2xMessage`, `FcdRecord`, NOT `V2XMessage`, `FCDRecord`)
- Boolean methods: start with `is` or `has` (`isValid()`, `hasData()`)
- Configuration classes: prefix with `C` (`CTseServerApp`, `CFcdTransmitterApp`)
- Abstract classes: prefix with `Abstract` when appropriate (`AbstractRecordBuilder`)

### Documentation

- Use Javadoc (`/** ... */`) for **all** public classes, constructors, and methods
- Include `@param` and `@return` annotations with descriptions
- Getters/setters do NOT require method comments if trivial
- Inline comments use `//` and explain **WHY**, not WHAT
- Document non-obvious null-handling behavior
- Reference related classes with `{@link ClassName}`

**Example:**
```java
/**
 * Processes FCD traversals and computes spatial-temporal traffic metrics.
 * This processor aggregates speed data over configurable spatial chunks.
 *
 * @param traversal the FCD traversal to process
 * @return computed traffic metrics for the traversal's edge
 * @throws IllegalArgumentException if traversal is null or invalid
 */
public SpatioTemporalTrafficMetric process(FcdTraversal traversal) {
  // Implementation
}
```

### Error Handling

- Validate public method arguments with `Objects.requireNonNull` or explicit checks
- Use `IllegalArgumentException` for invalid inputs, `IllegalStateException` for sequencing errors
- Prefer ternary for simple null checks: `value = config.param != null ? config.param : defaultValue`
- Log errors using SLF4J: `logger.error("Error processing FCD: {}", message, exception)`
- Provide actionable error messages (state expected vs. received)
- Wrap checked exceptions only when adding context

### Type Safety

- Use generics with proper type parameters (`List<FcdRecord>`, NOT `List`)
- Avoid raw types (triggers compiler warnings)
- Prefer `Optional<T>` only when absence is semantically meaningful
- Use `@SuppressWarnings` sparingly with justification comment
- Favor immutability: final fields, Collections.unmodifiable* for getters

### Logging

- Obtain loggers via `LoggerFactory.getLogger(getClass())`
- **NEVER** use `System.out` or `System.err`
- Use appropriate levels: `trace` (verbose), `debug` (diagnostics), `info` (lifecycle), `warn` (recoverable), `error` (failures)
- Parameterize messages: `logger.info("Processed {} records in {}ms", count, duration)`

## Python Code Style (evaluation/)

### Naming & Formatting

- Function/method names: `snake_case`
- Class names: `UpperCamelCase`
- Constants: `UPPER_CASE`
- Private members: `_leading_underscore`

### Type Hints & Documentation

```python
def load_fcd_data(file_path: str, max_records: int = 1000) -> pd.DataFrame:
    """
    Load FCD records from Parquet file.
    
    Parameters
    ----------
    file_path : str
        Path to Parquet file containing FCD data
    max_records : int, optional
        Maximum records to load (default: 1000)
        
    Returns
    -------
    pd.DataFrame
        DataFrame with columns: timestamp, vehicle_id, edge, speed, position
    """
    # Implementation
```

### Imports

```python
# 1. Standard library
import os
from pathlib import Path
from typing import List, Dict

# 2. Third-party packages
import numpy as np
import pandas as pd

# 3. Local modules
from evaluation.preprocessing import clean_data
```

### Notebooks

- Use clear section headers with markdown cells
- Document assumptions and data sources
- Include example outputs for key cells
- See `evaluation/notebooks/usage_example.ipynb` as reference

## Maven Flag Reference

### Reactor Control

```bash
# Build specific module only
mvn -pl . package

# Build with dependencies
mvn -pl . -am package

# Skip modules (requires quotes)
mvn -pl '!src/test' package
```

### Test Control

```bash
# Skip all tests
mvn install -DskipTests

# Run specific test pattern
mvn test -Dtest=*ProcessorTest

# Run with detailed output
mvn test -Dsurefire.printSummary=true
```

### Analysis & Reporting

```bash
# Enable Checkstyle
mvn checkstyle:check

# Enable SpotBugs
mvn spotbugs:check

# Generate site with reports
mvn site
```

## Workflow Guardrails

### Git Workflow

- Branch from `main` for features/fixes
- Keep commits focused and atomic
- Write descriptive commit messages (present tense, imperative mood)
- Squash before merging to keep history clean
- Example: "Add ThresholdProcessor for dynamic speed limits"

### Pull Request Process

1. Ensure all tests pass: `mvn clean install`
2. Run quality checks: `mvn checkstyle:check spotbugs:check`
3. Update documentation if changing user-facing behavior
4. Reference related issues in PR description
5. Keep PRs focused (single feature/fix)

### Code Review Focus

- Verify alignment with Eclipse MOSAIC patterns
- Check configuration backward compatibility
- Ensure processor implementations follow existing contracts
- Validate database migrations if schema changes
- Test with BeST scenario if available

## AI & Assistant Notes

- **No Cursor/Copilot rules**: This document is the authoritative source
- **Run commands directly**: No pseudocode; execute actual Maven/Python commands
- **Reference files**: Use workspace-relative paths (e.g., `src/main/java/com/dcaiti/mosaic/app/tse/...`)
- **Prefer structured tools**: Use Read/Edit/Write tools over ad-hoc shell commands
- **Default to action**: Execute unless blocked by destructive changes or ambiguity
- **Align with Eclipse MOSAIC**: Follow patterns from parent project (see `../mosaic/AGENTS.md`)
- **Test-driven**: Write/update tests when implementing features
- **Configuration compatibility**: Preserve backward compatibility for JSON configs
