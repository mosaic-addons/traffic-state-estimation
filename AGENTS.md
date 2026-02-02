# Agent Guidelines for traffic-state-estimation

## Build and Test Commands

### Maven Build Commands
```bash
# Full clean build
mvn clean install

# Package JAR only
mvn package

# Build without running tests
mvn clean install -DskipTests

# Run all tests
mvn test

# Run a single test class
mvn test -Dtest=ClassName

# Run a single test method
mvn test -Dtest=ClassName#methodName
```

### Python Evaluation Commands
```bash
# Install dependencies
python -m pip install -r evaluation/requirements.txt

# Run Jupyter notebooks
jupyter notebook evaluation/notebooks/
```

## Code Style Guidelines

### Java Code Style (Google Java Style Guide)

**Indentation & Formatting:**
- 4-space indentation (NO tabs)
- One statement per line
- Column limit: 140 characters
- UTF-8 file encoding

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

**Whitespace:**
- Space after reserved words: `if (`, `for (`, `catch (`
- Space before/after operators: `a == 1 && b == 3`
- Space after closing paren of cast: `((Target) object)`

### Imports
```java
// 1. org.eclipse.mosaic imports
import org.eclipse.mosaic.lib.database.Database;
import org.eclipse.mosaic.lib.util.gson.TimeFieldAdapter;

// 2. Third-party libraries
import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.ToStringBuilder;

// 3. Standard libraries
import java.util.List;
import java.util.Map;
```
- NO wildcard imports (e.g., `import java.util.*`)
- Blank line between import groups

### Naming Conventions

| Element          | Pattern        | Example           |
|------------------|----------------|-------------------|
| Class            | UpperCamelCase | TseServerApp      |
| Interface        | UpperCamelCase | DatabaseAccess    |
| Enum             | UpperCamelCase | ErrorLevel        |
| Enum value       | UPPER_CASE     | FATAL             |
| Field            | lowerCamelCase | fcdDataStorage    |
| Final field      | lowerCamelCase | maximumItems      |
| Static const     | UPPER_CASE     | MIN_SAMPLES       |
| Variable         | lowerCamelCase | listOfValues      |
| Method           | lowerCamelCase | toString          |
| Package          | lowercase      | com.dcaiti.mosaic |
| Parameter        | lowerCamelCase | typeName          |
| Type parameter   | UPPER_CASE     | T                 |

**Rules:**
- Interfaces should NOT use 'I' prefix (e.g., use `Vehicle`, NOT `IVehicle`)
- Abbreviations: only first letter capitalized (e.g., `V2xMessage`, not `V2XMessage`)
- Boolean methods: start with `is` or `has` (e.g., `isValid()`, `hasData()`)

### File Structure

Every Java file MUST have the license header:
```java
/*
 * Copyright (c) 2023 Fraunhofer FOKUS and others. All rights reserved.
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contact: mosaic@fokus.fraunhofer.de
 */
```

### Documentation
- Use Javadoc (`/** ... */`) for all public classes, constructors, and methods
- Use `@param` and `@return` annotations
- Getters/Setters do NOT require method comments
- Inline comments use `//` and explain WHY, not WHAT

### Error Handling
- Use null checks with ternary operators: `value = config.param == null ? defaultValue : config.param`
- Log errors using `logger.error()` or `logger.info()`
- Use `UnitLogger` for application logging

### Type Safety
- Use generics with proper type parameters
- Avoid raw types (use `List<String>`, not `List`)
- Use `@SuppressWarnings` sparingly with justification

### Project Structure
- `com.dcaiti.mosaic.app.fxd` - FCD message transmission/reception
- `com.dcaiti.mosaic.app.tse` - Traffic state estimation logic
- `processors/` - Data processing (TimeBased, TraversalBased, MessageBased)
- `persistence/` - Database access (FcdDataStorage, DatabaseHelper classes)
- `data/` - Data models and interfaces
- `config/` - Configuration classes (prefixed with 'C', e.g., `CTseServerApp`)

### Python Code Style (evaluation/)
- Function names: `snake_case`
- Constants: `UPPER_CASE`
- Use type hints for parameters and return types
- Docstrings in NumPy/Google style
- Imports: standard library, third-party, local modules

### Quality Checks
Before committing:
1. `mvn clean install` must pass
2. No new SpotBugs warnings
3. Code must pass Checkstyle verification
4. No new `FIXME`/`TODO` comments
5. New files must include license header
