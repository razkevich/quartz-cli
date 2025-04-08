# Quartz CLI

A simple, fast command-line interface for Quartz Scheduler database connections.

## Features

- Connect to a Quartz scheduler database
- List all jobs and triggers
- View detailed information for specific jobs and triggers
- Start/pause schedulers
- Support for PostgreSQL, MySQL, and Oracle databases
- Schema and table prefix customization

## Build

```bash
# Build the package including dependencies
mvn clean package assembly:single
```

This will create a self-contained JAR file in the `target` directory named `quartz-repl-0.0.1-SNAPSHOT-jar-with-dependencies.jar`.

## Usage

```bash
# Basic usage format
java -jar quartz-repl-0.0.1-SNAPSHOT-jar-with-dependencies.jar [options]
```

### Command-line Options

```
-u, --url=<jdbcUrl>        JDBC URL for Quartz database
-U, --user=<username>      Database username
-P, --password=<password>  Database password
-d, --driver=<driver>      JDBC driver class name (default: org.postgresql.Driver)
-s, --schema=<schema>      Database schema containing Quartz tables
-p, --prefix=<tablePrefix> Quartz table prefix (default: QRTZ_)
-c, --command=<command>    Command to execute (info, list-jobs, list-triggers, etc.)
-g, --group=<group>        Group filter for jobs or triggers
-v, --verbose              Enable verbose output with connection details
-h, --help                 Show help message
-V, --version              Display version information
```

### Command Examples

#### Get Scheduler Information

```bash
java -jar quartz-repl-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  -u "jdbc:postgresql://localhost:5432/mydb" -U username -P password -s myschema -c info
```

#### List Jobs

```bash
# List all jobs
java -jar quartz-repl-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  -u "jdbc:postgresql://localhost:5432/mydb" -U username -P password -s myschema -c list-jobs

# List jobs in a specific group
java -jar quartz-repl-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  -u "jdbc:postgresql://localhost:5432/mydb" -U username -P password -s myschema -c list-jobs -g MYGROUP
```

#### List Triggers

```bash
# List all triggers
java -jar quartz-repl-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  -u "jdbc:postgresql://localhost:5432/mydb" -U username -P password -s myschema -c list-triggers

# List triggers in a specific group
java -jar quartz-repl-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  -u "jdbc:postgresql://localhost:5432/mydb" -U username -P password -s myschema -c list-triggers -g MYGROUP
```

#### Get Job Details

```bash
# Format: [group].[name]
java -jar quartz-repl-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  -u "jdbc:postgresql://localhost:5432/mydb" -U username -P password -s myschema -c get-job DEFAULT.myJob
```

#### Get Trigger Details

```bash
# Format: [group].[name]
java -jar quartz-repl-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  -u "jdbc:postgresql://localhost:5432/mydb" -U username -P password -s myschema -c get-trigger DEFAULT.myTrigger
```

#### Start Scheduler

```bash
java -jar quartz-repl-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  -u "jdbc:postgresql://localhost:5432/mydb" -U username -P password -s myschema -c start
```

#### Pause Scheduler

```bash
java -jar quartz-repl-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  -u "jdbc:postgresql://localhost:5432/mydb" -U username -P password -s myschema -c pause
```

## Examples

Connect to a PostgreSQL database and list all jobs:

```bash
java -jar quartz-cli.jar list-jobs \
  -u "jdbc:postgresql://localhost:5432/mydb" \
  -U myuser \
  -P mypassword \
  -s public
```

