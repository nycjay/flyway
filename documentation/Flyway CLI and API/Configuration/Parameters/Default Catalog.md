---
pill: defaultCatalog
subtitle: flyway.defaultCatalog
redirect_from: Configuration/defaultCatalog/
---

# Default Catalog

## Description
The default catalog managed by Flyway. This catalog will be the one containing the [schema history table](Concepts/migrations#schema-history-table).

This catalog will also be the default for the database connection (provided the database supports this concept).

## Default
The database's default catalog.

## Usage

### Commandline
```powershell
./flyway -defaultCatalog="catalog2" info
```

### Configuration File
```properties
flyway.defaultCatalog=catalog2
```

### Environment Variable
```properties
FLYWAY_DEFAULT_CATALOG=catalog2
```

### API
```java
Flyway.configure()
    .defaultCatalog("catalog2")
    .load()
```

### Gradle
```groovy
flyway {
    defaultCatalog = 'catalog2'
}
```

### Maven
```xml
<configuration>
    <defaultCatalog>catalog2</defaultCatalog>
</configuration>
```
