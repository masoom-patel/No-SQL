# NoSQL Database - File-Based Document Storage System

## Overview

A Python-based NoSQL database that stores documents as JSON files with SQL-like query capabilities and an interactive command-line interface.

## Installation

```bash
pip install tabulate colorama
```

Required files:
- `nosql_database.py` - Core database implementation
- `query_executor.py` - Interactive CLI tool

## Quick Start

### Programmatic Usage
```python
from nosql_database import NoSQLDatabase

# Initialize database
db = NoSQLDatabase("./my_database")

# Create container and insert document
db.create_container("users")
db.insert_document("users", "user_1", {"name": "Alice", "age": 30})

# Query data
results = db.execute_sql_like_query("SELECT * FROM users WHERE age > 25")
```

### Interactive CLI
```bash
# Start interactive mode
python query_executor.py ./my_database

# Single query
python query_executor.py ./my_database -q "SELECT * FROM users"

# Execute from file
python query_executor.py ./my_database -f queries.sql
```

## Database Structure

- **Database**: Root folder containing all containers
- **Containers**: Subfolders representing collections/tables  
- **Documents**: Individual JSON files with automatic metadata

```
my_database/
├── users/
│   ├── user_1.json
│   └── user_2.json
└── products/
    └── product_1.json
```

Each document includes automatic metadata:
```json
{
  "_id": "user_1",
  "_created_at": "2024-06-13T10:30:00.123456",
  "_updated_at": "2024-06-13T10:30:00.123456",
  "name": "Alice",
  "age": 30
}
```

## Core API Reference

### Container Management
```python
# Create/delete containers
db.create_container("users")
db.delete_container("users")

# List and get info
containers = db.list_containers()
info = db.get_containers_info()

# Set working container
db.use_container("users")

# Analyze schema
schema = db.describe_container("users")
```

### Document Operations
```python
# Insert document
db.insert_document("users", "user_1", {"name": "Alice", "age": 30})

# Get document
user = db.get_document("users", "user_1")

# Update document
db.update_document("users", "user_1", {"age": 31})

# Delete document
db.delete_document("users", "user_1")

# Get all documents
all_users = db.get_all_documents("users")
```

### Query Operations
```python
# SQL-like queries
results = db.execute_sql_like_query("SELECT * FROM users WHERE age > 25")
results = db.execute_sql_like_query("SELECT name, age FROM users WHERE city LIKE 'New'")

# Programmatic queries
users = db.select(
    container_name="users",
    where_conditions=[("age", ">", 25)],
    fields=["name", "age"],
    limit=10,
    order_by="age"
)

# Count documents
count = db.count("users", [("age", ">", 25)])
```

### Data Import/Export
```python
# Export data
db.export_data("users", "./exports/users.json")  # Single file
db.export_data("users", "./exports/")            # Folder
db.export_data("all", "./exports/")              # All containers

# Import data
db.import_data("./data/users.json", "users")     # Single file
db.import_data("./data/")                        # Folder

# Backup
db.backup_database("./backup_20240613")
```

## Query Executor Commands

### Starting the Executor
```bash
NoSQL> help                           # Show help
NoSQL> show containers                # List containers
NoSQL> use users                      # Switch container
NoSQL:users> describe users           # Show schema
```

### SQL Queries
```sql
-- Select operations
SELECT * FROM users
SELECT name, email FROM users WHERE age > 25
SELECT * FROM users WHERE city LIKE 'New' ORDER BY age LIMIT 10

-- Insert operations  
INSERT INTO users VALUES ('user_123', '{"name": "John", "age": 30}')
```

### Management Commands
```bash
# Container operations
create container users
drop container users
show containers
show documents users

# Document operations
insert users user_1 '{"name": "Alice", "age": 30}'
update users user_1 '{"age": 31}'
delete users user_1
count users WHERE age > 25

# Data operations
export users ./backup/
import users ./data/users.json
backup ./full_backup/

# Utility
history                    # Show query history
clear                      # Clear screen
exit                       # Exit executor
```

## Query Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equal to | `age = 30` |
| `!=` | Not equal | `status != 'inactive'` |
| `>`, `<` | Greater/Less than | `price > 100` |
| `>=`, `<=` | Greater/Less equal | `age >= 18` |
| `LIKE` | Contains (case-insensitive) | `name LIKE 'John'` |
| `IN` | Value in list | `category IN ['tech', 'science']` |

## Result Format

All operations return structured results:
```python
{
    "success": True,
    "type": "select",
    "results": [...],     # List of documents
    "count": 5,          # Number of results
    "message": "Success"
}
```

## Advanced Features

### Schema Analysis
```python
schema_info = db.describe_container("users")
# Returns document count and field analysis with types and coverage percentages
```

### Batch Execution
Create `queries.sql`:
```sql
CREATE CONTAINER users;
INSERT INTO users VALUES ('user_1', '{"name": "Alice", "age": 30}');
SELECT * FROM users;
```

Execute:
```bash
python query_executor.py ./my_database -f queries.sql
```

### Query History
```python
# Get recent queries
history = db.get_query_history(10)

# Clear history
db.clear_history()
```

### Table Formatting
```python
results = db.select("users")
table = db.format_results_as_table(results, "User List")
print(table)
```

## Error Handling

All operations return success status:
```python
result = db.create_container("users")
if result["success"]:
    print(result["message"])
else:
    print(f"Error: {result['message']}")
```

The Query Executor provides colored output:
- **Red**: Errors
- **Green**: Success messages  
- **Yellow**: Warnings
- **Cyan**: Information

## Complete Example

```python
# Initialize
db = NoSQLDatabase("./ecommerce_db")

# Setup
db.create_container("users")
db.create_container("products")

# Insert data
users = [
    {"name": "Alice", "age": 30, "email": "alice@example.com"},
    {"name": "Bob", "age": 25, "email": "bob@example.com"}
]

for i, user in enumerate(users):
    db.insert_document("users", f"user_{i+1}", user)

# Query and display
young_users = db.execute_sql_like_query("SELECT * FROM users WHERE age < 28")
print(db.format_results_as_table(young_users["results"]))

# Export for backup
db.export_data("all", "./exports/")
```

## CLI Example Session

```bash
python query_executor.py ./ecommerce_db

NoSQL> create container users
NoSQL> insert users user_1 '{"name": "Alice", "age": 30}'
NoSQL> SELECT * FROM users WHERE age > 25
NoSQL> export all ./backup/
NoSQL> show containers
NoSQL> exit
```
