# PostgreSQL MCP Server - Usage Guide

## Overview

This PostgreSQL MCP Server provides a comprehensive, production-ready interface for PostgreSQL database management through the Model Context Protocol (MCP). It follows a sophisticated enterprise-grade architecture with modular design patterns.

## Architecture Overview

### PostgreSQL MCP Architecture Components

| Component | Implementation | Purpose |
|-----------|----------------|---------|
| **Configuration** | Multi-source config with validation | Database connection + pool config |
| **Base Infrastructure** | Database connection manager, pooling | Connection management + optimization |
| **Domain APIs** | Query, Tables, Schemas, Indexes, etc. | Comprehensive database operations |
| **Authentication** | Database credentials | Secure database access |
| **Tools Count** | 9 database-focused tools | Complete database management |
| **Validation** | SQL validation + type checking | Query safety + data integrity |
| **Error Handling** | Database-specific error handling | Robust error management |
| **Caching** | Query result caching | Performance optimization |
| **Monitoring** | Connection pool + query metrics | Operational visibility |

## Features

### 🔧 **Database Operations**
- **Query Execution**: Single queries, transactions, parameterized queries
- **Query Analysis**: EXPLAIN plans, performance analysis, syntax validation
- **Active Query Management**: List, monitor, and cancel running queries

### 🗃️ **Schema Management**
- **Tables**: Create, alter, drop, list tables with detailed metadata
- **Schemas**: Create, drop, manage database schemas
- **Indexes**: Create, drop, analyze index usage and performance
- **Constraints**: Primary keys, foreign keys, check constraints

### 📊 **Data Operations**
- **CRUD Operations**: Insert, update, delete with validation
- **Bulk Operations**: Batch inserts, bulk updates
- **Data Import/Export**: CSV, JSON data exchange

### 🔒 **Transaction Management**
- **ACID Transactions**: Begin, commit, rollback with isolation levels
- **Savepoints**: Nested transaction control
- **Read-only Transactions**: Safe query execution

### 👥 **Administration**
- **User Management**: Create, drop users, manage permissions
- **Database Maintenance**: VACUUM, ANALYZE, REINDEX operations
- **Security**: Role-based access control, permission management

### 📈 **Monitoring & Analytics**
- **Connection Pool**: Real-time pool statistics and health
- **Performance Metrics**: Query statistics, execution times
- **Resource Usage**: Disk usage, connection counts, lock analysis

## Tools Available

### 1. **`query`** - SQL Query Execution
```json
{
  "action": "execute",
  "sql": "SELECT * FROM users WHERE created_at > $1",
  "parameters": ["2024-01-01"],
  "options": {
    "limit": 100,
    "timeout": 30000
  }
}
```

**Actions:**
- `execute` - Run single SQL query
- `transaction` - Execute multiple queries in transaction
- `explain` - Get query execution plan
- `analyze` - Performance analysis with timing
- `validate` - Syntax validation without execution
- `active` - List currently running queries
- `cancel` - Cancel query by process ID

### 2. **`tables`** - Table Management
```json
{
  "action": "create",
  "tableName": "users",
  "columns": [
    {"name": "id", "type": "SERIAL", "primaryKey": true},
    {"name": "email", "type": "VARCHAR(255)", "nullable": false},
    {"name": "created_at", "type": "TIMESTAMP", "defaultValue": "CURRENT_TIMESTAMP"}
  ],
  "options": {"ifNotExists": true}
}
```

**Actions:**
- `list` - List all tables with metadata
- `info` - Detailed table information (columns, indexes, constraints)
- `create` - Create new table with columns
- `drop` - Remove table (with cascade option)
- `add_column` - Add column to existing table
- `rename` - Rename table or column

### 3. **`schemas`** - Schema Management
```json
{
  "action": "create",
  "schemaName": "analytics",
  "owner": "analyst_user",
  "options": {"ifNotExists": true}
}
```

### 4. **`indexes`** - Index Management
```json
{
  "action": "create",
  "tableName": "users",
  "indexName": "idx_users_email",
  "columns": ["email"],
  "options": {"unique": true, "concurrent": true}
}
```

### 5. **`data`** - Data Operations
```json
{
  "action": "insert",
  "tableName": "users",
  "data": {
    "email": "user@example.com",
    "name": "John Doe"
  },
  "options": {"returning": ["id", "created_at"]}
}
```

### 6. **`transactions`** - Transaction Control
```json
{
  "action": "begin",
  "readOnly": false,
  "isolationLevel": "READ COMMITTED"
}
```

### 7. **`admin`** - Database Administration
```json
{
  "operation": "create_user",
  "username": "analyst",
  "password": "secure_password",
  "permissions": ["SELECT", "INSERT"]
}
```

### 8. **`monitoring`** - Performance Monitoring
```json
{
  "metric": "performance",
  "timeRange": "24h",
  "limit": 50
}
```

### 9. **`connections`** - Connection Pool Management
```json
{
  "action": "stats"
}
```

## Configuration

### Environment Variables (.env)
```bash
# Database Connection
DATABASE_URL=postgresql://user:password@localhost:5432/dbname
# OR individual settings:
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=mypassword
POSTGRES_DATABASE=mydb
POSTGRES_SSL=false

# Connection Pool
POOL_MIN=2
POOL_MAX=10
POOL_IDLE_TIMEOUT=30000

# Security
READ_ONLY_MODE=false
MAX_QUERY_TIME=30000
ALLOWED_SCHEMAS=public,analytics
RESTRICTED_TABLES=sensitive_data

# Performance
CACHE_ENABLED=true
CACHE_TTL=300000
SQL_LOGGING=true
LOG_LEVEL=info
```

## Security Features

### 🔒 **Access Control**
- **Schema Filtering**: Restrict access to specific schemas
- **Table Restrictions**: Block access to sensitive tables
- **Read-only Mode**: Prevent write operations globally
- **Query Timeout**: Prevent long-running queries

### 🛡️ **SQL Injection Prevention**
- **Parameterized Queries**: All user input properly escaped
- **Query Validation**: Syntax checking before execution
- **Dangerous Pattern Detection**: Block DROP DATABASE, etc. in production

### 🔍 **Audit & Monitoring**
- **Query Logging**: All SQL operations logged with context
- **Performance Tracking**: Query execution times and statistics
- **Connection Monitoring**: Track database connections and pool usage

## Installation & Setup

### 1. Install Dependencies
```bash
npm install
```

### 2. Configure Environment
```bash
cp .env.example .env
# Edit .env with your database settings
```

### 3. Test Connection
```bash
npm test
```

### 4. Build & Start
```bash
npm run build
npm start
```

## Development Commands

```bash
# Development
npm run dev          # Start in development mode
npm run watch        # Watch for changes and rebuild
npm run build        # Build for production

# Testing
npm test             # Test database connection
npm run test:queries # Test query operations
npm run test:jest    # Run unit tests

# Code Quality
npm run lint         # Lint TypeScript code
npm run format       # Format code with Prettier
npm run clean        # Clean build directory
```

## Resource Discovery

The MCP server also provides resource discovery for database schemas:

- **List Resources**: GET `/postgres://{schema}/{table}/schema`
- **Read Schema**: Detailed table structure, columns, indexes, constraints

## Error Handling

The server provides comprehensive error handling:

- **Validation Errors**: Parameter validation with helpful suggestions
- **Database Errors**: PostgreSQL error translation and context
- **Connection Errors**: Pool management and retry logic
- **Timeout Errors**: Query timeout handling and cancellation

## Performance Features

### 🚀 **Connection Pooling**
- **Smart Pool Management**: Min/max connections with auto-scaling
- **Connection Reuse**: Efficient connection lifecycle management
- **Health Monitoring**: Connection health checks and recovery

### ⚡ **Query Optimization**
- **Query Analysis**: EXPLAIN plan generation and analysis
- **Performance Metrics**: Execution time tracking and statistics
- **Index Usage**: Index effectiveness monitoring

### 🧠 **Caching**
- **Result Caching**: Cache frequently accessed data
- **Schema Caching**: Cache table structures and metadata
- **TTL Management**: Automatic cache expiration and invalidation

## Comparison with Simple Postgres MCP

| Feature | Simple Postgres MCP | This Postgres MCP |
|---------|-------------------|------------------|
| **Lines of Code** | ~143 lines | ~3000+ lines |
| **Architecture** | Single file | Modular, layered |
| **Tools** | 1 basic tool | 9 comprehensive tools |
| **Validation** | Basic | Comprehensive with suggestions |
| **Error Handling** | Simple | Rich context and recovery |
| **Connection Management** | Basic pool | Advanced pool with monitoring |
| **Security** | Read-only transactions | Full security framework |
| **Monitoring** | None | Comprehensive metrics |
| **Transactions** | Basic | Full ACID with savepoints |
| **Resource Discovery** | Basic schema | Full metadata |

This PostgreSQL MCP server provides enterprise-grade database management capabilities through the MCP protocol, making it suitable for production AI agent workflows requiring sophisticated database operations.