# Postgres MCP Server

MCP server for PostgreSQL database management and operations, built with a sophisticated enterprise-grade architecture.

## Quick Setup

### 1. Installation
```bash
npm install
npm run build
```

### 2. Claude Desktop Configuration

Add this to your Claude Desktop `claude_desktop_config.json`:

**Windows:**
```json
{
  "mcpServers": {
    "postgres": {
      "command": "node",
      "args": ["C:\\path\\to\\postgres-mcp\\dist\\index.js"],
      "env": {
        "DATABASE_URL": "postgresql://username:password@localhost:5432/dbname"
      }
    }
  }
}
```

**macOS/Linux:**
```json
{
  "mcpServers": {
    "postgres": {
      "command": "node",
      "args": ["/path/to/postgres-mcp/dist/index.js"],
      "env": {
        "DATABASE_URL": "postgresql://username:password@localhost:5432/dbname"
      }
    }
  }
}
```

### 3. Environment Configuration

**Option A: Via Claude Desktop config (recommended)**
```json
{
  "mcpServers": {
    "postgres": {
      "command": "node",
      "args": ["/Users/itsalfredakku/McpServers/postgres-mcp/dist/index.js"],
      "env": {
        "DATABASE_URL": "postgresql://postgres:password@localhost:5432/mydb",
        "POOL_MAX": "20",
        "LOG_LEVEL": "info"
      }
    }
  }
}
```

**Option B: Using .env file**
Create `.env` in the project root:
```env
DATABASE_URL=postgresql://username:password@localhost:5432/dbname
POOL_MAX=10
LOG_LEVEL=info
```

## Features

- **Database Operations**: Query, insert, update, delete operations
- **Schema Management**: Create, alter, drop tables and indexes
- **Transaction Management**: Begin, commit, rollback transactions
- **Connection Management**: Advanced connection pooling
- **Data Management**: Import/export, backup/restore operations
- **Monitoring**: Performance metrics and query analysis
- **Admin Operations**: User management, permissions, database administration

## Installation

```bash
npm install
```

## Configuration Options

### Database Connection
```env
# Required - Primary connection string
DATABASE_URL=postgresql://username:password@localhost:5432/dbname

# Alternative - Individual connection parameters
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_DATABASE=your_database
POSTGRES_SSL=false
```

### Connection Pool Settings
```env
POOL_MIN=2                    # Minimum connections
POOL_MAX=10                   # Maximum connections  
POOL_IDLE_TIMEOUT=30000       # Idle timeout (ms)
POOL_ACQUIRE_TIMEOUT=60000    # Acquire timeout (ms)
```

### Performance & Caching
```env
CACHE_ENABLED=true            # Enable query result caching
CACHE_TTL=300000             # Cache TTL (ms)
LOG_LEVEL=info               # Logging level (error|warn|info|debug)
SQL_LOGGING=false            # Log SQL queries
```

## Usage

### Development
```bash
npm run dev
```

### Production
```bash
npm run build
npm start
```

### Testing
```bash
npm run test
npm run test:queries
```

## Tools

### Database Operations
- `query` - Execute SQL queries with transaction support, explain plans, analysis
- `tables` - List, create, alter, drop tables with detailed metadata
- `schemas` - **FULLY IMPLEMENTED** Create, drop, list schemas and manage permissions
- `indexes` - **FULLY IMPLEMENTED** Create, drop, analyze, reindex with usage statistics

### Data Management
- `data` - Insert, update, delete operations with bulk support
- `transactions` - Begin, commit, rollback with savepoint support

### Administration & Security
- `admin` - **FULLY IMPLEMENTED** Complete database administration and maintenance
- `permissions` - Complete user/role/privilege management
- `security` - SSL, authentication, encryption, auditing
- `monitoring` - Performance metrics and analysis
- `connections` - Connection pool management

### Schema Management Features ✅
- **Schema Operations**: Create, drop, list all schemas
- **Permission Management**: View and manage schema-level permissions
- **Owner Management**: Set schema ownership during creation
- **Conditional Operations**: IF EXISTS, IF NOT EXISTS support
- **System Schema Filtering**: Distinguish between user and system schemas

### Index Management Features ✅
- **Index Operations**: Create, drop, list, reindex indexes
- **Performance Analysis**: Analyze index usage statistics
- **Unused Index Detection**: Find indexes that are never used
- **Multiple Index Types**: Support for btree, hash, gist, gin, brin
- **Concurrent Operations**: Create and reindex with CONCURRENTLY
- **Size Monitoring**: Index size tracking and reporting

### Database Administration Features ✅
- **Database Information**: Complete database stats and configuration
- **User Management**: Create, drop, list users with detailed privileges
- **Permission Control**: Grant/revoke permissions on tables and schemas
- **Maintenance Operations**: VACUUM, ANALYZE, REINDEX with options
- **System Monitoring**: Connection counts, database size, uptime tracking
- **Configuration Access**: View database settings and parameters

## Architecture

The server follows a modular architecture with:

- **Configuration Management** - Environment and file-based configuration
- **Connection Pooling** - Advanced PostgreSQL connection management
- **Domain APIs** - Separated concerns for different database operations
- **Validation** - Comprehensive parameter validation
- **Error Handling** - Robust error handling with retries
- **Caching** - Intelligent caching for performance
- **Logging** - Structured logging with Winston

## Troubleshooting

### Common Issues

**Connection Refused**
```bash
# Check if PostgreSQL is running
brew services list | grep postgresql
# or
sudo systemctl status postgresql

# Test connection manually
psql -h localhost -p 5432 -U postgres -d your_database
```

**Permission Denied**
```sql
-- Grant necessary permissions
GRANT CONNECT ON DATABASE your_database TO your_user;
GRANT USAGE ON SCHEMA public TO your_user;
GRANT CREATE ON SCHEMA public TO your_user;
```

**MCP Server Not Found**
- Ensure the path in `claude_desktop_config.json` is absolute
- Verify `npm run build` completed successfully
- Check that `dist/index.js` exists

### Debug Mode
Set environment variables for detailed logging:
```json
{
  "mcpServers": {
    "postgres": {
      "command": "node",
      "args": ["/path/to/postgres-mcp/dist/index.js"],
      "env": {
        "DATABASE_URL": "postgresql://user:pass@localhost:5432/db",
        "LOG_LEVEL": "debug",
        "SQL_LOGGING": "true"
      }
    }
  }
}
```

## Database Permissions Setup

### Full Admin Access
For complete database management capabilities, ensure your PostgreSQL user has appropriate privileges:

```sql
-- Connect as superuser (postgres)
psql -U postgres

-- Create a dedicated MCP user with admin privileges
CREATE USER mcp_admin WITH PASSWORD 'secure_password';
ALTER USER mcp_admin SUPERUSER;
ALTER USER mcp_admin CREATEDB;
ALTER USER mcp_admin CREATEROLE;
ALTER USER mcp_admin REPLICATION;

-- Or grant specific privileges without superuser
CREATE USER mcp_user WITH PASSWORD 'secure_password';
GRANT ALL PRIVILEGES ON DATABASE your_database TO mcp_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO mcp_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO mcp_user;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO mcp_user;

-- Grant schema usage and creation
GRANT USAGE, CREATE ON SCHEMA public TO mcp_user;

-- Allow user management (requires elevated privileges)
ALTER USER mcp_user CREATEROLE;
```

### Using MCP Permission Tools
Once connected, you can use the MCP server to manage permissions:

```typescript
// List all users and their privileges
await mcpServer.callTool('permissions', { operation: 'list_users' });

// Create a new user
await mcpServer.callTool('permissions', { 
  operation: 'create_user', 
  username: 'newuser', 
  password: 'password123',
  attributes: { createdb: true, login: true }
});

// Grant all privileges to a user
await mcpServer.callTool('permissions', { 
  operation: 'grant_all_privileges', 
  username: 'newuser', 
  database: 'mydatabase' 
});

// Check user permissions
await mcpServer.callTool('permissions', { 
  operation: 'check_permissions', 
  username: 'newuser' 
});
```

## License

MIT