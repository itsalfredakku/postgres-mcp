# Postgres MCP Server

MCP server for PostgreSQL database management and operations, built with a sophisticated enterprise-grade architecture.

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

## Configuration

Create a `.env` file:

```env
# Required
DATABASE_URL=postgresql://username:password@localhost:5432/dbname

# Optional
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=
POSTGRES_DATABASE=postgres
POSTGRES_SSL=false

# Connection Pool Settings
POOL_MIN=2
POOL_MAX=10
POOL_IDLE_TIMEOUT=30000
POOL_ACQUIRE_TIMEOUT=60000

# Cache Settings
CACHE_ENABLED=true
CACHE_TTL=300000

# Logging
LOG_LEVEL=info
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
- `query` - Execute SQL queries with transaction support
- `tables` - List, create, alter, drop tables
- `indexes` - Manage database indexes
- `schemas` - Schema management operations

### Data Management
- `data` - Insert, update, delete operations
- `import_export` - Data import/export operations
- `backup` - Database backup and restore

### Administration
- `admin` - User management, permissions, database admin
- `monitoring` - Performance metrics and analysis
- `connections` - Connection pool management

## Architecture

The server follows a modular architecture with:

- **Configuration Management** - Environment and file-based configuration
- **Connection Pooling** - Advanced PostgreSQL connection management
- **Domain APIs** - Separated concerns for different database operations
- **Validation** - Comprehensive parameter validation
- **Error Handling** - Robust error handling with retries
- **Caching** - Intelligent caching for performance
- **Logging** - Structured logging with Winston

## License

MIT