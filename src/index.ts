#!/usr/bin/env node

// Set MCP server mode FIRST, before any imports that use logger
process.env.MCP_SERVER = 'true';

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ErrorCode,
  ListResourcesRequestSchema,
  ListToolsRequestSchema,
  ReadResourceRequestSchema,
  McpError,
} from '@modelcontextprotocol/sdk/types.js';
import dotenv from 'dotenv';

import { ConfigManager } from './config.js';
import { DatabaseConnectionManager } from './database/connection-manager.js';
import { QueryAPIClient } from './api/domains/query-api.js';
import { TablesAPIClient } from './api/domains/tables-api.js';
import { logger } from './logger.js';
import { 
  ParameterValidator, 
  ValidationError, 
  TOOL_NAME_MAPPINGS, 
  suggestToolName
} from './validation.js';

// Load environment variables
dotenv.config();

// Tool definitions with comprehensive database management capabilities
const toolDefinitions = [
  // QUERY EXECUTION TOOL
  {
    name: 'query',
    description: 'Execute SQL queries with transaction support, query analysis, and performance monitoring',
    inputSchema: {
      type: 'object',
      properties: {
        action: {
          type: 'string',
          enum: ['execute', 'transaction', 'explain', 'analyze', 'validate', 'cancel', 'active'],
          description: 'Action: execute (single query), transaction (multiple queries), explain (execution plan), analyze (performance), validate (syntax), cancel (query by PID), active (list active queries)'
        },
        sql: {
          type: 'string',
          description: 'SQL query to execute (required for execute, explain, analyze, validate actions)'
        },
        parameters: {
          type: 'array',
          items: { type: 'string' },
          description: 'Query parameters for parameterized queries'
        },
        queries: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              sql: { type: 'string' },
              parameters: { type: 'array', items: { type: 'string' } }
            },
            required: ['sql']
          },
          description: 'Array of queries for transaction action'
        },
        options: {
          type: 'object',
          properties: {
            timeout: { type: 'integer', description: 'Query timeout in milliseconds' },
            limit: { type: 'integer', description: 'Maximum number of rows to return' },
            offset: { type: 'integer', description: 'Number of rows to skip' },
            readOnly: { type: 'boolean', description: 'Execute as read-only transaction' }
          },
          description: 'Query execution options'
        },
        pid: {
          type: 'integer',
          description: 'Process ID of query to cancel (required for cancel action)'
        }
      },
      required: ['action']
    }
  },

  // TABLE MANAGEMENT TOOL
  {
    name: 'tables',
    description: 'Table management: list, create, alter, drop tables and get detailed table information',
    inputSchema: {
      type: 'object',
      properties: {
        action: {
          type: 'string',
          enum: ['list', 'info', 'create', 'drop', 'add_column', 'drop_column', 'rename'],
          description: 'Action: list (all tables), info (table details), create (new table), drop (remove table), add_column (add column), drop_column (remove column), rename (rename table)'
        },
        schemaName: {
          type: 'string',
          description: 'Schema name (default: public)',
          default: 'public'
        },
        tableName: {
          type: 'string',
          description: 'Table name (required for info, create, drop, add_column, drop_column, rename)'
        },
        columns: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              name: { type: 'string' },
              type: { type: 'string' },
              nullable: { type: 'boolean', default: true },
              defaultValue: { type: 'string' },
              primaryKey: { type: 'boolean', default: false }
            },
            required: ['name', 'type']
          },
          description: 'Column definitions for create action'
        },
        columnName: {
          type: 'string',
          description: 'Column name (required for add_column, drop_column)'
        },
        dataType: {
          type: 'string',
          description: 'Data type (required for add_column)'
        },
        newName: {
          type: 'string',
          description: 'New name (required for rename action)'
        },
        options: {
          type: 'object',
          properties: {
            includeViews: { type: 'boolean', default: false },
            includeSystemTables: { type: 'boolean', default: false },
            ifNotExists: { type: 'boolean', default: false },
            ifExists: { type: 'boolean', default: true },
            cascade: { type: 'boolean', default: false },
            temporary: { type: 'boolean', default: false }
          },
          description: 'Action-specific options'
        }
      },
      required: ['action']
    }
  },

  // SCHEMA MANAGEMENT TOOL
  {
    name: 'schemas',
    description: 'Schema management: list, create, drop schemas and manage schema permissions',
    inputSchema: {
      type: 'object',
      properties: {
        action: {
          type: 'string',
          enum: ['list', 'create', 'drop', 'permissions'],
          description: 'Action: list (all schemas), create (new schema), drop (remove schema), permissions (schema permissions)'
        },
        schemaName: {
          type: 'string',
          description: 'Schema name (required for create, drop, permissions)'
        },
        owner: {
          type: 'string',
          description: 'Schema owner (for create action)'
        },
        options: {
          type: 'object',
          properties: {
            ifNotExists: { type: 'boolean', default: false },
            ifExists: { type: 'boolean', default: true },
            cascade: { type: 'boolean', default: false }
          },
          description: 'Action-specific options'
        }
      },
      required: ['action']
    }
  },

  // INDEX MANAGEMENT TOOL
  {
    name: 'indexes',
    description: 'Index management: list, create, drop indexes and analyze index usage',
    inputSchema: {
      type: 'object',
      properties: {
        action: {
          type: 'string',
          enum: ['list', 'create', 'drop', 'analyze', 'reindex', 'unused'],
          description: 'Action: list (all indexes), create (new index), drop (remove index), analyze (index statistics), reindex (rebuild index), unused (find unused indexes)'
        },
        schemaName: {
          type: 'string',
          description: 'Schema name (default: public)',
          default: 'public'
        },
        tableName: {
          type: 'string',
          description: 'Table name (required for create, list by table)'
        },
        indexName: {
          type: 'string',
          description: 'Index name (required for drop, reindex)'
        },
        columns: {
          type: 'array',
          items: { type: 'string' },
          description: 'Column names for index (required for create)'
        },
        options: {
          type: 'object',
          properties: {
            unique: { type: 'boolean', default: false },
            concurrent: { type: 'boolean', default: false },
            ifNotExists: { type: 'boolean', default: false },
            ifExists: { type: 'boolean', default: true },
            method: { type: 'string', enum: ['btree', 'hash', 'gist', 'spgist', 'gin', 'brin'] }
          },
          description: 'Index creation options'
        }
      },
      required: ['action']
    }
  },

  // DATA MANAGEMENT TOOL
  {
    name: 'data',
    description: 'Data operations: insert, update, delete, bulk operations with validation',
    inputSchema: {
      type: 'object',
      properties: {
        action: {
          type: 'string',
          enum: ['insert', 'update', 'delete', 'bulk_insert', 'bulk_update', 'truncate'],
          description: 'Action: insert (single row), update (modify rows), delete (remove rows), bulk_insert (multiple rows), bulk_update (batch update), truncate (empty table)'
        },
        tableName: {
          type: 'string',
          description: 'Table name (required for all actions)'
        },
        schemaName: {
          type: 'string',
          description: 'Schema name (default: public)',
          default: 'public'
        },
        data: {
          type: 'object',
          description: 'Data object for insert/update (key-value pairs)'
        },
        rows: {
          type: 'array',
          items: { type: 'object' },
          description: 'Array of data objects for bulk operations'
        },
        where: {
          type: 'object',
          description: 'WHERE conditions for update/delete operations'
        },
        options: {
          type: 'object',
          properties: {
            onConflict: { type: 'string', description: 'ON CONFLICT action (DO NOTHING, DO UPDATE)' },
            returning: { type: 'array', items: { type: 'string' }, description: 'Columns to return' },
            validate: { type: 'boolean', default: true, description: 'Validate data before operation' }
          },
          description: 'Operation options'
        }
      },
      required: ['action', 'tableName']
    }
  },

  // TRANSACTION MANAGEMENT TOOL
  {
    name: 'transactions',
    description: 'Transaction management: begin, commit, rollback, savepoints',
    inputSchema: {
      type: 'object',
      properties: {
        action: {
          type: 'string',
          enum: ['begin', 'commit', 'rollback', 'savepoint', 'rollback_to', 'release', 'status'],
          description: 'Action: begin (start transaction), commit (commit transaction), rollback (rollback transaction), savepoint (create savepoint), rollback_to (rollback to savepoint), release (release savepoint), status (transaction status)'
        },
        transactionId: {
          type: 'string',
          description: 'Transaction ID (required for commit, rollback, and operations within transaction)'
        },
        savepointName: {
          type: 'string',
          description: 'Savepoint name (required for savepoint, rollback_to, release)'
        },
        readOnly: {
          type: 'boolean',
          description: 'Start read-only transaction (for begin action)',
          default: false
        },
        isolationLevel: {
          type: 'string',
          enum: ['READ UNCOMMITTED', 'READ COMMITTED', 'REPEATABLE READ', 'SERIALIZABLE'],
          description: 'Transaction isolation level (for begin action)'
        }
      },
      required: ['action']
    }
  },

  // DATABASE ADMINISTRATION TOOL
  {
    name: 'admin',
    description: 'Database administration: users, permissions, database info, maintenance operations',
    inputSchema: {
      type: 'object',
      properties: {
        operation: {
          type: 'string',
          enum: ['database_info', 'list_users', 'create_user', 'drop_user', 'grant_permissions', 'revoke_permissions', 'vacuum', 'analyze', 'reindex_database'],
          description: 'Admin operation to perform'
        },
        username: {
          type: 'string',
          description: 'Username (required for user operations)'
        },
        password: {
          type: 'string',
          description: 'Password (required for create_user)'
        },
        permissions: {
          type: 'array',
          items: { type: 'string' },
          description: 'Permissions to grant/revoke'
        },
        tableName: {
          type: 'string',
          description: 'Table name (for permission operations)'
        },
        options: {
          type: 'object',
          properties: {
            full: { type: 'boolean', default: false },
            verbose: { type: 'boolean', default: false },
            analyze: { type: 'boolean', default: false }
          },
          description: 'Operation options'
        }
      },
      required: ['operation']
    }
  },

  // MONITORING TOOL
  {
    name: 'monitoring',
    description: 'Database monitoring: performance metrics, statistics, health checks',
    inputSchema: {
      type: 'object',
      properties: {
        metric: {
          type: 'string',
          enum: ['connections', 'performance', 'locks', 'replication', 'disk_usage', 'query_stats', 'index_usage'],
          description: 'Metric type to retrieve'
        },
        timeRange: {
          type: 'string',
          enum: ['1h', '24h', '7d', '30d'],
          description: 'Time range for metrics',
          default: '1h'
        },
        limit: {
          type: 'integer',
          description: 'Maximum number of results',
          default: 50
        }
      },
      required: ['metric']
    }
  },

  // CONNECTION MANAGEMENT TOOL
  {
    name: 'connections',
    description: 'Connection pool management: status, statistics, configuration',
    inputSchema: {
      type: 'object',
      properties: {
        action: {
          type: 'string',
          enum: ['status', 'stats', 'test', 'reset'],
          description: 'Action: status (pool status), stats (detailed statistics), test (test connection), reset (reset pool)'
        }
      },
      required: ['action']
    }
  },

  // PERMISSIONS TOOL
  {
    name: 'permissions',
    description: 'Database permissions management: users, roles, grants, privileges',
    inputSchema: {
      type: 'object',
      properties: {
        operation: {
          type: 'string',
          enum: [
            'list_users', 'list_roles', 'list_grants', 'list_privileges',
            'create_user', 'create_role', 'drop_user', 'drop_role',
            'grant_role', 'revoke_role', 'grant_privilege', 'revoke_privilege',
            'alter_user', 'alter_role', 'check_permissions', 'grant_all_privileges'
          ],
          description: 'Permission operation to perform'
        },
        username: {
          type: 'string',
          description: 'Username for user operations'
        },
        rolename: {
          type: 'string',
          description: 'Role name for role operations'
        },
        password: {
          type: 'string',
          description: 'Password for user creation/modification'
        },
        database: {
          type: 'string',
          description: 'Database name for grants'
        },
        schema: {
          type: 'string',
          description: 'Schema name for grants'
        },
        table: {
          type: 'string',
          description: 'Table name for grants'
        },
        privileges: {
          type: 'array',
          items: {
            type: 'string',
            enum: ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER', 'CREATE', 'CONNECT', 'TEMPORARY', 'EXECUTE', 'USAGE', 'ALL']
          },
          description: 'Privileges to grant/revoke'
        },
        attributes: {
          type: 'object',
          properties: {
            superuser: { type: 'boolean', description: 'Superuser privilege' },
            createdb: { type: 'boolean', description: 'Create database privilege' },
            createrole: { type: 'boolean', description: 'Create role privilege' },
            replication: { type: 'boolean', description: 'Replication privilege' },
            login: { type: 'boolean', description: 'Login privilege' },
            inherit: { type: 'boolean', description: 'Inherit privileges' },
            bypassrls: { type: 'boolean', description: 'Bypass row level security' }
          },
          description: 'User/role attributes'
        },
        grantOption: {
          type: 'boolean',
          description: 'Grant with GRANT OPTION',
          default: false
        }
      },
      required: ['operation']
    }
  },

  // SECURITY TOOL
  {
    name: 'security',
    description: 'Database security management: SSL, authentication, encryption, auditing',
    inputSchema: {
      type: 'object',
      properties: {
        operation: {
          type: 'string',
          enum: [
            'check_ssl', 'list_auth_methods', 'check_encryption', 'audit_log',
            'password_policy', 'connection_limits', 'session_security',
            'row_level_security', 'column_encryption', 'security_labels'
          ],
          description: 'Security operation to perform'
        },
        table: {
          type: 'string',
          description: 'Table name for RLS operations'
        },
        policy_name: {
          type: 'string',
          description: 'RLS policy name'
        },
        policy_expression: {
          type: 'string',
          description: 'RLS policy expression'
        },
        audit_type: {
          type: 'string',
          enum: ['connections', 'queries', 'ddl', 'dml', 'errors'],
          description: 'Type of audit information'
        }
      },
      required: ['operation']
    }
  }
];

class PostgresMCPServer {
  private server: Server;
  private config: ConfigManager;
  private dbManager: DatabaseConnectionManager;
  private queryClient: QueryAPIClient;
  private tablesClient: TablesAPIClient;

  constructor() {
    this.config = new ConfigManager();
    this.config.validate();

    logger.info('Initializing PostgreSQL MCP Server', { 
      host: this.config.getDatabaseConfig().host,
      database: this.config.getDatabaseConfig().database,
      toolCount: toolDefinitions.length 
    });
    
    // Initialize database connection manager
    this.dbManager = new DatabaseConnectionManager(this.config);
    
    // Initialize API clients
    this.queryClient = new QueryAPIClient(this.dbManager);
    this.tablesClient = new TablesAPIClient(this.dbManager);

    this.server = new Server(
      {
        name: 'postgres-mcp',
        version: '1.0.0',
      },
      {
        capabilities: {
          resources: {},
          tools: {},
        },
      }
    );

    this.setupToolHandlers();
    this.setupResourceHandlers();
  }

  private setupResourceHandlers(): void {
    // List database resources (schemas, tables)
    this.server.setRequestHandler(ListResourcesRequestSchema, async () => {
      try {
        const tables = await this.tablesClient.listTables();
        
        const resources = tables.map(table => ({
          uri: `postgres://${table.schemaName}/${table.tableName}/schema`,
          mimeType: 'application/json',
          name: `${table.schemaName}.${table.tableName} schema`,
          description: `Schema information for table ${table.schemaName}.${table.tableName}`
        }));

        return { resources };
      } catch (error) {
        logger.error('Failed to list resources', { error: error instanceof Error ? error.message : error });
        return { resources: [] };
      }
    });

    // Read specific resource (table schema)
    this.server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
      try {
        const uri = new URL(request.params.uri);
        const pathParts = uri.pathname.split('/');
        const schemaName = pathParts[1];
        const tableName = pathParts[2];

        if (!schemaName || !tableName) {
          throw new Error('Invalid resource URI format');
        }

        const tableInfo = await this.tablesClient.getTableInfo(tableName, schemaName);

        return {
          contents: [{
            uri: request.params.uri,
            mimeType: 'application/json',
            text: JSON.stringify(tableInfo, null, 2)
          }]
        };
      } catch (error) {
        logger.error('Failed to read resource', { 
          uri: request.params.uri,
          error: error instanceof Error ? error.message : error 
        });
        throw new McpError(ErrorCode.InternalError, `Failed to read resource: ${error instanceof Error ? error.message : error}`);
      }
    });
  }

  private setupToolHandlers(): void {
    // Register tool definitions
    this.server.setRequestHandler(ListToolsRequestSchema, async () => ({
      tools: toolDefinitions,
    }));

    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      const { name, arguments: args = {} } = request.params;

      try {
        switch (name) {
          case 'query':
            return await this.handleQuery(args);
          
          case 'tables':
            return await this.handleTables(args);
          
          case 'schemas':
            return await this.handleSchemas(args);
          
          case 'indexes':
            return await this.handleIndexes(args);
          
          case 'data':
            return await this.handleData(args);
          
          case 'transactions':
            return await this.handleTransactions(args);
          
          case 'admin':
            return await this.handleAdmin(args);
          
          case 'monitoring':
            return await this.handleMonitoring(args);
          
          case 'connections':
            return await this.handleConnections(args);
          
          case 'permissions':
            return await this.handlePermissions(args);
          
          case 'security':
            return await this.handleSecurity(args);
          
          default: {
            const suggestion = suggestToolName(name);
            logger.warn('Unknown tool requested', { 
              tool: name, 
              suggestion: TOOL_NAME_MAPPINGS[name] || 'none',
              availableTools: toolDefinitions.map(t => t.name)
            });
            throw new McpError(
              ErrorCode.MethodNotFound,
              `Unknown tool: ${name}. ${suggestion}`
            );
          }
        }
      } catch (error) {
        logger.error('Tool execution error', { tool: name, error: error instanceof Error ? error.message : error });
        
        if (error instanceof McpError) {
          throw error;
        }
        
        if (error instanceof ValidationError) {
          throw ParameterValidator.toMcpError(error);
        }
        
        throw new McpError(
          ErrorCode.InternalError,
          `Tool execution failed: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    });
  }

  private async handleQuery(args: any) {
    const { action, sql, parameters, queries, options, pid } = args;

    switch (action) {
      case 'execute':
        ParameterValidator.validateRequired(sql, 'sql');
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(await this.queryClient.executeQuery(sql, parameters, options), null, 2)
          }]
        };

      case 'transaction':
        ParameterValidator.validateRequired(queries, 'queries');
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(await this.queryClient.executeTransaction(queries, options?.readOnly), null, 2)
          }]
        };

      case 'explain':
        ParameterValidator.validateRequired(sql, 'sql');
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(await this.queryClient.getExecutionPlan(sql, parameters), null, 2)
          }]
        };

      case 'analyze':
        ParameterValidator.validateRequired(sql, 'sql');
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(await this.queryClient.analyzeQuery(sql, parameters), null, 2)
          }]
        };

      case 'validate':
        ParameterValidator.validateRequired(sql, 'sql');
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(await this.queryClient.validateSyntax(sql), null, 2)
          }]
        };

      case 'active':
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(await this.queryClient.getActiveQueries(), null, 2)
          }]
        };

      case 'cancel':
        ParameterValidator.validateRequired(pid, 'pid');
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({ cancelled: await this.queryClient.cancelQuery(pid) }, null, 2)
          }]
        };

      default:
        throw new Error(`Unknown query action: ${action}`);
    }
  }

  private async handleTables(args: any) {
    const { action, tableName, schemaName = 'public', columns, columnName, dataType, newName, options = {} } = args;

    switch (action) {
      case 'list':
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(await this.tablesClient.listTables(schemaName, options.includeViews, options.includeSystemTables), null, 2)
          }]
        };

      case 'info':
        ParameterValidator.validateRequired(tableName, 'tableName');
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(await this.tablesClient.getTableInfo(tableName, schemaName), null, 2)
          }]
        };

      case 'create':
        ParameterValidator.validateRequired(tableName, 'tableName');
        ParameterValidator.validateRequired(columns, 'columns');
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(await this.tablesClient.createTable(tableName, columns, { schema: schemaName, ...options }), null, 2)
          }]
        };

      case 'drop':
        ParameterValidator.validateRequired(tableName, 'tableName');
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(await this.tablesClient.dropTable(tableName, schemaName, options.cascade, options.ifExists), null, 2)
          }]
        };

      case 'add_column':
        ParameterValidator.validateRequired(tableName, 'tableName');
        ParameterValidator.validateRequired(columnName, 'columnName');
        ParameterValidator.validateRequired(dataType, 'dataType');
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(await this.tablesClient.addColumn(tableName, columnName, dataType, schemaName, options), null, 2)
          }]
        };

      default:
        throw new Error(`Unknown tables action: ${action}`);
    }
  }

  private async handleSchemas(args: any) {
    const { action, schemaName, owner, options = {} } = args;

    switch (action) {
      case 'list':
        const schemas = await this.queryClient.executeQuery(`
          SELECT 
            schema_name,
            schema_owner,
            CASE 
              WHEN schema_name IN ('information_schema', 'pg_catalog', 'pg_toast') THEN 'system'
              ELSE 'user'
            END as schema_type
          FROM information_schema.schemata
          ORDER BY 
            CASE WHEN schema_name = 'public' THEN 1 ELSE 2 END,
            schema_type,
            schema_name
        `);
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(schemas.rows, null, 2)
          }]
        };

      case 'create':
        if (!schemaName) {
          throw new Error('Schema name is required for create action');
        }
        let createSQL = `CREATE SCHEMA ${options.ifNotExists ? 'IF NOT EXISTS ' : ''}${schemaName}`;
        if (owner) {
          createSQL += ` AUTHORIZATION ${owner}`;
        }
        await this.queryClient.executeQuery(createSQL);
        return {
          content: [{
            type: 'text',
            text: `Schema '${schemaName}' created successfully`
          }]
        };

      case 'drop':
        if (!schemaName) {
          throw new Error('Schema name is required for drop action');
        }
        const dropSQL = `DROP SCHEMA ${options.ifExists ? 'IF EXISTS ' : ''}${schemaName}${options.cascade ? ' CASCADE' : ''}`;
        await this.queryClient.executeQuery(dropSQL);
        return {
          content: [{
            type: 'text',
            text: `Schema '${schemaName}' dropped successfully`
          }]
        };

      case 'permissions':
        if (!schemaName) {
          throw new Error('Schema name is required for permissions action');
        }
        const permissions = await this.queryClient.executeQuery(`
          SELECT 
            grantee,
            privilege_type,
            is_grantable
          FROM information_schema.schema_privileges
          WHERE schema_name = $1
          ORDER BY grantee, privilege_type
        `, [schemaName]);
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(permissions.rows, null, 2)
          }]
        };

      default:
        throw new Error(`Unknown schema action: ${action}`);
    }
  }

  private async handleIndexes(args: any) {
    const { action, schemaName = 'public', tableName, indexName, columns, options = {} } = args;

    switch (action) {
      case 'list':
        let listQuery;
        let params: any[] = [];
        
        if (tableName) {
          listQuery = `
            SELECT 
              i.indexname as index_name,
              i.tablename as table_name,
              i.schemaname as schema_name,
              pg_get_indexdef(pgc.oid) as definition,
              CASE WHEN i.indexname ~ '^.*_pkey$' THEN 'PRIMARY KEY'
                   WHEN idx.indisunique THEN 'UNIQUE'
                   ELSE 'INDEX' END as index_type,
              pg_size_pretty(pg_relation_size(pgc.oid)) as size,
              idx.indisvalid as is_valid
            FROM pg_indexes i
            JOIN pg_class pgc ON pgc.relname = i.indexname
            JOIN pg_index idx ON idx.indexrelid = pgc.oid
            WHERE i.tablename = $1 AND i.schemaname = $2
            ORDER BY i.indexname
          `;
          params = [tableName, schemaName];
        } else {
          listQuery = `
            SELECT 
              i.indexname as index_name,
              i.tablename as table_name,
              i.schemaname as schema_name,
              pg_get_indexdef(pgc.oid) as definition,
              CASE WHEN i.indexname ~ '^.*_pkey$' THEN 'PRIMARY KEY'
                   WHEN idx.indisunique THEN 'UNIQUE'
                   ELSE 'INDEX' END as index_type,
              pg_size_pretty(pg_relation_size(pgc.oid)) as size,
              idx.indisvalid as is_valid
            FROM pg_indexes i
            JOIN pg_class pgc ON pgc.relname = i.indexname
            JOIN pg_index idx ON idx.indexrelid = pgc.oid
            WHERE i.schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY i.schemaname, i.tablename, i.indexname
          `;
        }
        
        const indexes = await this.queryClient.executeQuery(listQuery, params);
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(indexes.rows, null, 2)
          }]
        };

      case 'create':
        if (!tableName || !columns || columns.length === 0) {
          throw new Error('Table name and columns are required for index creation');
        }
        
        const indexNameToUse = indexName || `idx_${tableName}_${columns.join('_')}`;
        let createIndexSQL = `CREATE${options.unique ? ' UNIQUE' : ''} INDEX${options.concurrent ? ' CONCURRENTLY' : ''}${options.ifNotExists ? ' IF NOT EXISTS' : ''} ${indexNameToUse}`;
        createIndexSQL += ` ON ${schemaName}.${tableName}`;
        if (options.method) {
          createIndexSQL += ` USING ${options.method}`;
        }
        createIndexSQL += ` (${columns.join(', ')})`;
        
        await this.queryClient.executeQuery(createIndexSQL);
        return {
          content: [{
            type: 'text',
            text: `Index '${indexNameToUse}' created successfully on ${schemaName}.${tableName}`
          }]
        };

      case 'drop':
        if (!indexName) {
          throw new Error('Index name is required for drop action');
        }
        const dropSQL = `DROP INDEX${options.concurrent ? ' CONCURRENTLY' : ''}${options.ifExists ? ' IF EXISTS' : ''} ${schemaName}.${indexName}`;
        await this.queryClient.executeQuery(dropSQL);
        return {
          content: [{
            type: 'text',
            text: `Index '${indexName}' dropped successfully`
          }]
        };

      case 'analyze':
        const analyzeQuery = `
          SELECT 
            schemaname,
            tablename,
            indexname,
            idx_tup_read,
            idx_tup_fetch,
            idx_scan,
            CASE WHEN idx_scan = 0 THEN 'UNUSED'
                 WHEN idx_scan < 10 THEN 'LOW_USAGE'
                 ELSE 'ACTIVE' END as usage_status
          FROM pg_stat_user_indexes
          WHERE schemaname = $1
          ORDER BY idx_scan DESC
        `;
        const stats = await this.queryClient.executeQuery(analyzeQuery, [schemaName]);
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(stats.rows, null, 2)
          }]
        };

      case 'reindex':
        if (!indexName && !tableName) {
          throw new Error('Either index name or table name is required for reindex');
        }
        
        let reindexSQL;
        if (indexName) {
          reindexSQL = `REINDEX INDEX${options.concurrent ? ' CONCURRENTLY' : ''} ${schemaName}.${indexName}`;
        } else {
          reindexSQL = `REINDEX TABLE${options.concurrent ? ' CONCURRENTLY' : ''} ${schemaName}.${tableName}`;
        }
        
        await this.queryClient.executeQuery(reindexSQL);
        return {
          content: [{
            type: 'text',
            text: `Reindex completed for ${indexName || tableName}`
          }]
        };

      case 'unused':
        const unusedQuery = `
          SELECT 
            schemaname,
            tablename,
            indexname,
            pg_size_pretty(pg_relation_size(indexrelid)) as size,
            idx_scan as scans
          FROM pg_stat_user_indexes
          WHERE idx_scan = 0
            AND schemaname = $1
            AND indexname NOT LIKE '%_pkey'
          ORDER BY pg_relation_size(indexrelid) DESC
        `;
        const unused = await this.queryClient.executeQuery(unusedQuery, [schemaName]);
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(unused.rows, null, 2)
          }]
        };

      default:
        throw new Error(`Unknown index action: ${action}`);
    }
  }

  private async handleData(args: any) {
    // Placeholder for data operations
    return {
      content: [{
        type: 'text',
        text: JSON.stringify({ message: 'Data operations not yet implemented' }, null, 2)
      }]
    };
  }

  private async handleTransactions(args: any) {
    const { action, transactionId, readOnly, isolationLevel } = args;

    switch (action) {
      case 'begin':
        const txId = await this.dbManager.beginTransaction(readOnly);
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({ transactionId: txId, status: 'started' }, null, 2)
          }]
        };

      case 'commit':
        ParameterValidator.validateRequired(transactionId, 'transactionId');
        await this.dbManager.commitTransaction(transactionId);
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({ transactionId, status: 'committed' }, null, 2)
          }]
        };

      case 'rollback':
        ParameterValidator.validateRequired(transactionId, 'transactionId');
        await this.dbManager.rollbackTransaction(transactionId);
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({ transactionId, status: 'rolled_back' }, null, 2)
          }]
        };

      case 'status':
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(this.dbManager.getOperationalStats(), null, 2)
          }]
        };

      default:
        throw new Error(`Unknown transaction action: ${action}`);
    }
  }

  private async handleAdmin(args: any) {
    const { operation, username, password, permissions, tableName, options = {} } = args;

    switch (operation) {
      case 'database_info':
        const dbInfo = await this.queryClient.executeQuery(`
          SELECT 
            current_database() as database_name,
            current_user as current_user,
            session_user as session_user,
            current_setting('server_version') as postgres_version,
            current_setting('server_encoding') as encoding,
            current_setting('timezone') as timezone,
            pg_database_size(current_database()) as database_size_bytes,
            pg_size_pretty(pg_database_size(current_database())) as database_size,
            (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()) as active_connections,
            current_setting('max_connections') as max_connections,
            current_setting('shared_buffers') as shared_buffers,
            current_setting('effective_cache_size') as effective_cache_size
        `);
        
        const tableCount = await this.queryClient.executeQuery(`
          SELECT count(*) as table_count
          FROM information_schema.tables 
          WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        `);
        
        const result = {
          ...dbInfo.rows[0],
          table_count: parseInt(tableCount.rows[0].table_count),
          uptime: await this.getDatabaseUptime()
        };
        
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(result, null, 2)
          }]
        };

      case 'list_users':
        const users = await this.queryClient.executeQuery(`
          SELECT 
            usename as username,
            usesysid as user_id,
            usecreatedb as can_create_db,
            usesuper as is_superuser,
            userepl as can_replicate,
            usebypassrls as bypass_rls,
            valuntil as password_expires,
            (SELECT string_agg(datname, ', ') 
             FROM pg_database 
             WHERE has_database_privilege(usename, datname, 'CONNECT')) as accessible_databases
          FROM pg_user
          ORDER BY usename
        `);
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(users.rows, null, 2)
          }]
        };

      case 'create_user':
        if (!username || !password) {
          throw new Error('Username and password are required for user creation');
        }
        await this.queryClient.executeQuery(`CREATE USER ${username} WITH PASSWORD '${password}'`);
        return {
          content: [{
            type: 'text',
            text: `User '${username}' created successfully`
          }]
        };

      case 'drop_user':
        if (!username) {
          throw new Error('Username is required for user deletion');
        }
        await this.queryClient.executeQuery(`DROP USER ${username}`);
        return {
          content: [{
            type: 'text',
            text: `User '${username}' dropped successfully`
          }]
        };

      case 'grant_permissions':
        if (!username || !permissions || permissions.length === 0) {
          throw new Error('Username and permissions are required');
        }
        
        const target = tableName ? `TABLE ${tableName}` : 'ALL TABLES IN SCHEMA public';
        const grantSQL = `GRANT ${permissions.join(', ')} ON ${target} TO ${username}`;
        await this.queryClient.executeQuery(grantSQL);
        
        return {
          content: [{
            type: 'text',
            text: `Permissions ${permissions.join(', ')} granted to '${username}' on ${target}`
          }]
        };

      case 'revoke_permissions':
        if (!username || !permissions || permissions.length === 0) {
          throw new Error('Username and permissions are required');
        }
        
        const revokeTarget = tableName ? `TABLE ${tableName}` : 'ALL TABLES IN SCHEMA public';
        const revokeSQL = `REVOKE ${permissions.join(', ')} ON ${revokeTarget} FROM ${username}`;
        await this.queryClient.executeQuery(revokeSQL);
        
        return {
          content: [{
            type: 'text',
            text: `Permissions ${permissions.join(', ')} revoked from '${username}' on ${revokeTarget}`
          }]
        };

      case 'vacuum':
        if (tableName) {
          const vacuumSQL = `VACUUM${options.full ? ' FULL' : ''} ${tableName}`;
          await this.queryClient.executeQuery(vacuumSQL);
          return {
            content: [{
              type: 'text',
              text: `Vacuum completed for table '${tableName}'`
            }]
          };
        } else {
          await this.queryClient.executeQuery('VACUUM');
          return {
            content: [{
              type: 'text',
              text: 'Database vacuum completed'
            }]
          };
        }

      case 'analyze':
        if (tableName) {
          await this.queryClient.executeQuery(`ANALYZE ${tableName}`);
          return {
            content: [{
              type: 'text',
              text: `Analyze completed for table '${tableName}'`
            }]
          };
        } else {
          await this.queryClient.executeQuery('ANALYZE');
          return {
            content: [{
              type: 'text',
              text: 'Database analyze completed'
            }]
          };
        }

      case 'reindex_database':
        await this.queryClient.executeQuery('REINDEX DATABASE CONCURRENTLY');
        return {
          content: [{
            type: 'text',
            text: 'Database reindex completed'
          }]
        };

      default:
        throw new Error(`Unknown admin operation: ${operation}`);
    }
  }

  private async getDatabaseUptime(): Promise<string> {
    try {
      const uptime = await this.queryClient.executeQuery(`
        SELECT date_trunc('second', now() - pg_postmaster_start_time()) as uptime
      `);
      return uptime.rows[0].uptime;
    } catch (error) {
      return 'Unable to determine uptime';
    }
  }

  private async handleMonitoring(args: any) {
    const { metric } = args;
    
    switch (metric) {
      case 'connections':
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(this.dbManager.getPoolStats(), null, 2)
          }]
        };

      case 'performance':
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(this.dbManager.getOperationalStats(), null, 2)
          }]
        };

      default:
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({ message: `Monitoring metric '${metric}' not yet implemented` }, null, 2)
          }]
        };
    }
  }

  private async handleConnections(args: any) {
    const { action } = args;

    switch (action) {
      case 'status':
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(this.dbManager.getPoolStats(), null, 2)
          }]
        };

      case 'stats':
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(this.dbManager.getOperationalStats(), null, 2)
          }]
        };

      case 'test':
        const isHealthy = await this.dbManager.testConnection();
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({ connected: isHealthy, timestamp: new Date().toISOString() }, null, 2)
          }]
        };

      default:
        throw new Error(`Unknown connections action: ${action}`);
    }
  }

  private async handlePermissions(args: any) {
    const { operation, username, rolename, password, database, schema, table, privileges, attributes, grantOption } = args;

    switch (operation) {
      case 'list_users':
        const users = await this.queryClient.executeQuery(`
          SELECT 
            u.usename as username,
            u.usesysid as user_id,
            u.usecreatedb as can_create_db,
            u.usesuper as is_superuser,
            u.userepl as can_replicate,
            u.usebypassrls as bypass_rls,
            u.valuntil as password_expires,
            ARRAY(SELECT rolname FROM pg_roles WHERE oid = ANY(u.memberof)) as member_of
          FROM pg_user u
          ORDER BY u.usename
        `);
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(users.rows, null, 2)
          }]
        };

      case 'list_roles':
        const roles = await this.queryClient.executeQuery(`
          SELECT 
            rolname as role_name,
            rolsuper as is_superuser,
            rolinherit as inherits,
            rolcreaterole as can_create_role,
            rolcreatedb as can_create_db,
            rolcanlogin as can_login,
            rolreplication as can_replicate,
            rolconnlimit as connection_limit,
            rolvaliduntil as valid_until,
            rolbypassrls as bypass_rls
          FROM pg_roles
          ORDER BY rolname
        `);
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(roles.rows, null, 2)
          }]
        };

      case 'create_user':
        if (!username || !password) {
          throw new Error('Username and password are required for user creation');
        }
        const createUserSQL = `CREATE USER ${username} WITH PASSWORD '${password}'`;
        if (attributes) {
          const attrSQL = Object.entries(attributes)
            .filter(([, value]) => value === true)
            .map(([key]) => key.toUpperCase())
            .join(' ');
          if (attrSQL) {
            await this.queryClient.executeQuery(`${createUserSQL} ${attrSQL}`);
          } else {
            await this.queryClient.executeQuery(createUserSQL);
          }
        } else {
          await this.queryClient.executeQuery(createUserSQL);
        }
        return {
          content: [{
            type: 'text',
            text: `User '${username}' created successfully`
          }]
        };

      case 'grant_all_privileges':
        if (!username || !database) {
          throw new Error('Username and database are required for granting all privileges');
        }
        const grantAllSQL = [
          `GRANT ALL PRIVILEGES ON DATABASE ${database} TO ${username}`,
          `GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ${username}`,
          `GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ${username}`,
          `GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO ${username}`
        ];
        
        for (const sql of grantAllSQL) {
          await this.queryClient.executeQuery(sql);
        }
        
        return {
          content: [{
            type: 'text',
            text: `All privileges granted to '${username}' on database '${database}'`
          }]
        };

      case 'grant_privilege':
        if (!username || !privileges || privileges.length === 0) {
          throw new Error('Username and privileges are required');
        }
        const target = table ? `TABLE ${schema ? schema + '.' : ''}${table}` : 
                      schema ? `SCHEMA ${schema}` : 
                      database ? `DATABASE ${database}` : 'ALL TABLES IN SCHEMA public';
        
        const grantSQL = `GRANT ${privileges.join(', ')} ON ${target} TO ${username}${grantOption ? ' WITH GRANT OPTION' : ''}`;
        await this.queryClient.executeQuery(grantSQL);
        
        return {
          content: [{
            type: 'text',
            text: `Privileges ${privileges.join(', ')} granted to '${username}' on ${target}`
          }]
        };

      case 'check_permissions':
        if (!username) {
          throw new Error('Username is required for permission check');
        }
        const permissionsQuery = `
          SELECT 
            t.schemaname,
            t.tablename,
            p.privilege_type
          FROM information_schema.table_privileges p
          JOIN information_schema.tables t ON p.table_name = t.table_name AND p.table_schema = t.table_schema
          WHERE p.grantee = $1
          ORDER BY t.schemaname, t.tablename, p.privilege_type
        `;
        const permissions = await this.queryClient.executeQuery(permissionsQuery, [username]);
        
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(permissions.rows, null, 2)
          }]
        };

      default:
        throw new Error(`Unknown permissions operation: ${operation}`);
    }
  }

  private async handleSecurity(args: any) {
    const { operation, table, policy_name, policy_expression, audit_type } = args;

    switch (operation) {
      case 'check_ssl':
        const sslInfo = await this.queryClient.executeQuery(`
          SELECT 
            name,
            setting,
            context,
            short_desc
          FROM pg_settings 
          WHERE name LIKE '%ssl%' OR name LIKE '%tls%'
          ORDER BY name
        `);
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(sslInfo.rows, null, 2)
          }]
        };

      case 'list_auth_methods':
        const authMethods = await this.queryClient.executeQuery(`
          SELECT 
            type,
            database,
            user_name,
            address,
            netmask,
            auth_method,
            options,
            error
          FROM pg_hba_file_rules
          ORDER BY line_number
        `);
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(authMethods.rows, null, 2)
          }]
        };

      case 'session_security':
        const sessionInfo = await this.queryClient.executeQuery(`
          SELECT 
            inet_client_addr() as client_ip,
            inet_server_addr() as server_ip,
            current_user,
            session_user,
            current_database(),
            pg_backend_pid() as backend_pid,
            pg_is_in_recovery() as in_recovery,
            current_setting('ssl') as ssl_enabled
        `);
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(sessionInfo.rows[0], null, 2)
          }]
        };

      case 'row_level_security':
        if (!table) {
          // List all RLS policies
          const rlsPolicies = await this.queryClient.executeQuery(`
            SELECT 
              schemaname,
              tablename,
              policyname,
              permissive,
              roles,
              cmd,
              qual,
              with_check
            FROM pg_policies
            ORDER BY schemaname, tablename, policyname
          `);
          return {
            content: [{
              type: 'text',
              text: JSON.stringify(rlsPolicies.rows, null, 2)
            }]
          };
        } else {
          // Show RLS status for specific table
          const rlsStatus = await this.queryClient.executeQuery(`
            SELECT 
              schemaname,
              tablename,
              rowsecurity,
              forcerowsecurity
            FROM pg_tables 
            WHERE tablename = $1
          `, [table]);
          return {
            content: [{
              type: 'text',
              text: JSON.stringify(rlsStatus.rows, null, 2)
            }]
          };
        }

      case 'audit_log':
        const auditQuery = `
          SELECT 
            datname as database,
            usename as username,
            application_name,
            client_addr,
            backend_start,
            query_start,
            state,
            query
          FROM pg_stat_activity 
          WHERE state = 'active' 
          ORDER BY query_start DESC
          LIMIT 50
        `;
        const auditInfo = await this.queryClient.executeQuery(auditQuery);
        return {
          content: [{
            type: 'text',
            text: JSON.stringify(auditInfo.rows, null, 2)
          }]
        };

      default:
        throw new Error(`Unknown security operation: ${operation}`);
    }
  }

  async run(): Promise<void> {
    const transport = new StdioServerTransport();
    await this.server.connect(transport);
    
    logger.info(`PostgreSQL MCP Server running with ${toolDefinitions.length} tools`);
  }

  /**
   * Cleanup resources on shutdown
   */
  async cleanup(): Promise<void> {
    await this.dbManager.cleanup();
    logger.info('Server resources cleaned up');
  }
}

const skipRuntime = process.env.SKIP_CONFIG_VALIDATION === 'true' || process.env.CI === 'true';

let server: PostgresMCPServer | null = null;
try {
  server = new PostgresMCPServer();
  if (skipRuntime) {
    logger.info('Configuration missing or CI mode detected - skipping runtime server startup');
  } else {
    // Graceful shutdown handling
    process.on('SIGINT', async () => {
      logger.info('Received SIGINT, shutting down');
      if (server) await server.cleanup();
      process.exit(0);
    });
    process.on('SIGTERM', async () => {
      logger.info('Received SIGTERM, shutting down');
      if (server) await server.cleanup();
      process.exit(0);
    });
    server.run().catch((error) => {
      logger.error('Server failed to start', error);
      process.exit(1);
    });
  }
} catch (e) {
  logger.error('Failed to initialize server', e);
  if (!skipRuntime) {
    process.exit(1);
  }
}