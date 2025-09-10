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
    // Placeholder for schema management
    return {
      content: [{
        type: 'text',
        text: JSON.stringify({ message: 'Schema management not yet implemented' }, null, 2)
      }]
    };
  }

  private async handleIndexes(args: any) {
    // Placeholder for index management
    return {
      content: [{
        type: 'text',
        text: JSON.stringify({ message: 'Index management not yet implemented' }, null, 2)
      }]
    };
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
    // Placeholder for admin operations
    return {
      content: [{
        type: 'text',
        text: JSON.stringify({ message: 'Admin operations not yet implemented' }, null, 2)
      }]
    };
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