import { Pool, PoolClient, QueryResult, PoolConfig as PgPoolConfig, QueryResultRow } from 'pg';
import { ConfigManager, PoolConfig } from '../config.js';
import { logger, logConnection, logError, logQuery, logMetrics } from '../logger.js';
import { v4 as uuidv4 } from 'uuid';

export interface QueryOptions {
  timeout?: number;
  transaction?: boolean;
  readOnly?: boolean;
  parameters?: any[];
}

export interface TransactionContext {
  id: string;
  client: PoolClient;
  startTime: number;
  readOnly: boolean;
}

export interface ConnectionPoolStats {
  totalConnections: number;
  idleConnections: number;
  waitingCount: number;
  config: {
    min: number;
    max: number;
    idleTimeoutMillis: number;
  };
}

export class DatabaseConnectionManager {
  private pool: Pool;
  private config: ConfigManager;
  private activeTransactions: Map<string, TransactionContext> = new Map();
  private connectionStats = {
    totalQueries: 0,
    totalErrors: 0,
    totalTransactions: 0,
    averageQueryTime: 0,
    connectionCount: 0
  };

  constructor(config: ConfigManager) {
    this.config = config;
    this.pool = this.createPool();
    this.setupEventHandlers();
  }

  private createPool(): Pool {
    const dbConfig = this.config.getDatabaseConfig();
    const poolConfig = this.config.get().pool;

    const pgPoolConfig: PgPoolConfig = {
      connectionString: this.config.getConnectionString(),
      min: poolConfig.min,
      max: poolConfig.max,
      idleTimeoutMillis: poolConfig.idleTimeoutMillis,
      ssl: dbConfig.ssl ? { rejectUnauthorized: false } : false,
    };

    const pool = new Pool(pgPoolConfig);
    
    logConnection('pool_created', { 
      min: poolConfig.min, 
      max: poolConfig.max,
      ssl: dbConfig.ssl 
    });

    return pool;
  }

  private setupEventHandlers(): void {
    this.pool.on('connect', (client) => {
      this.connectionStats.connectionCount++;
      logConnection('client_connected', { 
        totalConnections: this.connectionStats.connectionCount 
      });
    });

    this.pool.on('acquire', (client) => {
      logConnection('client_acquired');
    });

    this.pool.on('release', (client) => {
      logConnection('client_released');
    });

    this.pool.on('remove', (client) => {
      this.connectionStats.connectionCount--;
      logConnection('client_removed', { 
        totalConnections: this.connectionStats.connectionCount 
      });
    });

    this.pool.on('error', (err, client) => {
      this.connectionStats.totalErrors++;
      logError(err, { context: 'pool_error' });
    });
  }

  /**
   * Execute a single query
   */
  async query<T extends QueryResultRow = any>(
    sql: string, 
    params?: any[], 
    options: QueryOptions = {}
  ): Promise<QueryResult<T>> {
    const startTime = Date.now();
    const queryId = uuidv4();
    
    try {
      // Security check for read-only mode
      if (this.config.isReadOnlyMode() && this.isWriteOperation(sql)) {
        throw new Error('Write operations are not allowed in read-only mode');
      }

      // Check query timeout
      const timeout = options.timeout || this.config.get().security.maxQueryTime;
      
      const client = await this.pool.connect();
      
      try {
        // Set statement timeout
        await client.query(`SET statement_timeout = ${timeout}`);
        
        // Execute query
        const result = await client.query(sql, params);
        
        const duration = Date.now() - startTime;
        this.connectionStats.totalQueries++;
        this.updateAverageQueryTime(duration);
        
        logQuery(sql, params, duration);
        
        return result;
      } finally {
        client.release();
      }
    } catch (error) {
      const duration = Date.now() - startTime;
      this.connectionStats.totalErrors++;
      
      logError(error as Error, { 
        sql: sql.substring(0, 100), 
        params, 
        duration,
        queryId 
      });
      
      throw error;
    }
  }

  /**
   * Begin a new transaction
   */
  async beginTransaction(readOnly: boolean = false): Promise<string> {
    const transactionId = uuidv4();
    const client = await this.pool.connect();
    
    try {
      if (readOnly) {
        await client.query('BEGIN READ ONLY');
      } else {
        if (this.config.isReadOnlyMode()) {
          throw new Error('Write transactions are not allowed in read-only mode');
        }
        await client.query('BEGIN');
      }
      
      const context: TransactionContext = {
        id: transactionId,
        client,
        startTime: Date.now(),
        readOnly
      };
      
      this.activeTransactions.set(transactionId, context);
      this.connectionStats.totalTransactions++;
      
      logConnection('transaction_started', { 
        transactionId, 
        readOnly,
        activeTransactions: this.activeTransactions.size 
      });
      
      return transactionId;
    } catch (error) {
      client.release();
      throw error;
    }
  }

  /**
   * Execute query within a transaction
   */
  async queryInTransaction<T extends QueryResultRow = any>(
    transactionId: string,
    sql: string,
    params?: any[]
  ): Promise<QueryResult<T>> {
    const context = this.activeTransactions.get(transactionId);
    if (!context) {
      throw new Error(`Transaction ${transactionId} not found or already closed`);
    }

    // Security check for read-only transactions
    if (context.readOnly && this.isWriteOperation(sql)) {
      throw new Error('Write operations are not allowed in read-only transactions');
    }

    const startTime = Date.now();
    
    try {
      const result = await context.client.query(sql, params);
      const duration = Date.now() - startTime;
      
      logQuery(sql, params, duration);
      
      return result;
    } catch (error) {
      logError(error as Error, { 
        transactionId, 
        sql: sql.substring(0, 100),
        params 
      });
      throw error;
    }
  }

  /**
   * Commit a transaction
   */
  async commitTransaction(transactionId: string): Promise<void> {
    const context = this.activeTransactions.get(transactionId);
    if (!context) {
      throw new Error(`Transaction ${transactionId} not found`);
    }

    try {
      await context.client.query('COMMIT');
      const duration = Date.now() - context.startTime;
      
      logConnection('transaction_committed', { 
        transactionId, 
        duration: `${duration}ms` 
      });
    } finally {
      context.client.release();
      this.activeTransactions.delete(transactionId);
    }
  }

  /**
   * Rollback a transaction
   */
  async rollbackTransaction(transactionId: string): Promise<void> {
    const context = this.activeTransactions.get(transactionId);
    if (!context) {
      throw new Error(`Transaction ${transactionId} not found`);
    }

    try {
      await context.client.query('ROLLBACK');
      const duration = Date.now() - context.startTime;
      
      logConnection('transaction_rolled_back', { 
        transactionId, 
        duration: `${duration}ms` 
      });
    } finally {
      context.client.release();
      this.activeTransactions.delete(transactionId);
    }
  }

  /**
   * Get connection pool statistics
   */
  getPoolStats(): ConnectionPoolStats {
    return {
      totalConnections: this.pool.totalCount,
      idleConnections: this.pool.idleCount,
      waitingCount: this.pool.waitingCount,
      config: {
        min: this.config.get().pool.min,
        max: this.config.get().pool.max,
        idleTimeoutMillis: this.config.get().pool.idleTimeoutMillis,
      }
    };
  }

  /**
   * Get operational statistics
   */
  getOperationalStats() {
    return {
      ...this.connectionStats,
      activeTransactions: this.activeTransactions.size,
      poolStats: this.getPoolStats()
    };
  }

  /**
   * Test database connection
   */
  async testConnection(): Promise<boolean> {
    try {
      const result = await this.query('SELECT 1 as test');
      return result.rows.length > 0 && result.rows[0].test === 1;
    } catch (error) {
      logError(error as Error, { context: 'connection_test' });
      return false;
    }
  }

  /**
   * Cleanup and close all connections
   */
  async cleanup(): Promise<void> {
    // Rollback any active transactions
    for (const [transactionId, context] of this.activeTransactions) {
      try {
        await context.client.query('ROLLBACK');
        context.client.release();
        logConnection('transaction_cleanup', { transactionId });
      } catch (error) {
        logError(error as Error, { context: 'transaction_cleanup', transactionId });
      }
    }
    
    this.activeTransactions.clear();
    
    // Close the pool
    await this.pool.end();
    logConnection('pool_closed');
  }

  /**
   * Check if SQL is a write operation
   */
  private isWriteOperation(sql: string): boolean {
    const writeOperations = [
      'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 
      'TRUNCATE', 'GRANT', 'REVOKE', 'COPY'
    ];
    
    const normalizedSql = sql.trim().toUpperCase();
    return writeOperations.some(op => normalizedSql.startsWith(op));
  }

  /**
   * Update average query time with exponential moving average
   */
  private updateAverageQueryTime(duration: number): void {
    if (this.connectionStats.averageQueryTime === 0) {
      this.connectionStats.averageQueryTime = duration;
    } else {
      // Exponential moving average with alpha = 0.1
      this.connectionStats.averageQueryTime = 
        0.9 * this.connectionStats.averageQueryTime + 0.1 * duration;
    }
  }
}