import { DatabaseConnectionManager } from '../../database/connection-manager.js';
import { ParameterValidator } from '../../validation.js';
import { logger } from '../../logger.js';

export interface QueryExecutionResult {
  rows: any[];
  rowCount: number;
  fields: Array<{
    name: string;
    dataTypeID: number;
    dataTypeSize: number;
    dataTypeModifier: number;
    format: string;
  }>;
  command: string;
  duration: number;
}

export interface QueryOptions {
  timeout?: number;
  limit?: number;
  offset?: number;
  explain?: boolean;
  analyze?: boolean;
}

export class QueryAPIClient {
  constructor(private dbManager: DatabaseConnectionManager) {}

  /**
   * Execute a SQL query
   */
  async executeQuery(
    sql: string, 
    parameters?: any[], 
    options: QueryOptions = {}
  ): Promise<QueryExecutionResult> {
    const startTime = Date.now();
    
    // Validate SQL
    const validatedSql = ParameterValidator.validateSql(sql);
    
    // Add LIMIT if specified and not already present
    let finalSql = validatedSql;
    if (options.limit && !finalSql.toUpperCase().includes('LIMIT')) {
      finalSql += ` LIMIT ${ParameterValidator.validateLimit(options.limit)}`;
    }
    
    // Add OFFSET if specified
    if (options.offset && !finalSql.toUpperCase().includes('OFFSET')) {
      finalSql += ` OFFSET ${ParameterValidator.validateOffset(options.offset)}`;
    }
    
    // Add EXPLAIN if requested
    if (options.explain) {
      const explainPrefix = options.analyze ? 'EXPLAIN (ANALYZE, BUFFERS)' : 'EXPLAIN';
      finalSql = `${explainPrefix} ${finalSql}`;
    }

    try {
      const result = await this.dbManager.query(finalSql, parameters, {
        timeout: options.timeout,
        readOnly: this.isReadOnlyQuery(validatedSql)
      });

      const duration = Date.now() - startTime;

      return {
        rows: result.rows,
        rowCount: result.rowCount || 0,
        fields: result.fields.map((field: any) => ({
          name: field.name,
          dataTypeID: field.dataTypeID,
          dataTypeSize: field.dataTypeSize,
          dataTypeModifier: field.dataTypeModifier,
          format: field.format
        })),
        command: result.command,
        duration
      };
    } catch (error) {
      logger.error('Query execution failed', { 
        sql: sql.substring(0, 100),
        parameters,
        error: error instanceof Error ? error.message : error 
      });
      throw error;
    }
  }

  /**
   * Execute multiple queries in a transaction
   */
  async executeTransaction(
    queries: Array<{ sql: string; parameters?: any[] }>,
    readOnly: boolean = false
  ): Promise<QueryExecutionResult[]> {
    const transactionId = await this.dbManager.beginTransaction(readOnly);
    
    try {
      const results: QueryExecutionResult[] = [];
      
      for (const query of queries) {
        const startTime = Date.now();
        const validatedSql = ParameterValidator.validateSql(query.sql);
        
        const result = await this.dbManager.queryInTransaction(
          transactionId,
          validatedSql,
          query.parameters
        );
        
        const duration = Date.now() - startTime;
        
        results.push({
          rows: result.rows,
          rowCount: result.rowCount || 0,
          fields: result.fields.map((field: any) => ({
            name: field.name,
            dataTypeID: field.dataTypeID,
            dataTypeSize: field.dataTypeSize,
            dataTypeModifier: field.dataTypeModifier,
            format: field.format
          })),
          command: result.command,
          duration
        });
      }
      
      await this.dbManager.commitTransaction(transactionId);
      return results;
      
    } catch (error) {
      await this.dbManager.rollbackTransaction(transactionId);
      throw error;
    }
  }

  /**
   * Get query execution plan
   */
  async getExecutionPlan(
    sql: string, 
    parameters?: any[],
    analyze: boolean = false
  ): Promise<any[]> {
    const validatedSql = ParameterValidator.validateSql(sql);
    const explainSql = analyze 
      ? `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${validatedSql}`
      : `EXPLAIN (FORMAT JSON) ${validatedSql}`;
    
    const result = await this.dbManager.query(explainSql, parameters, { readOnly: true });
    return result.rows[0]['QUERY PLAN'];
  }

  /**
   * Analyze query performance
   */
  async analyzeQuery(sql: string, parameters?: any[]): Promise<{
    executionPlan: any[];
    statistics: {
      planningTime: number;
      executionTime: number;
      totalCost: number;
      rows: number;
    };
  }> {
    const plan = await this.getExecutionPlan(sql, parameters, true);
    
    return {
      executionPlan: plan,
      statistics: {
        planningTime: plan[0]['Planning Time'] || 0,
        executionTime: plan[0]['Execution Time'] || 0,
        totalCost: plan[0]['Total Cost'] || 0,
        rows: plan[0]['Actual Rows'] || 0
      }
    };
  }

  /**
   * Validate SQL syntax without execution
   */
  async validateSyntax(sql: string): Promise<{ valid: boolean; error?: string }> {
    try {
      // Use EXPLAIN to validate syntax without execution
      const validatedSql = ParameterValidator.validateSql(sql);
      await this.dbManager.query(`EXPLAIN ${validatedSql}`, [], { readOnly: true });
      return { valid: true };
    } catch (error) {
      return { 
        valid: false, 
        error: error instanceof Error ? error.message : 'Unknown syntax error' 
      };
    }
  }

  /**
   * Get active queries
   */
  async getActiveQueries(): Promise<any[]> {
    const sql = `
      SELECT 
        pid,
        now() - pg_stat_activity.query_start AS duration,
        query,
        state,
        client_addr,
        application_name
      FROM pg_stat_activity 
      WHERE state = 'active'
        AND query NOT ILIKE '%pg_stat_activity%'
      ORDER BY duration DESC
    `;
    
    const result = await this.dbManager.query(sql, [], { readOnly: true });
    return result.rows;
  }

  /**
   * Cancel a query by PID
   */
  async cancelQuery(pid: number): Promise<boolean> {
    const validatedPid = ParameterValidator.validateNumber(pid, 'pid', 1);
    
    const result = await this.dbManager.query(
      'SELECT pg_cancel_backend($1) as cancelled',
      [validatedPid]
    );
    
    return result.rows[0]?.cancelled || false;
  }

  /**
   * Get query statistics
   */
  async getQueryStatistics(
    schemaName?: string,
    limit: number = 50
  ): Promise<any[]> {
    const validatedLimit = ParameterValidator.validateLimit(limit);
    
    let sql = `
      SELECT 
        schemaname,
        tablename,
        seq_scan,
        seq_tup_read,
        idx_scan,
        idx_tup_fetch,
        n_tup_ins,
        n_tup_upd,
        n_tup_del
      FROM pg_stat_user_tables
    `;
    
    const params: any[] = [];
    
    if (schemaName) {
      const validatedSchema = ParameterValidator.validateSchemaName(schemaName);
      sql += ` WHERE schemaname = $1`;
      params.push(validatedSchema);
    }
    
    sql += ` ORDER BY seq_scan + idx_scan DESC LIMIT $${params.length + 1}`;
    params.push(validatedLimit);
    
    const result = await this.dbManager.query(sql, params, { readOnly: true });
    return result.rows;
  }

  /**
   * Check if query is read-only
   */
  private isReadOnlyQuery(sql: string): boolean {
    const writeOperations = [
      'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER',
      'TRUNCATE', 'GRANT', 'REVOKE', 'COPY'
    ];
    
    const normalizedSql = sql.trim().toUpperCase();
    return !writeOperations.some(op => normalizedSql.startsWith(op));
  }
}