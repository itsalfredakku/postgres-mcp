import { DatabaseConnectionManager } from '../../database/connection-manager.js';
import { ParameterValidator } from '../../validation.js';
import { logger } from '../../logger.js';

export interface TableInfo {
  schemaName: string;
  tableName: string;
  tableType: 'BASE TABLE' | 'VIEW' | 'MATERIALIZED VIEW';
  owner: string;
  hasIndexes: boolean;
  hasRules: boolean;
  hasTriggers: boolean;
  rowCount?: number;
  sizeBytes?: number;
}

export interface ColumnInfo {
  columnName: string;
  dataType: string;
  isNullable: boolean;
  defaultValue?: string;
  isPrimaryKey: boolean;
  isForeignKey: boolean;
  maxLength?: number;
  precision?: number;
  scale?: number;
}

export interface CreateTableOptions {
  schema?: string;
  ifNotExists?: boolean;
  temporary?: boolean;
  unlogged?: boolean;
}

export class TablesAPIClient {
  constructor(private dbManager: DatabaseConnectionManager) {}

  /**
   * List all tables in the database
   */
  async listTables(
    schemaName?: string,
    includeViews: boolean = false,
    includeSystemTables: boolean = false
  ): Promise<TableInfo[]> {
    let sql = `
      SELECT 
        t.table_schema as schema_name,
        t.table_name,
        t.table_type,
        pg_catalog.pg_get_userbyid(c.relowner) as owner,
        c.relhasindex as has_indexes,
        c.relhasrules as has_rules,
        c.relhastriggers as has_triggers,
        pg_catalog.pg_relation_size(c.oid) as size_bytes
      FROM information_schema.tables t
      JOIN pg_catalog.pg_class c ON c.relname = t.table_name
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
      WHERE 1=1
    `;

    const params: any[] = [];
    let paramIndex = 1;

    if (schemaName) {
      const validatedSchema = ParameterValidator.validateSchemaName(schemaName);
      sql += ` AND t.table_schema = $${paramIndex}`;
      params.push(validatedSchema);
      paramIndex++;
    } else if (!includeSystemTables) {
      sql += ` AND t.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')`;
    }

    if (!includeViews) {
      sql += ` AND t.table_type = 'BASE TABLE'`;
    }

    sql += ` ORDER BY t.table_schema, t.table_name`;

    const result = await this.dbManager.query(sql, params, { readOnly: true });

    return result.rows.map((row: any) => ({
      schemaName: row.schema_name,
      tableName: row.table_name,
      tableType: row.table_type,
      owner: row.owner,
      hasIndexes: row.has_indexes,
      hasRules: row.has_rules,
      hasTriggers: row.has_triggers,
      sizeBytes: parseInt(row.size_bytes) || 0
    }));
  }

  /**
   * Get detailed information about a specific table
   */
  async getTableInfo(tableName: string, schemaName: string = 'public'): Promise<{
    table: TableInfo;
    columns: ColumnInfo[];
    indexes: any[];
    constraints: any[];
    statistics: any;
  }> {
    const validatedTable = ParameterValidator.validateTableName(tableName);
    const validatedSchema = ParameterValidator.validateSchemaName(schemaName);

    // Get table info
    const tableInfo = await this.getTableBasicInfo(validatedTable, validatedSchema);
    
    // Get columns
    const columns = await this.getTableColumns(validatedTable, validatedSchema);
    
    // Get indexes
    const indexes = await this.getTableIndexes(validatedTable, validatedSchema);
    
    // Get constraints
    const constraints = await this.getTableConstraints(validatedTable, validatedSchema);
    
    // Get statistics
    const statistics = await this.getTableStatistics(validatedTable, validatedSchema);

    return {
      table: tableInfo,
      columns,
      indexes,
      constraints,
      statistics
    };
  }

  /**
   * Create a new table
   */
  async createTable(
    tableName: string,
    columns: Array<{
      name: string;
      type: string;
      nullable?: boolean;
      defaultValue?: string;
      primaryKey?: boolean;
    }>,
    options: CreateTableOptions = {}
  ): Promise<{ success: boolean; message: string }> {
    const validatedTable = ParameterValidator.validateTableName(tableName);
    
    // Validate columns
    if (!columns || columns.length === 0) {
      throw new Error('At least one column is required');
    }

    for (const col of columns) {
      ParameterValidator.validateColumnName(col.name);
      ParameterValidator.validateDataType(col.type);
    }

    const schema = options.schema ? ParameterValidator.validateSchemaName(options.schema) : 'public';
    const fullTableName = `${schema}.${validatedTable}`;

    // Build CREATE TABLE statement
    let sql = 'CREATE';
    
    if (options.temporary) sql += ' TEMPORARY';
    if (options.unlogged) sql += ' UNLOGGED';
    
    sql += ' TABLE';
    
    if (options.ifNotExists) sql += ' IF NOT EXISTS';
    
    sql += ` ${fullTableName} (`;

    const columnDefinitions = columns.map(col => {
      let def = `${col.name} ${col.type}`;
      
      if (col.nullable === false) def += ' NOT NULL';
      if (col.defaultValue) def += ` DEFAULT ${col.defaultValue}`;
      if (col.primaryKey) def += ' PRIMARY KEY';
      
      return def;
    }).join(', ');

    sql += columnDefinitions + ')';

    try {
      await this.dbManager.query(sql);
      
      logger.info('Table created successfully', { 
        tableName: fullTableName,
        columns: columns.length 
      });
      
      return {
        success: true,
        message: `Table ${fullTableName} created successfully with ${columns.length} columns`
      };
    } catch (error) {
      logger.error('Failed to create table', { 
        tableName: fullTableName,
        error: error instanceof Error ? error.message : error 
      });
      throw error;
    }
  }

  /**
   * Drop a table
   */
  async dropTable(
    tableName: string,
    schemaName: string = 'public',
    cascade: boolean = false,
    ifExists: boolean = true
  ): Promise<{ success: boolean; message: string }> {
    const validatedTable = ParameterValidator.validateTableName(tableName);
    const validatedSchema = ParameterValidator.validateSchemaName(schemaName);
    const fullTableName = `${validatedSchema}.${validatedTable}`;

    let sql = 'DROP TABLE';
    if (ifExists) sql += ' IF EXISTS';
    sql += ` ${fullTableName}`;
    if (cascade) sql += ' CASCADE';

    try {
      await this.dbManager.query(sql);
      
      logger.info('Table dropped successfully', { tableName: fullTableName });
      
      return {
        success: true,
        message: `Table ${fullTableName} dropped successfully`
      };
    } catch (error) {
      logger.error('Failed to drop table', { 
        tableName: fullTableName,
        error: error instanceof Error ? error.message : error 
      });
      throw error;
    }
  }

  /**
   * Add a column to an existing table
   */
  async addColumn(
    tableName: string,
    columnName: string,
    dataType: string,
    schemaName: string = 'public',
    options: {
      nullable?: boolean;
      defaultValue?: string;
      ifNotExists?: boolean;
    } = {}
  ): Promise<{ success: boolean; message: string }> {
    const validatedTable = ParameterValidator.validateTableName(tableName);
    const validatedColumn = ParameterValidator.validateColumnName(columnName);
    const validatedType = ParameterValidator.validateDataType(dataType);
    const validatedSchema = ParameterValidator.validateSchemaName(schemaName);
    
    const fullTableName = `${validatedSchema}.${validatedTable}`;

    let sql = `ALTER TABLE ${fullTableName} ADD`;
    if (options.ifNotExists) sql += ' IF NOT EXISTS';
    sql += ` COLUMN ${validatedColumn} ${validatedType}`;
    
    if (options.nullable === false) sql += ' NOT NULL';
    if (options.defaultValue) sql += ` DEFAULT ${options.defaultValue}`;

    try {
      await this.dbManager.query(sql);
      
      logger.info('Column added successfully', { 
        tableName: fullTableName,
        columnName: validatedColumn 
      });
      
      return {
        success: true,
        message: `Column ${validatedColumn} added to ${fullTableName} successfully`
      };
    } catch (error) {
      logger.error('Failed to add column', { 
        tableName: fullTableName,
        columnName: validatedColumn,
        error: error instanceof Error ? error.message : error 
      });
      throw error;
    }
  }

  /**
   * Get table columns
   */
  private async getTableColumns(tableName: string, schemaName: string): Promise<ColumnInfo[]> {
    const sql = `
      SELECT 
        c.column_name,
        c.data_type,
        c.is_nullable = 'YES' as is_nullable,
        c.column_default as default_value,
        c.character_maximum_length as max_length,
        c.numeric_precision as precision,
        c.numeric_scale as scale,
        CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key,
        CASE WHEN fk.column_name IS NOT NULL THEN true ELSE false END as is_foreign_key
      FROM information_schema.columns c
      LEFT JOIN (
        SELECT ku.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage ku 
          ON tc.constraint_name = ku.constraint_name
          AND tc.table_schema = ku.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_name = $1
          AND tc.table_schema = $2
      ) pk ON pk.column_name = c.column_name
      LEFT JOIN (
        SELECT ku.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage ku 
          ON tc.constraint_name = ku.constraint_name
          AND tc.table_schema = ku.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_name = $1
          AND tc.table_schema = $2
      ) fk ON fk.column_name = c.column_name
      WHERE c.table_name = $1 AND c.table_schema = $2
      ORDER BY c.ordinal_position
    `;

    const result = await this.dbManager.query(sql, [tableName, schemaName], { readOnly: true });
    
    return result.rows.map((row: any) => ({
      columnName: row.column_name,
      dataType: row.data_type,
      isNullable: row.is_nullable,
      defaultValue: row.default_value,
      isPrimaryKey: row.is_primary_key,
      isForeignKey: row.is_foreign_key,
      maxLength: row.max_length,
      precision: row.precision,
      scale: row.scale
    }));
  }

  /**
   * Get table basic info
   */
  private async getTableBasicInfo(tableName: string, schemaName: string): Promise<TableInfo> {
    const sql = `
      SELECT 
        t.table_schema as schema_name,
        t.table_name,
        t.table_type,
        pg_catalog.pg_get_userbyid(c.relowner) as owner,
        c.relhasindex as has_indexes,
        c.relhasrules as has_rules,
        c.relhastriggers as has_triggers,
        pg_catalog.pg_relation_size(c.oid) as size_bytes,
        c.reltuples::bigint as row_count
      FROM information_schema.tables t
      JOIN pg_catalog.pg_class c ON c.relname = t.table_name
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
      WHERE t.table_name = $1 AND t.table_schema = $2
    `;

    const result = await this.dbManager.query(sql, [tableName, schemaName], { readOnly: true });
    
    if (result.rows.length === 0) {
      throw new Error(`Table ${schemaName}.${tableName} not found`);
    }

    const row = result.rows[0];
    return {
      schemaName: row.schema_name,
      tableName: row.table_name,
      tableType: row.table_type,
      owner: row.owner,
      hasIndexes: row.has_indexes,
      hasRules: row.has_rules,
      hasTriggers: row.has_triggers,
      sizeBytes: parseInt(row.size_bytes) || 0,
      rowCount: parseInt(row.row_count) || 0
    };
  }

  /**
   * Get table indexes
   */
  private async getTableIndexes(tableName: string, schemaName: string): Promise<any[]> {
    const sql = `
      SELECT 
        indexname,
        indexdef,
        schemaname
      FROM pg_indexes 
      WHERE tablename = $1 AND schemaname = $2
      ORDER BY indexname
    `;

    const result = await this.dbManager.query(sql, [tableName, schemaName], { readOnly: true });
    return result.rows;
  }

  /**
   * Get table constraints
   */
  private async getTableConstraints(tableName: string, schemaName: string): Promise<any[]> {
    const sql = `
      SELECT 
        tc.constraint_name,
        tc.constraint_type,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
      FROM information_schema.table_constraints tc 
      LEFT JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      LEFT JOIN information_schema.constraint_column_usage ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
      WHERE tc.table_name = $1 AND tc.table_schema = $2
      ORDER BY tc.constraint_type, tc.constraint_name
    `;

    const result = await this.dbManager.query(sql, [tableName, schemaName], { readOnly: true });
    return result.rows;
  }

  /**
   * Get table statistics
   */
  private async getTableStatistics(tableName: string, schemaName: string): Promise<any> {
    const sql = `
      SELECT 
        schemaname,
        tablename,
        seq_scan,
        seq_tup_read,
        idx_scan,
        idx_tup_fetch,
        n_tup_ins,
        n_tup_upd,
        n_tup_del,
        n_tup_hot_upd,
        n_live_tup,
        n_dead_tup,
        last_vacuum,
        last_autovacuum,
        last_analyze,
        last_autoanalyze
      FROM pg_stat_user_tables 
      WHERE tablename = $1 AND schemaname = $2
    `;

    const result = await this.dbManager.query(sql, [tableName, schemaName], { readOnly: true });
    return result.rows[0] || {};
  }
}