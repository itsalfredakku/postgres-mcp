import { DatabaseConnectionManager } from '../../database/connection-manager.js';
import { ParameterValidator } from '../../validation.js';
import { logger } from '../../logger.js';
import { QueryResultCache } from '../../common/cache.js';
import { DatabaseError, ErrorCode } from '../../common/errors.js';

export interface SchemaInfo {
  schemaName: string;
  owner: string;
  schemaType: 'system' | 'user';
  tableCount: number;
  viewCount: number;
  functionCount: number;
  sizeBytes: number;
  permissions: string[];
}

export interface SchemaPermission {
  grantee: string;
  privilege: string;
  isGrantable: boolean;
  grantor: string;
}

export interface CreateSchemaOptions {
  ifNotExists?: boolean;
  owner?: string;
  authorization?: string;
}

export class SchemaAPIClient {
  constructor(
    private dbManager: DatabaseConnectionManager,
    private cache?: QueryResultCache
  ) {}

  /**
   * List all schemas with detailed information
   */
  async listSchemas(includeSystem: boolean = false): Promise<SchemaInfo[]> {
    const cacheKey = `schemas_list_${includeSystem}`;
    
    // Check cache first
    if (this.cache) {
      const cached = this.cache.get<SchemaInfo[]>(cacheKey);
      if (cached) return cached;
    }

    // Simplified query to avoid potential issues with complex joins
    let sql = `
      SELECT 
        n.nspname as schema_name,
        pg_catalog.pg_get_userbyid(n.nspowner) as owner,
        CASE 
          WHEN n.nspname IN ('information_schema', 'pg_catalog', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1') 
          THEN 'system'
          ELSE 'user'
        END as schema_type
      FROM pg_catalog.pg_namespace n
      WHERE 1=1
    `;

    if (!includeSystem) {
      sql += ` AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
               AND n.nspname NOT LIKE 'pg_temp_%'
               AND n.nspname NOT LIKE 'pg_toast_temp_%'`;
    }

    sql += ` ORDER BY 
      CASE WHEN n.nspname = 'public' THEN 1 ELSE 2 END,
      CASE 
        WHEN n.nspname IN ('information_schema', 'pg_catalog', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1') 
        THEN 'system'
        ELSE 'user'
      END,
      n.nspname`;

    try {
      const result = await this.dbManager.query(sql, [], { readOnly: true });
      
      // Get additional stats for each schema separately to avoid complex joins
      const schemas: SchemaInfo[] = await Promise.all(
        result.rows.map(async (row: any) => {
          const schemaName = row.schema_name;
          
          // Get table count
          let tableCount = 0;
          try {
            const tableResult = await this.dbManager.query(
              'SELECT COUNT(*) as count FROM pg_tables WHERE schemaname = $1',
              [schemaName],
              { readOnly: true }
            );
            tableCount = parseInt(tableResult.rows[0]?.count) || 0;
          } catch (error) {
            logger.warn('Failed to get table count for schema', { schema: schemaName, error });
          }

          // Get view count
          let viewCount = 0;
          try {
            const viewResult = await this.dbManager.query(
              'SELECT COUNT(*) as count FROM pg_views WHERE schemaname = $1',
              [schemaName],
              { readOnly: true }
            );
            viewCount = parseInt(viewResult.rows[0]?.count) || 0;
          } catch (error) {
            logger.warn('Failed to get view count for schema', { schema: schemaName, error });
          }

          // Get function count
          let functionCount = 0;
          try {
            const functionResult = await this.dbManager.query(
              `SELECT COUNT(*) as count 
               FROM pg_proc p
               JOIN pg_namespace n ON p.pronamespace = n.oid
               WHERE n.nspname = $1`,
              [schemaName],
              { readOnly: true }
            );
            functionCount = parseInt(functionResult.rows[0]?.count) || 0;
          } catch (error) {
            logger.warn('Failed to get function count for schema', { schema: schemaName, error });
          }

          // Get permissions (simplified - don't fail if this doesn't work)
          let permissions: string[] = [];
          try {
            const schemaPermissions = await this.getSchemaPermissions(schemaName);
            permissions = schemaPermissions.map(p => `${p.grantee}:${p.privilege}`);
          } catch (error) {
            logger.warn('Failed to get permissions for schema', { schema: schemaName, error });
          }
          
          return {
            schemaName: row.schema_name,
            owner: row.owner,
            schemaType: row.schema_type,
            tableCount,
            viewCount,
            functionCount,
            sizeBytes: 0, // Calculate separately if needed
            permissions
          };
        })
      );

      // Cache the result
      if (this.cache) {
        this.cache.set(cacheKey, schemas);
      }

      return schemas;
    } catch (error) {
      logger.error('Failed to list schemas', { error: error instanceof Error ? error.message : error });
      throw new DatabaseError(
        ErrorCode.QUERY_FAILED,
        'Failed to retrieve schema information',
        { operation: 'listSchemas' },
        error as Error
      );
    }
  }

  /**
   * Get detailed information about a specific schema
   */
  async getSchemaInfo(schemaName: string): Promise<{
    schema: SchemaInfo;
    tables: any[];
    views: any[];
    functions: any[];
    permissions: SchemaPermission[];
    dependencies: any[];
  }> {
    const validatedSchema = ParameterValidator.validateSchemaName(schemaName);

    try {
      // Get basic schema info
      const schemas = await this.listSchemas(true);
      const schema = schemas.find(s => s.schemaName === validatedSchema);
      
      if (!schema) {
        throw new DatabaseError(
          ErrorCode.SCHEMA_NOT_FOUND,
          `Schema '${validatedSchema}' not found`
        );
      }

      // Get detailed information in parallel
      const [tables, views, functions, permissions, dependencies] = await Promise.all([
        this.getSchemaTables(validatedSchema),
        this.getSchemaViews(validatedSchema),
        this.getSchemaFunctions(validatedSchema),
        this.getSchemaPermissions(validatedSchema),
        this.getSchemaDependencies(validatedSchema)
      ]);

      return {
        schema,
        tables,
        views,
        functions,
        permissions,
        dependencies
      };
    } catch (error) {
      if (error instanceof DatabaseError) throw error;
      
      logger.error('Failed to get schema info', { 
        schema: validatedSchema,
        error: error instanceof Error ? error.message : error 
      });
      throw new DatabaseError(
        ErrorCode.QUERY_FAILED,
        `Failed to retrieve information for schema '${validatedSchema}'`,
        { schema: validatedSchema },
        error as Error
      );
    }
  }

  /**
   * Create a new schema
   */
  async createSchema(
    schemaName: string,
    options: CreateSchemaOptions = {}
  ): Promise<{ success: boolean; message: string }> {
    const validatedSchema = ParameterValidator.validateSchemaName(schemaName);

    // Build CREATE SCHEMA statement
    let sql = 'CREATE SCHEMA';
    
    if (options.ifNotExists) {
      sql += ' IF NOT EXISTS';
    }
    
    sql += ` ${validatedSchema}`;
    
    if (options.authorization) {
      sql += ` AUTHORIZATION ${options.authorization}`;
    } else if (options.owner) {
      sql += ` AUTHORIZATION ${options.owner}`;
    }

    try {
      await this.dbManager.query(sql);
      
      // Invalidate cache
      if (this.cache) {
        this.cache.invalidate('schemas_list');
      }
      
      logger.info('Schema created successfully', { 
        schema: validatedSchema,
        owner: options.owner || options.authorization 
      });
      
      return {
        success: true,
        message: `Schema '${validatedSchema}' created successfully`
      };
    } catch (error) {
      logger.error('Failed to create schema', { 
        schema: validatedSchema,
        error: error instanceof Error ? error.message : error 
      });
      throw new DatabaseError(
        ErrorCode.QUERY_FAILED,
        `Failed to create schema '${validatedSchema}'`,
        { schema: validatedSchema, options },
        error as Error
      );
    }
  }

  /**
   * Drop a schema
   */
  async dropSchema(
    schemaName: string,
    cascade: boolean = false,
    ifExists: boolean = true
  ): Promise<{ success: boolean; message: string }> {
    const validatedSchema = ParameterValidator.validateSchemaName(schemaName);

    // Security check - prevent dropping system schemas
    const systemSchemas = ['information_schema', 'pg_catalog', 'pg_toast', 'public'];
    if (systemSchemas.includes(validatedSchema)) {
      throw new DatabaseError(
        ErrorCode.PERMISSION_DENIED,
        `Cannot drop system schema '${validatedSchema}'`
      );
    }

    let sql = 'DROP SCHEMA';
    if (ifExists) sql += ' IF EXISTS';
    sql += ` ${validatedSchema}`;
    if (cascade) sql += ' CASCADE';

    try {
      await this.dbManager.query(sql);
      
      // Invalidate cache
      if (this.cache) {
        this.cache.invalidate('schemas_list');
      }
      
      logger.info('Schema dropped successfully', { 
        schema: validatedSchema,
        cascade 
      });
      
      return {
        success: true,
        message: `Schema '${validatedSchema}' dropped successfully`
      };
    } catch (error) {
      logger.error('Failed to drop schema', { 
        schema: validatedSchema,
        error: error instanceof Error ? error.message : error 
      });
      throw new DatabaseError(
        ErrorCode.QUERY_FAILED,
        `Failed to drop schema '${validatedSchema}'`,
        { schema: validatedSchema, cascade, ifExists },
        error as Error
      );
    }
  }

  /**
   * Rename a schema
   */
  async renameSchema(
    oldName: string,
    newName: string
  ): Promise<{ success: boolean; message: string }> {
    const validatedOldName = ParameterValidator.validateSchemaName(oldName);
    const validatedNewName = ParameterValidator.validateSchemaName(newName);

    // Security check - prevent renaming system schemas
    const systemSchemas = ['information_schema', 'pg_catalog', 'pg_toast', 'public'];
    if (systemSchemas.includes(validatedOldName)) {
      throw new DatabaseError(
        ErrorCode.PERMISSION_DENIED,
        `Cannot rename system schema '${validatedOldName}'`
      );
    }

    const sql = `ALTER SCHEMA ${validatedOldName} RENAME TO ${validatedNewName}`;

    try {
      await this.dbManager.query(sql);
      
      // Invalidate cache
      if (this.cache) {
        this.cache.invalidate('schemas_list');
      }
      
      logger.info('Schema renamed successfully', { 
        oldName: validatedOldName,
        newName: validatedNewName 
      });
      
      return {
        success: true,
        message: `Schema '${validatedOldName}' renamed to '${validatedNewName}' successfully`
      };
    } catch (error) {
      logger.error('Failed to rename schema', { 
        oldName: validatedOldName,
        newName: validatedNewName,
        error: error instanceof Error ? error.message : error 
      });
      throw new DatabaseError(
        ErrorCode.QUERY_FAILED,
        `Failed to rename schema '${validatedOldName}' to '${validatedNewName}'`,
        { oldName: validatedOldName, newName: validatedNewName },
        error as Error
      );
    }
  }

  /**
   * Get schema permissions
   */
  private async getSchemaPermissions(schemaName: string): Promise<SchemaPermission[]> {
    // Use a simpler approach that's more compatible across PostgreSQL versions
    const sql = `
      SELECT 
        r.rolname as grantee,
        'USAGE' as privilege_type,
        false as is_grantable,
        'postgres' as grantor
      FROM pg_namespace n
      JOIN pg_roles r ON r.oid = n.nspowner
      WHERE n.nspname = $1
      
      UNION ALL
      
      SELECT 
        'public' as grantee,
        'USAGE' as privilege_type,
        false as is_grantable,
        'postgres' as grantor
      WHERE $1 = 'public'
      
      ORDER BY grantee, privilege_type
    `;

    try {
      const result = await this.dbManager.query(sql, [schemaName], { readOnly: true });
      
      return result.rows.map((row: any) => ({
        grantee: row.grantee,
        privilege: row.privilege_type,
        isGrantable: row.is_grantable === true || row.is_grantable === 'YES',
        grantor: row.grantor
      }));
    } catch (error) {
      logger.warn('Failed to get schema permissions', { 
        schema: schemaName,
        error: error instanceof Error ? error.message : error 
      });
      // Return empty array instead of failing
      return [];
    }
  }

  /**
   * Get tables in schema
   */
  private async getSchemaTables(schemaName: string): Promise<any[]> {
    try {
      const sql = `
        SELECT 
          tablename,
          tableowner
        FROM pg_tables 
        WHERE schemaname = $1
        ORDER BY tablename
      `;

      const result = await this.dbManager.query(sql, [schemaName], { readOnly: true });
      return result.rows;
    } catch (error) {
      logger.warn('Failed to get schema tables', { schema: schemaName, error });
      return [];
    }
  }

  /**
   * Get views in schema
   */
  private async getSchemaViews(schemaName: string): Promise<any[]> {
    try {
      const sql = `
        SELECT 
          viewname,
          viewowner
        FROM pg_views 
        WHERE schemaname = $1
        ORDER BY viewname
      `;

      const result = await this.dbManager.query(sql, [schemaName], { readOnly: true });
      return result.rows;
    } catch (error) {
      logger.warn('Failed to get schema views', { schema: schemaName, error });
      return [];
    }
  }

  /**
   * Get functions in schema
   */
  private async getSchemaFunctions(schemaName: string): Promise<any[]> {
    try {
      const sql = `
        SELECT 
          p.proname as function_name,
          pg_catalog.pg_get_userbyid(p.proowner) as owner
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = $1
        ORDER BY p.proname
      `;

      const result = await this.dbManager.query(sql, [schemaName], { readOnly: true });
      return result.rows;
    } catch (error) {
      logger.warn('Failed to get schema functions', { schema: schemaName, error });
      return [];
    }
  }

  /**
   * Get schema dependencies
   */
  private async getSchemaDependencies(schemaName: string): Promise<any[]> {
    try {
      // Simplified dependency query
      const sql = `
        SELECT 
          'dependency' as type,
          c.relname as object_name
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = $1
          AND c.relkind IN ('r', 'v', 'f')  -- tables, views, foreign tables
        ORDER BY c.relname
      `;

      const result = await this.dbManager.query(sql, [schemaName], { readOnly: true });
      return result.rows;
    } catch (error) {
      logger.warn('Failed to get schema dependencies', { schema: schemaName, error });
      return [];
    }
  }
}