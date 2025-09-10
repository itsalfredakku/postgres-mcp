import Joi from 'joi';
import { ErrorCode, McpError } from '@modelcontextprotocol/sdk/types.js';

export class ValidationError extends Error {
  constructor(message: string, public field?: string, public code?: string) {
    super(message);
    this.name = 'ValidationError';
  }
}

export class ParameterValidator {
  /**
   * Validate SQL query
   */
  static validateSql(sql: string, field: string = 'sql'): string {
    if (!sql || typeof sql !== 'string') {
      throw new ValidationError(`${field} is required and must be a string`, field);
    }

    const trimmedSql = sql.trim();
    if (trimmedSql.length === 0) {
      throw new ValidationError(`${field} cannot be empty`, field);
    }

    // Check for potentially dangerous operations in production
    if (process.env.NODE_ENV === 'production') {
      const dangerousPatterns = [
        /DROP\s+DATABASE/i,
        /TRUNCATE\s+pg_/i,
        /DELETE\s+FROM\s+pg_/i,
        /ALTER\s+USER.*SUPERUSER/i,
      ];

      for (const pattern of dangerousPatterns) {
        if (pattern.test(trimmedSql)) {
          throw new ValidationError(`Potentially dangerous SQL operation detected in ${field}`, field);
        }
      }
    }

    return trimmedSql;
  }

  /**
   * Validate table name
   */
  static validateTableName(tableName: string, field: string = 'tableName'): string {
    const schema = Joi.string()
      .pattern(/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$/)
      .min(1)
      .max(63)
      .required();

    const { error, value } = schema.validate(tableName);
    if (error) {
      throw new ValidationError(
        `Invalid ${field}: ${error.details[0].message}`,
        field
      );
    }

    return value;
  }

  /**
   * Validate schema name
   */
  static validateSchemaName(schemaName: string, field: string = 'schemaName'): string {
    const schema = Joi.string()
      .pattern(/^[a-zA-Z_][a-zA-Z0-9_]*$/)
      .min(1)
      .max(63)
      .required();

    const { error, value } = schema.validate(schemaName);
    if (error) {
      throw new ValidationError(
        `Invalid ${field}: ${error.details[0].message}`,
        field
      );
    }

    return value;
  }

  /**
   * Validate column name
   */
  static validateColumnName(columnName: string, field: string = 'columnName'): string {
    const schema = Joi.string()
      .pattern(/^[a-zA-Z_][a-zA-Z0-9_]*$/)
      .min(1)
      .max(63)
      .required();

    const { error, value } = schema.validate(columnName);
    if (error) {
      throw new ValidationError(
        `Invalid ${field}: ${error.details[0].message}`,
        field
      );
    }

    return value;
  }

  /**
   * Validate index name
   */
  static validateIndexName(indexName: string, field: string = 'indexName'): string {
    const schema = Joi.string()
      .pattern(/^[a-zA-Z_][a-zA-Z0-9_]*$/)
      .min(1)
      .max(63)
      .required();

    const { error, value } = schema.validate(indexName);
    if (error) {
      throw new ValidationError(
        `Invalid ${field}: ${error.details[0].message}`,
        field
      );
    }

    return value;
  }

  /**
   * Validate data type
   */
  static validateDataType(dataType: string, field: string = 'dataType'): string {
    const validTypes = [
      'SERIAL', 'BIGSERIAL', 'SMALLSERIAL',
      'INTEGER', 'BIGINT', 'SMALLINT',
      'DECIMAL', 'NUMERIC', 'REAL', 'DOUBLE PRECISION',
      'MONEY',
      'CHARACTER VARYING', 'VARCHAR', 'CHARACTER', 'CHAR', 'TEXT',
      'BYTEA',
      'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP WITHOUT TIME ZONE',
      'DATE', 'TIME', 'TIME WITH TIME ZONE', 'TIME WITHOUT TIME ZONE',
      'INTERVAL',
      'BOOLEAN',
      'POINT', 'LINE', 'LSEG', 'BOX', 'PATH', 'POLYGON', 'CIRCLE',
      'CIDR', 'INET', 'MACADDR', 'MACADDR8',
      'BIT', 'BIT VARYING',
      'TSVECTOR', 'TSQUERY',
      'UUID',
      'XML',
      'JSON', 'JSONB',
      'ARRAY'
    ];

    const normalizedType = dataType.toUpperCase().trim();
    
    // Check for parameterized types (e.g., VARCHAR(255))
    const parameterizedPattern = /^(VARCHAR|CHARACTER VARYING|CHAR|CHARACTER|NUMERIC|DECIMAL|BIT|BIT VARYING)\s*\(\s*\d+(\s*,\s*\d+)?\s*\)$/;
    
    if (!validTypes.includes(normalizedType) && !parameterizedPattern.test(normalizedType)) {
      throw new ValidationError(
        `Invalid ${field}: ${dataType}. Must be a valid PostgreSQL data type`,
        field
      );
    }

    return dataType;
  }

  /**
   * Validate required parameter
   */
  static validateRequired(value: any, field: string): any {
    if (value === undefined || value === null || value === '') {
      throw new ValidationError(`${field} is required`, field);
    }
    return value;
  }

  /**
   * Validate numeric parameter
   */
  static validateNumber(value: any, field: string, min?: number, max?: number): number {
    const schema = Joi.number().integer();
    
    if (min !== undefined) schema.min(min);
    if (max !== undefined) schema.max(max);

    const { error, value: validatedValue } = schema.validate(value);
    if (error) {
      throw new ValidationError(
        `Invalid ${field}: ${error.details[0].message}`,
        field
      );
    }

    return validatedValue;
  }

  /**
   * Validate limit parameter
   */
  static validateLimit(limit: any, field: string = 'limit'): number {
    return this.validateNumber(limit, field, 1, 10000);
  }

  /**
   * Validate offset parameter
   */
  static validateOffset(offset: any, field: string = 'offset'): number {
    return this.validateNumber(offset, field, 0);
  }

  /**
   * Validate transaction ID
   */
  static validateTransactionId(transactionId: string, field: string = 'transactionId'): string {
    const schema = Joi.string().uuid().required();

    const { error, value } = schema.validate(transactionId);
    if (error) {
      throw new ValidationError(
        `Invalid ${field}: must be a valid UUID`,
        field
      );
    }

    return value;
  }

  /**
   * Validate file path for import/export operations
   */
  static validateFilePath(filePath: string, field: string = 'filePath'): string {
    const schema = Joi.string()
      .pattern(/^[^<>:"|?*\x00-\x1f]+$/)
      .min(1)
      .max(260)
      .required();

    const { error, value } = schema.validate(filePath);
    if (error) {
      throw new ValidationError(
        `Invalid ${field}: ${error.details[0].message}`,
        field
      );
    }

    // Additional security check for path traversal
    if (filePath.includes('..') || filePath.includes('~')) {
      throw new ValidationError(
        `Invalid ${field}: path traversal detected`,
        field
      );
    }

    return value;
  }

  /**
   * Validate JSON data
   */
  static validateJson(jsonString: string, field: string = 'json'): any {
    try {
      return JSON.parse(jsonString);
    } catch (error) {
      throw new ValidationError(
        `Invalid ${field}: must be valid JSON`,
        field
      );
    }
  }

  /**
   * Convert ValidationError to McpError
   */
  static toMcpError(error: ValidationError): McpError {
    return new McpError(
      ErrorCode.InvalidParams,
      `Validation failed for ${error.field || 'parameter'}: ${error.message}`
    );
  }
}

// Tool name mappings for better error messages
export const TOOL_NAME_MAPPINGS: Record<string, string> = {
  'execute': 'query',
  'run': 'query',
  'sql': 'query',
  'table': 'tables',
  'schema': 'schemas',
  'index': 'indexes',
  'transaction': 'transactions',
  'backup': 'backup',
  'restore': 'backup',
  'import': 'import_export',
  'export': 'import_export',
  'user': 'admin',
  'permission': 'admin',
  'monitor': 'monitoring',
  'stats': 'monitoring',
  'connection': 'connections'
};

/**
 * Suggest correct tool name for common misspellings
 */
export function suggestToolName(attemptedName: string): string {
  const mapped = TOOL_NAME_MAPPINGS[attemptedName.toLowerCase()];
  if (mapped) {
    return `Did you mean '${mapped}'?`;
  }

  // Find closest match using simple string similarity
  const availableTools = [
    'query', 'tables', 'schemas', 'indexes', 'data', 'transactions',
    'import_export', 'backup', 'admin', 'monitoring', 'connections'
  ];

  const closeMatch = availableTools.find(tool => 
    tool.includes(attemptedName.toLowerCase()) || 
    attemptedName.toLowerCase().includes(tool)
  );

  return closeMatch ? `Did you mean '${closeMatch}'?` : 'Please check available tools.';
}