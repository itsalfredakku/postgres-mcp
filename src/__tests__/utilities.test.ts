import { describe, it, expect } from '@jest/globals';

// Test validation utilities without external dependencies
describe('Validation Utilities', () => {
  
  describe('SQL Identifier Validation', () => {
    const validateSQLIdentifier = (name: string): boolean => {
      // Basic SQL identifier validation - starts with letter/underscore, contains alphanumeric/underscore
      return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name);
    };

    it('should validate valid SQL identifiers', () => {
      expect(validateSQLIdentifier('users')).toBe(true);
      expect(validateSQLIdentifier('user_accounts')).toBe(true);
      expect(validateSQLIdentifier('_private_table')).toBe(true);
      expect(validateSQLIdentifier('Table123')).toBe(true);
    });

    it('should reject invalid SQL identifiers', () => {
      expect(validateSQLIdentifier('123users')).toBe(false);
      expect(validateSQLIdentifier('user-accounts')).toBe(false);
      expect(validateSQLIdentifier('user accounts')).toBe(false);
      expect(validateSQLIdentifier('user.accounts')).toBe(false);
      expect(validateSQLIdentifier('')).toBe(false);
    });
  });

  describe('PostgreSQL Data Type Validation', () => {
    const validDataTypes = [
      'INTEGER', 'BIGINT', 'SMALLINT', 'SERIAL', 'BIGSERIAL',
      'VARCHAR', 'TEXT', 'CHAR', 'VARCHAR(255)', 'CHAR(10)',
      'BOOLEAN', 'DATE', 'TIMESTAMP', 'TIMESTAMPTZ',
      'NUMERIC', 'DECIMAL', 'REAL', 'DOUBLE PRECISION',
      'UUID', 'JSON', 'JSONB', 'ARRAY', 'BYTEA'
    ];

    const isValidDataType = (dataType: string): boolean => {
      const normalizedType = dataType.toUpperCase().trim();
      return validDataTypes.some(type => 
        normalizedType === type || 
        normalizedType.startsWith(type.split('(')[0])
      );
    };

    it('should validate common PostgreSQL data types', () => {
      expect(isValidDataType('integer')).toBe(true);
      expect(isValidDataType('VARCHAR(100)')).toBe(true);
      expect(isValidDataType('timestamp')).toBe(true);
      expect(isValidDataType('boolean')).toBe(true);
      expect(isValidDataType('jsonb')).toBe(true);
    });

    it('should reject invalid data types', () => {
      expect(isValidDataType('INVALID_TYPE')).toBe(false);
      expect(isValidDataType('string')).toBe(false);
      expect(isValidDataType('number')).toBe(false);
    });
  });

  describe('Connection String Validation', () => {
    const validateConnectionString = (connStr: string): boolean => {
      // Basic PostgreSQL connection string pattern
      const pattern = /^postgresql:\/\/([^:]+):([^@]+)@([^:\/]+):(\d+)\/(.+)$/;
      return pattern.test(connStr);
    };

    it('should validate PostgreSQL connection strings', () => {
      expect(validateConnectionString('postgresql://user:pass@localhost:5432/dbname')).toBe(true);
      expect(validateConnectionString('postgresql://admin:secret123@db.example.com:5432/production')).toBe(true);
    });

    it('should reject invalid connection strings', () => {
      expect(validateConnectionString('mysql://user:pass@localhost:3306/db')).toBe(false);
      expect(validateConnectionString('invalid-connection-string')).toBe(false);
      expect(validateConnectionString('')).toBe(false);
    });
  });
});

describe('Cache Key Generation', () => {
  const generateCacheKey = (operation: string, params: Record<string, any>): string => {
    const sortedParams = Object.keys(params)
      .sort()
      .reduce((result, key) => {
        result[key] = params[key];
        return result;
      }, {} as Record<string, any>);
    
    return `${operation}_${JSON.stringify(sortedParams)}`;
  };

  it('should generate consistent cache keys', () => {
    const params1 = { table: 'users', schema: 'public' };
    const params2 = { schema: 'public', table: 'users' };
    
    const key1 = generateCacheKey('list_tables', params1);
    const key2 = generateCacheKey('list_tables', params2);
    
    expect(key1).toBe(key2);
  });

  it('should generate different keys for different operations', () => {
    const params = { table: 'users' };
    
    const key1 = generateCacheKey('list_tables', params);
    const key2 = generateCacheKey('describe_table', params);
    
    expect(key1).not.toBe(key2);
  });
});

describe('SQL Query Builder Utilities', () => {
  const buildSelectQuery = (table: string, columns: string[] = ['*'], where?: string): string => {
    const columnList = columns.join(', ');
    let query = `SELECT ${columnList} FROM ${table}`;
    
    if (where) {
      query += ` WHERE ${where}`;
    }
    
    return query;
  };

  it('should build basic SELECT queries', () => {
    expect(buildSelectQuery('users')).toBe('SELECT * FROM users');
    expect(buildSelectQuery('users', ['id', 'name'])).toBe('SELECT id, name FROM users');
    expect(buildSelectQuery('users', ['*'], 'active = true')).toBe('SELECT * FROM users WHERE active = true');
  });

  const buildInsertQuery = (table: string, data: Record<string, any>): string => {
    const columns = Object.keys(data);
    const placeholders = columns.map((_, index) => `$${index + 1}`);
    
    return `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${placeholders.join(', ')})`;
  };

  it('should build INSERT queries with placeholders', () => {
    const data = { name: 'John', email: 'john@example.com', age: 30 };
    const query = buildInsertQuery('users', data);
    
    expect(query).toBe('INSERT INTO users (name, email, age) VALUES ($1, $2, $3)');
  });
});

describe('Error Classification', () => {
  const classifyError = (error: Error): string => {
    const message = error.message.toLowerCase();
    
    if (message.includes('connection')) return 'connection';
    if (message.includes('timeout')) return 'timeout';
    if (message.includes('syntax')) return 'syntax';
    if (message.includes('permission') || message.includes('access')) return 'permission';
    if (message.includes('constraint') || message.includes('violation')) return 'constraint';
    
    return 'unknown';
  };

  it('should classify database errors correctly', () => {
    expect(classifyError(new Error('Connection refused'))).toBe('connection');
    expect(classifyError(new Error('Query timeout exceeded'))).toBe('timeout');
    expect(classifyError(new Error('Syntax error near SELECT'))).toBe('syntax');
    expect(classifyError(new Error('Permission denied for table'))).toBe('permission');
    expect(classifyError(new Error('Foreign key constraint violation'))).toBe('constraint');
    expect(classifyError(new Error('Some unknown error'))).toBe('unknown');
  });
});