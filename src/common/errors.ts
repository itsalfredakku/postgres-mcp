/**
 * Custom error classes for better error handling and debugging
 */

export enum ErrorCode {
  // Database Errors
  CONNECTION_FAILED = 'CONNECTION_FAILED',
  QUERY_FAILED = 'QUERY_FAILED',
  TRANSACTION_FAILED = 'TRANSACTION_FAILED',
  TIMEOUT = 'TIMEOUT',
  
  // Validation Errors
  INVALID_PARAMETER = 'INVALID_PARAMETER',
  MISSING_PARAMETER = 'MISSING_PARAMETER',
  INVALID_SQL = 'INVALID_SQL',
  
  // Authorization Errors
  PERMISSION_DENIED = 'PERMISSION_DENIED',
  READ_ONLY_MODE = 'READ_ONLY_MODE',
  SCHEMA_ACCESS_DENIED = 'SCHEMA_ACCESS_DENIED',
  
  // Resource Errors
  TABLE_NOT_FOUND = 'TABLE_NOT_FOUND',
  SCHEMA_NOT_FOUND = 'SCHEMA_NOT_FOUND',
  INDEX_NOT_FOUND = 'INDEX_NOT_FOUND',
  
  // System Errors
  POOL_EXHAUSTED = 'POOL_EXHAUSTED',
  RATE_LIMIT_EXCEEDED = 'RATE_LIMIT_EXCEEDED',
  INTERNAL_ERROR = 'INTERNAL_ERROR'
}

export class DatabaseError extends Error {
  constructor(
    public code: ErrorCode,
    message: string,
    public context?: Record<string, any>,
    public cause?: Error
  ) {
    super(message);
    this.name = 'DatabaseError';
  }

  toJSON() {
    return {
      name: this.name,
      code: this.code,
      message: this.message,
      context: this.context,
      stack: this.stack
    };
  }
}

export class ConnectionError extends DatabaseError {
  constructor(message: string, context?: Record<string, any>, cause?: Error) {
    super(ErrorCode.CONNECTION_FAILED, message, context, cause);
    this.name = 'ConnectionError';
  }
}

export class QueryError extends DatabaseError {
  constructor(
    message: string, 
    public sql?: string,
    public parameters?: any[],
    context?: Record<string, any>,
    cause?: Error
  ) {
    super(ErrorCode.QUERY_FAILED, message, { sql, parameters, ...context }, cause);
    this.name = 'QueryError';
  }
}

export class ValidationError extends DatabaseError {
  constructor(
    message: string,
    public field?: string,
    context?: Record<string, any>
  ) {
    super(ErrorCode.INVALID_PARAMETER, message, { field, ...context });
    this.name = 'ValidationError';
  }
}

export class PermissionError extends DatabaseError {
  constructor(message: string, context?: Record<string, any>) {
    super(ErrorCode.PERMISSION_DENIED, message, context);
    this.name = 'PermissionError';
  }
}

/**
 * Error handler utility for consistent error processing
 */
export class ErrorHandler {
  static isDatabaseError(error: any): error is DatabaseError {
    return error instanceof DatabaseError;
  }

  static isRetryableError(error: any): boolean {
    if (!this.isDatabaseError(error)) {
      return false;
    }

    const retryableCodes = [
      ErrorCode.CONNECTION_FAILED,
      ErrorCode.TIMEOUT,
      ErrorCode.POOL_EXHAUSTED
    ];

    return retryableCodes.includes(error.code);
  }

  static sanitizeError(error: any): DatabaseError {
    if (this.isDatabaseError(error)) {
      return error;
    }

    // Handle PostgreSQL errors
    if (error.code) {
      switch (error.code) {
        case '28P01': // invalid_password
        case '28000': // invalid_authorization_specification
          return new PermissionError('Authentication failed', { pgCode: error.code });
        
        case '3D000': // invalid_catalog_name
          return new DatabaseError(ErrorCode.SCHEMA_NOT_FOUND, 'Database not found', { pgCode: error.code });
        
        case '42P01': // undefined_table
          return new DatabaseError(ErrorCode.TABLE_NOT_FOUND, 'Table not found', { pgCode: error.code });
        
        case '42601': // syntax_error
          return new DatabaseError(ErrorCode.INVALID_SQL, 'SQL syntax error', { pgCode: error.code });
        
        case '57014': // query_canceled
          return new DatabaseError(ErrorCode.TIMEOUT, 'Query was canceled', { pgCode: error.code });
        
        default:
          return new QueryError(error.message || 'Database operation failed', undefined, undefined, { pgCode: error.code });
      }
    }

    // Handle generic errors
    return new DatabaseError(
      ErrorCode.INTERNAL_ERROR,
      error.message || 'An unexpected error occurred',
      undefined,
      error
    );
  }

  static getRetryDelay(attempt: number, baseDelay: number = 1000): number {
    // Exponential backoff with jitter
    const delay = baseDelay * Math.pow(2, attempt - 1);
    const jitter = Math.random() * 0.1 * delay;
    return Math.min(delay + jitter, 30000); // Max 30 seconds
  }
}

/**
 * Retry decorator for database operations
 */
export function withRetry<T extends any[], R>(
  maxAttempts: number = 3,
  baseDelay: number = 1000
) {
  return function (
    target: any,
    propertyKey: string,
    descriptor: TypedPropertyDescriptor<(...args: T) => Promise<R>>
  ) {
    const originalMethod = descriptor.value!;

    descriptor.value = async function (...args: T): Promise<R> {
      let lastError: any;

      for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
          return await originalMethod.apply(this, args);
        } catch (error) {
          lastError = ErrorHandler.sanitizeError(error);

          if (attempt === maxAttempts || !ErrorHandler.isRetryableError(lastError)) {
            throw lastError;
          }

          const delay = ErrorHandler.getRetryDelay(attempt, baseDelay);
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }

      throw lastError;
    };

    return descriptor;
  };
}