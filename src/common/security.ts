import { logger } from '../logger.js';
import { ConfigManager } from '../config.js';
import { DatabaseError, ErrorCode } from './errors.js';

export interface RateLimitEntry {
  count: number;
  resetTime: number;
  lastRequest: number;
}

export interface SecurityContext {
  clientId?: string;
  userAgent?: string;
  ipAddress?: string;
  operation: string;
  timestamp: number;
}

export class RateLimiter {
  private requests: Map<string, RateLimitEntry> = new Map();
  private config: ConfigManager;
  private cleanupInterval: NodeJS.Timeout;

  constructor(config: ConfigManager) {
    this.config = config;
    
    // Cleanup expired entries every minute
    this.cleanupInterval = setInterval(() => {
      this.cleanup();
    }, 60000);
  }

  /**
   * Check if request is within rate limits
   */
  checkLimit(identifier: string, operation?: string): boolean {
    const rateLimitConfig = this.config.get().rateLimiting;
    const now = Date.now();
    const windowMs = rateLimitConfig.windowMs;
    const maxRequests = rateLimitConfig.maxRequests;

    // Get or create rate limit entry
    let entry = this.requests.get(identifier);
    
    if (!entry || now > entry.resetTime) {
      // Create new window
      entry = {
        count: 1,
        resetTime: now + windowMs,
        lastRequest: now
      };
      this.requests.set(identifier, entry);
      return true;
    }

    // Update existing entry
    entry.lastRequest = now;

    if (entry.count >= maxRequests) {
      logger.warn('Rate limit exceeded', { 
        identifier, 
        operation,
        count: entry.count,
        maxRequests,
        resetTime: new Date(entry.resetTime).toISOString()
      });
      return false;
    }

    entry.count++;
    return true;
  }

  /**
   * Get rate limit status
   */
  getStatus(identifier: string): {
    remaining: number;
    resetTime: number;
    totalRequests: number;
  } {
    const rateLimitConfig = this.config.get().rateLimiting;
    const entry = this.requests.get(identifier);
    
    if (!entry || Date.now() > entry.resetTime) {
      return {
        remaining: rateLimitConfig.maxRequests,
        resetTime: Date.now() + rateLimitConfig.windowMs,
        totalRequests: 0
      };
    }

    return {
      remaining: Math.max(0, rateLimitConfig.maxRequests - entry.count),
      resetTime: entry.resetTime,
      totalRequests: entry.count
    };
  }

  /**
   * Reset rate limit for identifier
   */
  reset(identifier: string): void {
    this.requests.delete(identifier);
    logger.info('Rate limit reset', { identifier });
  }

  /**
   * Get all current rate limit entries
   */
  getAllEntries(): Map<string, RateLimitEntry> {
    return new Map(this.requests);
  }

  /**
   * Cleanup expired entries
   */
  private cleanup(): void {
    const now = Date.now();
    let cleaned = 0;

    for (const [identifier, entry] of this.requests) {
      if (now > entry.resetTime) {
        this.requests.delete(identifier);
        cleaned++;
      }
    }

    if (cleaned > 0) {
      logger.debug('Rate limit cleanup', { entriesRemoved: cleaned });
    }
  }

  /**
   * Destroy rate limiter
   */
  destroy(): void {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
    }
    this.requests.clear();
  }
}

export class SecurityValidator {
  private config: ConfigManager;
  private suspiciousPatterns: RegExp[];
  private blockedOperations: Set<string>;

  constructor(config: ConfigManager) {
    this.config = config;
    
    // Suspicious SQL patterns that might indicate SQL injection
    this.suspiciousPatterns = [
      /(\bUNION\b.*\bSELECT\b)/i,
      /(\bOR\b.*=.*)/i,
      /(\bAND\b.*=.*)/i,
      /(;.*\bDROP\b)/i,
      /(;.*\bDELETE\b)/i,
      /(;.*\bUPDATE\b)/i,
      /(;.*\bINSERT\b)/i,
      /(\bxp_cmdshell\b)/i,
      /(\bsp_executesql\b)/i,
      /(\b--.*)/,
      /(\/\*.*\*\/)/,
      /(\bCONCAT\b.*\bCHAR\b)/i,
      /(\bEXEC\b.*\bXP_\b)/i
    ];

    // Operations that are blocked in read-only mode
    this.blockedOperations = new Set([
      'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER',
      'TRUNCATE', 'GRANT', 'REVOKE', 'COPY', 'VACUUM', 'ANALYZE'
    ]);
  }

  /**
   * Validate SQL query for security issues
   */
  validateSqlSecurity(sql: string, context: SecurityContext): void {
    const trimmedSql = sql.trim();
    
    // Check for suspicious patterns
    for (const pattern of this.suspiciousPatterns) {
      if (pattern.test(sql)) {
        logger.warn('Suspicious SQL pattern detected', {
          pattern: pattern.toString(),
          operation: context.operation,
          clientId: context.clientId,
          sql: sql.substring(0, 100)
        });
        
        // In production, we might want to block these
        if (process.env.NODE_ENV === 'production') {
          throw new DatabaseError(
            ErrorCode.INVALID_SQL,
            'SQL query contains potentially dangerous patterns',
            { pattern: pattern.toString() }
          );
        }
      }
    }

    // Check read-only mode restrictions
    if (this.config.isReadOnlyMode()) {
      const normalizedSql = trimmedSql.toUpperCase();
      for (const operation of this.blockedOperations) {
        if (normalizedSql.startsWith(operation)) {
          throw new DatabaseError(
            ErrorCode.READ_ONLY_MODE,
            `Operation '${operation}' is not allowed in read-only mode`
          );
        }
      }
    }

    // Check for potentially dangerous administrative operations
    if (this.isDangerousAdminOperation(sql)) {
      logger.warn('Potentially dangerous admin operation attempted', {
        operation: context.operation,
        clientId: context.clientId,
        sql: sql.substring(0, 100)
      });
      
      // Require special permission for these operations
      if (process.env.ALLOW_DANGEROUS_OPERATIONS !== 'true') {
        throw new DatabaseError(
          ErrorCode.PERMISSION_DENIED,
          'Dangerous administrative operations are disabled. Set ALLOW_DANGEROUS_OPERATIONS=true to enable.',
          { sql: sql.substring(0, 100) }
        );
      }
    }
  }

  /**
   * Validate schema access
   */
  validateSchemaAccess(schemaName: string): void {
    if (!this.config.isSchemaAllowed(schemaName)) {
      throw new DatabaseError(
        ErrorCode.SCHEMA_ACCESS_DENIED,
        `Access to schema '${schemaName}' is not allowed`,
        { schema: schemaName }
      );
    }
  }

  /**
   * Validate table access
   */
  validateTableAccess(tableName: string, schemaName: string = 'public'): void {
    const fullTableName = `${schemaName}.${tableName}`;
    
    if (this.config.isTableRestricted(tableName) || this.config.isTableRestricted(fullTableName)) {
      throw new DatabaseError(
        ErrorCode.PERMISSION_DENIED,
        `Access to table '${fullTableName}' is restricted`,
        { table: fullTableName }
      );
    }
  }

  /**
   * Generate security context from request
   */
  createSecurityContext(operation: string, additionalContext?: Record<string, any>): SecurityContext {
    return {
      operation,
      timestamp: Date.now(),
      clientId: additionalContext?.clientId || 'unknown',
      userAgent: additionalContext?.userAgent,
      ipAddress: additionalContext?.ipAddress
    };
  }

  /**
   * Check if operation is a dangerous admin operation
   */
  private isDangerousAdminOperation(sql: string): boolean {
    const dangerousPatterns = [
      /DROP\s+DATABASE/i,
      /ALTER\s+USER.*SUPERUSER/i,
      /CREATE\s+USER.*SUPERUSER/i,
      /GRANT\s+ALL\s+PRIVILEGES/i,
      /ALTER\s+SYSTEM/i,
      /SET\s+ROLE/i,
      /RESET\s+ROLE/i,
      /TRUNCATE\s+pg_/i,
      /DELETE\s+FROM\s+pg_/i,
      /UPDATE\s+pg_/i,
      /COPY.*FROM\s+PROGRAM/i,
      /CREATE\s+EXTENSION/i,
      /DROP\s+EXTENSION/i
    ];

    return dangerousPatterns.some(pattern => pattern.test(sql));
  }

  /**
   * Log security event
   */
  logSecurityEvent(
    level: 'info' | 'warn' | 'error',
    event: string,
    context: SecurityContext,
    additionalData?: Record<string, any>
  ): void {
    logger[level](`Security event: ${event}`, {
      ...context,
      ...additionalData,
      type: 'security_event'
    });
  }
}

/**
 * Decorator for rate limiting
 */
export function rateLimit(identifierKey: string = 'clientId') {
  return function (
    target: any,
    propertyKey: string,
    descriptor: PropertyDescriptor
  ) {
    const originalMethod = descriptor.value;

    descriptor.value = async function (...args: any[]) {
      const rateLimiter = (this as any).rateLimiter as RateLimiter;
      
      if (!rateLimiter) {
        return originalMethod.apply(this, args);
      }

      const identifier = args[0]?.[identifierKey] || 'anonymous';
      const operation = `${target.constructor.name}.${propertyKey}`;
      
      if (!rateLimiter.checkLimit(identifier, operation)) {
        throw new DatabaseError(
          ErrorCode.RATE_LIMIT_EXCEEDED,
          'Rate limit exceeded. Please try again later.',
          { identifier, operation }
        );
      }

      return originalMethod.apply(this, args);
    };

    return descriptor;
  };
}

/**
 * Decorator for security validation
 */
export function secure(options: { 
  validateSql?: boolean;
  requireSchema?: boolean;
  requireTable?: boolean;
} = {}) {
  return function (
    target: any,
    propertyKey: string,
    descriptor: PropertyDescriptor
  ) {
    const originalMethod = descriptor.value;

    descriptor.value = async function (...args: any[]) {
      const securityValidator = (this as any).securityValidator as SecurityValidator;
      
      if (!securityValidator) {
        return originalMethod.apply(this, args);
      }

      const context = securityValidator.createSecurityContext(
        `${target.constructor.name}.${propertyKey}`,
        args[0]
      );

      // Validate SQL if required
      if (options.validateSql && args[0]?.sql) {
        securityValidator.validateSqlSecurity(args[0].sql, context);
      }

      // Validate schema access if required
      if (options.requireSchema && args[0]?.schemaName) {
        securityValidator.validateSchemaAccess(args[0].schemaName);
      }

      // Validate table access if required
      if (options.requireTable && args[0]?.tableName) {
        securityValidator.validateTableAccess(
          args[0].tableName,
          args[0].schemaName || 'public'
        );
      }

      return originalMethod.apply(this, args);
    };

    return descriptor;
  };
}