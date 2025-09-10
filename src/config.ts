import * as fs from 'fs';
import { config as dotenvConfig } from 'dotenv';

// Load .env file
dotenvConfig();

export interface DatabaseConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  ssl: boolean;
  connectionString?: string;
}

export interface PoolConfig {
  min: number;
  max: number;
  idleTimeoutMillis: number;
  acquireTimeoutMillis: number;
  createTimeoutMillis: number;
  reapIntervalMillis: number;
}

export interface Config {
  database: DatabaseConfig;
  pool: PoolConfig;
  cache: {
    enabled: boolean;
    ttl: number;
    maxKeys: number;
  };
  rateLimiting: {
    maxRequests: number;
    windowMs: number;
  };
  logging: {
    level: string;
    enableSqlLogging: boolean;
  };
  security: {
    maxQueryTime: number;
    allowedSchemas: string[];
    restrictedTables: string[];
    readOnlyMode: boolean;
  };
}

export class ConfigManager {
  private config: Config;

  constructor() {
    this.config = this.loadConfig();
  }

  private loadConfig(): Config {
    // Priority: Environment variables > config file > defaults
    const configPath = process.env.POSTGRES_MCP_CONFIG || './config.json';
    
    let fileConfig: any = {};
    if (fs.existsSync(configPath)) {
      try {
        fileConfig = JSON.parse(fs.readFileSync(configPath, 'utf-8'));
      } catch (error) {
        console.warn('Failed to load config file:', error);
      }
    }

    // Database configuration
    const database: DatabaseConfig = {
      host: process.env.POSTGRES_HOST || fileConfig.database?.host || 'localhost',
      port: parseInt(process.env.POSTGRES_PORT || fileConfig.database?.port || '5432'),
      user: process.env.POSTGRES_USER || fileConfig.database?.user || 'postgres',
      password: process.env.POSTGRES_PASSWORD || fileConfig.database?.password || '',
      database: process.env.POSTGRES_DATABASE || fileConfig.database?.database || 'postgres',
      ssl: process.env.POSTGRES_SSL === 'true' || fileConfig.database?.ssl || false,
      connectionString: process.env.DATABASE_URL || fileConfig.database?.connectionString
    };

    // Pool configuration
    const pool: PoolConfig = {
      min: parseInt(process.env.POOL_MIN || fileConfig.pool?.min || '2'),
      max: parseInt(process.env.POOL_MAX || fileConfig.pool?.max || '10'),
      idleTimeoutMillis: parseInt(process.env.POOL_IDLE_TIMEOUT || fileConfig.pool?.idleTimeoutMillis || '30000'),
      acquireTimeoutMillis: parseInt(process.env.POOL_ACQUIRE_TIMEOUT || fileConfig.pool?.acquireTimeoutMillis || '60000'),
      createTimeoutMillis: parseInt(process.env.POOL_CREATE_TIMEOUT || fileConfig.pool?.createTimeoutMillis || '30000'),
      reapIntervalMillis: parseInt(process.env.POOL_REAP_INTERVAL || fileConfig.pool?.reapIntervalMillis || '1000')
    };

    return {
      database,
      pool,
      cache: {
        enabled: process.env.CACHE_ENABLED !== 'false',
        ttl: parseInt(process.env.CACHE_TTL || '300000'),
        maxKeys: parseInt(process.env.CACHE_MAX_KEYS || '1000'),
        ...fileConfig.cache,
      },
      rateLimiting: {
        maxRequests: parseInt(process.env.RATE_LIMIT_MAX || '100'),
        windowMs: parseInt(process.env.RATE_LIMIT_WINDOW || '60000'),
        ...fileConfig.rateLimiting,
      },
      logging: {
        level: process.env.LOG_LEVEL || fileConfig.logging?.level || 'info',
        enableSqlLogging: process.env.SQL_LOGGING === 'true' || fileConfig.logging?.enableSqlLogging || false,
        ...fileConfig.logging,
      },
      security: {
        maxQueryTime: parseInt(process.env.MAX_QUERY_TIME || '30000'),
        allowedSchemas: process.env.ALLOWED_SCHEMAS?.split(',') || fileConfig.security?.allowedSchemas || ['public'],
        restrictedTables: process.env.RESTRICTED_TABLES?.split(',') || fileConfig.security?.restrictedTables || [],
        readOnlyMode: process.env.READ_ONLY_MODE === 'true' || fileConfig.security?.readOnlyMode || false,
        ...fileConfig.security,
      }
    };
  }

  get(): Config {
    return this.config;
  }

  getDatabaseConfig(): DatabaseConfig {
    return this.config.database;
  }

  getConnectionString(): string {
    if (this.config.database.connectionString) {
      return this.config.database.connectionString;
    }

    const { host, port, user, password, database, ssl } = this.config.database;
    const sslParam = ssl ? '?ssl=true' : '';
    return `postgresql://${user}:${password}@${host}:${port}/${database}${sslParam}`;
  }

  validate(): void {
    // Allow skipping validation in CI or explicit opt-out
    if (process.env.SKIP_CONFIG_VALIDATION === 'true' || process.env.CI === 'true') {
      return;
    }

    const { database } = this.config;
    
    if (!database.connectionString && (!database.host || !database.user || !database.database)) {
      throw new Error('Database configuration is required. Provide either DATABASE_URL or individual connection parameters (POSTGRES_HOST, POSTGRES_USER, POSTGRES_DATABASE)');
    }

    // Validate pool configuration
    if (this.config.pool.min > this.config.pool.max) {
      throw new Error('Pool min connections cannot be greater than max connections');
    }

    // Validate security settings
    if (this.config.security.maxQueryTime < 1000) {
      throw new Error('Max query time cannot be less than 1000ms');
    }
  }

  isReadOnlyMode(): boolean {
    return this.config.security.readOnlyMode;
  }

  isSchemaAllowed(schema: string): boolean {
    return this.config.security.allowedSchemas.includes(schema);
  }

  isTableRestricted(table: string): boolean {
    return this.config.security.restrictedTables.includes(table);
  }
}