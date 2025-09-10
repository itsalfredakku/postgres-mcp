import { LRUCache } from 'lru-cache';
import { logger } from '../logger.js';
import { ConfigManager } from '../config.js';

export interface CacheEntry<T = any> {
  data: T;
  timestamp: number;
  hits: number;
  size: number;
}

export interface CacheStats {
  size: number;
  maxSize: number;
  itemCount: number;
  hitRate: number;
  totalHits: number;
  totalMisses: number;
  totalSets: number;
  totalDeletes: number;
}

export class QueryResultCache {
  private cache: LRUCache<string, CacheEntry>;
  private stats = {
    hits: 0,
    misses: 0,
    sets: 0,
    deletes: 0
  };
  private config: ConfigManager;

  constructor(config: ConfigManager) {
    this.config = config;
    const cacheConfig = config.get().cache;

    this.cache = new LRUCache({
      max: cacheConfig.maxKeys,
      ttl: cacheConfig.ttl,
      updateAgeOnGet: true,
      updateAgeOnHas: true,
      dispose: (value: CacheEntry, key: string) => {
        this.stats.deletes++;
        logger.debug('Cache entry disposed', { key, size: value.size });
      }
    });
  }

  /**
   * Generate cache key from SQL and parameters
   */
  private generateKey(sql: string, parameters?: any[], options?: any): string {
    const normalizedSql = sql.replace(/\s+/g, ' ').trim().toLowerCase();
    const paramString = parameters ? JSON.stringify(parameters) : '';
    const optionsString = options ? JSON.stringify(options) : '';
    
    // Use a simple hash function for the key
    return this.simpleHash(normalizedSql + paramString + optionsString);
  }

  /**
   * Simple hash function for cache keys
   */
  private simpleHash(str: string): string {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32-bit integer
    }
    return Math.abs(hash).toString(36);
  }

  /**
   * Check if SQL query should be cached
   */
  private shouldCache(sql: string): boolean {
    const nonCacheableOperations = [
      'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER',
      'TRUNCATE', 'GRANT', 'REVOKE', 'COPY', 'VACUUM', 'ANALYZE'
    ];

    const normalizedSql = sql.trim().toUpperCase();
    return !nonCacheableOperations.some(op => normalizedSql.startsWith(op));
  }

  /**
   * Get cached result
   */
  get<T = any>(sql: string, parameters?: any[], options?: any): T | null {
    if (!this.config.get().cache.enabled || !this.shouldCache(sql)) {
      return null;
    }

    const key = this.generateKey(sql, parameters, options);
    const entry = this.cache.get(key);

    if (entry) {
      entry.hits++;
      this.stats.hits++;
      logger.debug('Cache hit', { key, hits: entry.hits });
      return entry.data;
    }

    this.stats.misses++;
    logger.debug('Cache miss', { key });
    return null;
  }

  /**
   * Set cache entry
   */
  set<T = any>(sql: string, data: T, parameters?: any[], options?: any): void {
    if (!this.config.get().cache.enabled || !this.shouldCache(sql)) {
      return;
    }

    const key = this.generateKey(sql, parameters, options);
    const size = this.estimateSize(data);

    const entry: CacheEntry<T> = {
      data,
      timestamp: Date.now(),
      hits: 0,
      size
    };

    this.cache.set(key, entry);
    this.stats.sets++;

    logger.debug('Cache set', { key, size });
  }

  /**
   * Invalidate cache entries by pattern
   */
  invalidate(pattern?: string): number {
    if (!pattern) {
      const size = this.cache.size;
      this.cache.clear();
      logger.info('Cache cleared completely', { entriesRemoved: size });
      return size;
    }

    let removed = 0;
    const regex = new RegExp(pattern, 'i');

    for (const [key] of this.cache) {
      if (regex.test(key)) {
        this.cache.delete(key);
        removed++;
      }
    }

    logger.info('Cache invalidated by pattern', { pattern, entriesRemoved: removed });
    return removed;
  }

  /**
   * Invalidate cache for specific table
   */
  invalidateTable(tableName: string, schemaName: string = 'public'): number {
    const tablePattern = `${schemaName}\\.${tableName}|from\\s+${tableName}|join\\s+${tableName}`;
    return this.invalidate(tablePattern);
  }

  /**
   * Get cache statistics
   */
  getStats(): CacheStats {
    const totalRequests = this.stats.hits + this.stats.misses;
    const hitRate = totalRequests > 0 ? (this.stats.hits / totalRequests) * 100 : 0;

    return {
      size: this.cache.calculatedSize || 0,
      maxSize: this.cache.max,
      itemCount: this.cache.size,
      hitRate: Math.round(hitRate * 100) / 100,
      totalHits: this.stats.hits,
      totalMisses: this.stats.misses,
      totalSets: this.stats.sets,
      totalDeletes: this.stats.deletes
    };
  }

  /**
   * Estimate memory size of cached data
   */
  private estimateSize(data: any): number {
    try {
      return JSON.stringify(data).length * 2; // Rough estimate in bytes
    } catch {
      return 1000; // Default estimate
    }
  }

  /**
   * Cleanup expired entries manually
   */
  cleanup(): void {
    this.cache.purgeStale();
    logger.debug('Cache cleanup completed');
  }

  /**
   * Get cache entries by recency
   */
  getRecentEntries(limit: number = 10): Array<{ key: string; entry: CacheEntry }> {
    const entries: Array<{ key: string; entry: CacheEntry }> = [];
    
    for (const [key, value] of this.cache) {
      entries.push({ key, entry: value });
      if (entries.length >= limit) break;
    }

    return entries.sort((a, b) => b.entry.timestamp - a.entry.timestamp);
  }

  /**
   * Get most frequently accessed entries
   */
  getPopularEntries(limit: number = 10): Array<{ key: string; entry: CacheEntry }> {
    const entries: Array<{ key: string; entry: CacheEntry }> = [];
    
    for (const [key, value] of this.cache) {
      entries.push({ key, entry: value });
    }

    return entries
      .sort((a, b) => b.entry.hits - a.entry.hits)
      .slice(0, limit);
  }
}

/**
 * Performance metrics collector
 */
export class PerformanceMonitor {
  private metrics: Map<string, {
    count: number;
    totalTime: number;
    minTime: number;
    maxTime: number;
    avgTime: number;
    lastExecution: number;
  }> = new Map();

  /**
   * Record operation performance
   */
  record(operation: string, duration: number): void {
    const existing = this.metrics.get(operation);
    
    if (existing) {
      existing.count++;
      existing.totalTime += duration;
      existing.minTime = Math.min(existing.minTime, duration);
      existing.maxTime = Math.max(existing.maxTime, duration);
      existing.avgTime = existing.totalTime / existing.count;
      existing.lastExecution = Date.now();
    } else {
      this.metrics.set(operation, {
        count: 1,
        totalTime: duration,
        minTime: duration,
        maxTime: duration,
        avgTime: duration,
        lastExecution: Date.now()
      });
    }
  }

  /**
   * Get performance metrics
   */
  getMetrics(): Record<string, any> {
    const result: Record<string, any> = {};
    
    for (const [operation, metrics] of this.metrics) {
      result[operation] = {
        ...metrics,
        avgTime: Math.round(metrics.avgTime * 100) / 100
      };
    }
    
    return result;
  }

  /**
   * Get slow operations
   */
  getSlowOperations(threshold: number = 1000): Array<{ operation: string; metrics: any }> {
    const slow: Array<{ operation: string; metrics: any }> = [];
    
    for (const [operation, metrics] of this.metrics) {
      if (metrics.avgTime > threshold || metrics.maxTime > threshold * 2) {
        slow.push({ operation, metrics });
      }
    }
    
    return slow.sort((a, b) => b.metrics.avgTime - a.metrics.avgTime);
  }

  /**
   * Reset metrics
   */
  reset(): void {
    this.metrics.clear();
  }
}

/**
 * Decorator for performance monitoring
 */
export function monitor(operation?: string) {
  return function (
    target: any,
    propertyKey: string,
    descriptor: PropertyDescriptor
  ) {
    const originalMethod = descriptor.value;
    const operationName = operation || `${target.constructor.name}.${propertyKey}`;

    descriptor.value = async function (...args: any[]) {
      const startTime = Date.now();
      
      try {
        const result = await originalMethod.apply(this, args);
        const duration = Date.now() - startTime;
        
        // Check if the instance has a performance monitor
        if ((this as any).performanceMonitor) {
          (this as any).performanceMonitor.record(operationName, duration);
        }
        
        return result;
      } catch (error) {
        const duration = Date.now() - startTime;
        
        // Check if the instance has a performance monitor
        if ((this as any).performanceMonitor) {
          (this as any).performanceMonitor.record(`${operationName}_error`, duration);
        }
        
        throw error;
      }
    };

    return descriptor;
  };
}