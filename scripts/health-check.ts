#!/usr/bin/env tsx
import { ConfigManager } from '../src/config.js';
import { DatabaseConnectionManager } from '../src/database/connection-manager.js';
import { QueryResultCache } from '../src/common/cache.js';
import { logger } from '../src/logger.js';

interface HealthStatus {
  status: 'healthy' | 'unhealthy' | 'degraded';
  timestamp: string;
  version: string;
  uptime: number;
  checks: {
    database: HealthCheck;
    config: HealthCheck;
    cache: HealthCheck;
    memory: HealthCheck;
    performance: HealthCheck;
  };
  metrics: {
    database: any;
    cache: any;
    memory: any;
  };
}

interface HealthCheck {
  status: 'pass' | 'fail' | 'warn';
  message: string;
  duration: number;
  details?: any;
}

class HealthChecker {
  private config: ConfigManager;
  private dbManager: DatabaseConnectionManager;
  private cache: QueryResultCache;
  private startTime: number;

  constructor() {
    this.startTime = Date.now();
    this.config = new ConfigManager();
    this.dbManager = new DatabaseConnectionManager(this.config);
    this.cache = new QueryResultCache(this.config);
  }

  async performHealthCheck(): Promise<HealthStatus> {
    console.log('🏥 Performing PostgreSQL MCP Server Health Check...\n');

    const checks = {
      database: await this.checkDatabase(),
      config: await this.checkConfiguration(),
      cache: await this.checkCache(),
      memory: await this.checkMemory(),
      performance: await this.checkPerformance()
    };

    const metrics = {
      database: this.dbManager.getOperationalStats(),
      cache: this.cache.getStats(),
      memory: this.getMemoryStats()
    };

    const overallStatus = this.determineOverallStatus(checks);

    const healthStatus: HealthStatus = {
      status: overallStatus,
      timestamp: new Date().toISOString(),
      version: this.getVersion(),
      uptime: Date.now() - this.startTime,
      checks,
      metrics
    };

    this.reportHealthStatus(healthStatus);
    return healthStatus;
  }

  private async checkDatabase(): Promise<HealthCheck> {
    const startTime = Date.now();
    
    try {
      // Test basic connectivity
      const isConnected = await this.dbManager.testConnection();
      if (!isConnected) {
        return {
          status: 'fail',
          message: 'Database connection failed',
          duration: Date.now() - startTime
        };
      }

      // Test query performance
      const queryStart = Date.now();
      await this.dbManager.query('SELECT version(), current_database(), current_user');
      const queryDuration = Date.now() - queryStart;

      // Check pool status
      const poolStats = this.dbManager.getPoolStats();
      const poolUtilization = poolStats.totalConnections / poolStats.config.max;

      let status: 'pass' | 'warn' | 'fail' = 'pass';
      let message = 'Database connection healthy';

      if (queryDuration > 1000) {
        status = 'warn';
        message = `Database queries are slow (${queryDuration}ms)`;
      }

      if (poolUtilization > 0.8) {
        status = 'warn';
        message = `Connection pool utilization high (${Math.round(poolUtilization * 100)}%)`;
      }

      return {
        status,
        message,
        duration: Date.now() - startTime,
        details: {
          queryDuration,
          poolStats,
          poolUtilization: Math.round(poolUtilization * 100)
        }
      };

    } catch (error) {
      return {
        status: 'fail',
        message: `Database check failed: ${error instanceof Error ? error.message : error}`,
        duration: Date.now() - startTime,
        details: { error: error instanceof Error ? error.message : error }
      };
    }
  }

  private async checkConfiguration(): Promise<HealthCheck> {
    const startTime = Date.now();
    
    try {
      // Validate configuration
      this.config.validate();

      // Check for potential configuration issues
      const config = this.config.get();
      const warnings: string[] = [];

      if (config.pool.max < 5) {
        warnings.push('Pool max connections is very low');
      }

      if (config.security.maxQueryTime > 60000) {
        warnings.push('Max query time is very high');
      }

      if (!config.cache.enabled && process.env.NODE_ENV === 'production') {
        warnings.push('Cache is disabled in production');
      }

      const status = warnings.length > 0 ? 'warn' : 'pass';
      const message = warnings.length > 0 
        ? `Configuration warnings: ${warnings.join(', ')}`
        : 'Configuration is valid';

      return {
        status,
        message,
        duration: Date.now() - startTime,
        details: { warnings, config: config }
      };

    } catch (error) {
      return {
        status: 'fail',
        message: `Configuration validation failed: ${error instanceof Error ? error.message : error}`,
        duration: Date.now() - startTime
      };
    }
  }

  private async checkCache(): Promise<HealthCheck> {
    const startTime = Date.now();
    
    try {
      const stats = this.cache.getStats();
      
      let status: 'pass' | 'warn' | 'fail' = 'pass';
      let message = 'Cache is functioning normally';

      if (stats.hitRate < 20 && stats.totalHits + stats.totalMisses > 100) {
        status = 'warn';
        message = `Cache hit rate is low (${stats.hitRate}%)`;
      }

      if (stats.itemCount > stats.maxSize * 0.9) {
        status = 'warn';
        message = 'Cache is nearly full';
      }

      return {
        status,
        message,
        duration: Date.now() - startTime,
        details: stats
      };

    } catch (error) {
      return {
        status: 'fail',
        message: `Cache check failed: ${error instanceof Error ? error.message : error}`,
        duration: Date.now() - startTime
      };
    }
  }

  private async checkMemory(): Promise<HealthCheck> {
    const startTime = Date.now();
    
    try {
      const memoryUsage = process.memoryUsage();
      const totalMemoryMB = memoryUsage.heapTotal / 1024 / 1024;
      const usedMemoryMB = memoryUsage.heapUsed / 1024 / 1024;
      const memoryUtilization = usedMemoryMB / totalMemoryMB;

      let status: 'pass' | 'warn' | 'fail' = 'pass';
      let message = 'Memory usage is normal';

      if (memoryUtilization > 0.8) {
        status = 'warn';
        message = `High memory usage (${Math.round(memoryUtilization * 100)}%)`;
      }

      if (memoryUtilization > 0.95) {
        status = 'fail';
        message = `Critical memory usage (${Math.round(memoryUtilization * 100)}%)`;
      }

      return {
        status,
        message,
        duration: Date.now() - startTime,
        details: {
          totalMemoryMB: Math.round(totalMemoryMB),
          usedMemoryMB: Math.round(usedMemoryMB),
          memoryUtilization: Math.round(memoryUtilization * 100),
          memoryUsage
        }
      };

    } catch (error) {
      return {
        status: 'fail',
        message: `Memory check failed: ${error instanceof Error ? error.message : error}`,
        duration: Date.now() - startTime
      };
    }
  }

  private async checkPerformance(): Promise<HealthCheck> {
    const startTime = Date.now();
    
    try {
      const operationalStats = this.dbManager.getOperationalStats();
      
      let status: 'pass' | 'warn' | 'fail' = 'pass';
      let message = 'Performance metrics are healthy';

      if (operationalStats.averageQueryTime > 1000) {
        status = 'warn';
        message = `Slow average query time (${Math.round(operationalStats.averageQueryTime)}ms)`;
      }

      if (operationalStats.totalErrors > operationalStats.totalQueries * 0.1) {
        status = 'warn';
        message = 'High error rate detected';
      }

      if (operationalStats.averageQueryTime > 5000) {
        status = 'fail';
        message = `Critical query performance (${Math.round(operationalStats.averageQueryTime)}ms)`;
      }

      return {
        status,
        message,
        duration: Date.now() - startTime,
        details: {
          averageQueryTime: Math.round(operationalStats.averageQueryTime),
          totalQueries: operationalStats.totalQueries,
          totalErrors: operationalStats.totalErrors,
          errorRate: operationalStats.totalQueries > 0 
            ? Math.round((operationalStats.totalErrors / operationalStats.totalQueries) * 100)
            : 0
        }
      };

    } catch (error) {
      return {
        status: 'fail',
        message: `Performance check failed: ${error instanceof Error ? error.message : error}`,
        duration: Date.now() - startTime
      };
    }
  }

  private determineOverallStatus(checks: Record<string, HealthCheck>): 'healthy' | 'unhealthy' | 'degraded' {
    const statuses = Object.values(checks).map(check => check.status);
    
    if (statuses.includes('fail')) {
      return 'unhealthy';
    }
    
    if (statuses.includes('warn')) {
      return 'degraded';
    }
    
    return 'healthy';
  }

  private getMemoryStats() {
    const memoryUsage = process.memoryUsage();
    return {
      heapUsed: Math.round(memoryUsage.heapUsed / 1024 / 1024),
      heapTotal: Math.round(memoryUsage.heapTotal / 1024 / 1024),
      external: Math.round(memoryUsage.external / 1024 / 1024),
      rss: Math.round(memoryUsage.rss / 1024 / 1024)
    };
  }

  private getVersion(): string {
    try {
      const packageJson = require('../package.json');
      return packageJson.version || '1.0.0';
    } catch {
      return '1.0.0';
    }
  }

  private reportHealthStatus(status: HealthStatus): void {
    console.log(`\n🏥 Health Check Results`);
    console.log(`═══════════════════════════════════════`);
    console.log(`Overall Status: ${this.getStatusEmoji(status.status)} ${status.status.toUpperCase()}`);
    console.log(`Timestamp: ${status.timestamp}`);
    console.log(`Version: ${status.version}`);
    console.log(`Uptime: ${Math.round(status.uptime / 1000)}s`);
    console.log();

    console.log('Individual Checks:');
    console.log('─────────────────');
    
    for (const [checkName, check] of Object.entries(status.checks)) {
      console.log(`${this.getStatusEmoji(check.status)} ${checkName}: ${check.message} (${check.duration}ms)`);
      
      if (check.details && Object.keys(check.details).length > 0) {
        console.log(`   Details: ${JSON.stringify(check.details, null, 2).replace(/\n/g, '\n   ')}`);
      }
    }

    console.log();
    console.log('Metrics Summary:');
    console.log('───────────────');
    console.log(`📊 Database: ${status.metrics.database.totalQueries} queries, ${status.metrics.database.totalErrors} errors`);
    console.log(`💾 Cache: ${status.metrics.cache.hitRate}% hit rate, ${status.metrics.cache.itemCount} items`);
    console.log(`🧠 Memory: ${status.metrics.memory.heapUsed}MB used / ${status.metrics.memory.heapTotal}MB total`);
    
    console.log();
    
    if (status.status !== 'healthy') {
      console.log('⚠️ Recommendations:');
      this.generateRecommendations(status);
    }
  }

  private getStatusEmoji(status: string): string {
    switch (status) {
      case 'pass':
      case 'healthy':
        return '✅';
      case 'warn':
      case 'degraded':
        return '⚠️';
      case 'fail':
      case 'unhealthy':
        return '❌';
      default:
        return '❓';
    }
  }

  private generateRecommendations(status: HealthStatus): void {
    const recommendations: string[] = [];

    // Database recommendations
    if (status.checks.database.status === 'warn' || status.checks.database.status === 'fail') {
      recommendations.push('- Check database server status and network connectivity');
      recommendations.push('- Review slow queries and optimize indexes');
      recommendations.push('- Consider increasing connection pool size');
    }

    // Memory recommendations
    if (status.checks.memory.status === 'warn' || status.checks.memory.status === 'fail') {
      recommendations.push('- Monitor for memory leaks');
      recommendations.push('- Consider increasing server memory allocation');
      recommendations.push('- Review cache settings and reduce if necessary');
    }

    // Cache recommendations
    if (status.checks.cache.status === 'warn') {
      recommendations.push('- Review cache configuration and TTL settings');
      recommendations.push('- Consider increasing cache size if memory allows');
      recommendations.push('- Analyze query patterns for better caching strategy');
    }

    // Performance recommendations
    if (status.checks.performance.status === 'warn' || status.checks.performance.status === 'fail') {
      recommendations.push('- Review and optimize slow queries');
      recommendations.push('- Check database server performance');
      recommendations.push('- Consider query result caching');
    }

    recommendations.forEach(rec => console.log(rec));
  }

  async cleanup(): Promise<void> {
    await this.dbManager.cleanup();
  }
}

async function main() {
  const healthChecker = new HealthChecker();
  
  try {
    const healthStatus = await healthChecker.performHealthCheck();
    
    // Exit with appropriate code
    const exitCode = healthStatus.status === 'healthy' ? 0 : 
                    healthStatus.status === 'degraded' ? 1 : 2;
    
    process.exit(exitCode);
  } catch (error) {
    console.error('❌ Health check failed:', error);
    process.exit(2);
  } finally {
    await healthChecker.cleanup();
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}

export { HealthChecker };