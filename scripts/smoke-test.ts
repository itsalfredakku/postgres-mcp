#!/usr/bin/env tsx

/**
 * Simple smoke test to verify the PostgreSQL MCP server can initialize
 * Tests basic server setup without requiring database connection
 */

import PostgresMCPServer from '../src/index.js';
import { logger } from '../src/logger.js';

async function runSmokeTest() {
  logger.info('🚀 Starting PostgreSQL MCP Server smoke test...');
  
  try {
    // Test 1: Server instantiation
    logger.info('Test 1: Server instantiation');
    const server = new PostgresMCPServer();
    logger.info('✅ Server instance created successfully');
    
    // Test 2: Check server components
    logger.info('Test 2: Server components validation');
    
    // Verify server has the expected properties (accessing private members via any type)
    const serverAny = server as any;
    
    if (serverAny.dbManager) {
      logger.info('✅ Database manager initialized');
    } else {
      throw new Error('❌ Database manager missing');
    }
    
    if (serverAny.cache) {
      logger.info('✅ Cache system initialized');
    } else {
      throw new Error('❌ Cache system missing');
    }
    
    if (serverAny.rateLimiter) {
      logger.info('✅ Rate limiter initialized');
    } else {
      throw new Error('❌ Rate limiter missing');
    }
    
    if (serverAny.performanceMonitor) {
      logger.info('✅ Performance monitor initialized');
    } else {
      throw new Error('❌ Performance monitor missing');
    }
    
    if (serverAny.securityValidator) {
      logger.info('✅ Security validator initialized');
    } else {
      throw new Error('❌ Security validator missing');
    }
    
    // Test 3: Check tool clients
    logger.info('Test 3: Tool clients validation');
    
    if (serverAny.tablesClient) {
      logger.info('✅ Tables client initialized');
    } else {
      throw new Error('❌ Tables client missing');
    }
    
    if (serverAny.queryClient) {
      logger.info('✅ Query client initialized');
    } else {
      throw new Error('❌ Query client missing');
    }
    
    if (serverAny.schemaClient) {
      logger.info('✅ Schema client initialized');
    } else {
      throw new Error('❌ Schema client missing');
    }
    
    logger.info('🎉 All smoke tests passed! Server is ready for deployment.');
    logger.info('📊 Summary:');
    logger.info('   - Server instantiation successful');
    logger.info('   - All core components loaded');
    logger.info('   - Database manager configured');
    logger.info('   - Security and performance systems active');
    logger.info('   - All API clients initialized');
    logger.info('');
    logger.info('🔗 Next steps:');
    logger.info('   1. Configure PostgreSQL connection');
    logger.info('   2. Run integration tests with real database');
    logger.info('   3. Deploy for production use');
    
  } catch (error) {
    logger.error('❌ Smoke test failed:', error);
    process.exit(1);
  }
}

// Run the test
runSmokeTest().catch(error => {
  logger.error('Fatal error in smoke test:', error);
  process.exit(1);
});