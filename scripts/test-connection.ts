#!/usr/bin/env tsx
import { ConfigManager } from '../src/config.js';
import { DatabaseConnectionManager } from '../src/database/connection-manager.js';
import { logger } from '../src/logger.js';

async function testConnection() {
  console.log('🔍 Testing PostgreSQL MCP Server Connection...\n');
  
  try {
    // Load configuration
    const config = new ConfigManager();
    config.validate();
    
    console.log('✅ Configuration loaded successfully');
    console.log(`📍 Database: ${config.getDatabaseConfig().host}:${config.getDatabaseConfig().port}/${config.getDatabaseConfig().database}`);
    console.log(`👤 User: ${config.getDatabaseConfig().user}`);
    console.log(`🔒 SSL: ${config.getDatabaseConfig().ssl ? 'enabled' : 'disabled'}\n`);
    
    // Test database connection
    const dbManager = new DatabaseConnectionManager(config);
    
    console.log('🔌 Testing database connection...');
    const isConnected = await dbManager.testConnection();
    
    if (isConnected) {
      console.log('✅ Database connection successful!\n');
      
      // Get pool statistics
      const poolStats = dbManager.getPoolStats();
      console.log('📊 Connection Pool Status:');
      console.log(`   Total Connections: ${poolStats.totalConnections}`);
      console.log(`   Idle Connections: ${poolStats.idleConnections}`);
      console.log(`   Waiting Count: ${poolStats.waitingCount}`);
      console.log(`   Min Pool Size: ${poolStats.config.min}`);
      console.log(`   Max Pool Size: ${poolStats.config.max}\n`);
      
      // Test basic query
      console.log('🔍 Testing basic query...');
      const result = await dbManager.query('SELECT version() as version, current_database() as database, current_user as user');
      
      if (result.rows.length > 0) {
        console.log('✅ Query execution successful!');
        console.log(`   PostgreSQL Version: ${result.rows[0].version}`);
        console.log(`   Current Database: ${result.rows[0].database}`);
        console.log(`   Current User: ${result.rows[0].user}\n`);
      }
      
      // Test transaction
      console.log('🔄 Testing transaction...');
      const txId = await dbManager.beginTransaction(true);
      await dbManager.queryInTransaction(txId, 'SELECT 1 as test');
      await dbManager.commitTransaction(txId);
      console.log('✅ Transaction test successful!\n');
      
      // Get operational stats
      const stats = dbManager.getOperationalStats();
      console.log('📈 Operational Statistics:');
      console.log(`   Total Queries: ${stats.totalQueries}`);
      console.log(`   Total Errors: ${stats.totalErrors}`);
      console.log(`   Total Transactions: ${stats.totalTransactions}`);
      console.log(`   Average Query Time: ${stats.averageQueryTime.toFixed(2)}ms`);
      console.log(`   Active Transactions: ${stats.activeTransactions}\n`);
      
      console.log('🎉 All tests passed! PostgreSQL MCP Server is ready to use.');
      
    } else {
      console.log('❌ Database connection failed!');
      process.exit(1);
    }
    
    // Cleanup
    await dbManager.cleanup();
    
  } catch (error) {
    console.error('❌ Connection test failed:', error instanceof Error ? error.message : error);
    process.exit(1);
  }
}

// Run the test
testConnection().catch(console.error);