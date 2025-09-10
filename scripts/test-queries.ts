#!/usr/bin/env tsx
import { ConfigManager } from '../src/config.js';
import { DatabaseConnectionManager } from '../src/database/connection-manager.js';
import { QueryAPIClient } from '../src/api/domains/query-api.js';
import { TablesAPIClient } from '../src/api/domains/tables-api.js';
import { logger } from '../src/logger.js';

async function testQueries() {
  console.log('🧪 Testing PostgreSQL MCP Server Queries...\n');
  
  try {
    // Initialize components
    const config = new ConfigManager();
    config.validate();
    
    const dbManager = new DatabaseConnectionManager(config);
    const queryClient = new QueryAPIClient(dbManager);
    const tablesClient = new TablesAPIClient(dbManager);
    
    console.log('✅ Components initialized\n');
    
    // Test 1: Basic Query Execution
    console.log('🔍 Test 1: Basic Query Execution');
    const basicResult = await queryClient.executeQuery('SELECT 1 as test_number, \'Hello World\' as test_text');
    console.log(`   Result: ${basicResult.rows.length} rows, ${basicResult.duration}ms`);
    console.log(`   Data: ${JSON.stringify(basicResult.rows[0])}\n`);
    
    // Test 2: Query with Parameters
    console.log('🔍 Test 2: Parameterized Query');
    const paramResult = await queryClient.executeQuery(
      'SELECT $1::text as param1, $2::int as param2', 
      ['test parameter', '42']
    );
    console.log(`   Result: ${paramResult.rows.length} rows, ${paramResult.duration}ms`);
    console.log(`   Data: ${JSON.stringify(paramResult.rows[0])}\n`);
    
    // Test 3: Query Explanation
    console.log('🔍 Test 3: Query Explanation');
    const explainResult = await queryClient.getExecutionPlan('SELECT * FROM pg_tables LIMIT 5');
    console.log(`   Execution plan generated successfully\n`);
    
    // Test 4: List Tables
    console.log('🔍 Test 4: List Tables');
    const tables = await tablesClient.listTables();
    console.log(`   Found ${tables.length} tables`);
    if (tables.length > 0) {
      console.log(`   First table: ${tables[0].schemaName}.${tables[0].tableName}\n`);
    }
    
    // Test 5: Transaction Test
    console.log('🔍 Test 5: Transaction Execution');
    const transactionQueries = [
      { sql: 'SELECT 1 as step1' },
      { sql: 'SELECT 2 as step2' },
      { sql: 'SELECT 3 as step3' }
    ];
    
    const txResults = await queryClient.executeTransaction(transactionQueries, true);
    console.log(`   Transaction completed with ${txResults.length} steps`);
    console.log(`   Total duration: ${txResults.reduce((sum, r) => sum + r.duration, 0)}ms\n`);
    
    // Test 6: Query Validation
    console.log('🔍 Test 6: Query Validation');
    const validQuery = await queryClient.validateSyntax('SELECT current_timestamp');
    const invalidQuery = await queryClient.validateSyntax('SELCT invalid syntax');
    console.log(`   Valid query: ${validQuery.valid}`);
    console.log(`   Invalid query: ${invalidQuery.valid} (${invalidQuery.error})\n`);
    
    // Test 7: Active Queries
    console.log('🔍 Test 7: Active Queries');
    const activeQueries = await queryClient.getActiveQueries();
    console.log(`   Found ${activeQueries.length} active queries\n`);
    
    // Test 8: Query Statistics
    console.log('🔍 Test 8: Query Statistics');
    const queryStats = await queryClient.getQueryStatistics();
    console.log(`   Retrieved statistics for ${queryStats.length} tables\n`);
    
    // Test 9: Performance Analysis
    console.log('🔍 Test 9: Performance Analysis');
    const analysis = await queryClient.analyzeQuery('SELECT COUNT(*) FROM pg_tables');
    console.log(`   Analysis completed:`);
    console.log(`   - Planning Time: ${analysis.statistics.planningTime}ms`);
    console.log(`   - Execution Time: ${analysis.statistics.executionTime}ms`);
    console.log(`   - Total Cost: ${analysis.statistics.totalCost}\n`);
    
    // Test 10: Create and Drop Test Table
    console.log('🔍 Test 10: Table Operations');
    const testTableName = 'mcp_test_table_' + Date.now();
    
    try {
      // Create test table
      const createResult = await tablesClient.createTable(
        testTableName,
        [
          { name: 'id', type: 'SERIAL', primaryKey: true },
          { name: 'name', type: 'VARCHAR(100)', nullable: false },
          { name: 'created_at', type: 'TIMESTAMP', defaultValue: 'CURRENT_TIMESTAMP' }
        ],
        { ifNotExists: true }
      );
      console.log(`   ✅ Table created: ${createResult.message}`);
      
      // Get table info
      const tableInfo = await tablesClient.getTableInfo(testTableName);
      console.log(`   ✅ Table info retrieved: ${tableInfo.columns.length} columns`);
      
      // Drop test table
      const dropResult = await tablesClient.dropTable(testTableName, 'public', false, true);
      console.log(`   ✅ Table dropped: ${dropResult.message}\n`);
      
    } catch (error) {
      console.log(`   ⚠️  Table operation failed: ${error instanceof Error ? error.message : error}\n`);
    }
    
    // Cleanup
    await dbManager.cleanup();
    
    console.log('🎉 All query tests completed successfully!');
    
  } catch (error) {
    console.error('❌ Query test failed:', error instanceof Error ? error.message : error);
    console.error('Stack:', error instanceof Error ? error.stack : 'No stack trace');
    process.exit(1);
  }
}

// Run the tests
testQueries().catch(console.error);