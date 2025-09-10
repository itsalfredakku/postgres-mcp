#!/usr/bin/env tsx
import fs from 'fs';
import path from 'path';

async function verifyBuild() {
  console.log('🔧 Verifying PostgreSQL MCP Server Build...\n');
  
  try {
    // Check if dist directory exists
    const distPath = path.join(process.cwd(), 'dist');
    if (!fs.existsSync(distPath)) {
      console.error('❌ dist directory not found. Run "npm run build" first.');
      process.exit(1);
    }
    
    console.log('✅ dist directory exists');
    
    // Check for main entry point
    const mainFile = path.join(distPath, 'index.js');
    if (!fs.existsSync(mainFile)) {
      console.error('❌ Main entry point (dist/index.js) not found.');
      process.exit(1);
    }
    
    console.log('✅ Main entry point exists');
    
    // Check for essential modules
    const requiredFiles = [
      'config.js',
      'logger.js',
      'validation.js',
      'database/connection-manager.js',
      'api/domains/query-api.js',
      'api/domains/tables-api.js'
    ];
    
    for (const file of requiredFiles) {
      const filePath = path.join(distPath, file);
      if (!fs.existsSync(filePath)) {
        console.error(`❌ Required file missing: ${file}`);
        process.exit(1);
      }
    }
    
    console.log('✅ All required modules exist');
    
    // Try to import the main module (syntax check)
    try {
      // Use dynamic import to avoid issues with ES modules
      console.log('🔍 Testing module import...');
      
      // Set skip validation to avoid database connection requirements
      process.env.SKIP_CONFIG_VALIDATION = 'true';
      
      const { default: module } = await import(mainFile);
      console.log('✅ Module import successful');
      
    } catch (error) {
      console.error('❌ Module import failed:', error instanceof Error ? error.message : error);
      process.exit(1);
    }
    
    // Check package.json scripts
    const packagePath = path.join(process.cwd(), 'package.json');
    if (fs.existsSync(packagePath)) {
      const packageJson = JSON.parse(fs.readFileSync(packagePath, 'utf-8'));
      const requiredScripts = ['build', 'start', 'dev', 'test'];
      
      for (const script of requiredScripts) {
        if (!packageJson.scripts[script]) {
          console.warn(`⚠️  Missing script: ${script}`);
        }
      }
      
      console.log('✅ Package.json scripts verified');
    }
    
    console.log('\n🎉 Build verification completed successfully!');
    console.log('\nNext steps:');
    console.log('1. Set up your .env file (copy from .env.example)');
    console.log('2. Configure your PostgreSQL connection');
    console.log('3. Run "npm test" to test the connection');
    console.log('4. Run "npm start" to start the MCP server');
    
  } catch (error) {
    console.error('❌ Build verification failed:', error instanceof Error ? error.message : error);
    process.exit(1);
  }
}

// Run verification
verifyBuild().catch(console.error);