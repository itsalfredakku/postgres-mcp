import winston from 'winston';

// Set MCP server mode FIRST, before any logger configuration
const isMCPServer = process.env.MCP_SERVER === 'true';

// Create logger with MCP-compatible configuration
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [
    // Only log to file in MCP server mode to avoid interfering with stdio
    new winston.transports.File({ 
      filename: 'logs/error.log', 
      level: 'error' 
    }),
    new winston.transports.File({ 
      filename: 'logs/combined.log' 
    })
  ],
});

// Add console transport only in non-MCP mode (development/testing)
if (!isMCPServer) {
  logger.add(new winston.transports.Console({
    format: winston.format.combine(
      winston.format.colorize(),
      winston.format.simple()
    )
  }));
}

// Helper functions for structured logging
export const logQuery = (sql: string, params?: any[], duration?: number) => {
  if (process.env.SQL_LOGGING === 'true') {
    logger.debug('SQL Query executed', {
      sql: sql.replace(/\s+/g, ' ').trim(),
      params: params?.length ? params : undefined,
      duration: duration ? `${duration}ms` : undefined,
      type: 'database_query'
    });
  }
};

export const logConnection = (event: string, details?: any) => {
  logger.info('Database connection event', {
    event,
    ...details,
    type: 'database_connection'
  });
};

export const logError = (error: Error, context?: any) => {
  logger.error('Operation failed', {
    error: error.message,
    stack: error.stack,
    context,
    type: 'error'
  });
};

export const logMetrics = (operation: string, metrics: any) => {
  logger.info('Performance metrics', {
    operation,
    metrics,
    type: 'performance'
  });
};

export { logger };