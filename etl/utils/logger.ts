import winston from 'winston';
import * as path from 'path';

const logLevel = process.env.LOG_LEVEL || 'info';
const logDir = process.env.LOG_DIR || './logs';

// Custom format for console output
const consoleFormat = winston.format.combine(
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  winston.format.colorize(),
  winston.format.printf(({ timestamp, level, message, ...meta }) => {
    let logMessage = `${timestamp} [${level}]: ${message}`;
    
    if (Object.keys(meta).length > 0) {
      logMessage += '\n' + JSON.stringify(meta, null, 2);
    }
    
    return logMessage;
  })
);

// Custom format for file output
const fileFormat = winston.format.combine(
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  winston.format.errors({ stack: true }),
  winston.format.json()
);

// Create transports
const transports: winston.transport[] = [
  // Console transport
  new winston.transports.Console({
    level: logLevel,
    format: consoleFormat
  })
];

// Add file transports if log directory is specified
if (logDir) {
  try {
    // General log file
    transports.push(
      new winston.transports.File({
        filename: path.join(logDir, 'etl.log'),
        level: 'info',
        format: fileFormat,
        maxsize: 10 * 1024 * 1024, // 10MB
        maxFiles: 5
      })
    );

    // Error log file
    transports.push(
      new winston.transports.File({
        filename: path.join(logDir, 'etl-error.log'),
        level: 'error',
        format: fileFormat,
        maxsize: 10 * 1024 * 1024, // 10MB
        maxFiles: 5
      })
    );
  } catch (error) {
    console.warn('Failed to create file transports:', error);
  }
}

// Create logger instance
export const logger = winston.createLogger({
  level: logLevel,
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.metadata()
  ),
  transports,
  exitOnError: false
});

// Add request ID for tracking
export function createChildLogger(requestId: string): winston.Logger {
  return logger.child({ requestId });
}

// ETL specific logging methods
export class EtlLogger {
  private logger: winston.Logger;
  private context: string;

  constructor(context: string = 'ETL', requestId?: string) {
    this.context = context;
    this.logger = requestId ? createChildLogger(requestId) : logger;
  }

  info(message: string, meta?: any): void {
    this.logger.info(`[${this.context}] ${message}`, meta);
  }

  warn(message: string, meta?: any): void {
    this.logger.warn(`[${this.context}] ${message}`, meta);
  }

  error(message: string, error?: any, meta?: any): void {
    const errorMeta = error instanceof Error 
      ? { error: error.message, stack: error.stack, ...meta }
      : { error, ...meta };
    
    this.logger.error(`[${this.context}] ${message}`, errorMeta);
  }

  debug(message: string, meta?: any): void {
    this.logger.debug(`[${this.context}] ${message}`, meta);
  }

  verbose(message: string, meta?: any): void {
    this.logger.verbose(`[${this.context}] ${message}`, meta);
  }

  // ETL specific methods
  startProcess(processName: string, source?: string): void {
    this.info(`Starting ${processName}`, { source, timestamp: new Date().toISOString() });
  }

  endProcess(processName: string, duration: number, stats?: any): void {
    this.info(`Completed ${processName}`, { 
      duration: `${duration}ms`, 
      stats,
      timestamp: new Date().toISOString() 
    });
  }

  processBatch(batchNumber: number, batchSize: number, total?: number): void {
    const progress = total ? `(${Math.round((batchNumber * batchSize / total) * 100)}%)` : '';
    this.info(`Processing batch ${batchNumber}, size: ${batchSize} ${progress}`);
  }

  recordProgress(processed: number, total: number, type: string): void {
    const percentage = Math.round((processed / total) * 100);
    this.info(`Progress: ${processed}/${total} ${type} processed (${percentage}%)`);
  }

  logError(operation: string, error: any, context?: any): void {
    this.error(`Failed to ${operation}`, error, context);
  }

  logRetry(operation: string, attempt: number, maxAttempts: number, delay: number): void {
    this.warn(`Retrying ${operation}: attempt ${attempt}/${maxAttempts} after ${delay}ms delay`);
  }

  logSkipped(reason: string, count: number = 1): void {
    this.debug(`Skipped ${count} records: ${reason}`);
  }

  // Database operation logging
  logDatabaseOperation(operation: string, affectedRows: number, duration?: number): void {
    const durationText = duration ? ` in ${duration}ms` : '';
    this.info(`Database ${operation}: ${affectedRows} rows affected${durationText}`);
  }

  // API operation logging
  logApiCall(url: string, method: string = 'GET', responseTime?: number): void {
    const timeText = responseTime ? ` (${responseTime}ms)` : '';
    this.debug(`API ${method} ${url}${timeText}`);
  }

  logApiError(url: string, error: any, attempt?: number): void {
    const attemptText = attempt ? ` (attempt ${attempt})` : '';
    this.error(`API call failed: ${url}${attemptText}`, error);
  }

  // File operation logging
  logFileOperation(operation: string, filePath: string, size?: number): void {
    const sizeText = size ? ` (${this.formatBytes(size)})` : '';
    this.info(`File ${operation}: ${filePath}${sizeText}`);
  }

  // Utility methods
  private formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }
}

// Export default logger instance
export default logger;