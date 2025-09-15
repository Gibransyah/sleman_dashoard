import * as crypto from 'crypto';

/**
 * Sleep utility for delays
 */
export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Calculate SHA256 hash for deduplication
 */
export function calculateHash(input: string): string {
  return crypto.createHash('sha256').update(input, 'utf8').digest('hex');
}

/**
 * Safely parse integer values
 */
export function parseIntSafely(value: any): number | null {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  
  const parsed = parseInt(String(value), 10);
  return isNaN(parsed) ? null : parsed;
}

/**
 * Safely parse float values
 */
export function parseFloatSafely(value: any): number | null {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  
  const parsed = parseFloat(String(value).replace(/,/g, ''));
  return isNaN(parsed) ? null : parsed;
}

/**
 * Clean and normalize string values
 */
export function cleanString(value: any): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  
  const cleaned = String(value).trim();
  return cleaned === '' ? null : cleaned;
}

/**
 * Validate year range
 */
export function isValidYear(year: number | null): boolean {
  if (year === null) return true;
  return year >= 1900 && year <= new Date().getFullYear() + 10;
}

/**
 * Format number for display
 */
export function formatNumber(value: number | null, decimals: number = 2): string {
  if (value === null || value === undefined) {
    return '-';
  }
  return value.toLocaleString('id-ID', { 
    minimumFractionDigits: 0, 
    maximumFractionDigits: decimals 
  });
}

/**
 * Calculate percentage change
 */
export function calculatePercentageChange(oldValue: number, newValue: number): number {
  if (oldValue === 0) return newValue === 0 ? 0 : 100;
  return ((newValue - oldValue) / oldValue) * 100;
}

/**
 * Batch array into chunks
 */
export function batchArray<T>(array: T[], batchSize: number): T[][] {
  const batches: T[][] = [];
  for (let i = 0; i < array.length; i += batchSize) {
    batches.push(array.slice(i, i + batchSize));
  }
  return batches;
}

/**
 * Retry function with exponential backoff
 */
export async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  maxRetries: number = 3,
  baseDelay: number = 1000
): Promise<T> {
  let lastError: any;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      
      if (attempt === maxRetries) {
        throw error;
      }
      
      const delay = baseDelay * Math.pow(2, attempt - 1);
      await sleep(delay);
    }
  }
  
  throw lastError;
}

/**
 * Deep merge objects
 */
export function deepMerge(target: any, source: any): any {
  const result = { ...target };
  
  for (const key in source) {
    if (source[key] && typeof source[key] === 'object' && !Array.isArray(source[key])) {
      result[key] = deepMerge(target[key] || {}, source[key]);
    } else {
      result[key] = source[key];
    }
  }
  
  return result;
}

/**
 * Validate email format
 */
export function isValidEmail(email: string): boolean {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email);
}

/**
 * Generate unique ID
 */
export function generateId(): string {
  return crypto.randomBytes(16).toString('hex');
}

/**
 * Convert bytes to human readable format
 */
export function formatBytes(bytes: number, decimals: number = 2): string {
  if (bytes === 0) return '0 Bytes';
  
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

/**
 * Debounce function
 */
export function debounce<T extends (...args: any[]) => any>(
  func: T,
  delay: number
): (...args: Parameters<T>) => void {
  let timeoutId: NodeJS.Timeout;
  
  return (...args: Parameters<T>) => {
    clearTimeout(timeoutId);
    timeoutId = setTimeout(() => func.apply(null, args), delay);
  };
}

/**
 * Truncate string with ellipsis
 */
export function truncateString(str: string, maxLength: number): string {
  if (str.length <= maxLength) return str;
  return str.substring(0, maxLength - 3) + '...';
}

/**
 * Check if value is empty (null, undefined, empty string, empty array)
 */
export function isEmpty(value: any): boolean {
  if (value === null || value === undefined) return true;
  if (typeof value === 'string') return value.trim() === '';
  if (Array.isArray(value)) return value.length === 0;
  if (typeof value === 'object') return Object.keys(value).length === 0;
  return false;
}

/**
 * Safe JSON parse with default value
 */
export function safeJsonParse<T>(jsonString: string, defaultValue: T): T {
  try {
    return JSON.parse(jsonString);
  } catch {
    return defaultValue;
  }
}