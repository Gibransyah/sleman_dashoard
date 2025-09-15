#!/usr/bin/env tsx

import 'dotenv/config';
import { Command } from 'commander';
import * as dotenv from 'dotenv';
import * as fs from 'fs';
import * as path from 'path';
import { CKANApiLoader, ApiSourceConfig } from './loaders/loader_api_ckan';
import { CsvLoader, CsvSourceConfig } from './loaders/loader_csv';
import { DatabaseManager, DatabaseConfig } from './utils/database';
import { EtlLogger } from './utils/logger';
import { generateId } from './utils/helpers';


console.log('[DB-CONFIG]', process.env.DB_HOST, process.env.DB_PORT, process.env.DB_USER);

// Load environment variables
dotenv.config({ path: '../infra/.env' });

interface EtlConfig {
  sources: ApiSourceConfig[];
  csv_sources?: CsvSourceConfig[];
  settings: {
    base_url: string;
    batch_size: number;
    max_retries: number;
    retry_delay: number;
    timeout: number;
    enable_checkpoints: boolean;
    log_level: string;
  };
}

interface EtlOptions {
  mode: 'all' | 'api' | 'csv' | 'single';
  only?: string;
  dryRun?: boolean;
  since?: number;
  limit?: number;
  sourceId?: string;
  configPath?: string;
  verbose?: boolean;
}

class EtlRunner {
  private config: EtlConfig;
  private dbManager: DatabaseManager;
  private logger: EtlLogger;
  private requestId: string;

  constructor(configPath: string = './etl.config.json') {
    this.requestId = generateId();
    this.logger = new EtlLogger('ETL-Runner', this.requestId);
    
    // Load configuration
    this.config = this.loadConfig(configPath);
    
    // Initialize database manager
    const dbConfig: DatabaseConfig = {
      host: process.env.DB_HOST || 'localhost',
      port: parseInt(process.env.DB_PORT || '3306'),
      user: process.env.MYSQL_USER || 'appuser',
      password: process.env.MYSQL_PASSWORD || 'apppass123',
      database: process.env.MYSQL_DATABASE || 'sleman_dashboard'
    };
    
    this.dbManager = new DatabaseManager(dbConfig, this.logger);
  }

  private loadConfig(configPath: string): EtlConfig {
    try {
      const fullPath = path.resolve(configPath);
      
      if (!fs.existsSync(fullPath)) {
        throw new Error(`Configuration file not found: ${fullPath}`);
      }
      
      const configContent = fs.readFileSync(fullPath, 'utf8');
      const config = JSON.parse(configContent) as EtlConfig;
      
      this.logger.info(`Loaded configuration from ${fullPath}`);
      return config;
      
    } catch (error: any) {
      this.logger.error('Failed to load configuration', error);
      throw error;
    }
  }

  async run(options: EtlOptions): Promise<void> {
    const startTime = Date.now();
    
    try {
      this.logger.startProcess('ETL Runner', JSON.stringify(options));
      
      // Test database connection
      await this.dbManager.connect();
      const isConnected = await this.dbManager.testConnection();
      
      if (!isConnected) {
        throw new Error('Database connection test failed');
      }
      
      this.logger.info('Database connection verified');
      
      // Execute based on mode
      switch (options.mode) {
        case 'all':
          await this.runAll(options);
          break;
        case 'api':
          await this.runApiSources(options);
          break;
        case 'csv':
          await this.runCsvSources(options);
          break;
        case 'single':
          await this.runSingleSource(options);
          break;
        default:
          throw new Error(`Unknown mode: ${options.mode}`);
      }
      
      // Show final statistics
      await this.showStats();
      
      const duration = Date.now() - startTime;
      this.logger.endProcess('ETL Runner', duration);
      
    } catch (error: any) {
      this.logger.error('ETL Runner failed', error);
      throw error;
    } finally {
      await this.dbManager.disconnect();
    }
  }

  private async runAll(options: EtlOptions): Promise<void> {
    this.logger.info('Running all ETL sources');
    
    // Run API sources first
    if (this.config.sources.length > 0) {
      await this.runApiSources(options);
    }
    
    // Then run CSV sources
    if (this.config.csv_sources && this.config.csv_sources.length > 0) {
      await this.runCsvSources(options);
    }
  }

  private async runApiSources(options: EtlOptions): Promise<void> {
    this.logger.info(`Processing ${this.config.sources.length} API sources`);
    
    const apiLoader = new CKANApiLoader(this.config, this.logger);
    
    for (const sourceConfig of this.config.sources) {
      if (options.only && sourceConfig.kategori !== options.only) {
        this.logger.info(`Skipping ${sourceConfig.kategori} (not in --only filter)`);
        continue;
      }
      
      await this.processApiSource(apiLoader, sourceConfig, options);
    }
  }

  private async runCsvSources(options: EtlOptions): Promise<void> {
    if (!this.config.csv_sources || this.config.csv_sources.length === 0) {
      this.logger.info('No CSV sources configured');
      return;
    }
    
    this.logger.info(`Processing ${this.config.csv_sources.length} CSV sources`);
    
    const csvLoader = new CsvLoader(this.logger);
    
    for (const sourceConfig of this.config.csv_sources) {
      if (options.only && sourceConfig.kategori !== options.only) {
        this.logger.info(`Skipping ${sourceConfig.kategori} (not in --only filter)`);
        continue;
      }
      
      await this.processCsvSource(csvLoader, sourceConfig, options);
    }
  }

  private async runSingleSource(options: EtlOptions): Promise<void> {
    if (!options.sourceId) {
      throw new Error('--source-id is required for single mode');
    }
    
    // Try to find in API sources first
    const apiSource = this.config.sources.find(s => 
      s.resource_id === options.sourceId || s.kategori === options.sourceId
    );
    
    if (apiSource) {
      const apiLoader = new CKANApiLoader(this.config, this.logger);
      await this.processApiSource(apiLoader, apiSource, options);
      return;
    }
    
    // Try CSV sources
    const csvSource = this.config.csv_sources?.find(s =>
      s.kategori === options.sourceId || path.basename(s.file_path) === options.sourceId
    );
    
    if (csvSource) {
      const csvLoader = new CsvLoader(this.logger);
      await this.processCsvSource(csvLoader, csvSource, options);
      return;
    }
    
    throw new Error(`Source not found: ${options.sourceId}`);
  }

  private async processApiSource(
    loader: CKANApiLoader, 
    sourceConfig: ApiSourceConfig, 
    options: EtlOptions
  ): Promise<void> {
    const logEntry = {
      source_type: 'api' as const,
      source_ref: sourceConfig.resource_id,
      kategori: sourceConfig.kategori,
      status: 'started' as const
    };
    
    try {
      this.logger.info(`Processing API source: ${sourceConfig.kategori}`);
      
      // Log start
      await this.dbManager.logEtlActivity(logEntry);
      
      // Get checkpoint if enabled
      let startOffset = 0;
      if (this.config.settings.enable_checkpoints) {
        startOffset = await this.dbManager.getCheckpoint('api', sourceConfig.resource_id);
        if (startOffset > 0) {
          this.logger.info(`Resuming from offset ${startOffset}`);
        }
      }
      
      // Load data
      const records = await loader.loadData(sourceConfig, startOffset);
      
      if (records.length === 0) {
        this.logger.info(`No new records found for ${sourceConfig.kategori}`);
        return;
      }
      
      // Apply filters
      let filteredRecords = records;
      
      if (options.since) {
        filteredRecords = records.filter(r => !r.tahun || r.tahun >= options.since!);
        this.logger.info(`Filtered to ${filteredRecords.length} records since ${options.since}`);
      }
      
      if (options.limit) {
        filteredRecords = filteredRecords.slice(0, options.limit);
        this.logger.info(`Limited to ${filteredRecords.length} records`);
      }
      
      // Dry run check
      if (options.dryRun) {
        this.logger.info(`DRY RUN: Would process ${filteredRecords.length} records`);
        return;
      }
      
      // Upsert records
      const result = await this.dbManager.batchUpsertRecords(filteredRecords, this.config.settings.batch_size);
      
      // Update checkpoint
      if (this.config.settings.enable_checkpoints && records.length > 0) {
        await this.dbManager.saveCheckpoint('api', sourceConfig.resource_id, startOffset + records.length);
      }
      
      // Log completion
      await this.dbManager.logEtlActivity({
        ...logEntry,
        status: 'completed',
        total_records: records.length,
        new_records: result.inserted,
        updated_records: result.updated
      });
      
      this.logger.info(`Completed ${sourceConfig.kategori}: ${result.inserted} new, ${result.updated} updated`);
      
    } catch (error: any) {
      this.logger.error(`Failed to process API source ${sourceConfig.kategori}`, error);
      
      await this.dbManager.logEtlActivity({
        ...logEntry,
        status: 'failed',
        error_message: error.message
      });
      
      throw error;
    }
  }

  private async processCsvSource(
    loader: CsvLoader,
    sourceConfig: CsvSourceConfig,
    options: EtlOptions
  ): Promise<void> {
    const fileName = path.basename(sourceConfig.file_path);
    const logEntry = {
      source_type: 'csv' as const,
      source_ref: fileName,
      kategori: sourceConfig.kategori,
      status: 'started' as const
    };
    
    try {
      this.logger.info(`Processing CSV source: ${sourceConfig.kategori}`);
      
      // Validate file structure first
      const validation = loader.validateCsvStructure(sourceConfig);
      if (!validation.isValid) {
        throw new Error(`CSV validation failed: ${validation.errors.join(', ')}`);
      }
      
      // Log start
      await this.dbManager.logEtlActivity(logEntry);
      
      // Load data
      const records = await loader.loadData(sourceConfig);
      
      if (records.length === 0) {
        this.logger.info(`No records found in ${fileName}`);
        return;
      }
      
      // Apply filters
      let filteredRecords = records;
      
      if (options.since) {
        filteredRecords = records.filter(r => !r.tahun || r.tahun >= options.since!);
        this.logger.info(`Filtered to ${filteredRecords.length} records since ${options.since}`);
      }
      
      if (options.limit) {
        filteredRecords = filteredRecords.slice(0, options.limit);
        this.logger.info(`Limited to ${filteredRecords.length} records`);
      }
      
      // Dry run check
      if (options.dryRun) {
        this.logger.info(`DRY RUN: Would process ${filteredRecords.length} records`);
        return;
      }
      
      // Upsert records
      const result = await this.dbManager.batchUpsertRecords(filteredRecords, this.config.settings.batch_size);
      
      // Log completion
      await this.dbManager.logEtlActivity({
        ...logEntry,
        status: 'completed',
        total_records: records.length,
        new_records: result.inserted,
        updated_records: result.updated
      });
      
      this.logger.info(`Completed ${fileName}: ${result.inserted} new, ${result.updated} updated`);
      
    } catch (error: any) {
      this.logger.error(`Failed to process CSV source ${fileName}`, error);
      
      await this.dbManager.logEtlActivity({
        ...logEntry,
        status: 'failed',
        error_message: error.message
      });
      
      throw error;
    }
  }

  private async showStats(): Promise<void> {
    try {
      const stats = await this.dbManager.getStats();
      
      this.logger.info('=== DATABASE STATISTICS ===');
      
      if (stats.totalRecords && stats.totalRecords.length > 0) {
        this.logger.info(`Total Records: ${stats.totalRecords[0].count}`);
      }
      
      if (stats.totalKategori && stats.totalKategori.length > 0) {
        this.logger.info(`Total Categories: ${stats.totalKategori[0].count}`);
      }
      
      if (stats.totalElemen && stats.totalElemen.length > 0) {
        this.logger.info(`Total Elements: ${stats.totalElemen[0].count}`);
      }
      
      if (stats.yearRange && stats.yearRange.length > 0) {
        const range = stats.yearRange[0];
        this.logger.info(`Year Range: ${range.min_year} - ${range.max_year}`);
      }
      
      if (stats.kategoriStats && stats.kategoriStats.length > 0) {
        this.logger.info('Records by Category:');
        stats.kategoriStats.forEach((cat: any) => {
          this.logger.info(`  ${cat.kategori}: ${cat.count}`);
        });
      }
      
    } catch (error: any) {
      this.logger.warn('Failed to show statistics', error);
    }
  }
}

// CLI Setup
const program = new Command();

program
  .name('etl-runner')
  .description('ETL tool for Sleman Dashboard')
  .version('1.0.0');

program
  .option('-m, --mode <mode>', 'ETL mode: all, api, csv, single', 'all')
  .option('--only <kategori>', 'Process only specific category')
  .option('--dry-run', 'Show what would be processed without executing')
  .option('--since <year>', 'Process only records since year', parseInt)
  .option('--limit <count>', 'Limit number of records to process', parseInt)
  .option('--source-id <id>', 'Source ID for single mode (resource_id or kategori)')
  .option('-c, --config <path>', 'Path to config file', './etl.config.json')
  .option('-v, --verbose', 'Enable verbose logging')
  .action(async (options: EtlOptions) => {
    try {
      if (options.verbose) {
        process.env.LOG_LEVEL = 'debug';
      }
      
      const runner = new EtlRunner(options.configPath);
      await runner.run(options);
      
      console.log('\n✅ ETL process completed successfully!');
      process.exit(0);
      
    } catch (error: any) {
      console.error('\n❌ ETL process failed:', error.message);
      
      if (options.verbose && error.stack) {
        console.error('\nStack trace:');
        console.error(error.stack);
      }
      
      process.exit(1);
    }
  });

// Parse command line arguments
program.parse();