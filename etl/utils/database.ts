import mysql from 'mysql2/promise';
import { Logger } from 'winston';
import { ProcessedRecord } from '../loaders/loader_api_ckan';

export interface DatabaseConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
}

export interface UpsertResult {
  inserted: number;
  updated: number;
  skipped: number;
  errors: number;
}

export interface EtlLogEntry {
  source_type: 'api' | 'csv';
  source_ref: string;
  kategori: string;
  status: 'started' | 'completed' | 'failed';
  total_records?: number;
  new_records?: number;
  updated_records?: number;
  error_message?: string;
}

export class DatabaseManager {
  private connection: mysql.Connection | null = null;
  private config: DatabaseConfig;
  private logger: Logger;

  constructor(config: DatabaseConfig, logger: Logger) {
    this.config = config;
    this.logger = logger;
  }

  async connect(): Promise<void> {
    try {
      this.connection = await mysql.createConnection({
        host: this.config.host,
        port: this.config.port,
        user: this.config.user,
        password: this.config.password,
        database: this.config.database,
        charset: 'utf8mb4',
        timezone: '+00:00'
      });

      this.logger.info('Connected to MySQL database');
    } catch (error: any) {
      this.logger.error('Failed to connect to database:', error.message);
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    if (this.connection) {
      await this.connection.end();
      this.connection = null;
      this.logger.info('Disconnected from MySQL database');
    }
  }

async upsertRecords(records: ProcessedRecord[]): Promise<UpsertResult> {
  if (!this.connection) throw new Error('Database connection not established');

  const res: UpsertResult = { inserted: 0, updated: 0, skipped: 0, errors: 0 };
  if (records.length === 0) return res;

  // pastikan kolom-kolom ini ADA di tabel: facts_long
  // (lihat migrasi di bawah)
  const cols = [
    'kategori','elemen','tahun','nilai','satuan',
    'raw_json','source_type','source_ref','hash_key','ingested_at'
  ];

  const placeholders = records.map(() => `(${new Array(cols.length).fill('?').join(',')})`).join(',');
  const flatValues = records.flatMap(r => [
    r.kategori,
    r.elemen,
    r.tahun ?? null,
    r.nilai ?? null,
    r.satuan ?? null,
    // JSON harus string
    typeof r.raw_json === 'string' ? r.raw_json : JSON.stringify(r.raw_json ?? {}),
    r.source_type,
    r.source_ref,
    r.hash_key,
    new Date(), // ingested_at
  ]);

  const sql = `
    INSERT INTO facts_long (${cols.join(',')})
    VALUES ${placeholders}
    ON DUPLICATE KEY UPDATE
      nilai = VALUES(nilai),
      satuan = VALUES(satuan),
      raw_json = VALUES(raw_json),
      source_type = VALUES(source_type),
      source_ref = VALUES(source_ref),
      updated_at = CURRENT_TIMESTAMP
  `;

  try {
    await this.connection.beginTransaction();
    const [qr] = await this.connection.execute(sql, flatValues);
    const r = qr as mysql.ResultSetHeader;
    // Pada bulk upsert, affectedRows = inserted*1 + updated*2 (khas MySQL)
    const updated = r.affectedRows - records.length; // kira-kira
    const inserted = records.length - Math.max(updated, 0);
    res.inserted = Math.max(inserted, 0);
    res.updated  = Math.max(updated, 0);
    await this.connection.commit();
    this.logger.info(`Upsert OK: ${res.inserted} inserted, ${res.updated} updated`);
    return res;
  } catch (e:any) {
    await this.connection.rollback();
    this.logger.error('Error during upsert:', e.message);
    res.errors = records.length;
    throw e;
  }
}


  async batchUpsertRecords(records: ProcessedRecord[], batchSize: number = 1000): Promise<UpsertResult> {
    const totalResult: UpsertResult = {
      inserted: 0,
      updated: 0,
      skipped: 0,
      errors: 0
    };

    // Process records in batches
    for (let i = 0; i < records.length; i += batchSize) {
      const batch = records.slice(i, i + batchSize);
      
      try {
        const batchResult = await this.upsertRecords(batch);
        
        totalResult.inserted += batchResult.inserted;
        totalResult.updated += batchResult.updated;
        totalResult.skipped += batchResult.skipped;
        totalResult.errors += batchResult.errors;

        this.logger.info(`Processed batch ${Math.floor(i / batchSize) + 1}: ${batch.length} records`);

      } catch (error) {
        this.logger.error(`Error processing batch starting at index ${i}:`, error);
        totalResult.errors += batch.length;
      }
    }

    return totalResult;
  }

  async logEtlActivity(logEntry: EtlLogEntry): Promise<void> {
    if (!this.connection) {
      throw new Error('Database connection not established');
    }

    try {
      const query = `
        INSERT INTO etl_logs (
          source_type, source_ref, kategori, status, total_records,
          new_records, updated_records, error_message, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(), ?)
      `;

      const values = [
        logEntry.source_type,
        logEntry.source_ref,
        logEntry.kategori,
        logEntry.status,
        logEntry.total_records || 0,
        logEntry.new_records || 0,
        logEntry.updated_records || 0,
        logEntry.error_message || null,
        logEntry.status === 'completed' ? new Date() : null
      ];

      await this.connection.execute(query, values);

    } catch (error: any) {
      this.logger.warn('Failed to log ETL activity:', error.message);
      // Don't throw - logging failure shouldn't stop the ETL process
    }
  }

  async updateEtlLog(logId: number, updates: Partial<EtlLogEntry>): Promise<void> {
    if (!this.connection) {
      throw new Error('Database connection not established');
    }

    try {
      const setParts: string[] = [];
      const values: any[] = [];

      if (updates.status) {
        setParts.push('status = ?');
        values.push(updates.status);
      }

      if (updates.total_records !== undefined) {
        setParts.push('total_records = ?');
        values.push(updates.total_records);
      }

      if (updates.new_records !== undefined) {
        setParts.push('new_records = ?');
        values.push(updates.new_records);
      }

      if (updates.updated_records !== undefined) {
        setParts.push('updated_records = ?');
        values.push(updates.updated_records);
      }

      if (updates.error_message) {
        setParts.push('error_message = ?');
        values.push(updates.error_message);
      }

      if (updates.status === 'completed' || updates.status === 'failed') {
        setParts.push('completed_at = NOW()');
      }

      if (setParts.length === 0) {
        return;
      }

      values.push(logId);

      const query = `UPDATE etl_logs SET ${setParts.join(', ')} WHERE id = ?`;
      await this.connection.execute(query, values);

    } catch (error: any) {
      this.logger.warn('Failed to update ETL log:', error.message);
    }
  }

  async getCheckpoint(sourceType: 'api' | 'csv', sourceRef: string): Promise<number> {
    if (!this.connection) {
      throw new Error('Database connection not established');
    }

    try {
      const query = 'SELECT last_offset FROM etl_checkpoints WHERE source_type = ? AND source_ref = ?';
      const [rows] = await this.connection.execute(query, [sourceType, sourceRef]);
      
      const results = rows as any[];
      return results.length > 0 ? results[0].last_offset : 0;

    } catch (error: any) {
      this.logger.warn('Failed to get checkpoint:', error.message);
      return 0;
    }
  }

  async saveCheckpoint(sourceType: 'api' | 'csv', sourceRef: string, offset: number): Promise<void> {
    if (!this.connection) {
      throw new Error('Database connection not established');
    }

    try {
      const query = `
        INSERT INTO etl_checkpoints (source_type, source_ref, last_offset, last_processed_at)
        VALUES (?, ?, ?, NOW())
        ON DUPLICATE KEY UPDATE
          last_offset = VALUES(last_offset),
          last_processed_at = NOW()
      `;

      await this.connection.execute(query, [sourceType, sourceRef, offset]);

    } catch (error: any) {
      this.logger.warn('Failed to save checkpoint:', error.message);
    }
  }

  async getStats(): Promise<any> {
    if (!this.connection) {
      throw new Error('Database connection not established');
    }

    try {
      const queries = {
        totalRecords: 'SELECT COUNT(*) as count FROM facts_long',
        totalKategori: 'SELECT COUNT(DISTINCT kategori) as count FROM facts_long',
        totalElemen: 'SELECT COUNT(DISTINCT elemen) as count FROM facts_long',
        lastUpdate: 'SELECT MAX(updated_at) as last_update FROM facts_long',
        kategoriStats: 'SELECT kategori, COUNT(*) as count FROM facts_long GROUP BY kategori ORDER BY count DESC',
        yearRange: 'SELECT MIN(tahun) as min_year, MAX(tahun) as max_year FROM facts_long WHERE tahun IS NOT NULL'
      };

      const results: any = {};

      for (const [key, query] of Object.entries(queries)) {
        try {
          const [rows] = await this.connection.execute(query);
          results[key] = rows;
        } catch (error: any) {
          this.logger.warn(`Failed to execute stats query ${key}:`, error.message);
          results[key] = null;
        }
      }

      return results;

    } catch (error: any) {
      this.logger.error('Failed to get database stats:', error.message);
      throw error;
    }
  }

  async testConnection(): Promise<boolean> {
    try {
      if (!this.connection) {
        await this.connect();
      }

      const [rows] = await this.connection!.execute('SELECT 1 as test');
      return Array.isArray(rows) && rows.length > 0;

    } catch (error: any) {
      this.logger.error('Database connection test failed:', error.message);
      return false;
    }
  }
}