import axios, { AxiosResponse } from "axios";
import { Logger } from "winston";
import { sleep, calculateHash } from "../utils/helpers";

export interface CKANResponse {
  success: boolean;
  result: {
    resource_id: string;
    records: any[];
    total: number;
    limit: number;
    offset: number;
  };
}

export interface ApiSourceConfig {
  type: "api";
  kategori: string;
  resource_id: string;
  description?: string;
  mapping: {
    elemen_field: string;

    // mode long (lama)
    tahun_field?: string;
    nilai_field?: string;
    satuan_field?: string;

    // mode wide (baru)
    year_column_regex?: string;
    unit_field?: string;
  };
}

export interface ProcessedRecord {
  kategori: string;
  elemen: string;
  tahun: number | null;
  nilai: number | null;
  satuan: string | null;
  raw_json: string;
  source_type: 'api';
  source_ref: string;
  hash_key: string;
}

export class CKANApiLoader {
  private baseUrl: string;
  private batchSize: number;
  private maxRetries: number;
  private retryDelay: number;
  private timeout: number;
  private logger: Logger;

  constructor(config: any, logger: Logger) {
    this.baseUrl = config.settings.base_url;
    this.batchSize = config.settings.batch_size || 100;
    this.maxRetries = config.settings.max_retries || 3;
    this.retryDelay = config.settings.retry_delay || 1000;
    this.timeout = config.settings.timeout || 30000;
    this.logger = logger;
  }

  async loadData(sourceConfig: ApiSourceConfig, startOffset: number = 0): Promise<ProcessedRecord[]> {
    const allRecords: ProcessedRecord[] = [];
    let offset = startOffset;
    let hasMoreData = true;

    this.logger.info(`Starting API load for ${sourceConfig.kategori} from offset ${offset}`);

    while (hasMoreData) {
      try {
        const response = await this.fetchDataWithRetry(sourceConfig.resource_id, offset, this.batchSize);
        
        if (!response.success) {
          throw new Error(`CKAN API returned success: false for resource ${sourceConfig.resource_id}`);
        }

        const { records, total } = response.result;
        
        this.logger.info(`Fetched ${records.length} records (offset: ${offset}, total: ${total})`);

        if (records.length === 0) {
          hasMoreData = false;
          break;
        }

        // Process records
        const processedRecords = this.processRecords(records, sourceConfig);
        allRecords.push(...processedRecords);

        // Check if we have more data
        offset += this.batchSize;
        hasMoreData = offset < total;

        // Rate limiting
        if (hasMoreData) {
          await sleep(100); // 100ms delay between requests
        }

      } catch (error) {
        this.logger.error(`Error loading data for ${sourceConfig.kategori} at offset ${offset}:`, error);
        throw error;
      }
    }

    this.logger.info(`Completed API load for ${sourceConfig.kategori}. Total records: ${allRecords.length}`);
    return allRecords;
  }

  private async fetchDataWithRetry(resourceId: string, offset: number, limit: number): Promise<CKANResponse> {
    let lastError: any;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        const url = `${this.baseUrl}?resource_id=${resourceId}&offset=${offset}&limit=${limit}`;
        this.logger.debug(`Fetching: ${url} (attempt ${attempt})`);

        const response: AxiosResponse<CKANResponse> = await axios.get(url, {
          timeout: this.timeout,
          headers: {
            'User-Agent': 'Sleman-Dashboard-ETL/1.0',
            'Accept': 'application/json'
          }
        });

        return response.data;

      } catch (error: any) {
        lastError = error;
        this.logger.warn(`Attempt ${attempt} failed for resource ${resourceId}:`, error.message);

        if (attempt < this.maxRetries) {
          const delay = this.retryDelay * Math.pow(2, attempt - 1); // Exponential backoff
          this.logger.info(`Retrying in ${delay}ms...`);
          await sleep(delay);
        }
      }
    }

    throw new Error(`Failed to fetch data after ${this.maxRetries} attempts: ${lastError.message}`);
  }

private processRecords(records: any[], sourceConfig: ApiSourceConfig): ProcessedRecord[] {
  const processedRecords: ProcessedRecord[] = [];

  for (const record of records) {
    try {
      const chunk = this.transformRecord(record, sourceConfig); // <- now returns ProcessedRecord[]
      if (chunk && chunk.length) {
        processedRecords.push(...chunk);
      }
    } catch (error) {
      this.logger.warn(`Failed to process record from ${sourceConfig.kategori}:`, error);
    }
  }

  return processedRecords;
}

private transformRecord(record: any, sourceConfig: ApiSourceConfig): ProcessedRecord[] {
  const out: ProcessedRecord[] = [];
  const map = sourceConfig.mapping;

  // Helper bikin hash
  const mkHash = (elemen: string, tahun: number | null, nilai: number | null, satuan: string | null) =>
    calculateHash(`${sourceConfig.kategori}|${elemen}|${tahun}|${nilai}|${satuan}|${sourceConfig.resource_id}`);

  // CASE 1: mode long (punya tahun_field & nilai_field)
  if (map.tahun_field && map.nilai_field) {
    const elemen = String(this.extractField(record, map.elemen_field) ?? '').trim();
    if (!elemen) return out;

    const tahunRaw = this.extractField(record, map.tahun_field);
    const nilaiRaw = this.extractField(record, map.nilai_field);
    const satuanRaw = map.satuan_field ? this.extractField(record, map.satuan_field) : null;

    const tahun = this.parseIntSafely(tahunRaw);
    const nilai = this.parseFloatSafely(nilaiRaw);
    const satuan = (satuanRaw != null && String(satuanRaw).trim() !== '') ? String(satuanRaw).trim() : null;

    out.push({
      kategori: sourceConfig.kategori,
      elemen,
      tahun,
      nilai,
      satuan,
      raw_json: JSON.stringify(record),
      source_type: 'api',
      source_ref: sourceConfig.resource_id,
      hash_key: mkHash(elemen, tahun, nilai, satuan),
    });

    return out;
  }

  // CASE 2: mode wide (kolom tahun tersebar, mis. "Data 2019", "Data 2020", ...)
  if (map.year_column_regex) {
    const elemen = String(this.extractField(record, map.elemen_field) ?? '').trim();
    if (!elemen) return out;

    const yearRe = new RegExp(map.year_column_regex);
    const satuan = (() => {
      const unitFromMap = map.unit_field ? this.extractField(record, map.unit_field) : null;
      const unitFallback = this.extractField(record, 'Satuan'); // umum di CKAN daerah
      const s = (unitFromMap ?? unitFallback);
      return (s != null && String(s).trim() !== '') ? String(s).trim() : null;
    })();

    for (const key of Object.keys(record)) {
      const m = key.match(yearRe);
      if (!m) continue;

      // Ambil tahun dari grup tangkap pertama
      const tahun = this.parseIntSafely(m[1]);
      // Normalisasi angka (hilangkan koma/spasi)
      const nilai = this.parseFloatSafely(record[key]);

      out.push({
        kategori: sourceConfig.kategori,
        elemen,
        tahun,
        nilai,
        satuan,
        raw_json: JSON.stringify({ ...record, __value_from__: key }),
        source_type: 'api',
        source_ref: sourceConfig.resource_id,
        hash_key: mkHash(elemen, tahun, nilai, satuan),
      });
    }

    return out;
  }

  // Mapping tidak lengkap
  this.logger.warn(`[${sourceConfig.kategori}] Mapping tidak lengkap: tentukan (tahun_field & nilai_field) ATAU year_column_regex`);
  return out;
}

private extractField(record: any, fieldPath?: string): any {
  if (!fieldPath) return null; // allow undefined in wide mode
  const keys = fieldPath.split('.');
  let value = record;
  for (const key of keys) {
    if (value && typeof value === 'object' && key in value) {
      value = value[key];
    } else {
      return null;
    }
  }
  return value;
}


  private parseIntSafely(value: any): number | null {
    if (value === null || value === undefined || value === '') {
      return null;
    }
    
    const parsed = parseInt(String(value), 10);
    return isNaN(parsed) ? null : parsed;
  }

  private parseFloatSafely(value: any): number | null {
    if (value === null || value === undefined || value === '') {
      return null;
    }
    
    const parsed = parseFloat(String(value).replace(/,/g, ''));
    return isNaN(parsed) ? null : parsed;
  }
}
