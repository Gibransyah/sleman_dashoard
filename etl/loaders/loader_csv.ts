import * as fs from 'fs';
import * as path from 'path';
import { parse } from 'csv-parse/sync';
import * as XLSX from 'xlsx';
import { Logger } from 'winston';
import { calculateHash } from '../utils/helpers';

export interface CsvSourceConfig {
  type: 'csv';
  kategori: string;
  file_path: string;
  description?: string;
  mapping: {
    elemen_field: string;
    tahun_field: string;
    nilai_field: string;
    satuan_field: string;
  };
  csv_options?: {
    delimiter?: string;
    header?: boolean;
    encoding?: string;
  };
}

export interface ProcessedRecord {
  kategori: string;
  elemen: string;
  tahun: number | null;
  nilai: number | null;
  satuan: string | null;
  raw_json: string;
  source_type: 'csv';
  source_ref: string;
  hash_key: string;
}

export class CsvLoader {
  private logger: Logger;

  constructor(logger: Logger) {
    this.logger = logger;
  }

  async loadData(sourceConfig: CsvSourceConfig): Promise<ProcessedRecord[]> {
    const filePath = this.resolvePath(sourceConfig.file_path);
    
    if (!fs.existsSync(filePath)) {
      throw new Error(`File not found: ${filePath}`);
    }

    this.logger.info(`Loading data from ${filePath}`);

    const fileExtension = path.extname(filePath).toLowerCase();
    let records: any[];

    try {
      if (fileExtension === '.csv') {
        records = await this.loadCsv(filePath, sourceConfig.csv_options || {});
      } else if (fileExtension === '.xlsx' || fileExtension === '.xls') {
        records = await this.loadExcel(filePath);
      } else {
        throw new Error(`Unsupported file type: ${fileExtension}`);
      }

      this.logger.info(`Loaded ${records.length} raw records from ${path.basename(filePath)}`);

      const processedRecords = this.processRecords(records, sourceConfig);
      this.logger.info(`Processed ${processedRecords.length} valid records`);

      return processedRecords;

    } catch (error: any) {
      this.logger.error(`Error loading file ${filePath}:`, error.message);
      throw error;
    }
  }

  private resolvePath(filePath: string): string {
    if (path.isAbsolute(filePath)) {
      return filePath;
    }
    return path.resolve(process.cwd(), filePath);
  }

  private async loadCsv(filePath: string, options: any): Promise<any[]> {
    const encoding = options.encoding || 'utf8';
    const delimiter = options.delimiter || ',';
    const hasHeader = options.header !== false; // Default to true

    try {
      const fileContent = fs.readFileSync(filePath, { encoding: encoding as BufferEncoding });
      
      const parseOptions = {
        delimiter,
        columns: hasHeader,
        skip_empty_lines: true,
        trim: true,
        cast: false // Keep everything as strings initially
      };

      const records = parse(fileContent, parseOptions);
      return records;

    } catch (error: any) {
      throw new Error(`Failed to parse CSV: ${error.message}`);
    }
  }

  private async loadExcel(filePath: string): Promise<any[]> {
    try {
      const workbook = XLSX.readFile(filePath);
      const firstSheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[firstSheetName];
      
      const records = XLSX.utils.sheet_to_json(worksheet, {
        header: 1,
        defval: '',
        blankrows: false
      });

      // Convert array of arrays to array of objects using first row as headers
      if (records.length === 0) {
        return [];
      }

      const headers = records[0] as string[];
      const dataRows = records.slice(1) as any[][];

      const objectRecords = dataRows.map(row => {
        const record: any = {};
        headers.forEach((header, index) => {
          record[header] = row[index] || '';
        });
        return record;
      });

      return objectRecords;

    } catch (error: any) {
      throw new Error(`Failed to parse Excel file: ${error.message}`);
    }
  }

  private processRecords(records: any[], sourceConfig: CsvSourceConfig): ProcessedRecord[] {
    const processedRecords: ProcessedRecord[] = [];
    const fileName = path.basename(sourceConfig.file_path);

    for (let i = 0; i < records.length; i++) {
      try {
        const record = records[i];
        const processed = this.transformRecord(record, sourceConfig, fileName);
        
        if (processed) {
          processedRecords.push(processed);
        }
      } catch (error) {
        this.logger.warn(`Failed to process record ${i + 1} from ${fileName}:`, error);
        // Continue processing other records
      }
    }

    return processedRecords;
  }

  private transformRecord(record: any, sourceConfig: CsvSourceConfig, fileName: string): ProcessedRecord | null {
    const { mapping } = sourceConfig;

    // Extract fields using mapping configuration
    const elemen = this.extractField(record, mapping.elemen_field);
    const tahunRaw = this.extractField(record, mapping.tahun_field);
    const nilaiRaw = this.extractField(record, mapping.nilai_field);
    const satuan = this.extractField(record, mapping.satuan_field);

    if (!elemen || String(elemen).trim() === '') {
      this.logger.debug(`Skipping record with empty elemen field`);
      return null;
    }

    // Clean and parse values
    const cleanElemen = String(elemen).trim();
    const tahun = tahunRaw ? this.parseIntSafely(tahunRaw) : null;
    const nilai = nilaiRaw ? this.parseFloatSafely(nilaiRaw) : null;
    const cleanSatuan = satuan && String(satuan).trim() !== '' ? String(satuan).trim() : null;

    // Create hash for deduplication
    const hashInput = `${sourceConfig.kategori}|${cleanElemen}|${tahun}|${nilai}|${cleanSatuan}|${fileName}`;
    const hash_key = calculateHash(hashInput);

    return {
      kategori: sourceConfig.kategori,
      elemen: cleanElemen,
      tahun,
      nilai,
      satuan: cleanSatuan,
      raw_json: JSON.stringify(record),
      source_type: 'csv',
      source_ref: fileName,
      hash_key
    };
  }

  private extractField(record: any, fieldPath: string): any {
    // Support nested field access with dot notation
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
    if (value === null || value === undefined || String(value).trim() === '') {
      return null;
    }
    
    const cleaned = String(value).replace(/[^\d-]/g, '');
    const parsed = parseInt(cleaned, 10);
    return isNaN(parsed) ? null : parsed;
  }

  private parseFloatSafely(value: any): number | null {
    if (value === null || value === undefined || String(value).trim() === '') {
      return null;
    }
    
    // Remove thousands separators and keep decimal points
    const cleaned = String(value).replace(/[^\d.,-]/g, '').replace(/,/g, '');
    const parsed = parseFloat(cleaned);
    return isNaN(parsed) ? null : parsed;
  }

  // Utility method to validate CSV structure
  validateCsvStructure(sourceConfig: CsvSourceConfig): { isValid: boolean; errors: string[] } {
    const errors: string[] = [];
    const filePath = this.resolvePath(sourceConfig.file_path);

    if (!fs.existsSync(filePath)) {
      errors.push(`File not found: ${filePath}`);
      return { isValid: false, errors };
    }

    try {
      // Load a sample of the data to check structure
      const fileExtension = path.extname(filePath).toLowerCase();
      let sampleRecords: any[];

      if (fileExtension === '.csv') {
        const fileContent = fs.readFileSync(filePath, 'utf8');
        const lines = fileContent.split('\n').slice(0, 5); // First 5 lines
        sampleRecords = parse(lines.join('\n'), {
          columns: true,
          skip_empty_lines: true
        });
      } else {
        // For Excel files, just check if we can read them
        const workbook = XLSX.readFile(filePath);
        if (workbook.SheetNames.length === 0) {
          errors.push('Excel file has no worksheets');
          return { isValid: false, errors };
        }
        
        const worksheet = workbook.Sheets[workbook.SheetNames[0]];
        const records = XLSX.utils.sheet_to_json(worksheet, { header: 1 });
        
        if (records.length < 2) {
          errors.push('Excel file must have at least header and one data row');
          return { isValid: false, errors };
        }

        const headers = records[0] as string[];
        const dataRows = records.slice(1, 3); // Sample first 2 data rows
        
        sampleRecords = dataRows.map(row => {
          const record: any = {};
          headers.forEach((header, index) => {
            record[header] = (row as any[])[index] || '';
          });
          return record;
        });
      }

      if (sampleRecords.length === 0) {
        errors.push('No data records found in file');
        return { isValid: false, errors };
      }

      // Check if required mapping fields exist
      const { mapping } = sourceConfig;
      const sampleRecord = sampleRecords[0];
      const availableFields = Object.keys(sampleRecord);

      const requiredFields = [
        mapping.elemen_field,
        mapping.tahun_field,
        mapping.nilai_field,
        mapping.satuan_field
      ];

      for (const field of requiredFields) {
        if (!this.fieldExists(sampleRecord, field)) {
          errors.push(`Required field '${field}' not found. Available fields: ${availableFields.join(', ')}`);
        }
      }

      return { isValid: errors.length === 0, errors };

    } catch (error: any) {
      errors.push(`Error reading file: ${error.message}`);
      return { isValid: false, errors };
    }
  }

  private fieldExists(record: any, fieldPath: string): boolean {
    const keys = fieldPath.split('.');
    let current = record;
    
    for (const key of keys) {
      if (current && typeof current === 'object' && key in current) {
        current = current[key];
      } else {
        return false;
      }
    }
    
    return true;
  }
}