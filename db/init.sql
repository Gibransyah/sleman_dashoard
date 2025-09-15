USE sleman_dashboard;

-- Create facts_long table with proper indexing
CREATE TABLE IF NOT EXISTS facts_long (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  kategori VARCHAR(64) NOT NULL,
  elemen VARCHAR(255) NOT NULL,
  tahun INT,
  nilai DECIMAL(18,4),
  satuan VARCHAR(64),
  raw_json JSON,
  source_type ENUM('api','csv') NOT NULL,
  source_ref VARCHAR(255),
  hash_key CHAR(64) NOT NULL,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  
  UNIQUE KEY uniq_hash (hash_key),
  INDEX idx_kategori (kategori),
  INDEX idx_elemen (elemen),
  INDEX idx_tahun (tahun),
  INDEX idx_source_type (source_type),
  INDEX idx_ingested_at (ingested_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Create ETL log table for tracking ingestion status
CREATE TABLE IF NOT EXISTS etl_logs (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  source_type ENUM('api','csv') NOT NULL,
  source_ref VARCHAR(255) NOT NULL,
  kategori VARCHAR(64),
  status ENUM('started','completed','failed') NOT NULL,
  total_records INT DEFAULT 0,
  new_records INT DEFAULT 0,
  updated_records INT DEFAULT 0,
  error_message TEXT,
  started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  completed_at TIMESTAMP NULL,
  
  INDEX idx_source (source_type, source_ref),
  INDEX idx_status (status),
  INDEX idx_started_at (started_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Create ETL checkpoints table for resuming interrupted processes
CREATE TABLE IF NOT EXISTS etl_checkpoints (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  source_type ENUM('api','csv') NOT NULL,
  source_ref VARCHAR(255) NOT NULL,
  last_offset INT DEFAULT 0,
  last_processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  UNIQUE KEY uniq_source (source_type, source_ref),
  INDEX idx_last_processed (last_processed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Sample data views for easy querying
CREATE OR REPLACE VIEW v_kategori_stats AS
SELECT 
  kategori,
  COUNT(*) as total_records,
  COUNT(DISTINCT elemen) as unique_elements,
  MIN(tahun) as min_year,
  MAX(tahun) as max_year,
  COUNT(DISTINCT source_ref) as data_sources
FROM facts_long 
GROUP BY kategori;

CREATE OR REPLACE VIEW v_yearly_trends AS
SELECT 
  tahun,
  kategori,
  COUNT(*) as record_count,
  COUNT(DISTINCT elemen) as unique_elements
FROM facts_long 
WHERE tahun IS NOT NULL
GROUP BY tahun, kategori
ORDER BY tahun, kategori;