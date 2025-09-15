# 📊 Dashboard Data Publik Sleman

> **Fullstack Dashboard untuk Visualisasi Data Publik Kabupaten Sleman**  
> Next.js + TypeScript + MySQL + Metabase + ETL Pipeline

![Dashboard Preview](https://via.placeholder.com/800x400/0066cc/ffffff?text=Dashboard+Data+Publik+Sleman)

## 🎯 **Tujuan Proyek**

Membangun dashboard data publik yang komprehensif untuk:
- ✅ **Visualisasi KPI** dan trend data publik Sleman
- ✅ **ETL Pipeline** otomatis dari API CKAN dan file CSV/Excel
- ✅ **Business Intelligence** dengan Metabase
- ✅ **Kolaborasi Tim** dengan setup development yang standar
- ✅ **Single Source of Truth** dengan MySQL sebagai data warehouse

## 🏗️ **Arsitektur Sistem**

```mermaid
graph TB
    A[API CKAN Sleman] --> E[ETL Pipeline]
    B[CSV/Excel Files] --> E
    E --> D[(MySQL Database)]
    D --> F[Next.js Frontend]
    D --> G[Metabase BI]
    F --> H[Dashboard UI]
    G --> I[BI Reports]
    
    subgraph "Docker Services"
        D
        G
        J[Adminer]
    end
```

## 🚀 **Quick Start (< 15 menit)**

### Prerequisites
- **Node.js** 18+ dan **pnpm** 8+
- **Docker** dan **Docker Compose**
- **Git**

### 1️⃣ Clone & Setup
```bash
git clone <repository-url>
cd project-magang-dashboard

# Install dependencies dan setup environment
pnpm setup
```

### 2️⃣ Konfigurasi Environment
```bash
# Edit konfigurasi database dan API
nano infra/.env

# Minimal configuration needed:
MYSQL_ROOT_PASSWORD=admin123!@#
MYSQL_DATABASE=sleman_dashboard  
MYSQL_USER=appuser
MYSQL_PASSWORD=apppass123
```

### 3️⃣ Start Infrastructure
```bash
# Start semua services (MySQL + Metabase + Adminer)
pnpm infra:up

# Tunggu hingga services ready (~30 detik)
```

### 4️⃣ Run ETL & Frontend
```bash
# Jalankan ETL untuk import data awal
pnpm etl:all

# Start development server
pnpm dev
```

### 5️⃣ Akses Dashboard
- **Frontend Dashboard**: http://localhost:3001
- **Metabase BI**: http://localhost:3000
- **Database Admin**: http://localhost:8080

## 📁 **Struktur Proyek**

```
project-magang-dashboard/
├── 📁 infra/                    # Infrastructure & Docker
│   ├── docker-compose.yml       # Services: MySQL, Metabase, Adminer
│   └── .env.example             # Environment template
├── 📁 db/                       # Database schema & migrations
│   ├── init.sql                 # Initial schema setup
│   └── migrations/              # Database migrations
├── 📁 etl/                      # ETL Pipeline
│   ├── etl.config.json         # Data sources configuration
│   ├── loaders/                # API & CSV loaders
│   │   ├── loader_api_ckan.ts  # CKAN API loader
│   │   └── loader_csv.ts       # CSV/Excel loader  
│   ├── utils/                  # Utilities
│   │   ├── database.ts         # Database operations
│   │   ├── helpers.ts          # Helper functions
│   │   └── logger.ts           # Logging system
│   └── run.ts                  # CLI runner
├── 📁 frontend/                 # Next.js Frontend
│   ├── src/app/                # App Router pages
│   ├── src/components/         # UI Components
│   └── src/lib/                # Utilities & API
├── 📁 metabase/                # Metabase BI setup
│   ├── seed/                   # Dashboard templates
│   └── README.md              # BI setup guide
└── 📄 package.json             # Workspace configuration
```

## 🔧 **Perintah Penting**

### ETL Operations
```bash
# Run semua ETL sources (API + CSV)
pnpm etl:all

# Run hanya API sources
pnpm etl:api

# Run hanya CSV sources  
pnpm etl:csv

# Run ETL untuk kategori tertentu
pnpm etl:all --only kesehatan

# Dry run (tanpa menulis ke database)
pnpm etl:all --dry-run

# ETL dengan filter tahun
pnpm etl:all --since 2020

# Run single source
pnpm etl:single --source-id d5ad87ba-ee41-42bc-88e1-fb4d5845e139
```

### Infrastructure Management  
```bash
# Start all services
pnpm infra:up

# Stop all services
pnpm infra:down  

# Restart services
pnpm infra:restart

# View logs
pnpm infra:logs

# Database only
pnpm db:up
pnpm db:down
```

### Development
```bash
# Start frontend development
pnpm dev

# Build for production
pnpm build

# Run tests
pnpm test

# Lint code
pnpm lint

# Clean builds
pnpm clean
```

## 🗄️ **Database Schema**

### Long Table Format (facts_long)
```sql
CREATE TABLE facts_long (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  kategori VARCHAR(64) NOT NULL,      -- kesehatan, pendidikan, dll
  elemen VARCHAR(255) NOT NULL,       -- nama elemen data
  tahun INT,                          -- tahun data
  nilai DECIMAL(18,4),                -- nilai numerik
  satuan VARCHAR(64),                 -- satuan pengukuran
  raw_json JSON,                      -- data asli dari sumber
  source_type ENUM('api','csv'),      -- tipe sumber data
  source_ref VARCHAR(255),            -- referensi sumber
  hash_key CHAR(64) NOT NULL,         -- untuk deduplication
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  
  UNIQUE KEY uniq_hash (hash_key)
);
```

### ETL Logs & Checkpoints
- **etl_logs**: Track ETL execution status
- **etl_checkpoints**: Resume interrupted ETL processes  
- **Views**: Pre-built aggregations untuk dashboard

## 📊 **Data Sources**

### API CKAN (Primary Sources)
1. **Kesehatan**: `d5ad87ba-ee41-42bc-88e1-fb4d5845e139`
2. **Pendidikan**: `4f760c5b-c0bd-46a9-b20d-cd87f21f6298`  
3. **Kebudayaan**: `c5b380db-c242-4e28-b3f2-d9b0eb5d162a`
4. **Infrastruktur**: `4c3cb950-0677-40e8-8f97-b96575c42c79`

### CSV/Excel Support
- **Flexible mapping** untuk berbagai format file
- **Auto-detection** header dan tipe data
- **Validation** struktur file sebelum processing

## 🎨 **Frontend Features**

### Dashboard Components
- **📈 KPI Cards**: Total records, categories, elements
- **📊 Trend Charts**: Data trends per tahun menggunakan Recharts
- **🔍 Filters**: Kategori, tahun, pencarian
- **📋 Data Table**: Sortable, searchable table dengan pagination

### Technology Stack
- **Next.js 14** dengan App Router
- **TypeScript** untuk type safety
- **Tailwind CSS** + **shadcn/ui** untuk styling
- **Recharts** untuk visualisasi
- **SWR** untuk data fetching

## 📈 **Metabase BI Setup**

### Auto-Connection Setup
1. **Database Connection**: 
   - Host: `mysql`
   - Port: `3306`
   - Database: `sleman_dashboard`
   - Credentials dari `.env`

### Pre-built Dashboards
- **📊 Executive Summary**: KPI overview
- **📈 Trend Analysis**: Multi-year comparisons  
- **🗂️ Category Breakdown**: Detailed category analysis
- **📋 Raw Data Explorer**: Filtered data tables

### Dashboard Seeding
```bash
# Export current dashboards
cd metabase/
./export-dashboards.sh

# Import to new instance
./import-dashboards.sh
```

## 👥 **Kolaborasi Tim**

### Branching Strategy
- **main**: Production-ready code
- **develop**: Integration branch
- **feature/***: Feature development
- **fix/***: Bug fixes

### Developer Roles
```bash
# Frontend Developer
cd frontend/
pnpm dev

# ETL/Data Engineer  
cd etl/
pnpm etl:all --dry-run
pnpm etl:single --source-id kesehatan

# BI Developer
# Focus on Metabase dashboards and queries
```

### Code Quality
```bash
# Pre-commit checks
pnpm lint          # ESLint + Prettier
pnpm type-check    # TypeScript validation
pnpm test          # Unit tests
```

## 🚀 **Production Deployment**

### Frontend (Vercel)
```bash
# Build optimized version
pnpm build

# Deploy to Vercel
vercel --prod
```

### ETL (Railway/Render)
```bash
# Containerized ETL for scheduled runs
docker build -t sleman-etl ./etl
docker run --env-file .env sleman-etl
```

### Database Migration
```bash
# Production environment variables
DATABASE_URL="mysql://user:pass@host:port/db"

# Run migrations
pnpm db:migrate --env production
```

## 🔧 **Troubleshooting**

### Common Issues

**🐛 Docker Services Won't Start**
```bash
# Check ports availability
netstat -tulpn | grep :3306
netstat -tulpn | grep :3000

# Reset Docker volumes if corrupted
docker compose down -v
docker compose up -d
```

**🐛 ETL Connection Errors**
```bash
# Test database connection
pnpm -C etl run health-check

# Check API endpoints
curl "https://data.slemankab.go.id/data/api/3/action/datastore_search?resource_id=d5ad87ba-ee41-42bc-88e1-fb4d5845e139&limit=1"
```

**🐛 Frontend API Errors**
```bash
# Verify database connection
mysql -h localhost -u appuser -p sleman_dashboard
SELECT COUNT(*) FROM facts_long;

# Check Next.js API routes
curl http://localhost:3001/api/stats
```

**🐛 Metabase Connection Issues**
```bash
# Reset Metabase data
docker compose down
docker volume rm sleman_dashboard_metabase_data
docker compose up -d metabase
```

### Performance Optimization

**ETL Performance**
```bash
# Increase batch size for large datasets
pnpm etl:all --batch-size 1000

# Run ETL for specific time range
pnpm etl:all --since 2023 --limit 50000
```

**Database Performance**
```sql
-- Check index usage
EXPLAIN SELECT * FROM facts_long WHERE kategori = 'kesehatan';

-- Add custom indexes if needed
CREATE INDEX idx_custom ON facts_long(kategori, tahun);
```

## 📚 **Additional Resources**

### Documentation
- [ETL Configuration Guide](./etl/README.md)
- [Frontend Component Library](./frontend/README.md)
- [Metabase Setup Guide](./metabase/README.md)
- [API Documentation](./docs/api.md)

### External Links
- [CKAN Sleman Data Portal](https://data.slemankab.go.id)
- [Next.js Documentation](https://nextjs.org/docs)
- [Metabase Documentation](https://www.metabase.com/docs)
- [Docker Compose Guide](https://docs.docker.com/compose)

## 🤝 **Contributing**

1. **Fork** repository
2. **Create** feature branch: `git checkout -b feature/amazing-feature`
3. **Commit** changes: `git commit -m 'Add amazing feature'`
4. **Push** to branch: `git push origin feature/amazing-feature`
5. **Open** Pull Request

### Development Guidelines
- ✅ Tulis kode TypeScript yang type-safe
- ✅ Follow ESLint + Prettier conventions
- ✅ Tambahkan unit tests untuk fungsi penting
- ✅ Update documentation untuk perubahan API
- ✅ Test ETL pipeline dengan data sample

## 📝 **License**

Distributed under the MIT License. See `LICENSE` for more information.

## 👨‍💻 **Tim Pengembang**

- **Frontend Developer**: Dashboard UI, API integration
- **Backend/ETL Developer**: Data pipeline, database optimization  
- **BI Developer**: Metabase dashboards, analytics
- **DevOps**: Infrastructure, deployment, monitoring

---

**🎉 Selamat mengembangkan Dashboard Data Publik Sleman yang amazing!**
