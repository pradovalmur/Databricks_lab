
# Databricks Lakehouse Data Platform — Tesouro Direto

## Overview

This project is a production-oriented Lakehouse architecture built on Databricks using a modern Medallion + Data Vault approach.

The platform demonstrates how to build:

- Automated ingestion pipelines
- Bronze / Silver / Gold layers
- Data Vault modeling
- Data Quality validation
- Slack alerting
- CI/CD with Databricks Bundles
- GitOps deployment strategy
- Operational observability

The use case is based on Tesouro Direto public datasets, ingesting both:

- Operations dataset
- Investors dataset

---

# Architecture

## Core Technologies

| Component | Purpose |
|---|---|
| Databricks | Processing & orchestration |
| Delta Lake | Storage layer |
| Unity Catalog | Governance |
| Databricks Bundles | CI/CD deployment |
| GitHub Actions | Deployment automation |
| Slack | Alerting |
| YAML Metadata | Config-driven pipelines |
| Data Vault | Enterprise modeling |
| Delta Tables | Lakehouse storage |

---

# Data Flow

```text
Public CSV Sources
        ↓
Download & Raw Landing
        ↓
Bronze Layer
(raw standardized ingestion)
        ↓
Data Quality Validation
        ↓
Silver Layer (Data Vault)
  - Hubs
  - Links
  - Satellites
        ↓
Data Quality Validation
        ↓
Gold Layer
  - Bridges
  - PIT Tables
  - Business Marts
        ↓
Analytics / BI / APIs
```

---

# Medallion Architecture

## Bronze Layer

Raw ingestion layer.

Responsibilities:

- CSV ingestion
- Schema normalization
- Metadata enrichment
- Ingestion timestamps
- Duplicate protection
- Incremental-safe reprocessing

### Main Tables

- lakehouse_lab.bronze.operacoes
- lakehouse_lab.bronze.investidores

---

## Silver Layer — Data Vault

Enterprise modeling layer using Data Vault principles.

### Hubs

| Hub | Business Key |
|---|---|
| hubInvestidor | codigoDoInvestidor |
| hubTitulo | tipoTitulo |

### Links

| Link | Description |
|---|---|
| linkInvestidorTituloOperacao | Investor ↔ Title relationship |

### Satellites

| Satellite | Description |
|---|---|
| satInvestidor | Investor descriptive attributes |
| satOperacao | Operational transaction details |

---

## Gold Layer

Business-ready analytical structures.

### Bridge

- bridgeInvestidorTitulo

### PIT

- pitInvestidor

### Marts

- martResumoTitulo
- martPerfilInvestidor

---

# Data Quality Framework

The platform contains a fully metadata-driven Data Quality framework.

## Quality Checks

- Minimum row count
- Required columns
- Not null validation
- Duplicate validation
- Hash key validation
- Referential integrity validation

## Audit Layer

All quality validations are stored in:

```text
lakehouse_lab.audit.dataQualityResults
```

## Slack Alerting

Automatic Slack notifications are triggered when:

- Data Quality fails
- Technical task execution fails
- Audit registration is missing

---

# CI/CD Strategy

## GitOps Flow

```text
Developer → GitHub → GitHub Actions → Databricks Bundle Deploy → Databricks Workspace
```

## Deployment Features

- Environment-based deployment
- Relative path support
- Bundle-managed artifacts
- Workspace synchronization
- Automated job deployment

---

# Observability

The platform already includes:

- Audit tables
- Quality history
- Slack notifications
- Execution visibility
- Pipeline failure tracking

Future evolution:

- SLA monitoring
- Runtime metrics
- Data lineage visualization
- Drift detection
- Cost monitoring

---

# Lineage

## Operations Dataset

```text
bronze.operacoes
    ↓
hubTitulo
    ↓
linkInvestidorTituloOperacao
    ↓
satOperacao
    ↓
bridgeInvestidorTitulo
    ↓
martResumoTitulo
```

## Investors Dataset

```text
bronze.investidores
    ↓
hubInvestidor
    ↓
satInvestidor
    ↓
pitInvestidor
    ↓
martPerfilInvestidor
```

---

# Key Differentiators

- Enterprise-grade architecture
- Fully metadata-driven
- Reusable framework
- CI/CD integrated
- Data Quality native
- Operational alerting
- Lakehouse + Data Vault integration
- Production-oriented design

---

# Commercial Positioning

This framework can be adapted for:

- Financial institutions
- Retail
- E-commerce
- Telecom
- Industrial telemetry
- IoT platforms
- Customer analytics
- Regulatory reporting

The architecture is designed to scale from SME environments to enterprise-grade workloads.

---

# Author

Valmur Prado  
Chameleon Mountain
