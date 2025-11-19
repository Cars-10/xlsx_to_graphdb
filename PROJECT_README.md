# Windchill MCP to Neo4j Integration System

A comprehensive Python-based system for importing Windchill product data into Neo4j graph database with advanced change tracking and relationship mapping capabilities.

## 🎯 Project Overview

This project provides a complete solution for:
- **Windchill MCP Integration**: Connect to PTC Cloud Windchill servers via Model Context Protocol (MCP)
- **Data Analysis**: Parse and analyze Excel/CSV files from Windchill exports
- **Neo4j Import**: Load parts, BOM relationships, and change tracking into graph database
- **Change Management**: Track ECNs, ECOs, revisions, and state changes
- **Relationship Mapping**: Create comprehensive part-to-part and change-to-part relationships

## 📁 Project Structure

```
windchill_demo_data/
├── src/                          # Core source code
│   ├── core/                     # Core modules (exceptions, logging, validation)
│   ├── importers/                # Neo4j importers for different products
│   ├── utils/                    # Utility modules (spreadsheet loaders)
│   └── web/                      # Web UI components
├── scripts/                      # Executable scripts
│   ├── mcp/                      # MCP client scripts
│   ├── data_processing/          # Data analysis and processing scripts
│   ├── verification/             # Import verification scripts
│   └── *.sh                      # Shell scripts for automation
├── data/                         # Data files
│   ├── raw/                      # Original Windchill export files
│   └── processed/                  # Generated/imported data files
├── tests/                        # Test suites
│   ├── unit/                     # Unit tests
│   └── integration/              # Integration tests
├── docs/                         # Documentation
│   ├── api/                      # API documentation
│   ├── guides/                   # User guides
│   └── examples/                 # Usage examples
└── config/                       # Configuration files
```

## 🚀 Quick Start

### Prerequisites

```bash
pip install -r requirements.txt
```

### Basic Usage

1. **Analyze Snowmobile Data**:
```bash
python scripts/data_processing/analyze_snowmobile_data.py
```

2. **Import to Neo4j**:
```bash
python src/importers/snowmobile_neo4j_importer.py
```

3. **Verify Import**:
```bash
python scripts/verification/verify_snowmobile_graph.py
```

### Windchill MCP Connection

```bash
python scripts/mcp/connect_windchill_mcp.py
python scripts/mcp/enhanced_windchill_mcp_client.py
python scripts/mcp/mcp_windchill_client.py
```

## 📊 Supported Data Types

### Parts & Assemblies
- Part numbers, names, descriptions
- Part types and categories
- Revision information
- State tracking

### BOM Relationships
- Parent-child component relationships
- Assembly structures
- Part supersession chains
- Cross-references

### Change Tracking
- **ECO** (Engineering Change Orders)
- **ECN** (Engineering Change Notices)
- **DEV** (Development changes)
- **REV** (Revisions)

### Change States
- OPEN, IN_WORK, REVIEW, APPROVED
- IMPLEMENTED, CANCELLED
- Custom state workflows

## 🔗 Relationship Types

The system creates comprehensive relationship networks:

- **HAS_COMPONENT**: BOM parent-child relationships
- **AFFECTS_PART**: Change-to-part impact mapping
- **SUPERSEDES**: Part revision chains
- **PART_OF**: Assembly membership
- **DEPENDS_ON**: Change dependencies
- **RELATED_TO**: Related changes

## 🛠 Core Modules

### Importers
- `SnowmobileNeo4jImporter`: Comprehensive snowmobile data import
- `HelicopterChangeImporter`: Helicopter-specific change tracking
- `Neo4jImporter`: Generic Neo4j import utilities

### Data Processing
- `EnhancedSpreadsheetParser`: Advanced Excel/CSV parsing
- `DataValidator`: Comprehensive data validation
- `SpreadsheetLoader`: Basic spreadsheet handling

### Core Utilities
- `ValidationError`: Custom validation exceptions
- `ConfigurationError`: Configuration-related exceptions
- `setup_logging`: Consistent logging configuration

## 📈 Verification & Analytics

The system includes comprehensive verification tools:

- **Basic Counts**: Nodes and relationships verification
- **Change Analysis**: Change type and state distribution
- **Part Analysis**: Part categories and multi-change tracking
- **Relationship Networks**: Complex multi-hop relationship analysis
- **BOM Structures**: Assembly hierarchy verification

## 🔧 Configuration

### Neo4j Connection
Default connection: `bolt://localhost:7687`
Authentication: `neo4j/tstpwdpwd`

### Data Files
- Excel files: Product data exports
- CSV files: BOM relationships
- JSON files: Enhanced processed data

## 📋 Example Results

### Snowmobile Import Summary
- **132 Parts** with complete metadata
- **264 Change Records** (ECOs, ECNs, DEVs, REVs)
- **3,657 Total Relationships** mapped
- **2,087 BOM Relationships** showing component hierarchies
- **959 Change-Part Relationships** tracking impacts

### Sample Part Analysis
- **10 Parts with Multiple Changes** showing iterative development
- **Part Supersession Chains** up to 3 levels deep
- **Complex BOM Structures** with multi-level assemblies

## 🧪 Testing

Run the test suite:
```bash
python -m pytest tests/
```

Individual test categories:
```bash
python tests/unit/test_spreadsheet_loader.py
python tests/integration/demo_enhanced_error_handling.py
```

## 📚 Documentation

Detailed documentation available in `docs/`:
- API documentation
- Implementation guides
- Neo4j visualization guides
- Web UI documentation

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is part of the Windchill integration system.

## 🔗 Related Projects

- Windchill PLM System
- Neo4j Graph Database
- PTC Cloud Services
- Model Context Protocol (MCP)