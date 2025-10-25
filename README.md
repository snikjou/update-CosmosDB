# update-CosmosBD

Scripts for managing usage fields in Azure Cosmos DB message documents.

## Prerequisites

1. **Python 3.7+** installed
2. **Install required packages:**
   ```bash
   pip install azure-cosmos
   ```

## Configuration

Before running any script, update the CosmosDB configuration variables at the top of each file:

```python
ENDPOINT = "https://your-account.documents.azure.com:443/"  # Your CosmosDB endpoint
KEY = "your-primary-key-here"                                # Your CosmosDB primary key
DATABASE_NAME = "chathistory"                                # Your database name
CONTAINER_NAME = "messages"                                  # Your container name
```

## Scripts

### 1. `update_usage.py` - Add Usage Fields

**Purpose:** Adds a `usage` field with null values to message documents that don't have one.

**What it does:**
- Queries for assistant messages without a `usage` field
- Updates up to 100 documents (configurable via `MAX_RECORDS`)
- Adds `usage` field with null `completion_tokens`, `prompt_tokens`, and `total_tokens`
- Updates `updatedAt` timestamp and `updatedBy` fields

**How to run:**
```bash
python update_usage.py
```

**Configuration options:**
- `MAX_RECORDS = 100` - Maximum number of documents to update
- `UPDATED_BY = "121"` - User/system ID for tracking updates

---

### 2. `delete_usage.py` - Remove Usage Fields

**Purpose:** Deletes the `usage` field from documents that have one.

**What it does:**
- Queries for documents with a `usage` field
- Removes the `usage` field from up to 1000 documents (configurable via `MAX_RECORDS`)
- Saves the updated documents back to CosmosDB

**How to run:**
```bash
python delete_usage.py
```

**Configuration options:**
- `MAX_RECORDS = 1000` - Maximum number of documents to process

---

### 3. `old.py` - Legacy Script (Advanced)

**Purpose:** More comprehensive script with dry-run and revert capabilities.

**Features:**
- Environment variable configuration
- Dry-run mode (default)
- Execute mode (actually applies changes)
- Revert mode (removes usage fields)
- Azure DefaultAzureCredential support

**Environment setup:**

Windows PowerShell:
```powershell
$env:COSMOSDB_ENDPOINT="https://your-account.documents.azure.com:443/"
$env:COSMOSDB_KEY="your-primary-key-here"
```

**How to run:**

Dry run (preview changes):
```bash
python old.py
```

Execute changes:
```bash
python old.py --execute
```

Revert changes (dry run):
```bash
python old.py --revert
```

Revert changes (execute):
```bash
python old.py --revert --execute
```

## Notes

- All scripts use asynchronous operations for better performance
- Scripts include pagination to handle large result sets
- Progress is displayed during execution
- Error handling is included for individual document operations

## Quick Start

1. Install dependencies: `pip install azure-cosmos`
2. Edit the script you want to use and update the configuration variables
3. Run the script: `python <script_name>.py`
4. Monitor the console output for progress and results