"""
Simplified CosmosDB Update Script
Updates 100 message documents by adding a 'usage' field with null values.
"""

import asyncio
from datetime import datetime, timezone
from azure.cosmos.aio import CosmosClient

# CosmosDB Configuration
# 
ENDPOINT = "https://your-cosmosdb-account.documents.azure.com:443/"
KEY = ""
DATABASE_NAME = "chathistory"  
CONTAINER_NAME = "messages"

# Update configuration
MAX_RECORDS = 100
UPDATED_BY = "121"


def utc_now():
    """Generate current UTC timestamp"""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


async def main():
    """Main function to update CosmosDB documents"""
    print("🚀 Starting CosmosDB Update")
    print(f"📦 Database: {DATABASE_NAME}")
    print(f"📦 Container: {CONTAINER_NAME}")
    print(f"📊 Max records to update: {MAX_RECORDS}\n")
    
    # Initialize CosmosDB client
    client = CosmosClient(ENDPOINT, credential=KEY)
    database = client.get_database_client(DATABASE_NAME)
    container = database.get_container_client(CONTAINER_NAME)
    
    try:
        # Query for document IDs only to avoid header size issues
        # We'll fetch full documents one at a time using read_item
        query = "SELECT c.id, c._partitionKey FROM c WHERE c.type = 'message' AND c.role = 'assistant' AND NOT IS_DEFINED(c.usage)"
        
        print("🔍 Querying for document IDs...")
        doc_ids = []
        
        # Fetch document IDs
        print(f"📄 Fetching document IDs...")
        try:
            async for item in container.query_items(query=query, max_item_count=100):
                doc_ids.append({'id': item['id'], 'partition_key': item.get('_partitionKey', item['id'])})
                if len(doc_ids) >= MAX_RECORDS:
                    break
        except Exception as e:
            print(f"⚠️  Error during query: {e}")
            raise
        
        print(f"📋 Found {len(doc_ids)} documents to update\n")
        
        if not doc_ids:
            print("✅ No documents need updating!")
            return
        
        # Update documents one by one
        updated_count = 0
        for i, doc_ref in enumerate(doc_ids, 1):
            try:
                # Read the full document
                doc = await container.read_item(item=doc_ref['id'], partition_key=doc_ref['partition_key'])
                
                # Add usage field with null values
                doc['usage'] = {
                    'completion_tokens': None,
                    'prompt_tokens': None,
                    'total_tokens': None
                }
                doc['updatedAt'] = utc_now()
                doc['updatedBy'] = UPDATED_BY
                
                # Save document
                await container.upsert_item(doc)
                updated_count += 1
                
                if i % 10 == 0:
                    print(f"✅ Updated {i}/{len(doc_ids)} documents...")
                
            except Exception as e:
                print(f"❌ Error updating document {doc_ref['id']}: {e}")
        
        print(f"\n🎉 Update Complete!")
        print(f"✅ Successfully updated: {updated_count} documents")

        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
