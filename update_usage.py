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
        # Use a simpler query with TOP to limit results and avoid header issues
        # Query in very small batches to work around header size limitation
        query = "SELECT TOP 1 c.id FROM c WHERE c.type = 'message' AND c.role = 'assistant' AND NOT IS_DEFINED(c.usage)"
        
        print("🔍 Querying and updating documents in small batches...")
        
        updated_count = 0
        processed_ids = set()
        
        # Process documents in a loop, querying one at a time
        for batch_num in range(MAX_RECORDS):
            try:
                # Query for one document ID at a time
                items = []
                async for item in container.query_items(query=query, max_item_count=1):
                    if item['id'] not in processed_ids:
                        items.append(item)
                        processed_ids.add(item['id'])
                        break
                
                if not items:
                    print(f"\n✅ No more documents to update!")
                    break
                
                doc_id = items[0]['id']
                
                # Read the full document (use id as partition key if not specified)
                doc = await container.read_item(item=doc_id, partition_key=doc_id)
                
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
                
                if updated_count % 10 == 0:
                    print(f"✅ Updated {updated_count} documents...")
                
            except Exception as e:
                print(f"❌ Error in batch {batch_num + 1}: {e}")
                # Continue with next document
                continue
        
        print(f"\n🎉 Update Complete!")
        print(f"✅ Successfully updated: {updated_count} documents")

        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
