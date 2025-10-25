"""
Delete usage field from 1000 CosmosDB records
"""

import asyncio
from azure.cosmos.aio import CosmosClient

# CosmosDB Configuration
ENDPOINT = "https://your-account.documents.azure.com:443/"
KEY = "your-primary-key-here"
DATABASE_NAME = "chathistory"
CONTAINER_NAME = "messages"

MAX_RECORDS = 1000

async def main():
    client = CosmosClient(ENDPOINT, credential=KEY)
    database = client.get_database_client(DATABASE_NAME)
    container = database.get_container_client(CONTAINER_NAME)
    
    try:
        # Query for documents with usage field
        query = "SELECT * FROM c WHERE IS_DEFINED(c.usage)"
        
        print(f"🔍 Querying for up to {MAX_RECORDS} documents with usage field...")
        documents = []
        
        async for item in container.query_items(query=query, max_item_count=10):
            documents.append(item)
            if len(documents) >= MAX_RECORDS:
                break
        
        print(f"📋 Found {len(documents)} documents\n")
        
        # Delete usage field from each document
        deleted_count = 0
        for i, doc in enumerate(documents, 1):
            try:
                # Remove the usage field
                if 'usage' in doc:
                    del doc['usage']
                
                # Save document back
                await container.upsert_item(doc)
                deleted_count += 1
                
                if i % 10 == 0:
                    print(f"✅ Processed {i}/{len(documents)} documents...")
                    
            except Exception as e:
                print(f"❌ Error updating document {doc.get('id')}: {e}")
        
        print(f"\n🎉 Complete! Deleted usage field from {deleted_count} documents")
        
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())