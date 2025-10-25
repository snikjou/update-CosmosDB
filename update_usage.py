"""
Simplified CosmosDB Update Script
Updates 100 message documents by adding a 'usage' field with null values.
"""

import asyncio
from datetime import datetime, timezone
from azure.cosmos.aio import CosmosClient

# CosmosDB Configuration
ENDPOINT = "https://your-account.documents.azure.com:443/"
KEY = "your-primary-key-here"
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
        # Query for message documents without usage field using pagination
        query = "SELECT * FROM c WHERE c.type = 'message' AND c.role = 'assistant' AND NOT IS_DEFINED(c.usage)"
        
        print("🔍 Querying for documents using pagination...")
        documents = []
        page_size = 10  # Small page size to avoid header size issues
        
        # Use pagination to fetch documents in small batches
        try:
            async for item in container.query_items(query=query, max_item_count=page_size):
                documents.append(item)
                if len(documents) >= MAX_RECORDS:
                    break
        except Exception as e:
            if "Header value is too long" in str(e):
                print("⚠️  Reducing page size to 5 due to header size limit...")
                page_size = 5
                documents = []
                async for item in container.query_items(query=query, max_item_count=page_size):
                    documents.append(item)
                    if len(documents) >= MAX_RECORDS:
                        break
            else:
                raise
        
        print(f"📋 Found {len(documents)} documents to update\n")
        
        if not documents:
            print("✅ No documents need updating!")
            return
        
        # Update documents
        updated_count = 0
        for i, doc in enumerate(documents, 1):
            try:
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
                    print(f"✅ Updated {i}/{len(documents)} documents...")
                
            except Exception as e:
                print(f"❌ Error updating document {doc.get('id')}: {e}")
        
        print(f"\n🎉 Update Complete!")
        print(f"✅ Successfully updated: {updated_count} documents")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
