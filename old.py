"""
Script to add usage fields to CosmosDB message documents that don't already have them.
This script will add a 'usage' field with null subfields (completion_tokens, prompt_tokens, total_tokens)
to all message documents that are missing this field.

Requirements:
    pip install azure-cosmos azure-identity

Usage:
    # Dry run (default - shows what would be changed)
    python pcr-121.py
    
    # Actually apply changes
    python pcr-121.py --execute
    
    # Revert changes (dry run)
    python pcr-121.py --revert
    
    # Actually revert changes
    python pcr-121.py --revert --execute

Environment Variables Required:
    COSMOSDB_ENDPOINT - CosmosDB endpoint URL
    COSMOSDB_KEY - CosmosDB key (optional, will use DefaultAzureCredential if not provided)

Environment Variable Setup Examples:
    # Windows Command Prompt
    set COSMOSDB_ENDPOINT=https://your-account.documents.azure.com:443/
    set COSMOSDB_KEY=your-primary-key-here
    
    # PowerShell
    $env:COSMOSDB_ENDPOINT="https://your-account.documents.azure.com:443/"
    $env:COSMOSDB_KEY="your-primary-key-here"
    
    # Linux/macOS
    export COSMOSDB_ENDPOINT="https://your-account.documents.azure.com:443/"
    export COSMOSDB_KEY="your-primary-key-here"
"""

import asyncio
import os
import sys
from datetime import datetime, timezone
from azure.cosmos.aio import CosmosClient
from azure.cosmos import exceptions
from azure.identity import DefaultAzureCredential


def _utc_now():
    """Generate current UTC timestamp in ISO format"""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ===================================================================
# CONFIGURATION - Update these values for your environment
# ===================================================================

COSMOSDB_DATABASE = "db_conversation_history"
COSMOSDB_CONTAINER = "conversations"

UPDATED_BY = "121"

# ===================================================================
# COMMON COSMOS DB SETUP (reusable for other scripts)
# ===================================================================

async def create_cosmos_client(endpoint, credential, database_name, container_name):
    """Initialize and validate CosmosDB client connection"""
    try:
        client = CosmosClient(endpoint, credential=credential)
        database = client.get_database_client(database_name)
        container = database.get_container_client(container_name)
        
        # Test the connection
        await database.read()
        await container.read()
        
        print(f"✅ Connected to CosmosDB: {endpoint}")
        print(f"🗃️  Database: {database_name}")
        print(f"📦 Container: {container_name}")
        
        return client, container
    
    except exceptions.CosmosHttpResponseError as e:
        if e.status_code == 401:
            raise ValueError("Invalid credentials") from e
        else:
            raise ValueError("Invalid CosmosDB endpoint") from e
    except exceptions.CosmosResourceNotFoundError:
        raise ValueError("Invalid database or container name")


def validate_environment():
    """Validate required configuration"""
    # Check environment variable for endpoint
    endpoint = os.getenv("COSMOSDB_ENDPOINT")
    if not endpoint or endpoint.strip() == "":
        print("❌ COSMOSDB_ENDPOINT environment variable is empty - please set it")
        return False
    
    # Check global configuration variables
    if not COSMOSDB_DATABASE or COSMOSDB_DATABASE.strip() == "":
        print("❌ COSMOSDB_DATABASE is empty - please update in the script")
        return False
    
    if not COSMOSDB_CONTAINER or COSMOSDB_CONTAINER.strip() == "":
        print("❌ COSMOSDB_CONTAINER is empty - please update in the script")
        return False
    
    if not UPDATED_BY or UPDATED_BY.strip() == "":
        print("❌ UPDATED_BY is empty - please update in the script")
        return False
    
    print("✅ Configuration validated")
    return True


def get_configuration():
    """Get configuration from global variables and environment"""
    endpoint = os.getenv("COSMOSDB_ENDPOINT")
    database_name = COSMOSDB_DATABASE
    container_name = COSMOSDB_CONTAINER
    cosmosdb_key = os.getenv("COSMOSDB_KEY")
    
    if cosmosdb_key:
        credential = cosmosdb_key
        print("🔑 Using CosmosDB key for authentication")
    else:
        credential = DefaultAzureCredential()
        print("🔑 Using DefaultAzureCredential for authentication")
    
    return endpoint, credential, database_name, container_name


async def process_in_batches(documents, process_func, batch_size=50, concurrency=10):
    """Process documents in batches with controlled concurrency"""
    updated_count = 0
    error_count = 0
    
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i + batch_size]
        print(f"📦 Processing batch {i//batch_size + 1}/{(len(documents) + batch_size - 1)//batch_size}")
        
        semaphore = asyncio.Semaphore(concurrency)
        
        async def process_with_semaphore(doc):
            async with semaphore:
                return await process_func(doc)
        
        tasks = [process_with_semaphore(doc) for doc in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Count results
        for result in results:
            if isinstance(result, Exception):
                error_count += 1
            elif result:
                updated_count += 1
            else:
                error_count += 1
        
        print(f"✅ Batch completed. Updated so far: {updated_count}")
    
    return updated_count, error_count


# ===================================================================
# SPECIFIC LOGIC FOR ADDING USAGE FIELDS (customize for other tasks)
# ===================================================================

async def query_documents_with_count_pagination(container, base_query):
    """
    Query documents using count-based pagination with OFFSET/LIMIT
    """
    print("🔄 Using count-based pagination approach...")
    
    all_documents = []
    page_size = 1000  # Start with reasonable page size
    page_count = 0
    offset = 0
    
    try:
        # Add ORDER BY to ensure consistent pagination
        ordered_query = base_query + " ORDER BY c.id"
        
        while True:
            page_count += 1
            print(f"📦 Processing page {page_count} (offset: {offset}, page size: {page_size})")
            
            # Create paginated query using OFFSET and LIMIT
            paginated_query = f"{ordered_query} OFFSET {offset} LIMIT {page_size}"
            
            page_docs = []
            
            try:
                async for item in container.query_items(query=paginated_query, 
                parameters=[dict(name="@continuation_token_limit", value="4096")]):
                    page_docs.append(item)
                
                if not page_docs:
                    print("✅ No more documents to process")
                    break
                    
                print(f"   📄 Retrieved {len(page_docs)} documents")
                all_documents.extend(page_docs)
                
                # Move to next page
                offset += len(page_docs)
                
                # If we got fewer documents than page_size, we're at the end
                if len(page_docs) < page_size:
                    print("✅ Reached end of results")
                    break
                    
            except Exception as e:
                print(f"Exception: {e}")
                if "Header value is too long" in str(e):
                    if page_size > 100:
                        # Reduce page size and restart pagination from beginning
                        page_size = max(100, page_size // 2)
                        page_count = 0
                        offset = 0
                        all_documents = []  # Clear any partial results
                        print(f"   ⚠️  Page too large, reducing to {page_size} documents per page and restarting")
                        continue
                    else:
                        # Skip this problematic offset range and continue
                        print(f"   ❌ Skipping problematic offset {offset}, moving to next range")
                        offset += page_size  # Move to next offset range
                        continue
                else:
                    raise e
        
        print(f"✅ Retrieved {len(all_documents)} total documents using count-based pagination")
        return all_documents
        
    except Exception as e:
        print(f"❌ Count-based pagination failed: {e}")
        return []


async def run_update(container, dry_run=True, revert=False):
    """
    Main update logic - customize this function for different update tasks.
    """
    # UPDATE CONFIG - Change these for different updates
    if revert:
        update_name = "Remove Usage Fields from Message Documents"
    else:
        update_name = "Add Usage Fields to Message Documents"
    query = "SELECT * FROM c WHERE c.type = 'message' and c.role = 'assistant'"
    
    print(f"🔍 Querying for documents: {update_name}")
    print(f"📋 Query: {query}")
    
    # Try the original approach first
    all_msg_documents = []
    try:
        async for item in container.query_items(query=query, max_item_count=1000):
            all_msg_documents.append(item)
        
        print(f"📊 Found {len(all_msg_documents)} message documents")
        
    except Exception as e:
        error_msg = str(e)
        if "Header value is too long" in error_msg or "LineTooLong" in error_msg:
            print(f"⚠️  Standard query failed due to header size limits. Falling back to time-based pagination...")
            all_msg_documents = await query_documents_with_count_pagination(container, query)
            
            if not all_msg_documents:
                print("❌ Time-based pagination also failed. Dataset may be too large.")
                return
            
            print(f"📊 Found {len(all_msg_documents)} message documents via pagination")
        else:
            raise e
    
    # FILTER LOGIC - Determine which documents need updating
    documents_to_update = []
    for doc in all_msg_documents:
        # Change this condition for different updates
        if revert:
            # For revert: only process documents that have usage field AND were updated by this script
            if 'usage' in doc and doc.get('updatedBy') == UPDATED_BY:
                documents_to_update.append(doc)
        else:
            if 'usage' not in doc:  # Documents that need the usage field added
                documents_to_update.append(doc)
    
    skipped_count = len(all_msg_documents) - len(documents_to_update)
    if revert:
        print(f"📋 Documents needing revert (updated by {UPDATED_BY}): {len(documents_to_update)}")
        print(f"📋 Documents skipped (not updated by {UPDATED_BY} or already reverted): {skipped_count}")
        
        if not documents_to_update:
            print(f"✅ No documents found that were updated by {UPDATED_BY} and need reverting!")
            return
    else:
        print(f"📋 Documents needing updates: {len(documents_to_update)}")
        print(f"📋 Documents already updated: {skipped_count}")
        
        if not documents_to_update:
            print(f"✅ All documents have already been updated!")
            return
    
    if dry_run:
        # DRY RUN DISPLAY - Show examples of what would be changed
        print(f"\n🔍 DRY RUN MODE - No changes will be made")
        if revert:
            print(f"\nExample documents that would be reverted:")
        else:
            print(f"\nExample documents that would be updated:")
        
        for i, doc in enumerate(documents_to_update[:3]):
            print(f"\nDocument {i + 1}:")
            print(f"  ID: {doc.get('id', 'unknown')}")
            print(f"  User ID: {doc.get('userId', 'unknown')}")
            print(f"  Conversation ID: {doc.get('conversationId', 'unknown')}")
            print(f"  Role: {doc.get('role', 'unknown')}")
            print(f"  Created At: {doc.get('createdAt', 'unknown')}")
            print(f"  Has usage field: {'usage' in doc}")
        
        if len(documents_to_update) > 3:
            print(f"\n  ... and {len(documents_to_update) - 3} more documents")
        
        return
    
    else:
        # ACTUAL UPDATE - Perform the data updates
        if revert:
            print(f"\n🚀 REVERTING {len(documents_to_update)} documents...")
        else:
            print(f"\n🚀 UPDATING {len(documents_to_update)} documents...")
        
        async def update_single_document(doc):
            """Update a single document - customize this logic"""
            try:
                # UPDATE LOGIC - Change this for different updates
                if revert:
                    # Remove the usage field only if it was added by this script
                    if 'usage' in doc and doc.get('updatedBy') == UPDATED_BY:
                        del doc['usage']
                    # Update the updated info
                    doc['updatedAt'] = _utc_now()
                    doc['updatedBy'] = -1
                else:
                    # Add the usage field with null values
                    doc['usage'] = {
                        'completion_tokens': None,
                        'prompt_tokens': None,
                        'total_tokens': None
                    }
                    # Update the updated info
                    doc['updatedAt'] = _utc_now()
                    doc['updatedBy'] = UPDATED_BY
                
                # Save the document back to CosmosDB
                result = await container.upsert_item(doc)
                return bool(result)
                
            except Exception as e:
                action = "reverting" if revert else "updating"
                print(f"❌ Error {action} document {doc.get('id', 'unknown')}: {e}")
                return False
        
        # SPOT CHECK - Sample a few records before updating for verification
        spot_check_sample = documents_to_update[:min(5, len(documents_to_update))]
        spot_check_before = {}
        
        print(f"\n🔍 SPOT CHECK - Saving {len(spot_check_sample)} sample records before update...")
        for doc in spot_check_sample:
            # Create a deep copy of the document for comparison
            spot_check_before[doc['id']] = {
                'id': doc.get('id'),
                'type': doc.get('type'),
                'userId': doc.get('userId'),
                'createdAt': doc.get('createdAt'),
                'updatedAt': doc.get('updatedAt'),
                'conversationId': doc.get('conversationId'),
                'role': doc.get('role'),
                'content': doc.get('content'),
                'feedback': doc.get('feedback'),
                'updatedBy': doc.get('updatedBy'),
                'usage_value': doc.get('usage') if 'usage' in doc else None
            }
        
        # Process documents in batches
        updated_count, error_count = await process_in_batches(
            documents_to_update, 
            update_single_document
        )
        
        # SPOT CHECK VERIFICATION - Compare before and after
        print(f"\n🔍 SPOT CHECK VERIFICATION - Retrieving updated records...")
        spot_check_passed = True
        
        for doc_id in spot_check_before.keys():
            try:
                # Retrieve the updated document using query (no partition key needed)
                query = f"SELECT * FROM c WHERE c.id = '{doc_id}'"                items = []
                async for item in container.query_items(query=query, max_item_count=1):
                    items.append(item)
                
                if not items:
                    print(f"   ❌ ERROR: Document {doc_id} not found after update")
                    spot_check_passed = False
                    continue
                
                updated_doc = items[0]
                before = spot_check_before[doc_id]
                
                print(f"\n📋 Spot Check for Document ID: {doc_id}")
                print(f"   Before - Usage field present: {before['usage_value'] is not None}")
                print(f"   After  - Usage field present: {'usage' in updated_doc}")
                
                # Verify all fields remain the same except for usage, updatedAt, and updatedBy
                fields_to_check = ['id', 'type', 'userId', 'conversationId', 'role', 'content', 'feedback', 'createdAt']
                field_check_passed = True
                
                for field in fields_to_check:
                    if before[field] != updated_doc.get(field):
                        print(f"   ❌ MISMATCH - {field}: '{before[field]}' != '{updated_doc.get(field)}'")
                        field_check_passed = False
                        spot_check_passed = False
                
                # Verify usage field changes
                if revert:
                    # In revert mode, usage field should be removed
                    if 'usage' in updated_doc:
                        print(f"   ❌ USAGE FIELD NOT REMOVED - Still present after revert")
                        spot_check_passed = False
                    else:
                        print(f"   ✅ Usage field correctly removed")
                    # Verify updatedAt and updatedBy fields
                    if updated_doc.get('updatedBy') != -1:
                        print(f"   ❌ UPDATED_BY INCORRECT - Expected: -1, Got: {updated_doc.get('updatedBy')}")
                        spot_check_passed = False
                    else:
                        print(f"   ✅ UpdatedBy correctly set to: -1")
                else:
                    # In update mode, check if document already had usage field with values
                    before_usage = before['usage_value']
                    if before_usage is not None:
                        # Document already had usage field - verify values remain unchanged
                        if 'usage' not in updated_doc:
                            print(f"   ❌ USAGE FIELD LOST - Was present before update but missing after")
                            spot_check_passed = False
                        else:
                            after_usage = updated_doc['usage']
                            usage_fields_match = True
                            
                            # Check each usage field
                            for field in ['completion_tokens', 'prompt_tokens', 'total_tokens']:
                                before_val = before_usage.get(field)
                                after_val = after_usage.get(field)
                                
                                if before_val != after_val:
                                    print(f"   ❌ USAGE.{field.upper()} CHANGED - Before: {before_val}, After: {after_val}")
                                    usage_fields_match = False
                                    spot_check_passed = False
                            
                            if usage_fields_match:
                                print(f"   ✅ Usage field values remain unchanged")
                    else:
                        # Document didn't have usage field - should be added with null values
                        expected_usage = {
                            'completion_tokens': None,
                            'prompt_tokens': None,
                            'total_tokens': None
                        }
                        if 'usage' not in updated_doc:
                            print(f"   ❌ USAGE FIELD NOT ADDED - Missing after update")
                            spot_check_passed = False
                        elif updated_doc['usage'] != expected_usage:
                            print(f"   ❌ USAGE FIELD INCORRECT - Expected: {expected_usage}, Got: {updated_doc['usage']}")
                            spot_check_passed = False
                        else:
                            print(f"   ✅ Usage field correctly added with null values")
                
                    # Verify updatedAt and updatedBy fields
                    if updated_doc.get('updatedBy') != UPDATED_BY:
                        print(f"   ❌ UPDATED_BY INCORRECT - Expected: {UPDATED_BY}, Got: {updated_doc.get('updatedBy')}")
                        spot_check_passed = False
                    else:
                        print(f"   ✅ UpdatedBy correctly set to: {UPDATED_BY}")
                
                if field_check_passed:
                    print(f"   ✅ All other fields remain unchanged")
                
            except Exception as e:
                print(f"   ❌ ERROR retrieving updated document {doc_id}: {e}")
                spot_check_passed = False
        
        print(f"\n🔍 SPOT CHECK RESULT: {'✅ PASSED' if spot_check_passed else '❌ FAILED'}")
        if not spot_check_passed:
            print("⚠️  WARNING: Spot check detected issues. Please review the changes carefully.")
        
        # Print summary for actual update
        print("\n" + "="*60)
        if revert:
            print("📊 REVERT SUMMARY")
            print("="*60)
            print(f"✅ Documents reverted: {updated_count}")
            print(f"⏭️  Documents skipped (already reverted): {skipped_count}")
            print(f"❌ Documents with errors: {error_count}")
            print(f"📊 Total processed: {updated_count + skipped_count + error_count}")
        else:
            print("📊 UPDATE SUMMARY")
            print("="*60)
            print(f"✅ Documents updated: {updated_count}")
            print(f"⏭️  Documents skipped (already updated): {skipped_count}")
            print(f"❌ Documents with errors: {error_count}")
            print(f"📊 Total processed: {updated_count + skipped_count + error_count}")


# ===================================================================
# MAIN EXECUTION LOGIC (reusable for other scripts)
# ===================================================================

async def main():
    """Main function to run the update"""
    print("🚀 CosmosDB Data Update Script")
    print("="*60)
    
    # Check for help flag
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help']:
        print(__doc__)
        return 0
    
    # Validate environment and get configuration
    if not validate_environment():
        return 1
    
    endpoint, credential, database_name, container_name = get_configuration()
    
    # Check for revert mode
    revert_mode = False
    if '--revert' in sys.argv:
        revert_mode = True
        print("🔄 REVERT MODE - Will remove usage fields")
    
    # Determine if this is a dry run
    dry_run = True
    if '--execute' in sys.argv or '--run' in sys.argv or '--apply' in sys.argv:
        dry_run = False
        action = "revert changes" if revert_mode else "apply changes"
        print(f"\n⚠️  LIVE MODE - Will {action}!")
        response = input("Are you sure you want to proceed? (y/N): ")
        if response.lower() != 'y':
            print("❌ Operation cancelled")
            return 0
    else:
        mode_text = "revert mode" if revert_mode else "update mode"
        print(f"\n🔍 Running in DRY RUN {mode_text} (use --execute to apply changes)")
    
    cosmos_client = None
    try:
        # Initialize CosmosDB connection
        cosmos_client, container = await create_cosmos_client(
            endpoint, credential, database_name, container_name
        )
        
        # Run the update
        await run_update(container, dry_run, revert_mode)
        
        if dry_run:
            if revert_mode:
                print("\n💡 To apply these reverts, run the script with --revert --execute")
                print("   Example: python pcr-121.py --revert --execute")
            else:
                print("\n💡 To apply these changes, run the script with --execute flag")
                print("   Example: python pcr-121.py --execute")
        
        return 0
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        print(f"❌ Stack trace: {traceback.format_exc()}")
        return 1
    
    finally:
        # Clean up the connection
        if cosmos_client:
            try:
                await cosmos_client.close()
            except Exception as e:
                print(f"⚠️  Warning: Error closing connection: {e}")


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n❌ Operation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)