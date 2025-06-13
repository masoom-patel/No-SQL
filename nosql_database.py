import os
import json
import re
import shutil
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from pathlib import Path
import tabulate

class NoSQLDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.ensure_database_exists()
        self.query_history = []
        self.current_container = None
    
    def ensure_database_exists(self):
        """Create database directory if it doesn't exist"""
        if not os.path.exists(self.db_path):
            os.makedirs(self.db_path)
    
    def create_container(self, container_name: str) -> Dict[str, Any]:
        """Create a new container (folder)"""
        container_path = os.path.join(self.db_path, container_name)
        try:
            os.makedirs(container_path, exist_ok=True)
            return {"success": True, "message": f"Container '{container_name}' created successfully."}
        except Exception as e:
            return {"success": False, "message": f"Error creating container: {e}"}
    
    def delete_container(self, container_name: str) -> Dict[str, Any]:
        """Delete a container and all its documents"""
        container_path = os.path.join(self.db_path, container_name)
        try:
            if os.path.exists(container_path):
                shutil.rmtree(container_path)
                # Reset current container if it was deleted
                if self.current_container == container_name:
                    self.current_container = None
                return {"success": True, "message": f"Container '{container_name}' deleted successfully."}
            return {"success": False, "message": f"Container '{container_name}' not found."}
        except Exception as e:
            return {"success": False, "message": f"Error deleting container: {e}"}
    
    def list_containers(self) -> List[str]:
        """List all containers in the database"""
        try:
            return [d for d in os.listdir(self.db_path) 
                   if os.path.isdir(os.path.join(self.db_path, d))]
        except Exception as e:
            print(f"Error listing containers: {e}")
            return []
    
    def get_containers_info(self) -> List[Dict[str, Any]]:
        """Get detailed information about all containers"""
        containers = []
        for container_name in self.list_containers():
            doc_count = len(self.get_all_documents(container_name))
            containers.append({
                "name": container_name,
                "document_count": doc_count
            })
        return containers
    
    def use_container(self, container_name: str) -> Dict[str, Any]:
        """Set current working container"""
        if container_name in self.list_containers():
            self.current_container = container_name
            return {"success": True, "message": f"Now using container: {container_name}"}
        else:
            return {"success": False, "message": f"Container '{container_name}' not found."}
    
    def describe_container(self, container_name: str = None) -> Dict[str, Any]:
        """Analyze and describe container schema"""
        if not container_name:
            container_name = self.current_container
        
        if not container_name:
            return {"success": False, "message": "Please specify a container."}
        
        documents = self.get_all_documents(container_name)
        if not documents:
            return {
                "success": True, 
                "container": container_name,
                "document_count": 0,
                "message": f"Container '{container_name}' is empty.",
                "schema": {}
            }
        
        # Analyze schema
        fields = {}
        sample_doc = documents[0] if documents else None
        
        for doc in documents:
            for field, value in doc.items():
                if field not in fields:
                    fields[field] = {'type': type(value).__name__, 'count': 0}
                fields[field]['count'] += 1
        
        # Calculate percentages
        schema_info = {}
        for field, info in fields.items():
            percentage = (info['count'] / len(documents)) * 100
            schema_info[field] = {
                'type': info['type'],
                'count': info['count'],
                'percentage': percentage
            }
        
        return {
            "success": True,
            "container": container_name,
            "document_count": len(documents),
            "schema": schema_info,
            "sample_document": sample_doc
        }
    
    def insert_document(self, container_name: str, doc_id: str, document: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a document into a container"""
        container_path = os.path.join(self.db_path, container_name)
        if not os.path.exists(container_path):
            self.create_container(container_name)
        
        # Add metadata
        document['_id'] = doc_id
        document['_created_at'] = datetime.now().isoformat()
        document['_updated_at'] = datetime.now().isoformat()
        
        doc_path = os.path.join(container_path, f"{doc_id}.json")
        try:
            with open(doc_path, 'w') as f:
                json.dump(document, f, indent=2)
            return {"success": True, "message": f"Document '{doc_id}' inserted successfully."}
        except Exception as e:
            return {"success": False, "message": f"Error inserting document: {e}"}
    
    def update_document(self, container_name: str, doc_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update a document in a container"""
        doc_path = os.path.join(self.db_path, container_name, f"{doc_id}.json")
        try:
            if os.path.exists(doc_path):
                with open(doc_path, 'r') as f:
                    document = json.load(f)
                
                # Update fields
                document.update(updates)
                document['_updated_at'] = datetime.now().isoformat()
                
                with open(doc_path, 'w') as f:
                    json.dump(document, f, indent=2)
                return {"success": True, "message": f"Document '{doc_id}' updated successfully."}
            return {"success": False, "message": f"Document '{doc_id}' not found."}
        except Exception as e:
            return {"success": False, "message": f"Error updating document: {e}"}
    
    def delete_document(self, container_name: str, doc_id: str) -> Dict[str, Any]:
        """Delete a document from a container"""
        doc_path = os.path.join(self.db_path, container_name, f"{doc_id}.json")
        try:
            if os.path.exists(doc_path):
                os.remove(doc_path)
                return {"success": True, "message": f"Document '{doc_id}' deleted successfully."}
            return {"success": False, "message": f"Document '{doc_id}' not found."}
        except Exception as e:
            return {"success": False, "message": f"Error deleting document: {e}"}
    
    def get_document(self, container_name: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific document by ID"""
        doc_path = os.path.join(self.db_path, container_name, f"{doc_id}.json")
        try:
            if os.path.exists(doc_path):
                with open(doc_path, 'r') as f:
                    return json.load(f)
            return None
        except Exception as e:
            print(f"Error getting document: {e}")
            return None
    
    def get_all_documents(self, container_name: str) -> List[Dict[str, Any]]:
        """Get all documents from a container"""
        container_path = os.path.join(self.db_path, container_name)
        documents = []
        
        if not os.path.exists(container_path):
            return documents
        
        try:
            for filename in os.listdir(container_path):
                if filename.endswith('.json'):
                    doc_path = os.path.join(container_path, filename)
                    with open(doc_path, 'r') as f:
                        documents.append(json.load(f))
            return documents
        except Exception as e:
            print(f"Error getting documents: {e}")
            return []
    
    def _match_condition(self, document: Dict[str, Any], field: str, operator: str, value: Any) -> bool:
        """Check if a document matches a condition"""
        if field not in document:
            return False
        
        doc_value = document[field]
        
        if operator == '=':
            return doc_value == value
        elif operator == '!=':
            return doc_value != value
        elif operator == '>':
            return doc_value > value
        elif operator == '<':
            return doc_value < value
        elif operator == '>=':
            return doc_value >= value
        elif operator == '<=':
            return doc_value <= value
        elif operator == 'LIKE':
            return str(value).lower() in str(doc_value).lower()
        elif operator == 'IN':
            return doc_value in value
        
        return False
    
    def select(self, container_name: str, where_conditions: List[tuple] = None, 
               fields: List[str] = None, limit: int = None, order_by: str = None) -> List[Dict[str, Any]]:
        """
        SQL-like SELECT query
        
        Args:
            container_name: Container to query
            where_conditions: List of tuples (field, operator, value)
            fields: List of fields to return (None for all)
            limit: Maximum number of results
            order_by: Field to sort by
        """
        documents = self.get_all_documents(container_name)
        
        # Apply WHERE conditions
        if where_conditions:
            filtered_docs = []
            for doc in documents:
                match = True
                for field, operator, value in where_conditions:
                    if not self._match_condition(doc, field, operator, value):
                        match = False
                        break
                if match:
                    filtered_docs.append(doc)
            documents = filtered_docs
        
        # Apply field selection
        if fields:
            selected_docs = []
            for doc in documents:
                selected_doc = {}
                for field in fields:
                    if field in doc:
                        selected_doc[field] = doc[field]
                selected_docs.append(selected_doc)
            documents = selected_docs
        
        # Apply ordering
        if order_by:
            try:
                documents.sort(key=lambda x: x.get(order_by, ''))
            except Exception as e:
                print(f"Error sorting by {order_by}: {e}")
        
        # Apply limit
        if limit:
            documents = documents[:limit]
        
        return documents
    
    def count(self, container_name: str, where_conditions: List[tuple] = None) -> int:
        """Count documents matching conditions"""
        return len(self.select(container_name, where_conditions))
    
    def execute_sql_like_query(self, query: str) -> Dict[str, Any]:
        """
        Execute a simplified SQL-like query string and return structured result
        
        Supported formats:
        - SELECT * FROM container
        - SELECT field1, field2 FROM container WHERE field = 'value'
        - SELECT * FROM container WHERE field > 10 LIMIT 5
        - INSERT INTO container VALUES ('doc_id', '{"field": "value"}')
        """
        try:
            # Add to history
            self._add_to_history(query)
            
            query = query.strip()
            
            # Handle INSERT queries
            if query.upper().startswith('INSERT'):
                return self._execute_insert_sql(query)
            
            # Handle SELECT queries
            if query.upper().startswith('SELECT'):
                results = self._execute_select_sql(query)
                return {
                    "success": True,
                    "type": "select",
                    "results": results,
                    "count": len(results)
                }
            
            return {"success": False, "message": "Unsupported query type. Only SELECT and INSERT are supported."}
            
        except Exception as e:
            return {"success": False, "message": f"Error executing query: {str(e)}"}
    
    def _execute_select_sql(self, query: str) -> List[Dict[str, Any]]:
        """Execute SELECT SQL query"""
        # Parse SELECT
        select_match = re.match(r'SELECT\s+(.*?)\s+FROM\s+(\w+)', query, re.IGNORECASE)
        if not select_match:
            raise ValueError("Invalid SELECT query format")
        
        fields_str = select_match.group(1).strip()
        container_name = select_match.group(2).strip()
        
        # Parse fields
        fields = None
        if fields_str != '*':
            fields = [f.strip() for f in fields_str.split(',')]
        
        # Parse WHERE conditions
        where_conditions = []
        where_match = re.search(r'WHERE\s+(.*?)(?:\s+LIMIT|\s+ORDER\s+BY|$)', query, re.IGNORECASE)
        if where_match:
            where_str = where_match.group(1).strip()
            # Simple parsing for basic conditions
            condition_parts = re.split(r'\s+AND\s+', where_str, flags=re.IGNORECASE)
            for part in condition_parts:
                # Match field operator value
                cond_match = re.match(r'(\w+)\s*(=|!=|>|<|>=|<=|LIKE|IN)\s*(.+)', part.strip())
                if cond_match:
                    field = cond_match.group(1)
                    operator = cond_match.group(2)
                    value_str = cond_match.group(3).strip()
                    
                    # Parse value
                    if value_str.startswith("'") and value_str.endswith("'"):
                        value = value_str[1:-1]
                    elif value_str.startswith('"') and value_str.endswith('"'):
                        value = value_str[1:-1]
                    else:
                        try:
                            value = int(value_str)
                        except ValueError:
                            try:
                                value = float(value_str)
                            except ValueError:
                                value = value_str
                    
                    where_conditions.append((field, operator, value))
        
        # Parse LIMIT
        limit = None
        limit_match = re.search(r'LIMIT\s+(\d+)', query, re.IGNORECASE)
        if limit_match:
            limit = int(limit_match.group(1))
        
        # Parse ORDER BY
        order_by = None
        order_match = re.search(r'ORDER\s+BY\s+(\w+)', query, re.IGNORECASE)
        if order_match:
            order_by = order_match.group(1)
        
        return self.select(container_name, where_conditions, fields, limit, order_by)
    
    def _execute_insert_sql(self, query: str) -> Dict[str, Any]:
        """Execute INSERT SQL query"""
        # Extract container name
        container_match = re.search(r'INSERT\s+INTO\s+(\w+)', query, re.IGNORECASE)
        if not container_match:
            return {"success": False, "message": "Invalid INSERT syntax. Missing container name."}
        
        container_name = container_match.group(1)
        
        # Extract VALUES content
        values_match = re.search(r'VALUES\s*\((.*)\)', query, re.IGNORECASE | re.DOTALL)
        if not values_match:
            return {"success": False, "message": "Invalid INSERT syntax. Missing VALUES clause."}
        
        values_content = values_match.group(1).strip()
        
        # Parse doc_id and JSON data
        doc_id = None
        json_str = None
        
        in_quotes = False
        quote_char = None
        paren_depth = 0
        brace_depth = 0
        comma_pos = -1
        
        for i, char in enumerate(values_content):
            if char in ['"', "'"] and (i == 0 or values_content[i-1] != '\\'):
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char:
                    in_quotes = False
                    quote_char = None
            elif not in_quotes:
                if char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
                elif char == '{':
                    brace_depth += 1
                elif char == '}':
                    brace_depth -= 1
                elif char == ',' and paren_depth == 0 and brace_depth == 0:
                    comma_pos = i
                    break
        
        if comma_pos == -1:
            return {"success": False, "message": "Invalid INSERT syntax. Expected format: INSERT INTO container VALUES ('doc_id', 'json_data')"}
        
        # Extract doc_id and json parts
        doc_id_part = values_content[:comma_pos].strip()
        json_part = values_content[comma_pos + 1:].strip()
        
        # Clean up quotes from doc_id
        if (doc_id_part.startswith('"') and doc_id_part.endswith('"')) or \
           (doc_id_part.startswith("'") and doc_id_part.endswith("'")):
            doc_id = doc_id_part[1:-1]
        else:
            doc_id = doc_id_part
        
        # Clean up quotes from json if they exist
        if (json_part.startswith('"') and json_part.endswith('"')) or \
           (json_part.startswith("'") and json_part.endswith("'")):
            json_str = json_part[1:-1]
            # Unescape quotes
            json_str = json_str.replace('\\"', '"').replace("\\'", "'")
        else:
            json_str = json_part
        
        # Parse JSON
        try:
            document = json.loads(json_str)
        except json.JSONDecodeError as e:
            return {"success": False, "message": f"Invalid JSON format: {str(e)}"}
        
        return self.insert_document(container_name, doc_id, document)
    
    def format_results_as_table(self, results: List[Dict[str, Any]], title: str = "Query Results") -> str:
        """Format query results as a table"""
        if not results:
            return "No results found."
        
        # Get all unique keys for headers
        headers = set()
        for result in results:
            headers.update(result.keys())
        headers = sorted(list(headers))
        
        # Prepare data for tabulation
        table_data = []
        for result in results:
            row = [str(result.get(header, '')) for header in headers]
            table_data.append(row)
        
        # Create table
        table = tabulate.tabulate(table_data, headers=headers, tablefmt='grid')
        
        output = f"\n{title}\n"
        output += f"Found {len(results)} record(s)\n\n"
        output += table
        
        return output
    
    def export_data(self, target: str, path: str) -> Dict[str, Any]:
        """
        Export containers to folder or single file
        
        Args:
            target: 'all' or container_name
            path: folder path or file path
        """
        try:
            path_obj = Path(path)
            
            if target.lower() == 'all':
                # Export all containers
                if path_obj.suffix == '.json':
                    return {"success": False, "message": "Cannot export all containers to a single JSON file. Use a folder path."}
                return self._export_all_to_folder(path)
            else:
                # Export specific container
                if path_obj.suffix == '.json':
                    # Export to single file 
                    return self._export_container_to_file(target, path)
                else:
                    # Export to folder
                    return self._export_container_to_folder(target, path)
        except Exception as e:
            return {"success": False, "message": f"Export failed: {str(e)}"}
    
    def _export_all_to_folder(self, folder_path: str) -> Dict[str, Any]:
        """Export all containers to separate JSON files in a folder"""
        # Create folder if it doesn't exist
        folder = Path(folder_path)
        folder.mkdir(parents=True, exist_ok=True)
        
        containers = self.list_containers()
        if not containers:
            return {"success": False, "message": "No containers to export."}
        
        exported_count = 0
        total_docs = 0
        
        for container_name in containers:
            documents = self.get_all_documents(container_name)
            if documents:
                filename = folder / f"{container_name}.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(documents, f, indent=2, ensure_ascii=False)
                exported_count += 1
                total_docs += len(documents)
        
        return {
            "success": True,
            "message": f"Exported {exported_count} containers ({total_docs} documents) to folder: {folder_path}"
        }
    
    def _export_container_to_folder(self, container_name: str, folder_path: str) -> Dict[str, Any]:
        """Export single container to a folder"""
        # Create folder if it doesn't exist
        folder = Path(folder_path)
        folder.mkdir(parents=True, exist_ok=True)
        
        documents = self.get_all_documents(container_name)
        if not documents:
            return {"success": False, "message": f"Container '{container_name}' is empty."}
        
        filename = folder / f"{container_name}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(documents, f, indent=2, ensure_ascii=False)
        
        return {
            "success": True,
            "message": f"Exported container '{container_name}' ({len(documents)} documents) to: {filename}"
        }
    
    def _export_container_to_file(self, container_name: str, filename: str) -> Dict[str, Any]:
        """Export single container to a specific file"""
        documents = self.get_all_documents(container_name)
        if not documents:
            return {"success": False, "message": f"Container '{container_name}' is empty."}
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(documents, f, indent=2, ensure_ascii=False)
        
        return {
            "success": True,
            "message": f"Exported {len(documents)} documents from '{container_name}' to {filename}"
        }
    
    def import_data(self, source: str, container_name: str = None) -> Dict[str, Any]:
        """
        Import data from folder or single file
        
        Args:
            source: folder path or file path
            container_name: specific container name (for single file import)
        """
        try:
            source_path = Path(source)
            
            if source_path.is_dir():
                # Import from folder
                return self._import_from_folder(source)
            elif source_path.is_file() and source_path.suffix == '.json':
                # Import single file
                if not container_name:
                    container_name = source_path.stem
                return self._import_file_to_container(container_name, source)
            else:
                return {"success": False, "message": f"Invalid source: {source}"}
        except Exception as e:
            return {"success": False, "message": f"Import failed: {str(e)}"}
    
    def _import_from_folder(self, folder_path: str) -> Dict[str, Any]:
        """Import all JSON files from a folder"""
        folder = Path(folder_path)
        if not folder.exists():
            return {"success": False, "message": f"Folder not found: {folder_path}"}
        
        if not folder.is_dir():
            return {"success": False, "message": f"Path is not a directory: {folder_path}"}
        
        # Find all JSON files
        json_files = list(folder.glob("*.json"))
        if not json_files:
            return {"success": False, "message": f"No JSON files found in folder: {folder_path}"}
        
        total_imported = 0
        total_files = 0
        results = []
        
        for json_file in json_files:
            container_name = json_file.stem  # filename without extension
            result = self._import_file_to_container(container_name, str(json_file))
            results.append(f"{container_name}: {result['message']}")
            
            # Extract number of imported documents
            if result['success'] and 'imported' in result:
                total_imported += result['imported']
            total_files += 1
        
        return {
            "success": True,
            "message": f"Folder import completed ({total_files} files processed)",
            "details": results,
            "total_imported": total_imported
        }
    
    def _import_file_to_container(self, container_name: str, filename: str) -> Dict[str, Any]:
        """Import single file to specific container"""
        if not Path(filename).exists():
            return {"success": False, "message": f"File not found: {filename}"}
        
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            return {"success": False, "message": f"Invalid JSON in file {filename}: {str(e)}"}
        
        # Handle different JSON structures
        documents = []
        if isinstance(data, list):
            documents = data
        elif isinstance(data, dict):
            # If it's a single document, wrap it in a list
            documents = [data]
        else:
            return {"success": False, "message": "Invalid JSON format. Expected array or object."}
        
        # Create container if it doesn't exist
        if container_name not in self.list_containers():
            self.create_container(container_name)
        
        imported = 0
        failed = 0
        
        for i, doc in enumerate(documents):
            if not isinstance(doc, dict):
                failed += 1
                continue
            
            # Generate doc_id if not present
            doc_id = doc.get('_id') or doc.get('id') or f"imported_{i+1}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Remove _id from document data if present
            doc_data = {k: v for k, v in doc.items() if k not in ['_id', 'id']}
            if not doc_data:
                doc_data = doc  # Keep original if no other fields
            
            result = self.insert_document(container_name, str(doc_id), doc_data)
            if result['success']:
                imported += 1
            else:
                failed += 1
        
        message = f"Imported {imported}/{len(documents)} documents"
        if failed > 0:
            message += f" ({failed} failed)"
        
        return {
            "success": True,
            "message": message,
            "imported": imported,
            "failed": failed
        }
    
    def backup_database(self, backup_path: str = None) -> Dict[str, Any]:
        """Backup entire database"""
        if not backup_path:
            backup_path = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            shutil.copytree(self.db_path, backup_path)
            return {"success": True, "message": f"Database backed up to: {backup_path}"}
        except Exception as e:
            return {"success": False, "message": f"Backup failed: {str(e)}"}
    
    def _add_to_history(self, query: str):
        """Add query to history"""
        self.query_history.append({
            'query': query,
            'timestamp': datetime.now().isoformat(),
            'container': self.current_container
        })
    
    def get_query_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get query history"""
        return self.query_history[-limit:] if limit else self.query_history
    
    def clear_history(self) -> Dict[str, Any]:
        """Clear query history"""
        self.query_history.clear()
        return {"success": True, "message": "Query history cleared."}


# Example usage and testing
if __name__ == "__main__":
    # Initialize database
    db = NoSQLDatabase("./my_database")
    
    # Create containers
    print(db.create_container("users"))
    print(db.create_container("products"))
    
    # Insert sample data
    users = [
        {"name": "Alice", "age": 30, "email": "alice@example.com", "city": "New York"},
        {"name": "Bob", "age": 25, "email": "bob@example.com", "city": "Los Angeles"},
        {"name": "Charlie", "age": 35, "email": "charlie@example.com", "city": "Chicago"},
        {"name": "Diana", "age": 28, "email": "diana@example.com", "city": "New York"}
    ]
    
    products = [
        {"name": "Laptop", "price": 999.99, "category": "Electronics", "stock": 50},
        {"name": "Phone", "price": 699.99, "category": "Electronics", "stock": 100},
        {"name": "Book", "price": 19.99, "category": "Books", "stock": 200},
        {"name": "Chair", "price": 149.99, "category": "Furniture", "stock": 25}
    ]
    
    # Insert users
    for i, user in enumerate(users):
        result = db.insert_document("users", f"user_{i+1}", user)
        print(result['message'])
    
    # Insert products
    for i, product in enumerate(products):
        result = db.insert_document("products", f"product_{i+1}", product)
        print(result['message'])
    
    print("\n=== Database Operations Demo ===\n")
    
    # Test various operations
    print("1. Container info:")
    containers_info = db.get_containers_info()
    for container in containers_info:
        print(f"   {container['name']}: {container['document_count']} documents")
    
    print("\n2. Describe users container:")
    users_desc = db.describe_container("users")
    if users_desc["success"]:
        print(f"   Container: {users_desc['container']}")
        print(f"   Documents: {users_desc['document_count']}")
        print("   Schema:")
        for field, info in users_desc['schema'].items():
            print(f"     {field}: {info['type']} ({info['percentage']:.1f}%)")
    
    print("\n3. SQL-like queries:")
    
    # Select all users
    print("\n   SELECT * FROM users:")
    result = db.execute_sql_like_query("SELECT * FROM users")
    if result["success"]:
        print(db.format_results_as_table(result["results"], "All Users"))
    
    # Select specific fields
    print("\n   SELECT name, age FROM users:")
    result = db.execute_sql_like_query("SELECT name, age FROM users")
    if result["success"]:
        print(db.format_results_as_table(result["results"], "Users - Name & Age"))
    
    # WHERE clause
    print("\n   SELECT * FROM users WHERE age > 28:")
    result = db.execute_sql_like_query("SELECT * FROM users WHERE age > 28")
    if result["success"]:
        print(db.format_results_as_table(result["results"], "Users older than 28"))
    
    # LIKE operator
    print("\n   SELECT * FROM users WHERE city LIKE 'New':")
    result = db.execute_sql_like_query("SELECT * FROM users WHERE city LIKE 'New'")
    if result["success"]:
        print(db.format_results_as_table(result["results"], "Users in cities containing 'New'"))
    
    # Products queries
    print("\n   SELECT * FROM products WHERE price < 200:")
    result = db.execute_sql_like_query("SELECT * FROM products WHERE price < 200")
    if result["success"]:
        print(db.format_results_as_table(result["results"], "Products under $200"))
    
    # ORDER BY and LIMIT
    print("\n   SELECT name, price FROM products ORDER BY price LIMIT 2:")
    result = db.execute_sql_like_query("SELECT name, price FROM products ORDER BY price LIMIT 2")
    if result["success"]:
        print(db.format_results_as_table(result["results"], "Cheapest 2 Products"))
    
    print("\n4. Direct API usage:")
    
    # Use container
    print("\n   Setting current container to 'users':")
    print(db.use_container("users"))
    
    # Count documents
    print(f"\n   Count all users: {db.count('users')}")
    print(f"   Count users in New York: {db.count('users', [('city', '=', 'New York')])}")
    
    # Get specific document
    print("\n   Get user_1:")
    user1 = db.get_document("users", "user_1")
    if user1:
        print(f"     Name: {user1['name']}, Age: {user1['age']}, Email: {user1['email']}")
    
    # Update document
    print("\n   Update user_1 age to 31:")
    update_result = db.update_document("users", "user_1", {"age": 31})
    print(f"     {update_result['message']}")
    
    # Verify update
    user1_updated = db.get_document("users", "user_1")
    if user1_updated:
        print(f"     Updated age: {user1_updated['age']}")
    
    print("\n5. Advanced queries:")
    
    # Multiple conditions
    print("\n   Users in New York with age >= 30:")
    ny_users = db.select("users", [("city", "=", "New York"), ("age", ">=", 30)])
    print(db.format_results_as_table(ny_users, "NY Users 30+"))
    
    # Complex product query
    print("\n   Electronics with stock > 50:")
    electronics = db.select("products", [("category", "=", "Electronics"), ("stock", ">", 50)])
    print(db.format_results_as_table(electronics, "Popular Electronics"))
    
    print("\n6. Data management:")
    
    # Export data
    print("\n   Exporting users container:")
    export_result = db.export_data("users", "./exports/users_export.json")
    print(f"     {export_result['message']}")
    
    # Backup database
    print("\n   Creating database backup:")
    backup_result = db.backup_database("./backup_demo")
    print(f"     {backup_result['message']}")
    
    print("\n7. Query history:")
    history = db.get_query_history(5)
    print(f"\n   Last {len(history)} queries:")
    for i, query in enumerate(history, 1):
        print(f"     {i}. {query['query']}")
    
    print("\n8. Testing INSERT via SQL:")
    
    # Insert new user via SQL
    insert_query = '''INSERT INTO users VALUES ('user_5', '{"name": "Eve", "age": 26, "email": "eve@example.com", "city": "Miami"}')'''
    insert_result = db.execute_sql_like_query(insert_query)
    print(f"\n   Insert result: {insert_result['message']}")
    
    # Verify insertion
    print("\n   All users after insertion:")
    all_users = db.execute_sql_like_query("SELECT name, age, city FROM users")
    if all_users["success"]:
        print(db.format_results_as_table(all_users["results"], "All Users"))
    
    print("\n9. Cleanup test:")
    
    # Delete a document
    delete_result = db.delete_document("users", "user_5")
    print(f"\n   Delete user_5: {delete_result['message']}")
    
    # Final count
    final_count = db.count("users")
    print(f"   Final user count: {final_count}")
    
    print("\n=== Demo Complete ===")
    print(f"Database location: {db.db_path}")
    print(f"Containers: {', '.join(db.list_containers())}")
    print("Check the database folder to see the JSON files created!")
