import os
import json
import re
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import shutil

class NoSQLDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.ensure_database_exists()
    
    def ensure_database_exists(self):
        """Create database directory if it doesn't exist"""
        if not os.path.exists(self.db_path):
            os.makedirs(self.db_path)
    
    def create_container(self, container_name: str) -> bool:
        """Create a new container (folder)"""
        container_path = os.path.join(self.db_path, container_name)
        try:
            os.makedirs(container_path, exist_ok=True)
            return True
        except Exception as e:
            print(f"Error creating container: {e}")
            return False
    
    def delete_container(self, container_name: str) -> bool:
        """Delete a container and all its documents"""
        container_path = os.path.join(self.db_path, container_name)
        try:
            if os.path.exists(container_path):
                shutil.rmtree(container_path)
                return True
            return False
        except Exception as e:
            print(f"Error deleting container: {e}")
            return False
    
    def list_containers(self) -> List[str]:
        """List all containers in the database"""
        try:
            return [d for d in os.listdir(self.db_path) 
                   if os.path.isdir(os.path.join(self.db_path, d))]
        except Exception as e:
            print(f"Error listing containers: {e}")
            return []
    
    def insert_document(self, container_name: str, doc_id: str, document: Dict[str, Any]) -> bool:
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
            return True
        except Exception as e:
            print(f"Error inserting document: {e}")
            return False
    
    def update_document(self, container_name: str, doc_id: str, updates: Dict[str, Any]) -> bool:
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
                return True
            return False
        except Exception as e:
            print(f"Error updating document: {e}")
            return False
    
    def delete_document(self, container_name: str, doc_id: str) -> bool:
        """Delete a document from a container"""
        doc_path = os.path.join(self.db_path, container_name, f"{doc_id}.json")
        try:
            if os.path.exists(doc_path):
                os.remove(doc_path)
                return True
            return False
        except Exception as e:
            print(f"Error deleting document: {e}")
            return False
    
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
    
    def execute_sql_like_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a simplified SQL-like query string
        
        Supported formats:
        - SELECT * FROM container
        - SELECT field1, field2 FROM container WHERE field = 'value'
        - SELECT * FROM container WHERE field > 10 LIMIT 5
        """
        query = query.strip()
        
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


# Example usage and testing
if __name__ == "__main__":
    # Initialize database
    db = NoSQLDatabase("./my_database")
    
    # Create containers
    db.create_container("users")
    db.create_container("products")
    
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
        db.insert_document("users", f"user_{i+1}", user)
    
    # Insert products
    for i, product in enumerate(products):
        db.insert_document("products", f"product_{i+1}", product)
    
    print("=== Database Operations Demo ===\n")
    
    # Basic queries
    print("1. All users:")
    all_users = db.select("users")
    for user in all_users:
        print(f"   {user['name']} ({user['age']}) - {user['city']}")
    
    print("\n2. Users older than 28:")
    older_users = db.select("users", [("age", ">", 28)])
    for user in older_users:
        print(f"   {user['name']} ({user['age']})")
    
    print("\n3. Users from New York:")
    ny_users = db.select("users", [("city", "=", "New York")])
    for user in ny_users:
        print(f"   {user['name']}")
    
    print("\n4. Products under $100:")
    cheap_products = db.select("products", [("price", "<", 100)])
    for product in cheap_products:
        print(f"   {product['name']} - ${product['price']}")
    
    print("\n5. Electronics with stock > 50:")
    electronics = db.select("products", [("category", "=", "Electronics"), ("stock", ">", 50)])
    for product in electronics:
        print(f"   {product['name']} - Stock: {product['stock']}")
    
    print("\n6. First 2 users ordered by age:")
    limited_users = db.select("users", order_by="age", limit=2)
    for user in limited_users:
        print(f"   {user['name']} ({user['age']})")
    
    print("\n7. Only names and ages of users:")
    user_info = db.select("users", fields=["name", "age"])
    for user in user_info:
        print(f"   {user}")
    
    # SQL-like queries
    print("\n=== SQL-like Queries ===\n")
    
    print("8. SQL: SELECT * FROM users WHERE age > 28")
    results = db.execute_sql_like_query("SELECT * FROM users WHERE age > 28")
    for result in results:
        print(f"   {result['name']} ({result['age']})")
    
    print("\n9. SQL: SELECT name, email FROM users WHERE city = 'New York'")
    results = db.execute_sql_like_query("SELECT name, email FROM users WHERE city = 'New York'")
    for result in results:
        print(f"   {result}")
    
    print("\n10. SQL: SELECT * FROM products WHERE price < 500 LIMIT 2")
    results = db.execute_sql_like_query("SELECT * FROM products WHERE price < 500 LIMIT 2")
    for result in results:
        print(f"   {result['name']} - ${result['price']}")
    
    # Document operations
    print("\n=== Document Operations ===\n")
    
    print("11. Get specific user:")
    user = db.get_document("users", "user_1")
    if user:
        print(f"   Found: {user['name']}")
    
    print("\n12. Update user:")
    success = db.update_document("users", "user_1", {"age": 31, "city": "Boston"})
    if success:
        updated_user = db.get_document("users", "user_1")
        print(f"   Updated: {updated_user['name']} now {updated_user['age']} in {updated_user['city']}")
    
    print("\n13. Count operations:")
    total_users = db.count("users")
    young_users = db.count("users", [("age", "<", 30)])
    print(f"   Total users: {total_users}")
    print(f"   Users under 30: {young_users}")
    
    print("\n14. List containers:")
    containers = db.list_containers()
    print(f"   Containers: {containers}")