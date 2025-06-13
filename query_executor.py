import os
import sys
import json
import argparse
import readline
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
import tabulate
from colorama import init, Fore, Style, Back
import re

try:
    from nosql_database import NoSQLDatabase
except ImportError:
    print("Error: nosql_database.py not found. Please save the database code as 'nosql_database.py'")
    sys.exit(1)

class QueryExecutor:
    def __init__(self, db_path: str):
        self.db = NoSQLDatabase(db_path)
        self.db_path = db_path
        self.history = []
        self.current_container = None
        
        # Initialize colorama for cross-platform colored output
        init()
        
        # Command mappings
        self.commands = {
            'help': self.show_help,
            'exit': self.exit_executor,
            'quit': self.exit_executor,
            'show': self.show_command,
            'use': self.use_container,
            'describe': self.describe_container,
            'insert': self.insert_command,
            'update': self.update_command,
            'delete': self.delete_command,
            'create': self.create_command,
            'drop': self.drop_command,
            'count': self.count_command,
            'clear': self.clear_screen,
            'history': self.show_history,
            'export': self.export_command,
            'import': self.import_command,
            'backup': self.backup_command
        }
    
    def colorize(self, text: str, color: str) -> str:
        """Add color to text"""
        colors = {
            'red': Fore.RED,
            'green': Fore.GREEN,
            'yellow': Fore.YELLOW,
            'blue': Fore.BLUE,
            'magenta': Fore.MAGENTA,
            'cyan': Fore.CYAN,
            'white': Fore.WHITE,
            'bright_red': Fore.LIGHTRED_EX,
            'bright_green': Fore.LIGHTGREEN_EX,
            'bright_yellow': Fore.LIGHTYELLOW_EX,
            'bright_blue': Fore.LIGHTBLUE_EX,
            'bright_magenta': Fore.LIGHTMAGENTA_EX,
            'bright_cyan': Fore.LIGHTCYAN_EX
        }
        return f"{colors.get(color, '')}{text}{Style.RESET_ALL}"
    
    def print_banner(self):
        """Print application banner"""
        banner = """
╔═══════════════════════════════════════════════════════════════╗
║                    NoSQL Database Query Executor              ║
║                         Version 1.0                           ║
╚═══════════════════════════════════════════════════════════════╝
        """
        print(self.colorize(banner, 'cyan'))
        print(self.colorize(f"Database Path: {self.db_path}", 'yellow'))
        print(self.colorize("Type 'help' for available commands", 'green'))
        print("-" * 63)
    
    def format_results(self, results: List[Dict[str, Any]], title: str = "Query Results") -> str:
        """Format query results as a table"""
        if not results:
            return self.colorize("No results found.", 'yellow')
        
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
        
        output = f"\n{self.colorize(title, 'bright_cyan')}\n"
        output += f"{self.colorize(f'Found {len(results)} record(s)', 'green')}\n\n"
        output += table
        
        return output
    
    def execute_query(self, query: str) -> str:
        """Execute a query and return formatted results"""
        try:
            # Add to history
            self.history.append({
                'query': query,
                'timestamp': datetime.now().isoformat(),
                'container': self.current_container
            })
            
            # Handle SQL queries
            if query.upper().startswith('SELECT'):
                results = self.db.execute_sql_like_query(query)
                return self.format_results(results, f"SELECT Query Results")
            
            # Handle INSERT SQL queries
            if query.upper().startswith('INSERT'):
                return self.execute_insert_sql(query)
            
            # Handle command queries
            parts = query.strip().split()
            if not parts:
                return ""
            
            command = parts[0].lower()
            
            if command in self.commands:
                return self.commands[command](parts[1:])
            else:
                return self.colorize(f"Unknown command: {command}. Type 'help' for available commands.", 'red')
        
        except Exception as e:
            return self.colorize(f"Error executing query: {str(e)}", 'red')
    
    def execute_insert_sql(self, query: str) -> str:
        """Execute SQL INSERT statement"""
        try:
         
            
            # Extract container name
            container_match = re.search(r'INSERT\s+INTO\s+(\w+)', query, re.IGNORECASE)
            if not container_match:
                return self.colorize("Invalid INSERT syntax. Missing container name.", 'red')
            
            container_name = container_match.group(1)
            
            # Extract VALUES content
            values_match = re.search(r'VALUES\s*\((.*)\)', query, re.IGNORECASE | re.DOTALL)
            if not values_match:
                return self.colorize("Invalid INSERT syntax. Missing VALUES clause.", 'red')
            
            values_content = values_match.group(1).strip()
            
            
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
                return self.colorize("Invalid INSERT syntax. Expected format: INSERT INTO container VALUES ('doc_id', 'json_data')", 'red')
            
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
                
                escaped_double = '\\"'
                escaped_single = "\\'"
                json_str = json_str.replace(escaped_double, '"').replace(escaped_single, "'")
            else:
                json_str = json_part
            
            # Parse JSON
            try:
                document = json.loads(json_str)
            except json.JSONDecodeError as e:
                error_msg = f"Invalid JSON format: {str(e)}\nJSON: {json_str}"
                return self.colorize(error_msg, 'red')
            
            if self.db.insert_document(container_name, doc_id, document):
                return self.colorize(f"Document '{doc_id}' inserted into '{container_name}' successfully.", 'green')
            else:
                return self.colorize(f"Failed to insert document '{doc_id}'.", 'red')
                
        except Exception as e:
            return self.colorize(f"Error in INSERT statement: {str(e)}", 'red')
    
    def show_help(self, args: List[str]) -> str:
        """Show help information"""
        help_text = f"""
{self.colorize('Available Commands:', 'bright_cyan')}

{self.colorize('SQL Queries:', 'bright_yellow')}
  SELECT * FROM container_name
  SELECT field1, field2 FROM container_name WHERE condition
  SELECT * FROM container_name WHERE field = 'value' LIMIT 10
  SELECT * FROM container_name WHERE field > 10 ORDER BY field
  INSERT INTO container_name VALUES ('doc_id', '{{"field": "value"}}')

{self.colorize('Database Commands:', 'bright_yellow')}
  help                           - Show this help
  show containers               - List all containers
  show documents [container]    - List documents in container
  use container_name            - Switch to container
  describe container_name       - Show container schema
  
{self.colorize('Data Manipulation:', 'bright_yellow')}
  create container name         - Create new container
  drop container name           - Delete container
  insert container_name doc_id '{{"field": "value"}}'
  update container_name doc_id '{{"field": "new_value"}}'
  delete container_name doc_id  - Delete document
  count container_name [WHERE condition]
  
{self.colorize('Import/Export Commands:', 'bright_yellow')}
  export [all|container_name] folder_path    - Export to folder
    export all /path/to/export              - Export all containers
    export users /path/to/export            - Export specific container
    export container_name file.json         - Export to single file
  
  import folder_path                         - Import all JSONs from folder
    import /path/to/import                  - Import all JSON files
    import container_name file.json         - Import single file to container
  
{self.colorize('Utility Commands:', 'bright_yellow')}
  clear                         - Clear screen
  history                       - Show query history  
  backup [path]                 - Backup entire database
  exit/quit                     - Exit the executor

{self.colorize('Examples:', 'bright_green')}
  SELECT * FROM users WHERE age > 25
  SELECT name, email FROM users WHERE city = 'New York'
  insert users user_123 '{{"name": "John", "age": 30}}'
  count products WHERE price > 100
  export all ./data_backup
  import ./data_import
        """
        return help_text
    
    def show_command(self, args: List[str]) -> str:
        """Handle show commands"""
        if not args:
            return self.colorize("Usage: show [containers|documents]", 'yellow')
        
        if args[0].lower() == 'containers':
            containers = self.db.list_containers()
            if containers:
                output = f"{self.colorize('Containers:', 'bright_cyan')}\n"
                for i, container in enumerate(containers, 1):
                    count = len(self.db.get_all_documents(container))
                    output += f"  {i}. {self.colorize(container, 'green')} ({count} documents)\n"
                return output
            else:
                return self.colorize("No containers found.", 'yellow')
        
        elif args[0].lower() == 'documents':
            container = args[1] if len(args) > 1 else self.current_container
            if not container:
                return self.colorize("Please specify a container or use 'use container_name'", 'yellow')
            
            documents = self.db.get_all_documents(container)
            if documents:
                return self.format_results(documents, f"Documents in '{container}'")
            else:
                return self.colorize(f"No documents found in container '{container}'.", 'yellow')
        
        else:
            return self.colorize("Usage: show [containers|documents]", 'yellow')
    
    def use_container(self, args: List[str]) -> str:
        """Switch to a container"""
        if not args:
            return self.colorize("Usage: use container_name", 'yellow')
        
        container = args[0]
        if container in self.db.list_containers():
            self.current_container = container
            return self.colorize(f"Now using container: {container}", 'green')
        else:
            return self.colorize(f"Container '{container}' not found.", 'red')
    
    def describe_container(self, args: List[str]) -> str:
        """Describe container schema"""
        if not args:
            container = self.current_container
        else:
            container = args[0]
        
        if not container:
            return self.colorize("Please specify a container.", 'yellow')
        
        documents = self.db.get_all_documents(container)
        if not documents:
            return self.colorize(f"Container '{container}' is empty.", 'yellow')
        
        # Analyze schema
        fields = {}
        sample_doc = None
        
        for doc in documents:
            if sample_doc is None:
                sample_doc = doc
            for field, value in doc.items():
                if field not in fields:
                    fields[field] = {'type': type(value).__name__, 'count': 0}
                fields[field]['count'] += 1
        
        output = f"{self.colorize(f'Container: {container}', 'bright_cyan')}\n"
        output += f"{self.colorize(f'Total Documents: {len(documents)}', 'green')}\n\n"
        output += f"{self.colorize('Schema Analysis:', 'bright_yellow')}\n"
        
        for field, info in fields.items():
            percentage = (info['count'] / len(documents)) * 100
            output += f"  {field:<20} {info['type']:<10} ({info['count']}/{len(documents)} - {percentage:.1f}%)\n"
        
        if sample_doc:
            output += f"\n{self.colorize('Sample Document:', 'bright_yellow')}\n"
            output += json.dumps(sample_doc, indent=2)
        
        return output
    
    def create_command(self, args: List[str]) -> str:
        """Create container"""
        if len(args) < 2 or args[0].lower() != 'container':
            return self.colorize("Usage: create container container_name", 'yellow')
        
        container_name = args[1]
        if self.db.create_container(container_name):
            return self.colorize(f"Container '{container_name}' created successfully.", 'green')
        else:
            return self.colorize(f"Failed to create container '{container_name}'.", 'red')
    
    def drop_command(self, args: List[str]) -> str:
        """Drop container"""
        if len(args) < 2 or args[0].lower() != 'container':
            return self.colorize("Usage: drop container container_name", 'yellow')
        
        container_name = args[1]
        if self.db.delete_container(container_name):
            if self.current_container == container_name:
                self.current_container = None
            return self.colorize(f"Container '{container_name}' dropped successfully.", 'green')
        else:
            return self.colorize(f"Failed to drop container '{container_name}'.", 'red')
    
    def insert_command(self, args: List[str]) -> str:
        """Insert document"""
        if len(args) < 3:
            return self.colorize("Usage: insert container_name doc_id '{\"field\": \"value\"}'", 'yellow')
        
        container_name = args[0]
        doc_id = args[1]
        json_data = ' '.join(args[2:])
        
        try:
            document = json.loads(json_data)
            if self.db.insert_document(container_name, doc_id, document):
                return self.colorize(f"Document '{doc_id}' inserted successfully.", 'green')
            else:
                return self.colorize(f"Failed to insert document '{doc_id}'.", 'red')
        except json.JSONDecodeError:
            return self.colorize("Invalid JSON format.", 'red')
    
    def update_command(self, args: List[str]) -> str:
        """Update document"""
        if len(args) < 3:
            return self.colorize("Usage: update container_name doc_id '{\"field\": \"new_value\"}'", 'yellow')
        
        container_name = args[0]
        doc_id = args[1]
        json_data = ' '.join(args[2:])
        
        try:
            updates = json.loads(json_data)
            if self.db.update_document(container_name, doc_id, updates):
                return self.colorize(f"Document '{doc_id}' updated successfully.", 'green')
            else:
                return self.colorize(f"Failed to update document '{doc_id}'.", 'red')
        except json.JSONDecodeError:
            return self.colorize("Invalid JSON format.", 'red')
    
    def delete_command(self, args: List[str]) -> str:
        """Delete document"""
        if len(args) < 2:
            return self.colorize("Usage: delete container_name doc_id", 'yellow')
        
        container_name = args[0]
        doc_id = args[1]
        
        if self.db.delete_document(container_name, doc_id):
            return self.colorize(f"Document '{doc_id}' deleted successfully.", 'green')
        else:
            return self.colorize(f"Failed to delete document '{doc_id}'.", 'red')
    
    def count_command(self, args: List[str]) -> str:
        """Count documents"""
        if not args:
            return self.colorize("Usage: count container_name [WHERE condition]", 'yellow')
        
        container_name = args[0]
        
        # Simple count
        if len(args) == 1:
            count = self.db.count(container_name)
            return self.colorize(f"Count: {count}", 'green')
        
        # Count with WHERE - convert to SQL for parsing
        where_clause = ' '.join(args[1:])
        sql_query = f"SELECT * FROM {container_name} {where_clause}"
        
        try:
            results = self.db.execute_sql_like_query(sql_query)
            return self.colorize(f"Count: {len(results)}", 'green')
        except Exception as e:
            return self.colorize(f"Error in count query: {str(e)}", 'red')
    
    def clear_screen(self, args: List[str]) -> str:
        """Clear screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
        self.print_banner()
        return ""
    
    def show_history(self, args: List[str]) -> str:
        """Show query history"""
        if not self.history:
            return self.colorize("No query history.", 'yellow')
        
        output = f"{self.colorize('Query History:', 'bright_cyan')}\n"
        for i, entry in enumerate(self.history[-10:], 1):  # Show last 10
            timestamp = entry['timestamp'][:19]  # Remove microseconds
            container = entry.get('container', 'N/A')
            query = entry['query'][:50] + '...' if len(entry['query']) > 50 else entry['query']
            output += f"  {i}. [{timestamp}] [{container}] {query}\n"
        
        return output
    
    def export_command(self, args: List[str]) -> str:
        """Export containers to folder or single file"""
        if not args:
            return self.colorize("Usage: export [all|container_name] [folder_path|file.json]", 'yellow')
        
        if len(args) == 1:
            # Single argument could be folder path for all containers
            path = args[0]
            return self._export_all_to_folder(path)
        
        target = args[0]
        path = args[1]
        
        # Check if path is a directory or file
        path_obj = Path(path)
        
        if target.lower() == 'all':
            # Export all containers
            if path_obj.suffix == '.json':
                return self.colorize("Cannot export all containers to a single JSON file. Use a folder path.", 'red')
            return self._export_all_to_folder(path)
        else:
            # Export specific container
            if path_obj.suffix == '.json':
                # Export to single file 
                return self._export_container_to_file(target, path)
            else:
                # Export to folder
                return self._export_container_to_folder(target, path)
    
    def _export_all_to_folder(self, folder_path: str) -> str:
        """Export all containers to separate JSON files in a folder"""
        try:
            # Create folder if it doesn't exist
            folder = Path(folder_path)
            folder.mkdir(parents=True, exist_ok=True)
            
            containers = self.db.list_containers()
            if not containers:
                return self.colorize("No containers to export.", 'yellow')
            
            exported_count = 0
            total_docs = 0
            
            for container_name in containers:
                documents = self.db.get_all_documents(container_name)
                if documents:
                    filename = folder / f"{container_name}.json"
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(documents, f, indent=2, ensure_ascii=False)
                    exported_count += 1
                    total_docs += len(documents)
            
            return self.colorize(
                f"Exported {exported_count} containers ({total_docs} documents) to folder: {folder_path}", 
                'green'
            )
            
        except Exception as e:
            return self.colorize(f"Export failed: {str(e)}", 'red')
    
    def _export_container_to_folder(self, container_name: str, folder_path: str) -> str:
        """Export single container to a folder"""
        try:
            # Create folder if it doesn't exist
            folder = Path(folder_path)
            folder.mkdir(parents=True, exist_ok=True)
            
            documents = self.db.get_all_documents(container_name)
            if not documents:
                return self.colorize(f"Container '{container_name}' is empty.", 'yellow')
            
            filename = folder / f"{container_name}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(documents, f, indent=2, ensure_ascii=False)
            
            return self.colorize(
                f"Exported container '{container_name}' ({len(documents)} documents) to: {filename}", 
                'green'
            )
            
        except Exception as e:
            return self.colorize(f"Export failed: {str(e)}", 'red')
    
    def _export_container_to_file(self, container_name: str, filename: str) -> str:
        """Export single container to a specific file (original behavior)"""
        try:
            documents = self.db.get_all_documents(container_name)
            if not documents:
                return self.colorize(f"Container '{container_name}' is empty.", 'yellow')
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(documents, f, indent=2, ensure_ascii=False)
            
            return self.colorize(
                f"Exported {len(documents)} documents from '{container_name}' to {filename}", 
                'green'
            )
            
        except Exception as e:
            return self.colorize(f"Export failed: {str(e)}", 'red')
    
    def import_command(self, args: List[str]) -> str:
        """Import data from folder or single file"""
        if not args:
            return self.colorize("Usage: import [folder_path|container_name file.json]", 'yellow')
        
        if len(args) == 1:
            # Single argument - treat as folder path
            folder_path = args[0]
            return self._import_from_folder(folder_path)
        elif len(args) == 2:
            # Two arguments - container name and file
            container_name = args[0]
            filename = args[1]
            return self._import_file_to_container(container_name, filename)
        else:
            return self.colorize("Usage: import [folder_path|container_name file.json]", 'yellow')
    
    def _import_from_folder(self, folder_path: str) -> str:
        """Import all JSON files from a folder"""
        try:
            folder = Path(folder_path)
            if not folder.exists():
                return self.colorize(f"Folder not found: {folder_path}", 'red')
            
            if not folder.is_dir():
                return self.colorize(f"Path is not a directory: {folder_path}", 'red')
            
            # Find all JSON files
            json_files = list(folder.glob("*.json"))
            if not json_files:
                return self.colorize(f"No JSON files found in folder: {folder_path}", 'yellow')
            
            total_imported = 0
            total_files = 0
            results = []
            
            for json_file in json_files:
                container_name = json_file.stem  # filename without extension
                result = self._import_file_to_container(container_name, str(json_file))
                results.append(f"  {container_name}: {result}")
                
                # Extract number of imported documents from result string
                if "Imported" in result and "/" in result:
                    try:
                        imported_part = result.split("Imported ")[1].split("/")[0]
                        total_imported += int(imported_part)
                    except:
                        pass
                total_files += 1
            
            output = self.colorize(f"Folder Import Results ({total_files} files processed):", 'bright_cyan') + "\n"
            output += "\n".join(results) + "\n\n"
            output += self.colorize(f"Total documents imported: {total_imported}", 'green')
            
            return output
            
        except Exception as e:
            return self.colorize(f"Import from folder failed: {str(e)}", 'red')
    
    def _import_file_to_container(self, container_name: str, filename: str) -> str:
        """Import single file to specific container"""
        try:
            if not Path(filename).exists():
                return self.colorize(f"File not found: {filename}", 'red')
            
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle different JSON structures
            documents = []
            if isinstance(data, list):
                documents = data
            elif isinstance(data, dict):
                # If it's a single document, wrap it in a list
                documents = [data]
            else:
                return self.colorize("Invalid JSON format. Expected array or object.", 'red')
            
            # Create container if it doesn't exist
            if container_name not in self.db.list_containers():
                self.db.create_container(container_name)
            
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
                
                if self.db.insert_document(container_name, str(doc_id), doc_data):
                    imported += 1
                else:
                    failed += 1
            
            result_msg = f"Imported {imported}/{len(documents)} documents"
            if failed > 0:
                result_msg += f" ({failed} failed)"
            
            return self.colorize(result_msg, 'green' if failed == 0 else 'yellow')
            
        except json.JSONDecodeError as e:
            return self.colorize(f"Invalid JSON in file {filename}: {str(e)}", 'red')
        except Exception as e:
            return self.colorize(f"Import failed for {filename}: {str(e)}", 'red')
    
    def backup_command(self, args: List[str]) -> str:
        """Backup entire database"""
        backup_path = args[0] if args else f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            import shutil
            shutil.copytree(self.db_path, backup_path)
            return self.colorize(f"Database backed up to: {backup_path}", 'green')
        except Exception as e:
            return self.colorize(f"Backup failed: {str(e)}", 'red')
    
    def exit_executor(self, args: List[str]) -> str:
        """Exit the executor"""
        print(self.colorize("Goodbye!", 'cyan'))
        sys.exit(0)
    
    def run_interactive(self):
        """Run interactive mode"""
        self.clear_screen([])
        
        while True:
            try:
                # Create prompt
                prompt = f"{self.colorize('NoSQL', 'bright_cyan')}"
                if self.current_container:
                    prompt += f"{self.colorize(f':{self.current_container}', 'bright_yellow')}"
                prompt += f"{self.colorize('> ', 'white')}"
                
                # Get user input
                query = input(prompt).strip()
                
                if query:
                    result = self.execute_query(query)
                    if result:
                        print(result)
                        print()  # Add spacing
            
            except KeyboardInterrupt:
                print(f"\n{self.colorize('Use exit or quit to exit.', 'yellow')}")
            except EOFError:
                print(f"\n{self.colorize('Goodbye!', 'cyan')}")
                break
    
    def run_single_query(self, query: str):
        """Run a single query and exit"""
        result = self.execute_query(query)
        print(result)


def main():
    parser = argparse.ArgumentParser(description='NoSQL Database Query Executor')
    parser.add_argument('database_path', help='Path to the database directory')
    parser.add_argument('-q', '--query', help='Execute a single query and exit')
    parser.add_argument('-f', '--file', help='Execute queries from a file')
    parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    
    args = parser.parse_args()
    

    if args.no_color:
        init(strip=True)
    
    # Initialize executor
    executor = QueryExecutor(args.database_path)
    
    # Handle different execution modes
    if args.query:
        # Single query mode
        executor.run_single_query(args.query)
    elif args.file:
        # File execution mode
        try:
            with open(args.file, 'r') as f:
                queries = f.readlines()
            
            for i, query in enumerate(queries, 1):
                query = query.strip()
                if query and not query.startswith('#'):  
                    print(f"Query {i}: {query}")
                    result = executor.execute_query(query)
                    print(result)
                    print("-" * 50)
        except FileNotFoundError:
            print(f"File not found: {args.file}")
        except Exception as e:
            print(f"Error reading file: {e}")
    else:
        
        executor.run_interactive()


if __name__ == "__main__":
    main()