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
            'backup': self.backup_command,
            'select': self.select_command
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
║                         Version 2.0                           ║
╚═══════════════════════════════════════════════════════════════╝
        """
        print(self.colorize(banner, 'cyan'))
        print(self.colorize(f"Database Path: {self.db_path}", 'yellow'))
        print(self.colorize("Type 'help' for available commands", 'green'))
        print("-" * 63)
    
    def format_database_results(self, db_result: Dict[str, Any]) -> str:
        """Format results from database operations"""
        if not db_result.get('success', False):
            return self.colorize(f"Error: {db_result.get('message', 'Unknown error')}", 'red')
        
        # Handle different result types
        result_type = db_result.get('type', '')
        
        if result_type == 'select':
            results = db_result.get('results', [])
            count = db_result.get('count', len(results))
            
            if not results:
                return self.colorize("No results found.", 'yellow')
            
            # Use the database's built-in table formatting
            if hasattr(self.db, 'format_results_as_table'):
                return self.db.format_results_as_table(results, "Query Results").get('formatted_output', '')
            else:
                return self.format_results_as_table(results, "Query Results")
        
        elif result_type in ['insert', 'update', 'delete', 'create', 'drop']:
            return self.colorize(db_result.get('message', 'Operation completed'), 'green')
        
        elif result_type == 'count':
            count = db_result.get('count', 0)
            return self.colorize(f"Count: {count}", 'green')
        
        else:
            # Generic success message
            return self.colorize(db_result.get('message', 'Operation completed successfully'), 'green')
    
    def format_results_as_table(self, results: List[Dict[str, Any]], title: str = "Query Results") -> str:
        """Fallback table formatting if database doesn't provide it"""
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
            
            # Handle SQL queries using the database's built-in SQL processor
            if query.upper().startswith(('SELECT', 'INSERT')):
                result = self.db.execute_sql_like_query(query)
                return self.format_database_results(result)
            
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
    
    def select_command(self, args: List[str]) -> str:
        """Handle SELECT command directly"""
        if not args:
            return self.colorize("Usage: select * from container_name [where conditions] [limit n] [order by field]", 'yellow')
        
        # Reconstruct the SELECT query
        select_query = "SELECT " + " ".join(args)
        result = self.db.execute_sql_like_query(select_query)
        return self.format_database_results(result)
    
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
            result = self.db.get_containers_info()
            if result['success']:
                containers_info = result.get('containers', [])
                if containers_info:
                    output = f"{self.colorize('Containers:', 'bright_cyan')}\n"
                    for i, container_info in enumerate(containers_info, 1):
                        name = container_info['name']
                        count = container_info['document_count']
                        output += f"  {i}. {self.colorize(name, 'green')} ({count} documents)\n"
                    return output
                else:
                    return self.colorize("No containers found.", 'yellow')
            else:
                return self.colorize(f"Error: {result.get('message', 'Failed to list containers')}", 'red')
        
        elif args[0].lower() == 'documents':
            container = args[1] if len(args) > 1 else self.current_container
            if not container:
                return self.colorize("Please specify a container or use 'use container_name'", 'yellow')
            
            result = self.db.get_all_documents(container)
            if result['success']:
                documents = result.get('documents', [])
                if documents:
                    return self.format_results_as_table(documents, f"Documents in '{container}'")
                else:
                    return self.colorize(f"No documents found in container '{container}'.", 'yellow')
            else:
                return self.colorize(f"Error: {result.get('message', 'Failed to get documents')}", 'red')
        
        else:
            return self.colorize("Usage: show [containers|documents]", 'yellow')
    
    def use_container(self, args: List[str]) -> str:
        """Switch to a container"""
        if not args:
            return self.colorize("Usage: use container_name", 'yellow')
        
        container = args[0]
        result = self.db.use_container(container)
        if result['success']:
            self.current_container = container
            return self.colorize(f"Now using container: {container}", 'green')
        else:
            return self.colorize(f"Error: {result.get('message', 'Container not found')}", 'red')
    
    def describe_container(self, args: List[str]) -> str:
        """Describe container schema"""
        if not args:
            container = self.current_container
        else:
            container = args[0]
        
        if not container:
            return self.colorize("Please specify a container.", 'yellow')
        
        result = self.db.describe_container(container)
        if result['success']:
            description = result.get('description', {})
            
            output = f"{self.colorize(f'Container: {container}', 'bright_cyan')}\n"
            
            # Fix the f-string issue by extracting the value first
            total_docs = description.get('total_documents', 0)
            output += f"{self.colorize(f'Total Documents: {total_docs}', 'green')}\n\n"
            
            if 'schema_analysis' in description:
                output += f"{self.colorize('Schema Analysis:', 'bright_yellow')}\n"
                schema = description['schema_analysis']
                total_docs = description.get('total_documents', 1)
                
                for field, info in schema.items():
                    count = info.get('count', 0)
                    field_type = info.get('type', 'unknown')
                    percentage = (count / total_docs) * 100 if total_docs > 0 else 0
                    output += f"  {field:<20} {field_type:<10} ({count}/{total_docs} - {percentage:.1f}%)\n"
            
            if 'sample_document' in description:
                output += f"\n{self.colorize('Sample Document:', 'bright_yellow')}\n"
                output += json.dumps(description['sample_document'], indent=2)
            
            return output
        else:
            return self.colorize(f"Error: {result.get('message', 'Failed to describe container')}", 'red')
    
    def create_command(self, args: List[str]) -> str:
        """Create container"""
        if len(args) < 2 or args[0].lower() != 'container':
            return self.colorize("Usage: create container container_name", 'yellow')
        
        container_name = args[1]
        result = self.db.create_container(container_name)
        return self.format_database_results(result)
    
    def drop_command(self, args: List[str]) -> str:
        """Drop container"""
        if len(args) < 2 or args[0].lower() != 'container':
            return self.colorize("Usage: drop container container_name", 'yellow')
        
        container_name = args[1]
        result = self.db.delete_container(container_name)
        if result['success'] and self.current_container == container_name:
            self.current_container = None
        return self.format_database_results(result)
    
    def insert_command(self, args: List[str]) -> str:
        """Insert document"""
        if len(args) < 3:
            return self.colorize("Usage: insert container_name doc_id '{\"field\": \"value\"}'", 'yellow')
        
        container_name = args[0]
        doc_id = args[1]
        json_data = ' '.join(args[2:])
        
        try:
            document = json.loads(json_data)
            result = self.db.insert_document(container_name, doc_id, document)
            return self.format_database_results(result)
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
            result = self.db.update_document(container_name, doc_id, updates)
            return self.format_database_results(result)
        except json.JSONDecodeError:
            return self.colorize("Invalid JSON format.", 'red')
    
    def delete_command(self, args: List[str]) -> str:
        """Delete document"""
        if len(args) < 2:
            return self.colorize("Usage: delete container_name doc_id", 'yellow')
        
        container_name = args[0]
        doc_id = args[1]
        
        result = self.db.delete_document(container_name, doc_id)
        return self.format_database_results(result)
    
    def count_command(self, args: List[str]) -> str:
        """Count documents"""
        if not args:
            return self.colorize("Usage: count container_name [WHERE condition]", 'yellow')
        
        container_name = args[0]
        
        # Simple count
        if len(args) == 1:
            result = self.db.count(container_name)
            return self.format_database_results(result)
        
        # Count with WHERE - use database's built-in WHERE parsing
        where_clause = ' '.join(args[1:])
        
        # Parse WHERE conditions using database's condition parser
        if where_clause.upper().startswith('WHERE'):
            where_clause = where_clause[5:].strip()  # Remove 'WHERE' prefix
        
        try:
            
            result = self.db.count(container_name, where_clause)
            return self.format_database_results(result)
        except Exception as e:
            return self.colorize(f"Error in count query: {str(e)}", 'red')
    
    def clear_screen(self, args: List[str]) -> str:
        """Clear screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
        self.print_banner()
        return ""
    def show_history(self, args: List[str]) -> str:
        """Show query history"""
        try:
            if hasattr(self.db, 'get_query_history'):
                db_history_result = self.db.get_query_history()
                if db_history_result.get('success', False):
                    db_history = db_history_result.get('history', [])
                    if db_history and isinstance(db_history, list):
                        output = f"{self.colorize('Database Query History:', 'bright_cyan')}\n"
                        for i, entry in enumerate(db_history[-10:], 1):  
                            if isinstance(entry, dict):
                                timestamp = entry.get('timestamp', '')[:19]
                                query = entry.get('query', '')
                            else:
                                timestamp = ''
                                query = str(entry)
                            query_display = query[:50] + '...' if len(query) > 50 else query
                            output += f"  {i}. [{timestamp}] {query_display}\n"
                        return output
        except Exception:
            pass  
        
        # Fallback to local history
        if not self.history:
            return self.colorize("No query history.", 'yellow')
        
        output = f"{self.colorize('Local Query History:', 'bright_cyan')}\n"
        for i, entry in enumerate(self.history[-10:], 1):  
            timestamp = entry['timestamp'][:19]  
            container = entry.get('container', 'N/A')
            query = entry['query'][:50] + '...' if len(entry['query']) > 50 else entry['query']
            output += f"  {i}. [{timestamp}] [{container}] {query}\n"
        
        return output
    
    
    def export_command(self, args: List[str]) -> str:
        """Export containers using database's export functionality"""
        if not args:
            return self.colorize("Usage: export [all|container_name] [folder_path|file.json]", 'yellow')
        
        if len(args) == 1:
            # Export all to folder
            result = self.db.export_data('all', args[0])
            return self.format_database_results(result)
        
        target = args[0]
        path = args[1]
        
        result = self.db.export_data(target, path)
        return self.format_database_results(result)
    
    def import_command(self, args: List[str]) -> str:
        """Import data using database's import functionality"""
        if not args:
            return self.colorize("Usage: import [folder_path|container_name file.json]", 'yellow')
        
        if len(args) == 1:
            # Import from folder
            result = self.db.import_data(args[0])
            return self.format_database_results(result)
        elif len(args) == 2:
            # Import file to specific container
            container_name = args[0]
            filename = args[1]
            result = self.db.import_data(filename, container_name)
            return self.format_database_results(result)
        else:
            return self.colorize("Usage: import [folder_path|container_name file.json]", 'yellow')
    
    def backup_command(self, args: List[str]) -> str:
        """Backup database using database's backup functionality"""
        backup_path = args[0] if args else None
        result = self.db.backup_database(backup_path)
        return self.format_database_results(result)
    
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
