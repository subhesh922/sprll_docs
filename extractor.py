import requests
import pandas as pd
import json
from typing import Optional, Dict, Any

def extract_jira_issues_to_csv(
    jira_url: str,
    pat_token: str,
    jql_query: str,
    output_file: str = "jira_issues.csv"
) -> pd.DataFrame:
    """
    Extract Jira issues using JQL query and save to CSV
    
    Args:
        jira_url: Your Jira server URL (e.g., "https://jira.zebra.com")
        pat_token: Your Personal Access Token
        jql_query: JQL query string
        output_file: Output CSV filename
        
    Returns:
        pandas DataFrame with the extracted data
    """
    
    # API endpoint
    url = f"{jira_url}/rest/api/2/search"
    
    # Headers for authentication
    headers = {
        "Authorization": f"Bearer {pat_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    # Parameters
    params = {
        "jql": jql_query,
        "fields": "summary,description,customfield_10606,customfield_11222",
        "maxResults": 1000  # Adjust as needed
    }
    
    try:
        # Make API request
        print(f"Making API request to: {url}")
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        print(f"Found {data['total']} issues")
        
        # Extract data into list of dictionaries
        extracted_data = []
        
        for issue in data['issues']:
            fields = issue['fields']
            
            # Extract and clean data
            row = {
                'key': issue['key'],
                'summary': clean_text(fields.get('summary', '')),
                'description': clean_text(fields.get('description', '')),
                'root_cause': clean_text(fields.get('customfield_10606', '')),
                'corrective_action': clean_text(fields.get('customfield_11222', ''))
            }
            
            extracted_data.append(row)
        
        # Create DataFrame
        df = pd.DataFrame(extracted_data)
        
        # Save to CSV
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Data saved to {output_file}")
        
        # Display summary
        print(f"\nSummary:")
        print(f"- Total issues: {len(df)}")
        print(f"- Issues with root cause: {df['root_cause'].notna().sum()}")
        print(f"- Issues with corrective action: {df['corrective_action'].notna().sum()}")
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return pd.DataFrame()
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON response: {e}")
        return pd.DataFrame()

def clean_text(text: Any) -> str:
    """
    Clean and normalize text fields
    
    Args:
        text: Text to clean (can be None, string, or other type)
        
    Returns:
        Cleaned string
    """
    if text is None:
        return ""
    
    if not isinstance(text, str):
        text = str(text)
    
    # Remove extra whitespace and normalize line breaks
    text = text.strip()
    text = text.replace('\r\n', '\n')
    text = text.replace('\r', '\n')
    
    return text

def preview_data(df: pd.DataFrame, num_rows: int = 3) -> None:
    """
    Preview the extracted data
    
    Args:
        df: DataFrame to preview
        num_rows: Number of rows to display
    """
    if df.empty:
        print("No data to preview")
        return
    
    print(f"\n--- Preview of first {num_rows} rows ---")
    for idx, row in df.head(num_rows).iterrows():
        print(f"\nIssue {idx + 1}: {row['key']}")
        print(f"Summary: {row['summary'][:100]}{'...' if len(row['summary']) > 100 else ''}")
        print(f"Root Cause: {row['root_cause'][:100]}{'...' if len(row['root_cause']) > 100 else ''}")
        print(f"Corrective Action: {row['corrective_action'][:100]}{'...' if len(row['corrective_action']) > 100 else ''}")
        print("-" * 80)

def get_available_components(jira_url: str, pat_token: str, project: str = "SPRLL") -> list:
    """
    Get all available components for a project
    
    Args:
        jira_url: Your Jira server URL
        pat_token: Your Personal Access Token
        project: Project key
        
    Returns:
        List of component names
    """
    url = f"{jira_url}/rest/api/2/project/{project}/components"
    headers = {
        "Authorization": f"Bearer {pat_token}",
        "Accept": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        components = response.json()
        return [comp['name'] for comp in components]
    except requests.exceptions.RequestException as e:
        print(f"Failed to get components: {e}")
        # Fallback to common components
        return [
            "Display", "Touch panel", "Battery", "Enclosure", "Antenna",
            "Camera", "Scanner", "Audio", "Keypad", "USB", "WiFi", 
            "Bluetooth", "GPS", "Sensor", "Power", "Memory", "Processor",
            "I/O", "Charging", "Accessories"
        ]

def display_menu(components: list) -> None:
    """Display component selection menu"""
    print("\n" + "="*60)
    print("AVAILABLE COMPONENTS:")
    print("="*60)
    
    for i, comp in enumerate(components, 1):
        print(f"{i:2}. {comp}")
    
    print(f"\n{len(components) + 1:2}. ALL COMPONENTS")
    print("="*60)

def get_user_component_selection(components: list) -> list:
    """
    Interactive component selection
    
    Args:
        components: List of available components
        
    Returns:
        List of selected component names
    """
    display_menu(components)
    
    while True:
        try:
            choice = input(f"\nSelect components (1-{len(components) + 1}) or comma-separated numbers: ").strip()
            
            if not choice:
                print("Please enter a selection")
                continue
            
            # Handle "all" option
            if choice == str(len(components) + 1):
                return components
            
            # Parse comma-separated choices
            choices = [int(x.strip()) for x in choice.split(',')]
            
            # Validate choices
            if all(1 <= c <= len(components) for c in choices):
                selected = [components[c-1] for c in choices]
                return selected
            else:
                print(f"Please enter numbers between 1 and {len(components) + 1}")
                
        except ValueError:
            print("Please enter valid numbers separated by commas")

def create_jql_query(project: str, components: list) -> str:
    """
    Create JQL query for selected components
    
    Args:
        project: Project key
        components: List of component names
        
    Returns:
        JQL query string
    """
    if len(components) == 1:
        return f'project = {project} AND component = "{components[0]}"'
    else:
        component_list = ', '.join([f'"{comp}"' for comp in components])
        return f'project = {project} AND component IN ({component_list})'

def generate_output_filename(components: list, project: str = "SPRLL") -> str:
    """
    Generate appropriate output filename
    
    Args:
        components: List of component names
        project: Project key
        
    Returns:
        Output filename
    """
    if len(components) == 1:
        safe_name = components[0].lower().replace(' ', '_').replace('/', '_')
        return f"{project.lower()}_{safe_name}_issues.csv"
    elif len(components) <= 3:
        safe_names = [comp.lower().replace(' ', '_').replace('/', '_') for comp in components]
        combined = '_'.join(safe_names)
        return f"{project.lower()}_{combined}_issues.csv"
    else:
        return f"{project.lower()}_multiple_components_issues.csv"

def load_env_file(env_file: str = '.env') -> None:
    """
    Load environment variables from .env file
    
    Args:
        env_file: Path to .env file
    """
    import os
    
    if not os.path.exists(env_file):
        return
    
    try:
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")  # Remove quotes if present
                    os.environ[key] = value
        print(f"Loaded environment variables from {env_file}")
    except Exception as e:
        print(f"Warning: Could not load {env_file}: {e}")

def main():
    """Main execution with user interaction"""
    import argparse
    import os
    
    # Load .env file first
    load_env_file()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract Jira issues by component')
    parser.add_argument('--token', help='PAT token (or set in .env file as JIRA_PAT_TOKEN)')
    parser.add_argument('--url', default='https://jira.zebra.com', help='Jira server URL')
    parser.add_argument('--project', default='SPRLL', help='Project key')
    parser.add_argument('--components', nargs='+', help='Component names (space-separated)')
    parser.add_argument('--output', help='Output CSV filename')
    parser.add_argument('--interactive', action='store_true', help='Interactive mode')
    parser.add_argument('--env-file', default='.env', help='Path to .env file')
    
    args = parser.parse_args()
    
    # Load custom .env file if specified
    if args.env_file != '.env':
        load_env_file(args.env_file)
    
    # Get PAT token from multiple sources
    pat_token = (
        args.token or                           # Command line argument
        os.getenv('JIRA_PAT_TOKEN') or         # Environment variable
        os.getenv('JIRA_TOKEN') or             # Alternative env var
        os.getenv('PAT_TOKEN')                 # Another alternative
    )
    
    if not pat_token:
        print("PAT token not found in:")
        print(f"  - Command line argument (--token)")
        print(f"  - Environment variable JIRA_PAT_TOKEN")
        print(f"  - .env file")
        pat_token = input("\nEnter your PAT token: ").strip()
    else:
        print("✅ PAT token loaded successfully")
    
    if not pat_token:
        print("Error: PAT token is required")
        return
    
    # Configuration
    JIRA_URL = args.url
    PROJECT = args.project
    
    print(f"Connecting to: {JIRA_URL}")
    print(f"Project: {PROJECT}")
    
    # Get available components
    print("Fetching available components...")
    available_components = get_available_components(JIRA_URL, pat_token, PROJECT)
    
    # Component selection
    if args.components and not args.interactive:
        # Command line specified components
        selected_components = []
        for comp in args.components:
            # Find matching component (case-insensitive)
            matches = [c for c in available_components if c.lower() == comp.lower()]
            if matches:
                selected_components.append(matches[0])
            else:
                print(f"Warning: Component '{comp}' not found. Available components:")
                for c in available_components:
                    print(f"  - {c}")
                return
    else:
        # Interactive selection
        selected_components = get_user_component_selection(available_components)
    
    print(f"\nSelected components: {', '.join(selected_components)}")
    
    # Create JQL query
    jql_query = create_jql_query(PROJECT, selected_components)
    print(f"JQL Query: {jql_query}")
    
    # Generate output filename
    output_file = args.output or generate_output_filename(selected_components, PROJECT)
    print(f"Output file: {output_file}")
    
    # Confirm before proceeding
    proceed = input(f"\nProceed with extraction? (y/N): ").strip().lower()
    if proceed != 'y':
        print("Extraction cancelled")
        return
    
    # Extract data
    df = extract_jira_issues_to_csv(
        jira_url=JIRA_URL,
        pat_token=pat_token,
        jql_query=jql_query,
        output_file=output_file
    )
    
    # Preview the data
    if not df.empty:
        preview_data(df)
        
        # Component breakdown
        if len(selected_components) > 1:
            print(f"\n--- Component Breakdown ---")
            # This would require getting component info from the API
            # For now, just show total
            print(f"Total issues across {len(selected_components)} components: {len(df)}")
        
        print(f"\n--- Export Complete ---")
        print(f"File saved: {output_file}")
        print(f"Total records: {len(df)}")

# Batch processing function
def batch_extract_components(
    jira_url: str,
    pat_token: str,
    project: str,
    components: list,
    output_dir: str = "output"
) -> Dict[str, pd.DataFrame]:
    """
    Extract each component separately in batch
    
    Args:
        jira_url: Jira server URL
        pat_token: PAT token
        project: Project key
        components: List of component names
        output_dir: Output directory
        
    Returns:
        Dictionary of {component: DataFrame}
    """
    import os
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    results = {}
    
    for component in components:
        print(f"\n--- Processing {component} ---")
        
        jql_query = f'project = {project} AND component = "{component}"'
        safe_name = component.lower().replace(' ', '_').replace('/', '_')
        output_file = os.path.join(output_dir, f"{project.lower()}_{safe_name}_issues.csv")
        
        df = extract_jira_issues_to_csv(
            jira_url=jira_url,
            pat_token=pat_token,
            jql_query=jql_query,
            output_file=output_file
        )
        
        results[component] = df
    
    return results

if __name__ == "__main__":
    main()
