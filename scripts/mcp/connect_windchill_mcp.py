#!/usr/bin/env python3
"""
Connect to Windchill MCP server and search for snowmobile data.
"""

import json
import logging
import requests
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WindchillMCPClient:
    """Client for connecting to Windchill MCP server."""
    
    def __init__(self, base_url: str = "http://localhost:3000"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    
    def test_connection(self) -> bool:
        """Test connection to MCP server."""
        try:
            response = self.session.get(urljoin(self.base_url, "/"))
            logger.info(f"MCP server response: {response.status_code}")
            if response.status_code == 200:
                logger.info("Successfully connected to Windchill MCP server")
                return True
            else:
                logger.warning(f"MCP server returned status {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Failed to connect to MCP server: {e}")
            return False
    
    def get_available_tools(self) -> List[str]:
        """Get list of available tools from MCP server."""
        try:
            response = self.session.get(urljoin(self.base_url, "/tools"))
            if response.status_code == 200:
                tools = response.json()
                logger.info(f"Available tools: {tools}")
                return tools
            else:
                logger.warning(f"Failed to get tools: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Error getting tools: {e}")
            return []
    
    def search_parts(self, search_term: str) -> List[Dict[str, Any]]:
        """Search for parts in Windchill."""
        try:
            payload = {
                "tool": "search_parts",
                "arguments": {
                    "search_term": search_term,
                    "limit": 100
                }
            }
            
            response = self.session.post(
                urljoin(self.base_url, "/tools/call"),
                json=payload
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Found {len(result.get('results', []))} parts for search term '{search_term}'")
                return result.get('results', [])
            else:
                logger.warning(f"Search failed: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Error searching parts: {e}")
            return []
    
    def get_part_details(self, part_number: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific part."""
        try:
            payload = {
                "tool": "get_part_details",
                "arguments": {
                    "part_number": part_number
                }
            }
            
            response = self.session.post(
                urljoin(self.base_url, "/tools/call"),
                json=payload
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Retrieved details for part {part_number}")
                return result
            else:
                logger.warning(f"Failed to get part details: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error getting part details: {e}")
            return None
    
    def get_part_changes(self, part_number: str) -> List[Dict[str, Any]]:
        """Get change information for a specific part."""
        try:
            payload = {
                "tool": "get_part_changes",
                "arguments": {
                    "part_number": part_number
                }
            }
            
            response = self.session.post(
                urljoin(self.base_url, "/tools/call"),
                json=payload
            )
            
            if response.status_code == 200:
                result = response.json()
                changes = result.get('changes', [])
                logger.info(f"Found {len(changes)} changes for part {part_number}")
                return changes
            else:
                logger.warning(f"Failed to get part changes: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Error getting part changes: {e}")
            return []
    
    def get_bom_structure(self, part_number: str) -> Optional[Dict[str, Any]]:
        """Get BOM structure for a specific part."""
        try:
            payload = {
                "tool": "get_bom_structure",
                "arguments": {
                    "part_number": part_number,
                    "depth": 3
                }
            }
            
            response = self.session.post(
                urljoin(self.base_url, "/tools/call"),
                json=payload
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Retrieved BOM structure for part {part_number}")
                return result
            else:
                logger.warning(f"Failed to get BOM structure: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error getting BOM structure: {e}")
            return None

def main():
    """Main function to search for snowmobile data."""
    logger.info("Starting Windchill MCP client for snowmobile search")
    
    # Initialize client
    client = WindchillMCPClient()
    
    # Test connection
    if not client.test_connection():
        logger.error("Failed to connect to Windchill MCP server")
        return
    
    # Get available tools
    tools = client.get_available_tools()
    logger.info(f"Available MCP tools: {tools}")
    
    # Search for snowmobile-related parts
    search_terms = ["snowmobile", "snow", "mobile", "SNO"]
    all_parts = []
    
    for term in search_terms:
        logger.info(f"Searching for term: {term}")
        parts = client.search_parts(term)
        all_parts.extend(parts)
        logger.info(f"Found {len(parts)} parts for '{term}'")
    
    # Remove duplicates
    unique_parts = {part['number']: part for part in all_parts}.values()
    logger.info(f"Total unique parts found: {len(unique_parts)}")
    
    # Get detailed information for snowmobile parts
    snowmobile_data = {
        'parts': [],
        'changes': [],
        'bom_structures': []
    }
    
    for part in unique_parts:
        part_number = part['number']
        logger.info(f"Processing part: {part_number}")
        
        # Get part details
        details = client.get_part_details(part_number)
        if details:
            snowmobile_data['parts'].append(details)
        
        # Get changes
        changes = client.get_part_changes(part_number)
        if changes:
            snowmobile_data['changes'].extend(changes)
        
        # Get BOM structure
        bom = client.get_bom_structure(part_number)
        if bom:
            snowmobile_data['bom_structures'].append(bom)
    
    # Save data to files
    with open('snowmobile_windchill_data.json', 'w') as f:
        json.dump(snowmobile_data, f, indent=2)
    
    logger.info(f"Saved {len(snowmobile_data['parts'])} parts, {len(snowmobile_data['changes'])} changes, {len(snowmobile_data['bom_structures'])} BOM structures")
    logger.info("Data saved to snowmobile_windchill_data.json")

if __name__ == "__main__":
    main()