#!/usr/bin/env python3
"""
Enhanced Windchill MCP client with proper protocol support.
"""

import json
import logging
import requests
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedWindchillMCPClient:
    """Enhanced client for Windchill MCP server with proper protocol support."""
    
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
    
    def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Call a specific MCP tool with proper protocol."""
        try:
            # MCP protocol format
            payload = {
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {
                    "name": tool_name,
                    "arguments": arguments
                },
                "id": 1
            }
            
            response = self.session.post(
                urljoin(self.base_url, "/message"),
                json=payload
            )
            
            logger.info(f"Tool {tool_name} response: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                if 'result' in result:
                    return result['result']
                elif 'error' in result:
                    logger.error(f"Tool error: {result['error']}")
                    return None
                else:
                    return result
            else:
                logger.warning(f"Tool call failed: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error calling tool {tool_name}: {e}")
            return None
    
    def search_parts(self, search_term: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Search for parts in Windchill using proper MCP protocol."""
        result = self.call_tool("partmgmt_search_parts", {
            "query": search_term,
            "limit": limit
        })
        
        if result:
            parts = result.get('results', [])
            logger.info(f"Found {len(parts)} parts for search term '{search_term}'")
            return parts
        return []
    
    def get_part_details(self, part_number: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific part."""
        # First search to get the part OID
        search_result = self.call_tool("partmgmt_search_parts", {
            "query": part_number,
            "limit": 1
        })
        
        if search_result and search_result.get('results'):
            part_oid = search_result['results'][0].get('oid')
            if part_oid:
                result = self.call_tool("partmgmt_get_part", {
                    "partId": part_oid,
                    "expand": "Number,Name,State,Version,Iteration,Container,Creator,CreateDate,Modifier,ModifyDate"
                })
                if result:
                    logger.info(f"Retrieved details for part {part_number}")
                    return result
        
        logger.warning(f"Failed to get details for part {part_number}")
        return None
    
    def get_part_changes(self, part_number: str) -> List[Dict[str, Any]]:
        """Get change information for a specific part."""
        changes = []
        
        # Search for change objects related to this part
        change_result = self.call_tool("changemgmt_search_change_objects", {
            "query": part_number,
            "limit": 50
        })
        
        if change_result and change_result.get('results'):
            for change in change_result['results']:
                change_oid = change.get('oid')
                if change_oid:
                    # Get detailed change information
                    change_detail = self.call_tool("changemgmt_get_change_object", {
                        "changeId": change_oid,
                        "expand": "Number,Name,State,CreateDate,Creator,Description,Priority,NeedDate,AffectedObjects"
                    })
                    if change_detail:
                        changes.append(change_detail)
        
        logger.info(f"Found {len(changes)} changes for part {part_number}")
        return changes
    
    def get_bom_structure(self, part_number: str, depth: int = 3) -> Optional[Dict[str, Any]]:
        """Get BOM structure for a specific part."""
        # First search to get the part OID
        search_result = self.call_tool("partmgmt_search_parts", {
            "query": part_number,
            "limit": 1
        })
        
        if search_result and search_result.get('results'):
            part_oid = search_result['results'][0].get('oid')
            if part_oid:
                result = self.call_tool("partmgmt_get_part_structure", {
                    "partId": part_oid,
                    "depth": depth,
                    "viewName": "Design"
                })
                if result:
                    logger.info(f"Retrieved BOM structure for part {part_number}")
                    return result
        
        logger.warning(f"Failed to get BOM structure for part {part_number}")
        return None
    
    def get_all_change_objects(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Get all change objects from Windchill."""
        result = self.call_tool("changemgmt_list_change_objects", {
            "limit": limit,
            "select": "Number,Name,State,CreateDate,Creator,Description,Priority,NeedDate"
        })
        
        if result:
            changes = result.get('results', [])
            logger.info(f"Found {len(changes)} total change objects")
            return changes
        return []
    
    def get_change_affected_objects(self, change_id: str) -> List[Dict[str, Any]]:
        """Get objects affected by a specific change."""
        result = self.call_tool("changemgmt_get_change_affected_objects", {
            "changeId": change_id
        })
        
        if result:
            affected_objects = result.get('results', [])
            logger.info(f"Found {len(affected_objects)} objects affected by change {change_id}")
            return affected_objects
        return []

def main():
    """Main function to search for snowmobile data."""
    logger.info("Starting Enhanced Windchill MCP client for snowmobile search")
    
    # Initialize client
    client = EnhancedWindchillMCPClient()
    
    # Test connection
    if not client.test_connection():
        logger.error("Failed to connect to Windchill MCP server")
        return
    
    # Search for snowmobile-related parts
    search_terms = ["snowmobile", "snow", "mobile", "SNO"]
    all_parts = []
    
    for term in search_terms:
        logger.info(f"Searching for term: {term}")
        parts = client.search_parts(term, limit=50)
        all_parts.extend(parts)
        logger.info(f"Found {len(parts)} parts for '{term}'")
    
    # Remove duplicates based on part number
    seen_numbers = set()
    unique_parts = []
    for part in all_parts:
        number = part.get('number', part.get('Number', ''))
        if number and number not in seen_numbers:
            seen_numbers.add(number)
            unique_parts.append(part)
    
    logger.info(f"Total unique parts found: {len(unique_parts)}")
    
    # Get detailed information for snowmobile parts
    snowmobile_data = {
        'parts': [],
        'changes': [],
        'bom_structures': [],
        'change_objects': []
    }
    
    # Process each unique part
    for i, part in enumerate(unique_parts):
        part_number = part.get('number', part.get('Number', ''))
        logger.info(f"Processing part {i+1}/{len(unique_parts)}: {part_number}")
        
        # Get part details
        details = client.get_part_details(part_number)
        if details:
            snowmobile_data['parts'].append(details)
        
        # Get changes for this part
        changes = client.get_part_changes(part_number)
        if changes:
            snowmobile_data['changes'].extend(changes)
        
        # Get BOM structure
        bom = client.get_bom_structure(part_number)
        if bom:
            snowmobile_data['bom_structures'].append(bom)
    
    # Get all change objects to identify snowmobile-related changes
    logger.info("Fetching all change objects...")
    all_changes = client.get_all_change_objects(limit=1000)
    
    # Filter changes that might be related to snowmobile
    snowmobile_related_changes = []
    for change in all_changes:
        change_name = change.get('Name', '').lower()
        change_desc = change.get('Description', '').lower()
        
        if any(term in change_name or change_desc for term in ['snow', 'sno', 'mobile']):
            snowmobile_related_changes.append(change)
            # Get affected objects for this change
            change_id = change.get('oid')
            if change_id:
                affected_objects = client.get_change_affected_objects(change_id)
                change['affected_objects'] = affected_objects
    
    snowmobile_data['change_objects'] = snowmobile_related_changes
    logger.info(f"Found {len(snowmobile_related_changes)} snowmobile-related change objects")
    
    # Save data to files
    with open('snowmobile_windchill_data.json', 'w') as f:
        json.dump(snowmobile_data, f, indent=2)
    
    logger.info(f"Saved {len(snowmobile_data['parts'])} parts, {len(snowmobile_data['changes'])} changes, {len(snowmobile_data['bom_structures'])} BOM structures, {len(snowmobile_data['change_objects'])} change objects")
    logger.info("Data saved to snowmobile_windchill_data.json")

if __name__ == "__main__":
    main()