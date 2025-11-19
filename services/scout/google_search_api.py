# services/scout/google_search_api.py

import os
import requests
from typing import List, Dict, Optional
from dotenv import load_dotenv

load_dotenv()

class GoogleSearchAPI:
    """
    Google Custom Search JSON API wrapper
    Get your API key: https://developers.google.com/custom-search/v1/overview
    Get your Search Engine ID: https://programmablesearchengine.google.com/
    """
    
    def __init__(self, api_key: str = None, search_engine_id: str = None):
        self.api_key = api_key or os.getenv('GOOGLE_SEARCH_API_KEY')
        self.search_engine_id = search_engine_id or os.getenv('GOOGLE_SEARCH_ENGINE_ID')
        self.base_url = "https://www.googleapis.com/customsearch/v1"
        
        if not self.api_key:
            raise ValueError("GOOGLE_SEARCH_API_KEY not set in environment")
        if not self.search_engine_id:
            raise ValueError("GOOGLE_SEARCH_ENGINE_ID not set in environment")
    
    def search(self, query: str, num_results: int = 10) -> List[Dict]:
        """
        Perform a search and return results
        
        Args:
            query: Search query string
            num_results: Number of results to return (max 10 per request)
            
        Returns:
            List of result dictionaries with 'title', 'link', 'snippet'
        """
        params = {
            'key': self.api_key,
            'cx': self.search_engine_id,
            'q': query,
            'num': min(num_results, 10)  # API max is 10
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            results = []
            if 'items' in data:
                for item in data['items']:
                    results.append({
                        'title': item.get('title', ''),
                        'link': item.get('link', ''),
                        'snippet': item.get('snippet', ''),
                        'displayLink': item.get('displayLink', '')
                    })
            
            return results
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Search API error: {e}")
            return []
    
    def search_county_records(self, county_name: str, state: str, record_type: str) -> Optional[str]:
        """
        Search for county public records portal
        
        Args:
            county_name: County name (e.g., "Harris")
            state: State code (e.g., "TX")
            record_type: Type of records ('property', 'tax', 'probate', 'judgment')
            
        Returns:
            Best matching URL or None
        """
        keywords = {
            'property': 'property records search appraisal district',
            'tax': 'property tax search delinquent',
            'probate': 'probate court case search',
            'judgment': 'county clerk records search liens judgments'
        }
        
        query = f"{county_name} county {state} {keywords.get(record_type, 'records')}"
        
        print(f"  🔍 API Search: {query}")
        
        results = self.search(query, num_results=5)
        
        # Filter out non-official sites
        excluded_domains = ['google.com', 'facebook.com', 'yelp.com', 'wikipedia.org']
        
        for result in results:
            link = result['link']
            domain = result['displayLink']
            
            # Skip excluded domains
            if any(excluded in domain for excluded in excluded_domains):
                continue
            
            # Prefer .gov or official county sites
            if '.gov' in domain or 'county' in domain.lower():
                print(f"    ✅ Found: {result['title']}")
                print(f"       {link}")
                return link
        
        # If no .gov found, return first valid result
        if results:
            print(f"    ⚠️  No .gov found, using: {results[0]['link']}")
            return results[0]['link']
        
        return None