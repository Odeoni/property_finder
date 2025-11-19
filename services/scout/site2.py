"""
scout_agent.py

Hybrid AgentQL Scout for County Public Records Discovery
Uses Claude for planning navigation steps, AgentQL for execution
WITH COMPREHENSIVE ANALYSIS + INTELLIGENT ERROR RECOVERY
"""

import sys
import os

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)


import os
import json
import asyncio
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from decimal import Decimal

# External imports
from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page, Browser
import agentql
import requests
from services.captcha.solver import CaptchaSolver
from services.scout.google_search_api import GoogleSearchAPI
from anthropic import Anthropic
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Text, DECIMAL, Date, JSON, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

load_dotenv(dotenv_path=r'C:\Users\KISFECO\Documents\heir-finder\.env')

# ============================================================================
# DATABASE SCHEMA (County class only - needed for scout)
# ============================================================================

Base = declarative_base()

class County(Base):
    """
    County configuration table
    Stores discovered website URLs and selectors for each county's public records
    This is populated by the Scout service
    """
    __tablename__ = 'counties'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), index=True)
    state = Column(String(2), index=True)
    fips_code = Column(String(5), unique=True)
    population = Column(Integer)
    
    # Property search configuration
    property_search_url = Column(Text)
    property_search_selectors = Column(JSON)
    property_scraper_generated = Column(Boolean, default=False)
    property_scraper_path = Column(Text)
    property_last_tested = Column(DateTime)
    
    property_requires_proxy = Column(Boolean, default=False)
    property_requires_us_ip = Column(Boolean, default=False)
    property_blocks_datacenter_ips = Column(Boolean, default=False)
    
    # Tax delinquency configuration
    tax_search_url = Column(Text)
    tax_search_selectors = Column(JSON)
    tax_scraper_generated = Column(Boolean, default=False)
    tax_scraper_path = Column(Text)
    tax_last_tested = Column(DateTime)
    
    tax_requires_proxy = Column(Boolean, default=False)
    tax_requires_us_ip = Column(Boolean, default=False)
    tax_blocks_datacenter_ips = Column(Boolean, default=False)
    
    # Probate search configuration
    probate_search_url = Column(Text)
    probate_search_selectors = Column(JSON)
    probate_scraper_generated = Column(Boolean, default=False)
    probate_scraper_path = Column(Text)
    probate_last_tested = Column(DateTime)
    
    probate_requires_proxy = Column(Boolean, default=False)
    probate_requires_us_ip = Column(Boolean, default=False)
    probate_blocks_datacenter_ips = Column(Boolean, default=False)
    
    # Judgments/liens configuration
    judgment_search_url = Column(Text)
    judgment_search_selectors = Column(JSON)
    judgment_scraper_generated = Column(Boolean, default=False)
    judgment_scraper_path = Column(Text)
    judgment_last_tested = Column(DateTime)
    
    judgment_requires_proxy = Column(Boolean, default=False)
    judgment_requires_us_ip = Column(Boolean, default=False)
    judgment_blocks_datacenter_ips = Column(Boolean, default=False)
    
    # Global proxy settings
    requires_proxy = Column(Boolean, default=False)
    proxy_type = Column(String(20))
    proxy_provider = Column(String(50))
    proxy_cost_per_request = Column(DECIMAL(6, 4))
    
    # Scout metadata
    scouted_at = Column(DateTime)
    scout_confidence = Column(DECIMAL(3, 2))
    scout_notes = Column(Text)
    
    # Operational flags
    is_active = Column(Boolean, default=True)
    requires_captcha = Column(Boolean, default=False)
    requires_authentication = Column(Boolean, default=False)
    blocks_vpn = Column(Boolean, default=False)
    
    # Rate limiting info
    rate_limit_requests_per_minute = Column(Integer)
    rate_limit_cooldown_seconds = Column(Integer)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# ============================================================================
# CONFIGURATION
# ============================================================================

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:password123@localhost:5432/heir_finder')
CLAUDE_API_KEY = os.getenv('CLAUDE_API_KEY')
AGENTQL_API_KEY = os.getenv('AGENTQL_API_KEY')
CAPSOLVER_API_KEY = os.getenv('CAPSOLVER_API_KEY')

if not CLAUDE_API_KEY:
    raise ValueError("CLAUDE_API_KEY environment variable not set")
if not AGENTQL_API_KEY:
    raise ValueError("AGENTQL_API_KEY environment variable not set")

# Initialize captcha solver
if CAPSOLVER_API_KEY:
    captcha_solver = CaptchaSolver(CAPSOLVER_API_KEY)
    print("✅ CAPTCHA solver initialized")
else:
    captcha_solver = None
    print("⚠️ CAPSOLVER_API_KEY not set - CAPTCHA solving disabled")

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Initialize Claude client
claude_client = Anthropic(api_key=CLAUDE_API_KEY)

# Initialize Google Search API
google_api = GoogleSearchAPI()

# Search types and their specifications
SEARCH_TYPES = {
    'documents': {
        'keywords': ['property records', 'property search', 'parcel search', 'real estate records', 'liens', 'judgements', 'recorded at', 'public documents'],
        'test_search': {'last_name': 'Smith'},
        'validation_indicators': ['owner', 'address', 'parcel', 'apn', 'assessed value', 'property type'],
        'url_field': 'property_search_url',
        'selectors_field': 'property_search_selectors',
        'requires_proxy_field': 'property_requires_proxy',
        'requires_us_ip_field': 'property_requires_us_ip',
        'blocks_datacenter_field': 'property_blocks_datacenter_ips'
    },
    'tax': {
        'keywords': ['tax search', 'tax records', 'delinquent tax', 'tax collector', 'tax assessor', 'property tax'],
        'test_search': {'last_name': 'Smith'},
        'validation_indicators': ['tax year', 'amount due', 'payment history', 'delinquent', 'balance', 'tax bill'],
        'url_field': 'tax_search_url',
        'selectors_field': 'tax_search_selectors',
        'requires_proxy_field': 'tax_requires_proxy',
        'requires_us_ip_field': 'tax_requires_us_ip',
        'blocks_datacenter_field': 'tax_blocks_datacenter_ips'
    },
    'probate': {
        'keywords': ['probate search', 'probate court', 'estate records', 'probate records', 'court records', 'case search'],
        'test_search': {'last_name': 'Smith'},
        'validation_indicators': ['case number', 'case status', 'filing date', 'probate', 'estate', 'decedent'],
        'url_field': 'probate_search_url',
        'selectors_field': 'probate_search_selectors',
        'requires_proxy_field': 'probate_requires_proxy',
        'requires_us_ip_field': 'probate_requires_us_ip',
        'blocks_datacenter_field': 'probate_blocks_datacenter_ips'
    }
}


# ============================================================================
# GOOGLE SEARCH API INTEGRATION
# ============================================================================

def api_search_for_county_records(county_name: str, state: str, search_type: str) -> Optional[str]:
    """
    Use Google Search API to find county records (no browser, no CAPTCHA!)
    Returns: Best URL or None
    """
    try:
        print(f"  🔍 Google API search: {county_name} County {state} {search_type}")
        
        # Get search results from API
        url = google_api.search_county_records(county_name, state, search_type)
        
        if not url:
            print(f"    ⚠️  No results from Google API")
            return None
        
        # Get multiple results for Claude to analyze
        results = google_api.search(
            f"{county_name} county {state} {SEARCH_TYPES[search_type]['keywords'][3]}", 
            num_results=5
        )
        
        if not results:
            return url  # Fallback to first result
        
        # Format results for Claude to choose the best one
        results_text = "\n".join([
            f"  - {r['title']}\n    {r['link']}" 
            for r in results
        ])
        
        # Ask Claude to pick the best URL
        claude_prompt = f"""You are analyzing search results to find the best county public records search portal.

County: {county_name} County, {state}
Record Type: {search_type}

Search Results:
{results_text}

Choose the BEST URL that leads directly to a searchable database for {search_type} records.
Prefer:
1. URLs with "search" in them
2. Official .gov domains
3. Direct access to search forms (not informational pages)

Respond with ONLY the complete URL, nothing else."""

        response = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=200,
            messages=[{"role": "user", "content": claude_prompt}]
        )
        
        selected_url = response.content[0].text.strip()
        
        print(f"    ✅ Claude selected: {selected_url}")
        
        return selected_url
        
    except Exception as e:
        print(f"    ❌ API search error: {e}")
        return None


# ============================================================================
# ANTI-BOT DETECTION
# ============================================================================

async def check_antibot_measures(page: Page) -> Dict[str, bool]:
    """
    Lightweight check for immediate anti-bot measures
    Returns: dict with detection results
    """
    result = {
        'has_cloudflare': False,
        'has_captcha': False,
        'has_access_denied': False,
        'has_403_429': False,
        'requires_proxy': False
    }
    
    try:
        # Check page content
        content = await page.content()
        content_lower = content.lower()
        
        # Cloudflare detection
        if 'cloudflare' in content_lower or 'cf-browser-verification' in content_lower:
            result['has_cloudflare'] = True
            result['requires_proxy'] = True
        
        # CAPTCHA detection (multiple types)
        captcha_indicators = ['recaptcha', 'hcaptcha', 'captcha', 'g-recaptcha', 'h-captcha']
        if any(indicator in content_lower for indicator in captcha_indicators):
            result['has_captcha'] = True
        
        # Access denied messages
        denied_phrases = ['access denied', 'forbidden', 'not authorized', 'verify you are human', 'unusual traffic']
        if any(phrase in content_lower for phrase in denied_phrases):
            result['has_access_denied'] = True
            result['requires_proxy'] = True
        
    except Exception as e:
        print(f"  ⚠️  Error checking anti-bot measures: {e}")
    
    return result


# ============================================================================
# PAGE LOAD VERIFICATION
# ============================================================================

async def wait_for_page_fully_loaded(page: Page, timeout: int = 30000) -> bool:
    """
    Wait for page to be FULLY loaded and interactive
    Uses multiple strategies to ensure readiness
    """
    try:
        print(f"      ⏳ Waiting for page to fully load...")
        
        # Strategy 1: Wait for network to be idle
        await page.wait_for_load_state('networkidle', timeout=timeout)
        
        # Strategy 2: Wait for DOM to be ready
        await page.wait_for_load_state('domcontentloaded', timeout=timeout)
        
        # Strategy 3: Additional wait for dynamic content
        await asyncio.sleep(3)
        
        # Strategy 4: Check if document is ready
        is_ready = await page.evaluate('''() => {
            return document.readyState === 'complete';
        }''')
        
        if not is_ready:
            print(f"      ⚠️  Document not fully ready, waiting more...")
            await asyncio.sleep(2)
        
        print(f"      ✅ Page fully loaded and ready")
        return True
        
    except Exception as e:
        print(f"      ⚠️  Load verification error: {e}")
        return False


# ============================================================================
# COMPREHENSIVE PAGE ANALYZER (Jina + AgentQL Combined)
# ============================================================================

async def analyze_page_comprehensively(page: Page, page_url: str) -> Dict:
    """
    Get COMPLETE page intelligence in one pass:
    - Jina AI: Semantic understanding of page purpose
    - AgentQL: All interactive elements (buttons, links, forms)
    - Playwright: Fallback for any missed elements
    
    Returns comprehensive dict with ALL actionable information
    """
    print(f"    🔍 Running comprehensive page analysis...")
    
    # Ensure page is fully loaded first
    await wait_for_page_fully_loaded(page)
    
    analysis = {
        'url': page_url,
        'semantic_summary': '',
        'page_purpose': '',
        'has_search_form': False,
        'all_buttons': [],
        'all_links': [],
        'all_inputs': [],
        'all_tabs': [],
        'search_keywords_found': [],
        'raw_text_preview': '',
        'confidence_score': 0.0
    }
    
    try:
        # PART 1: Jina AI - Semantic understanding
        print(f"      → Jina AI: Analyzing page semantics...")
        jina_url = f"https://r.jina.ai/{page_url}"
        headers = {
            'Accept': 'text/plain',
            'X-Return-Format': 'text'
        }
        
        jina_response = requests.get(jina_url, headers=headers, timeout=30)
        jina_response.raise_for_status()
        jina_content = jina_response.text
        
        # Extract semantic info from Jina
        analysis['semantic_summary'] = jina_content[:800]  # First 800 chars
        analysis['raw_text_preview'] = jina_content[:2000]  # Longer preview
        
        # Detect page purpose and calculate confidence
        content_lower = jina_content.lower()
        
        # Calculate confidence based on keywords
        search_keywords = ['search', 'submit', 'go', 'find', 'lookup', 'properties', 'records', 'continue', 'begin', 'query']
        record_keywords = ['property', 'tax', 'parcel', 'owner', 'assessment', 'probate', 'court']
        
        search_matches = sum(1 for kw in search_keywords if kw in content_lower)
        record_matches = sum(1 for kw in record_keywords if kw in content_lower)
        
        analysis['confidence_score'] = min((search_matches + record_matches) / 10.0, 1.0)
        
        if search_matches >= 2:
            analysis['page_purpose'] = 'search_portal'
            analysis['has_search_form'] = True
        elif record_matches >= 2:
            analysis['page_purpose'] = 'records_database'
        else:
            analysis['page_purpose'] = 'informational'
        
        # Extract search-related keywords
        analysis['search_keywords_found'] = [kw for kw in search_keywords if kw in content_lower]
        
    except Exception as e:
        print(f"      ⚠️  Jina analysis failed: {e}")
        analysis['semantic_summary'] = 'Jina analysis failed'
    
    try:
        # PART 2: AgentQL - Extract ALL interactive elements
        print(f"      → AgentQL: Extracting interactive elements...")
        
        # Query for buttons
        button_query = """
        {
            all_buttons[] {
                text
                type
                aria_label
            }
        }
        """
        button_response = await page.query_data(button_query)
        if button_response and 'all_buttons' in button_response:
            for btn in button_response['all_buttons']:
                if btn.get('text'):
                    analysis['all_buttons'].append({
                        'text': btn['text'].strip(),
                        'type': btn.get('type', 'button'),
                        'aria_label': btn.get('aria_label', '')
                    })
        
        # Query for links
        link_query = """
        {
            all_links[] {
                text
                href
            }
        }
        """
        link_response = await page.query_data(link_query)
        if link_response and 'all_links' in link_response:
            for link in link_response['all_links'][:50]:  # Limit to 50 most relevant
                if link.get('text') and link.get('href'):
                    analysis['all_links'].append({
                        'text': link['text'].strip(),
                        'href': link['href'].strip()
                    })
        
        # Query for input fields
        input_query = """
        {
            all_inputs[] {
                placeholder
                name
                type
                label
            }
        }
        """
        input_response = await page.query_data(input_query)
        if input_response and 'all_inputs' in input_response:
            for inp in input_response['all_inputs']:
                if inp.get('placeholder') or inp.get('name'):
                    analysis['all_inputs'].append({
                        'placeholder': inp.get('placeholder', ''),
                        'name': inp.get('name', ''),
                        'type': inp.get('type', 'text'),
                        'label': inp.get('label', '')
                    })
        
    except Exception as e:
        print(f"      ⚠️  AgentQL extraction failed, using Playwright fallback: {e}")
        
        # PART 3: Playwright Fallback
        try:
            # Get buttons
            buttons = await page.query_selector_all('button, input[type="button"], input[type="submit"]')
            for btn in buttons[:20]:
                text = await btn.text_content() or await btn.get_attribute('value')
                if text:
                    analysis['all_buttons'].append({
                        'text': text.strip(),
                        'type': 'button',
                        'aria_label': ''
                    })
            
            # Get links
            links = await page.query_selector_all('a')
            for link in links[:50]:
                text = await link.text_content()
                href = await link.get_attribute('href')
                if text and href:
                    analysis['all_links'].append({
                        'text': text.strip(),
                        'href': href.strip()
                    })
            
            # Get inputs
            inputs = await page.query_selector_all('input[type="text"], input[type="search"], input:not([type])')
            for inp in inputs[:20]:
                placeholder = await inp.get_attribute('placeholder')
                name = await inp.get_attribute('name')
                if placeholder or name:
                    analysis['all_inputs'].append({
                        'placeholder': placeholder or '',
                        'name': name or '',
                        'type': 'text',
                        'label': ''
                    })
        
        except Exception as fallback_error:
            print(f"      ❌ Playwright fallback also failed: {fallback_error}")
    
    # Boost confidence if we found interactive elements
    if len(analysis['all_inputs']) > 0 and len(analysis['all_buttons']) > 0:
        analysis['confidence_score'] = min(analysis['confidence_score'] + 0.3, 1.0)
    
    # Summary stats
    print(f"      ✅ Analysis complete:")
    print(f"         - Page purpose: {analysis['page_purpose']}")
    print(f"         - Confidence: {analysis['confidence_score']:.2f}")
    print(f"         - Buttons found: {len(analysis['all_buttons'])}")
    print(f"         - Links found: {len(analysis['all_links'])}")
    print(f"         - Input fields: {len(analysis['all_inputs'])}")
    print(f"         - Search keywords: {len(analysis['search_keywords_found'])}")
    
    return analysis


# ============================================================================
# ENHANCED CLAUDE DECISION MAKER (Single Comprehensive Call)
# ============================================================================

def plan_actions_with_full_context(
    comprehensive_analysis: Dict, 
    search_goal: str, 
    search_type: str,
    previous_errors: List[str] = None
) -> List[Dict[str, str]]:
    """
    Give Claude EVERYTHING at once and let it decide the best action plan
    NOW WITH ERROR FEEDBACK for adaptive retry logic
    
    Args:
        comprehensive_analysis: Full page analysis from analyze_page_comprehensively()
        search_goal: What we're trying to accomplish
        search_type: 'documents', 'tax', or 'probate'
        previous_errors: List of errors from previous attempts (for retry intelligence)
    
    Returns:
        List of action steps with full context
    """
    
    if previous_errors is None:
        previous_errors = []
    
    # Format all the data for Claude in one comprehensive prompt
    buttons_text = "\n".join([
        f"  BUTTON: '{btn['text']}' (type: {btn['type']})" 
        for btn in comprehensive_analysis['all_buttons'][:20]
    ]) or "  [No buttons found]"
    
    links_text = "\n".join([
        f"  LINK: '{link['text']}' → {link['href']}" 
        for link in comprehensive_analysis['all_links'][:30]
    ]) or "  [No links found]"
    
    inputs_text = "\n".join([
        f"  INPUT: {inp['name'] or inp['placeholder']} (type: {inp['type']})" 
        for inp in comprehensive_analysis['all_inputs'][:15]
    ]) or "  [No input fields found]"
    
    keywords_text = ", ".join(comprehensive_analysis['search_keywords_found']) or "none"
    
    # Format previous errors if any
    errors_section = ""
    if previous_errors:
        errors_section = f"""
─────────────────────────────────────────────────────────────────
⚠️  PREVIOUS ERRORS (Learn from these and adjust your approach):
─────────────────────────────────────────────────────────────────
{chr(10).join([f"  - {error}" for error in previous_errors])}

IMPORTANT: Analyze these errors and adjust your element selection strategy.
For example:
- If "element not found", try more generic descriptions or look for alternatives
- If "NoneType error", the element may need more time to load - add WAIT actions
- If "fill failed", try different input field selectors
"""
    
    # Comprehensive prompt with ALL information
    prompt = f"""You are an expert web navigation planner with COMPLETE visibility into a webpage's structure.

═══════════════════════════════════════════════════════════════════
CURRENT PAGE ANALYSIS
═══════════════════════════════════════════════════════════════════

URL: {comprehensive_analysis['url']}
Page Purpose: {comprehensive_analysis['page_purpose']}
Confidence Score: {comprehensive_analysis['confidence_score']:.2f}
Has Search Form: {comprehensive_analysis['has_search_form']}
Search Keywords Found: {keywords_text}

─────────────────────────────────────────────────────────────────
SEMANTIC UNDERSTANDING (From Jina AI):
─────────────────────────────────────────────────────────────────
{comprehensive_analysis['semantic_summary']}

─────────────────────────────────────────────────────────────────
ALL AVAILABLE BUTTONS:
─────────────────────────────────────────────────────────────────
{buttons_text}

─────────────────────────────────────────────────────────────────
ALL AVAILABLE LINKS:
─────────────────────────────────────────────────────────────────
{links_text}

─────────────────────────────────────────────────────────────────
ALL INPUT FIELDS:
─────────────────────────────────────────────────────────────────
{inputs_text}
{errors_section}
═══════════════════════════════════════════════════════════════════
YOUR TASK
═══════════════════════════════════════════════════════════════════

Search Goal: {search_goal}
Target: {search_type} records search functionality

DECISION HIERARCHY (CRITICAL - Follow in order):

1. **IF we're already on a search page with input fields and a search button:**
   → Add WAIT action first (let page stabilize)
   → Fill the form and click the search button (DON'T navigate away!)
   
2. **IF there's a button/tab that clearly leads to search (contains keywords like "search", "lookup", "properties", "records"):**
   → Add WAIT action first
   → Click that button FIRST
   
3. **IF there are input fields but no obvious search button:**
   → Look for submit buttons, "Go" buttons, or buttons near the inputs
   
4. **IF there's a link that clearly leads to a search portal:**
   → Navigate to that link
   
5. **IF nothing matches above:**
   → Return empty array []

RESPONSE FORMAT:

Respond with a JSON array of action steps. Each step must specify:
- "action": One of ["BUTTON_CLICK", "FORM_FILL", "LINK_NAVIGATE", "WAIT"]
- "description": What you're doing and why
- "element": Exact text/identifier OR generic description (e.g., "any input for name or owner")
- "priority": "HIGH", "MEDIUM", or "LOW"
- "wait_after": seconds to wait after this action (default 2)

Example response:
[
  {{
    "action": "WAIT",
    "description": "Wait for page to fully stabilize before interaction",
    "element": "page",
    "priority": "HIGH",
    "wait_after": 3
  }},
  {{
    "action": "BUTTON_CLICK",
    "description": "Click the 'Search for Properties' button because it directly leads to search functionality",
    "element": "Search for Properties",
    "priority": "HIGH",
    "wait_after": 2
  }},
  {{
    "action": "FORM_FILL",
    "description": "Fill any name/owner input field with test data",
    "element": "input field for name or owner or last name",
    "value": "Smith",
    "priority": "HIGH",
    "wait_after": 1
  }},
  {{
    "action": "BUTTON_CLICK",
    "description": "Submit the search form",
    "element": "submit or search button",
    "priority": "HIGH",
    "wait_after": 3
  }}
]

IMPORTANT:
- ALWAYS start with a WAIT action to let the page stabilize
- Maximum 6 steps
- Prioritize HIGH priority actions that directly accomplish the goal
- Be flexible with element descriptions - use generic terms if specific text isn't found
- If previous errors occurred, adjust your approach (try alternative selectors)
- Add appropriate wait_after times, especially after clicks
- Respond with ONLY the JSON array, nothing else

Now analyze the above data and provide your action plan:"""

    try:
        print(f"    🧠 Asking Claude to analyze and plan...")
        if previous_errors:
            print(f"       (Providing {len(previous_errors)} previous errors for learning)")
        
        response = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )
        
        response_text = response.content[0].text.strip()
        
        # Clean markdown formatting
        if response_text.startswith('```'):
            response_text = response_text.split('```')[1]
            if response_text.startswith('json'):
                response_text = response_text[4:]
            response_text = response_text.strip('`').strip()
        
        steps = json.loads(response_text)
        
        if not isinstance(steps, list):
            print(f"  ⚠️  Claude returned non-list response")
            return []
        
        # Validate and sort by priority
        validated_steps = []
        for step in steps:
            if isinstance(step, dict) and 'action' in step and 'element' in step:
                # Set default wait_after if not specified
                if 'wait_after' not in step:
                    step['wait_after'] = 2
                validated_steps.append(step)
        
        # Sort by priority
        priority_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
        validated_steps.sort(key=lambda x: priority_order.get(x.get('priority', 'LOW'), 2))
        
        print(f"    ✅ Claude planned {len(validated_steps)} prioritized actions")
        for i, step in enumerate(validated_steps, 1):
            print(f"       {i}. [{step.get('priority', 'LOW')}] {step['action']}: {step['description'][:60]}...")
        
        return validated_steps
    
    except json.JSONDecodeError as e:
        print(f"  ❌ Claude response was not valid JSON: {e}")
        print(f"  Response was: {response_text[:200]}...")
        return []
    except Exception as e:
        print(f"  ❌ Claude planning error: {e}")
        import traceback
        traceback.print_exc()
        return []


# ============================================================================
# ENHANCED AGENTQL EXECUTOR (With Error Reporting)
# ============================================================================

async def execute_action_step(page: Page, step: Dict[str, str], timeout: int = 15000) -> Tuple[bool, Optional[str]]:
    """
    Execute navigation step based on action type (BUTTON, FORM, LINK)
    NOW RETURNS ERROR MESSAGES for Claude to learn from
    
    Args:
        page: Playwright page
        step: Action step dict with 'action', 'description', 'element'
        timeout: Timeout in milliseconds
    
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    try:
        action_type = step.get('action', 'LINK_NAVIGATE')
        description = step.get('description', '')
        element_desc = step.get('element', description)
        wait_after = step.get('wait_after', 2)
        
        print(f"    ▶️  [{action_type}] {description}")
        
        if action_type == "WAIT":
            # Simple wait action
            wait_seconds = step.get('wait_after', 3)
            print(f"       ⏳ Waiting {wait_seconds} seconds...")
            await asyncio.sleep(wait_seconds)
            return True, None
        
        elif action_type == "BUTTON_CLICK":
            # Priority: Click buttons (search, submit, etc.)
            query = f"""
            {{
                action_button(description: "{element_desc}")
            }}
            """
            response = await page.query_elements(query)
            
            if response and hasattr(response, 'action_button'):
                await response.action_button.click()
                await asyncio.sleep(wait_after)
                await page.wait_for_load_state('networkidle', timeout=timeout)
                return True, None
            else:
                error_msg = f"Button not found with description: '{element_desc}'"
                print(f"    ⚠️  {error_msg}")
                return False, error_msg
        
        elif action_type == "FORM_FILL":
            # Fill form fields with validation
            field_name = step.get('field', element_desc)
            value = step.get('value', 'Smith')
            
            query = f"""
            {{
                input_field(description: "{field_name}")
            }}
            """
            response = await page.query_elements(query)
            
            if response and hasattr(response, 'input_field'):
                # Verify element is not None before filling
                if response.input_field is None:
                    error_msg = f"Input field found but is None (may not be loaded yet): '{field_name}'"
                    print(f"    ⚠️  {error_msg}")
                    return False, error_msg
                
                await response.input_field.fill(value)
                await asyncio.sleep(wait_after)
                return True, None
            else:
                error_msg = f"Input field not found with description: '{field_name}'"
                print(f"    ⚠️  {error_msg}")
                return False, error_msg
        
        elif action_type == "LINK_NAVIGATE":
            # Navigate to new URL
            query = f"""
            {{
                nav_link(description: "{element_desc}")
            }}
            """
            response = await page.query_elements(query)
            
            if response and hasattr(response, 'nav_link'):
                await response.nav_link.click()
                await asyncio.sleep(wait_after)
                await page.wait_for_load_state('networkidle', timeout=timeout)
                return True, None
            else:
                error_msg = f"Link not found with description: '{element_desc}'"
                print(f"    ⚠️  {error_msg}")
                return False, error_msg
        
        return False, f"Unknown action type: {action_type}"
        
    except Exception as e:
        error_msg = f"Action execution exception: {str(e)}"
        print(f"    ❌ {error_msg}")
        return False, error_msg


# ============================================================================
# SEARCH FORM DISCOVERY
# ============================================================================

async def discover_search_form(page: Page) -> Optional[Dict[str, str]]:
    """
    Discover search form fields using AgentQL semantic queries
    Returns: Dict with selector information or None
    """
    try:
        # Ensure page is loaded
        await wait_for_page_fully_loaded(page)
        
        # Define the search form structure we're looking for
        query = """
        {
            search_form {
                name_input(description: "input field for owner name, person name, or last name")
                search_button(description: "the primary search button or submit button")
            }
        }
        """
        
        response = await page.query_data(query)
        
        if response and 'search_form' in response:
            form = response['search_form']
            if form.get('name_input') and form.get('search_button'):
                return {
                    'owner_name_input': str(form['name_input']),
                    'search_button': str(form['search_button'])
                }
        
        print(f"    ⚠️  Search form not found or incomplete")
        return None
        
    except Exception as e:
        print(f"    ❌ Form discovery error: {e}")
        return None


# ============================================================================
# SEARCH VALIDATION
# ============================================================================

async def validate_search_results(page: Page, search_type: str, test_name: str = "Smith") -> Tuple[bool, float]:
    """
    Perform a test search and validate results contain expected data
    Returns: (is_valid, confidence_score)
    """
    try:
        print(f"    🔍 Performing test search with name: {test_name}")
        
        # Ensure page is loaded
        await wait_for_page_fully_loaded(page)
        
        # Fill the form
        name_input_query = """
        {
            name_field(description: "input field for name or last name")
        }
        """
        input_response = await page.query_elements(name_input_query)
        
        if input_response and hasattr(input_response, 'name_field') and input_response.name_field is not None:
            await input_response.name_field.fill(test_name)
        else:
            print(f"    ⚠️  Could not find name input field")
            return False, 0.0
        
        await asyncio.sleep(1)
        
        # Click search button
        search_button_query = """
        {
            search_btn(description: "search button or submit button")
        }
        """
        button_response = await page.query_elements(search_button_query)
        
        if button_response and hasattr(button_response, 'search_btn') and button_response.search_btn is not None:
            await button_response.search_btn.click()
            await asyncio.sleep(2)
            await page.wait_for_load_state('networkidle', timeout=15000)
        else:
            print(f"    ⚠️  Could not find search button")
            return False, 0.0
        
        # Wait a bit for results
        await asyncio.sleep(3)
        
        # Check for validation indicators
        content = await page.content()
        content_lower = content.lower()
        
        indicators = SEARCH_TYPES[search_type]['validation_indicators']
        matches = sum(1 for indicator in indicators if indicator.lower() in content_lower)
        
        # Calculate confidence score
        confidence = min(matches / len(indicators), 1.0)
        
        # Also check for "no results" messages
        no_results_phrases = ['no results', 'no records found', 'no matches', '0 results']
        has_no_results = any(phrase in content_lower for phrase in no_results_phrases)
        
        if has_no_results:
            print(f"    ℹ️  Got 'no results' message, but form seems functional")
            # Form works, just no results for "Smith"
            confidence = max(confidence, 0.6)  # Moderate confidence
        
        is_valid = confidence >= 0.4  # At least 40% match threshold
        
        print(f"    {'✅' if is_valid else '❌'} Validation: {matches}/{len(indicators)} indicators found (confidence: {confidence:.2f})")
        
        return is_valid, confidence
        
    except Exception as e:
        print(f"    ❌ Validation error: {e}")
        return False, 0.0


# ============================================================================
# MAIN SCOUT LOGIC WITH INTELLIGENT RETRY
# ============================================================================

async def scout_county_search_type(
    page: Page,
    county: County,
    search_type: str,
    session
) -> bool:
    """
    Scout a single search type for a county with INTELLIGENT RETRY on high-confidence pages
    Returns: True if successful, False if needs human review
    """
    print(f"\n🎯 Scouting {county.name}, {county.state} - {search_type.upper()}")
    
    search_config = SEARCH_TYPES[search_type]
    max_url_attempts = 3  # Try up to 3 different URLs
    max_retries_per_url = 5  # On high-confidence pages, retry up to 5 times
    
    for url_attempt in range(1, max_url_attempts + 1):
        try:
            print(f"\n  📍 URL Attempt {url_attempt}/{max_url_attempts}")
            
            # Step 1: Find starting URL via Google API
            start_url = api_search_for_county_records(
                county.name, county.state, search_type
            )
            
            if not start_url:
                print(f"  ⚠️  No URL found via API")
                continue
            
            # Step 2: Navigate to the site
            print(f"  🌐 Navigating to: {start_url}")
            await page.goto(start_url, wait_until='networkidle', timeout=20000)
            await wait_for_page_fully_loaded(page)
            
            # Step 3: Check for anti-bot measures
            antibot_result = await check_antibot_measures(page)
            
            if antibot_result['has_cloudflare']:
                print(f"  🛡️  Cloudflare detected")
                setattr(county, search_config['requires_proxy_field'], True)
                county.requires_proxy = True
            
            if antibot_result['has_captcha']:
                print(f"  🤖 CAPTCHA detected")
                county.requires_captcha = True
                
                # Try to solve with CapSolver
                if captcha_solver:
                    page_content = await page.content()
                    captcha_type, site_key = captcha_solver.detect_captcha_type(page_content, page)
                    if captcha_type:
                        token = captcha_solver.solve(captcha_type, site_key, page.url)
                        if token:
                            await page.evaluate(f'''() => {{
                                const textarea = document.getElementById('g-recaptcha-response');
                                if (textarea) {{
                                    textarea.innerHTML = "{token}";
                                    textarea.value = "{token}";
                                }}
                            }}''')
                            print(f"  ✅ CAPTCHA solved with CapSolver")
                            await asyncio.sleep(2)
            
            if antibot_result['has_access_denied']:
                print(f"  🚫 Access denied - likely needs proxy")
                setattr(county, search_config['requires_proxy_field'], True)
                county.requires_proxy = True
                continue
            
            # Step 4: Comprehensive page analysis
            comprehensive_analysis = await analyze_page_comprehensively(page, page.url)
            
            page_confidence = comprehensive_analysis['confidence_score']
            print(f"  📊 Page confidence: {page_confidence:.2f}")
            
            # Determine if this is a high-confidence page worth retrying
            is_high_confidence = page_confidence >= 0.6
            retry_limit = max_retries_per_url if is_high_confidence else 2
            
            if is_high_confidence:
                print(f"  ✨ HIGH CONFIDENCE PAGE - Will retry up to {retry_limit} times on errors")
            
            # Step 5: Retry loop with error feedback to Claude
            error_history = []
            
            for retry in range(1, retry_limit + 1):
                print(f"\n  🔄 Execution Attempt {retry}/{retry_limit}")
                
                # Claude plans with full context + previous errors
                action_steps = plan_actions_with_full_context(
                    comprehensive_analysis,
                    search_goal=f"Find and interact with the {search_type} records search functionality",
                    search_type=search_type,
                    previous_errors=error_history if retry > 1 else None
                )
                
                if not action_steps:
                    print(f"  ⚠️  No valid action plan generated")
                    break
                
                # Execute actions and collect errors
                all_actions_successful = True
                current_run_errors = []
                
                for step in action_steps:
                    success, error_msg = await execute_action_step(page, step)
                    if not success:
                        all_actions_successful = False
                        current_run_errors.append(error_msg)
                        # Don't break - try remaining steps
                
                # If we got errors, add them to history for next retry
                if current_run_errors:
                    error_history.extend(current_run_errors)
                    print(f"  ⚠️  Encountered {len(current_run_errors)} error(s), retry {retry}/{retry_limit}")
                    
                    if retry < retry_limit:
                        print(f"  🔄 Will retry with error feedback to Claude...")
                        await asyncio.sleep(2)
                        continue
                    else:
                        print(f"  ⚠️  Max retries reached")
                        break
                
                # If all actions succeeded, try to discover and validate the form
                if all_actions_successful:
                    print(f"  ✅ All actions executed successfully")
                    
                    # Step 6: Discover search form
                    form_selectors = await discover_search_form(page)
                    
                    if not form_selectors:
                        error_history.append("Search form not found after navigation")
                        if retry < retry_limit:
                            print(f"  ⚠️  Search form not found, retrying...")
                            continue
                        else:
                            break
                    
                    print(f"  ✅ Search form discovered!")
                    
                    # Step 7: Validate with test search
                    is_valid, confidence = await validate_search_results(page, search_type)
                    
                    if not is_valid:
                        error_history.append(f"Validation failed (confidence: {confidence:.2f})")
                        if retry < retry_limit:
                            print(f"  ⚠️  Validation failed, retrying...")
                            continue
                        else:
                            break
                    
                    # SUCCESS!
                    print(f"\n  🎉 SUCCESS! Found valid {search_type} search")
                    print(f"     URL: {page.url}")
                    print(f"     Confidence: {confidence:.2f}")
                    print(f"     Required {retry} attempt(s)")
                    
                    # Update county record
                    setattr(county, search_config['url_field'], page.url)
                    setattr(county, search_config['selectors_field'], form_selectors)
                    county.scouted_at = datetime.utcnow()
                    county.scout_confidence = Decimal(str(round(confidence, 2)))
                    county.scout_notes = f"Successfully scouted {search_type} on URL attempt {url_attempt}, execution attempt {retry}"
                    
                    session.commit()
                    
                    return True
            
            # If we exhausted retries on this URL, try next URL
            print(f"  ⚠️  Exhausted retries on this URL, trying next...")
            
        except Exception as e:
            print(f"  ❌ URL Attempt {url_attempt} error: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Failed after all URL attempts
    print(f"\n  ⚠️  Failed after {max_url_attempts} URL attempts - marking for human review")
    county.scout_notes = f"Failed to scout {search_type} after {max_url_attempts} URL attempts - needs human review"
    county.scout_confidence = Decimal('0.00')
    session.commit()
    
    return False


# ============================================================================
# MAIN EXECUTION
# ============================================================================

async def scout_all_counties():
    """
    Main loop: Scout all counties for all search types
    """
    print("🚀 Starting County Scout Agent with Intelligent Retry")
    print("=" * 60)
    
    session = SessionLocal()
    
    try:
        counties = session.query(County).filter(County.is_active == True).all()
        
        print(f"📋 Found {len(counties)} counties to scout")
        
        async with async_playwright() as p:
            # Launch browser with stealth settings
            browser = await p.chromium.launch(
                headless=False,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox'
                ]
            )
            page = await agentql.wrap_async(await browser.new_page())
            
            # ENABLE STEALTH MODE
            await page.enable_stealth_mode()
            
            for county in counties:
                print(f"\n{'='*60}")
                print(f"🏛️  Processing: {county.name} County, {county.state}")
                print(f"{'='*60}")
                
                # Scout each search type sequentially
                for search_type in ['documents', 'tax', 'probate']:
                    url_field = SEARCH_TYPES[search_type]['url_field']
                    if getattr(county, url_field):
                        print(f"  ⏭️  {search_type.upper()} already scouted, skipping")
                        continue
                    
                    await scout_county_search_type(page, county, search_type, session)
                    await asyncio.sleep(3)
            
            await browser.close()
        
        print(f"\n{'='*60}")
        print("✅ Scout completed!")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        session.close()


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    asyncio.run(scout_all_counties())