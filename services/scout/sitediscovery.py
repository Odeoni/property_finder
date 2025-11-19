"""
scout_agent.py

Hybrid AgentQL Scout for County Public Records Discovery
Uses Claude for planning navigation steps, AgentQL for execution
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

# Claude prompt for navigation planning
CLAUDE_PLAN_PROMPT = """You are a navigation planner for web scraping. Your task is to analyze a webpage's navigation links and create a step-by-step plan to reach specific information through a search form, essentially validating the search form once that information is reached.

You will be given:
1. A search goal (e.g., "Find a taxes owed amount annually broken down for a test name/property in a specific county, doing so in order to validate and find the URL for specific tax searches in said county, upon which a static scraper could then be built. You are extracting the website structure and information along the way in order to make the static scraper robust and functional")
2. A list of navigation links with their text and href attributes

Your response MUST be a valid JSON array of natural language navigation steps. Each step should describe either clicking on a specific link, entering a name, extracting key information, completing a simple captcha, or identifying other website components such as the type of captcha present or the need for a proxy.

Example response format:
["Click the link with text 'Online Services'", "Click the link containing 'Property Search'", "Click the button labeled 'Search Records'", "Write the name 'Smith' then click search", "Find taxes due broken down by year", "Find closed and/or open probate cases for (individual)", "Find all liens and judgmenets on this property"]

Rules:
- Only include steps that navigate through links visible on the page or retrieve website information
- Be specific about link text but allow for partial matches (e.g., "containing" rather than exact match)
- Keep steps simple and actionable
- Maximum 5 steps
- If the goal seems unreachable with the available links, return an empty array: []

Search Goal: {search_goal}

Available Links (text -> href):
{links}

Respond with ONLY a JSON array, nothing else."""


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
# CLAUDE NAVIGATION PLANNER
# ============================================================================

async def get_navigation_links(page: Page, max_links: int = 100) -> List[Dict[str, str]]:
    """
    Extract navigation links from page (text + href)
    Limits to max_links to stay under Claude token limits
    """
    links = []
    try:
        # Use AgentQL to get all navigation-relevant links
        query = """
        {
            navigation_links[] {
                text
                href
            }
        }
        """
        response = await page.query_data(query)
        
        if response and 'navigation_links' in response:
            for link in response['navigation_links'][:max_links]:
                if link.get('text') and link.get('href'):
                    links.append({
                        'text': link['text'].strip(),
                        'href': link['href'].strip()
                    })
    except Exception as e:
        print(f"  ⚠️  AgentQL link extraction failed, falling back to Playwright: {e}")
        # Fallback to Playwright
        try:
            all_links = await page.query_selector_all('a')
            for link in all_links[:max_links]:
                text = await link.text_content()
                href = await link.get_attribute('href')
                if text and href:
                    links.append({'text': text.strip(), 'href': href.strip()})
        except Exception as fallback_error:
            print(f"  ❌ Fallback link extraction also failed: {fallback_error}")
    
    return links


def plan_navigation_with_claude(links: List[Dict[str, str]], search_goal: str) -> List[str]:
    """
    Use Claude to plan navigation steps
    Returns: List of natural language steps
    """
    # Format links for Claude (keep under 1KB)
    links_text = "\n".join([f"'{link['text']}' -> {link['href']}" for link in links[:50]])
    
    # Truncate if needed
    if len(links_text) > 1000:
        links_text = links_text[:1000] + "\n... (truncated)"
    
    prompt = CLAUDE_PLAN_PROMPT.format(
        search_goal=search_goal,
        links=links_text
    )
    
    try:
        response = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        
        # Parse response
        response_text = response.content[0].text.strip()
        
        # Extract JSON array (handle potential markdown formatting)
        if response_text.startswith('```'):
            # Remove markdown code blocks
            response_text = response_text.split('```')[1]
            if response_text.startswith('json'):
                response_text = response_text[4:]
        
        steps = json.loads(response_text)
        
        if not isinstance(steps, list):
            print(f"  ⚠️  Claude returned non-list response: {steps}")
            return []
        
        return steps
    
    except Exception as e:
        print(f"  ❌ Claude planning error: {e}")
        return []


# ============================================================================
# AGENTQL NAVIGATION EXECUTOR
# ============================================================================

async def execute_navigation_step(page: Page, step: str, timeout: int = 10000) -> bool:
    """
    Execute a single navigation step using AgentQL
    Returns: True if successful, False otherwise
    """
    try:
        print(f"    ▶️  Executing: {step}")
        
        # Convert natural language step to AgentQL query
        # AgentQL can handle natural language directly in query_elements
        query = f"""
        {{
            target_element(description: "{step}")
        }}
        """
        
        response = await page.query_elements(query)
        
        if response and hasattr(response, 'target_element'):
            element = response.target_element
            await element.click()
            await page.wait_for_load_state('networkidle', timeout=timeout)
            return True
        else:
            print(f"    ⚠️  Element not found for step: {step}")
            return False
            
    except Exception as e:
        print(f"    ❌ Navigation step failed: {e}")
        return False


# ============================================================================
# SEARCH FORM DISCOVERY
# ============================================================================

async def discover_search_form(page: Page) -> Optional[Dict[str, str]]:
    """
    Discover search form fields using AgentQL semantic queries
    Returns: Dict with selector information or None
    """
    try:
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
        
        # Fill the form
        name_input_query = """
        {
            name_field(description: "input field for name or last name")
        }
        """
        input_response = await page.query_elements(name_input_query)
        
        if input_response and hasattr(input_response, 'name_field'):
            await input_response.name_field.fill(test_name)
        else:
            print(f"    ⚠️  Could not find name input field")
            return False, 0.0
        
        # Click search button
        search_button_query = """
        {
            search_btn(description: "search button or submit button")
        }
        """
        button_response = await page.query_elements(search_button_query)
        
        if button_response and hasattr(button_response, 'search_btn'):
            await button_response.search_btn.click()
            await page.wait_for_load_state('networkidle', timeout=15000)
        else:
            print(f"    ⚠️  Could not find search button")
            return False, 0.0
        
        # Wait a bit for results
        await asyncio.sleep(2)
        
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
# MAIN SCOUT LOGIC
# ============================================================================

async def scout_county_search_type(
    page: Page,
    county: County,
    search_type: str,
    session
) -> bool:
    """
    Scout a single search type for a county
    Returns: True if successful, False if needs human review
    """
    print(f"\n🎯 Scouting {county.name}, {county.state} - {search_type.upper()}")
    
    search_config = SEARCH_TYPES[search_type]
    max_attempts = 3  # Reduced since we're using API
    
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"\n  📍 Attempt {attempt}/{max_attempts}")
            
            # Step 1: Find starting URL via Google API (no browser needed!)
            start_url = api_search_for_county_records(
                county.name, county.state, search_type
            )
            
            if not start_url:
                print(f"  ⚠️  No URL found via API")
                continue
            
            # Step 2: Navigate to the site
            print(f"  🌐 Navigating to: {start_url}")
            await page.goto(start_url, wait_until='networkidle', timeout=20000)
            await asyncio.sleep(2)
            
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
            
            # Step 4: Extract navigation links
            links = await get_navigation_links(page)
            
            if not links:
                print(f"  ⚠️  No navigation links found")
                continue
            
            print(f"  📊 Found {len(links)} navigation links")
            
            # Step 5: Claude plans the navigation
            search_goal = f"Find the {search_type} records search page where users can search by name or address with name preferred"
            nav_steps = plan_navigation_with_claude(links, search_goal)
            
            if not nav_steps:
                print(f"  ⚠️  Claude could not plan navigation")
                continue
            
            print(f"  🧭 Claude planned {len(nav_steps)} steps")
            
            # Step 6: Execute navigation steps with AgentQL
            navigation_successful = True
            for step in nav_steps:
                success = await execute_navigation_step(page, step)
                if not success:
                    navigation_successful = False
                    break
                await asyncio.sleep(1)
            
            if not navigation_successful:
                print(f"  ⚠️  Navigation incomplete, trying next result")
                continue
            
            # Step 7: Discover search form
            form_selectors = await discover_search_form(page)
            
            if not form_selectors:
                print(f"  ⚠️  Search form not found")
                continue
            
            print(f"  ✅ Search form discovered!")
            
            # Step 8: Validate with test search
            is_valid, confidence = await validate_search_results(page, search_type)
            
            if not is_valid:
                print(f"  ⚠️  Validation failed, trying next result")
                continue
            
            # SUCCESS! Save to database
            print(f"\n  🎉 SUCCESS! Found valid {search_type} search")
            print(f"     URL: {page.url}")
            print(f"     Confidence: {confidence:.2f}")
            
            # Update county record
            setattr(county, search_config['url_field'], page.url)
            setattr(county, search_config['selectors_field'], form_selectors)
            county.scouted_at = datetime.utcnow()
            county.scout_confidence = Decimal(str(round(confidence, 2)))
            county.scout_notes = f"Successfully scouted {search_type} on attempt {attempt}"
            
            session.commit()
            
            return True
            
        except Exception as e:
            print(f"  ❌ Attempt {attempt} error: {e}")
            continue
    
    # Failed after max attempts
    print(f"\n  ⚠️  Failed after {max_attempts} attempts - marking for human review")
    county.scout_notes = f"Failed to scout {search_type} after {max_attempts} attempts - needs human review"
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
    print("🚀 Starting County Scout Agent")
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