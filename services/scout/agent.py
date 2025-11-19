"""
Scout Agent - Autonomous Scraper Generator

This agent uses Claude + Playwright to:
1. Explore a county website (tax, probate, or property)
2. Generate a static Python scraper
3. Test it with sample names from database
4. Iteratively fix errors until it works
5. Save the final working scraper

The agent learns from your existing Dallas scrapers as examples.
"""

import os
import sys
import json
import time
import subprocess
import traceback
import requests
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright
import anthropic

# Add project root to path (go up 2 levels: scout -> services -> root)
project_root = Path(__file__).parent.parent.parent.resolve()
sys.path.insert(0, str(project_root))

from database.models import SessionLocal, County, DeceasedIndividual

# ============================================================================
# CONFIGURATION
# ============================================================================

ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
if not ANTHROPIC_API_KEY:
    raise ValueError("ANTHROPIC_API_KEY environment variable not set!")

JINA_API_KEY = os.getenv('JINA_API_KEY')
if not JINA_API_KEY:
    print("⚠️ JINA_API_KEY not set - will use raw HTML (higher token usage)")

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent  # Go up to project root
SCRAPERS_DIR = PROJECT_ROOT / "scrapers"
EXAMPLES_DIR = PROJECT_ROOT / "services" / "scout" / "examples"

# Create directories
SCRAPERS_DIR.mkdir(exist_ok=True)
EXAMPLES_DIR.mkdir(exist_ok=True)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def take_screenshot_base64(page):
    """Take screenshot and convert to base64 for Claude"""
    import base64
    screenshot_bytes = page.screenshot()
    return base64.b64encode(screenshot_bytes).decode('utf-8')


def get_page_content_with_jina(current_url):
    """
    Use Jina.ai Reader API to convert page HTML to clean markdown.
    This dramatically reduces token usage compared to raw HTML.
    
    Jina Reader API: https://jina.ai/reader
    Usage: GET https://r.jina.ai/{url}
    """
    if not JINA_API_KEY:
        return None, None
    
    try:
        import requests
        
        # Jina Reader API endpoint
        jina_url = f"https://r.jina.ai/{current_url}"
        
        headers = {
            'Authorization': f'Bearer {JINA_API_KEY}',
            'X-Return-Format': 'markdown',
            'X-With-Generated-Alt': 'true',
            'X-With-Links-Summary': 'true',
            # Request more technical details
            'X-With-Iframe': 'true',
            'X-Retain-Images': 'none',  # Skip images to save tokens
        }
        
        response = requests.get(jina_url, headers=headers, timeout=15)
        
        if response.status_code == 200:
            markdown_content = response.text[:20000]
            
            # Also try to get the raw HTML for targeted extraction
            # Jina returns both if we parse headers
            raw_html = None
            if 'X-Responded-With' in response.headers:
                raw_html = response.text
            
            return markdown_content, raw_html
        else:
            print(f"   ⚠️ Jina API error: {response.status_code}")
            return None, None
            
    except Exception as e:
        print(f"   ⚠️ Jina API failed: {e}")
        return None, None


def extract_targeted_html(page, element_types=['form', 'button', 'input', 'table']):
    """
    Extract ONLY specific HTML elements with their complete paths/selectors.
    This gives Claude precise selector information without overwhelming with full HTML.
    
    Args:
        page: Playwright page object
        element_types: List of HTML tags to extract
    
    Returns:
        Dict with extracted elements and their selectors
    """
    try:
        targeted_html = {
            'forms': [],
            'buttons': [],
            'inputs': [],
            'tables': [],
            'selects': []
        }
        
        # Extract all forms with complete structure
        forms = page.locator('form').all()
        for i, form in enumerate(forms):
            try:
                form_html = form.evaluate('el => el.outerHTML')
                form_id = form.get_attribute('id') or f'form_{i}'
                form_action = form.get_attribute('action') or 'N/A'
                form_method = form.get_attribute('method') or 'GET'
                
                targeted_html['forms'].append({
                    'index': i,
                    'id': form_id,
                    'selector': f'form#{form_id}' if form.get_attribute('id') else f'form:nth-of-type({i+1})',
                    'action': form_action,
                    'method': form_method,
                    'html': form_html[:2000]  # Limit each form to 2k chars
                })
            except:
                continue
        
        # Extract all buttons with paths
        buttons = page.locator('button, input[type="submit"], input[type="button"]').all()
        for i, button in enumerate(buttons):
            try:
                btn_id = button.get_attribute('id')
                btn_class = button.get_attribute('class')
                btn_type = button.get_attribute('type')
                btn_text = button.inner_text() if button.evaluate('el => el.tagName') == 'BUTTON' else button.get_attribute('value')
                btn_name = button.get_attribute('name')
                
                # Build best selector
                if btn_id:
                    selector = f'#{btn_id}'
                elif btn_name:
                    selector = f'[name="{btn_name}"]'
                elif btn_class:
                    selector = f'button.{btn_class.split()[0]}' if 'button' in str(button.evaluate('el => el.tagName')).lower() else f'input.{btn_class.split()[0]}'
                else:
                    selector = f'button:nth-of-type({i+1})'
                
                targeted_html['buttons'].append({
                    'index': i,
                    'id': btn_id,
                    'name': btn_name,
                    'class': btn_class,
                    'type': btn_type,
                    'text': btn_text,
                    'selector': selector,
                    'full_selector': f'{selector}[type="{btn_type}"]' if btn_type else selector
                })
            except:
                continue
        
        # Extract all input fields with paths
        inputs = page.locator('input[type="text"], input[type="search"], input:not([type])').all()
        for i, inp in enumerate(inputs):
            try:
                inp_id = inp.get_attribute('id')
                inp_name = inp.get_attribute('name')
                inp_class = inp.get_attribute('class')
                inp_placeholder = inp.get_attribute('placeholder')
                inp_type = inp.get_attribute('type') or 'text'
                
                # Build best selector
                if inp_id:
                    selector = f'#{inp_id}'
                elif inp_name:
                    selector = f'input[name="{inp_name}"]'
                elif inp_placeholder:
                    selector = f'input[placeholder="{inp_placeholder}"]'
                else:
                    selector = f'input:nth-of-type({i+1})'
                
                targeted_html['inputs'].append({
                    'index': i,
                    'id': inp_id,
                    'name': inp_name,
                    'class': inp_class,
                    'type': inp_type,
                    'placeholder': inp_placeholder,
                    'selector': selector
                })
            except:
                continue
        
        # Extract select dropdowns
        selects = page.locator('select').all()
        for i, select in enumerate(selects):
            try:
                sel_id = select.get_attribute('id')
                sel_name = select.get_attribute('name')
                
                options = select.locator('option').all()
                option_values = []
                for opt in options[:10]:  # First 10 options
                    try:
                        option_values.append({
                            'value': opt.get_attribute('value'),
                            'text': opt.inner_text()
                        })
                    except:
                        continue
                
                selector = f'#{sel_id}' if sel_id else f'select[name="{sel_name}"]' if sel_name else f'select:nth-of-type({i+1})'
                
                targeted_html['selects'].append({
                    'index': i,
                    'id': sel_id,
                    'name': sel_name,
                    'selector': selector,
                    'options': option_values
                })
            except:
                continue
        
        # Extract result tables (for data structure)
        tables = page.locator('table').all()
        for i, table in enumerate(tables[:3]):  # Only first 3 tables
            try:
                table_id = table.get_attribute('id')
                table_class = table.get_attribute('class')
                
                # Get headers
                headers = []
                header_cells = table.locator('th').all()
                for th in header_cells[:10]:  # First 10 headers
                    try:
                        headers.append(th.inner_text())
                    except:
                        continue
                
                # Get first row as sample
                first_row = []
                first_row_cells = table.locator('tbody tr:first-child td').all()
                for td in first_row_cells[:10]:
                    try:
                        first_row.append(td.inner_text()[:50])  # First 50 chars
                    except:
                        continue
                
                selector = f'#{table_id}' if table_id else f'table.{table_class.split()[0]}' if table_class else f'table:nth-of-type({i+1})'
                
                targeted_html['tables'].append({
                    'index': i,
                    'id': table_id,
                    'class': table_class,
                    'selector': selector,
                    'headers': headers,
                    'sample_row': first_row
                })
            except:
                continue
        
        return targeted_html
        
    except Exception as e:
        print(f"   ⚠️ Targeted extraction failed: {e}")
        return None


def format_targeted_html_for_prompt(targeted_html):
    """Format extracted HTML into a clean, developer-friendly string"""
    if not targeted_html:
        return "No targeted HTML extracted"
    
    output = []
    
    # Forms
    if targeted_html['forms']:
        output.append("=== FORMS ===")
        for form in targeted_html['forms']:
            output.append(f"\nForm #{form['index']}:")
            output.append(f"  Selector: {form['selector']}")
            output.append(f"  Action: {form['action']}")
            output.append(f"  Method: {form['method']}")
    
    # Input fields
    if targeted_html['inputs']:
        output.append("\n=== INPUT FIELDS ===")
        for inp in targeted_html['inputs']:
            output.append(f"\nInput #{inp['index']}:")
            output.append(f"  Selector: {inp['selector']}")
            output.append(f"  ID: {inp['id']}")
            output.append(f"  Name: {inp['name']}")
            output.append(f"  Type: {inp['type']}")
            output.append(f"  Placeholder: {inp['placeholder']}")
    
    # Buttons
    if targeted_html['buttons']:
        output.append("\n=== BUTTONS ===")
        for btn in targeted_html['buttons']:
            output.append(f"\nButton #{btn['index']}:")
            output.append(f"  Selector: {btn['selector']}")
            output.append(f"  Full Selector: {btn['full_selector']}")
            output.append(f"  Text: {btn['text']}")
            output.append(f"  ID: {btn['id']}")
            output.append(f"  Name: {btn['name']}")
            output.append(f"  Type: {btn['type']}")
    
    # Selects
    if targeted_html['selects']:
        output.append("\n=== SELECT DROPDOWNS ===")
        for sel in targeted_html['selects']:
            output.append(f"\nSelect #{sel['index']}:")
            output.append(f"  Selector: {sel['selector']}")
            output.append(f"  Options: {len(sel['options'])} total")
            for opt in sel['options'][:5]:  # Show first 5
                output.append(f"    - {opt['text']} (value={opt['value']})")
    
    # Tables
    if targeted_html['tables']:
        output.append("\n=== TABLES (Results) ===")
        for tbl in targeted_html['tables']:
            output.append(f"\nTable #{tbl['index']}:")
            output.append(f"  Selector: {tbl['selector']}")
            output.append(f"  Headers: {', '.join(tbl['headers'])}")
            if tbl['sample_row']:
                output.append(f"  Sample Row: {', '.join(tbl['sample_row'])}")
    
    return '\n'.join(output)


def get_page_html_fallback(page):
    """Fallback: Get raw HTML if Jina fails (use sparingly due to tokens)"""
    try:
        # Get HTML but be very aggressive with limits
        html_content = page.content()[:15000]  # Reduced from 100k
        return html_content
    except:
        return "Could not extract HTML"


def extract_code_from_response(response_text):
    """Extract Python code from Claude's response"""
    import re
    
    # Look for code blocks with ```python or ```
    pattern = r'```(?:python)?\n(.*?)```'
    matches = re.findall(pattern, response_text, re.DOTALL)
    
    if matches:
        return matches[0].strip()
    
    # If no code blocks, return the whole response
    return response_text.strip()


def get_test_names_from_db(county_name, state, limit=3):
    """Get test names from database for a specific county"""
    db = SessionLocal()
    try:
        # Get deceased individuals from this county
        individuals = db.query(DeceasedIndividual).filter(
            DeceasedIndividual.last_residence_county == county_name,
            DeceasedIndividual.last_residence_state == state
        ).limit(limit).all()
        
        test_names = []
        for ind in individuals:
            test_names.append({
                'first_name': ind.first_name,
                'last_name': ind.last_name,
                'full_name': f"{ind.first_name} {ind.last_name}"
            })
        
        # If no names in DB, use generic test names
        if not test_names:
            test_names = [
                {'first_name': 'John', 'last_name': 'Smith', 'full_name': 'John Smith'},
                {'first_name': 'Mary', 'last_name': 'Johnson', 'full_name': 'Mary Johnson'},
                {'first_name': 'Robert', 'last_name': 'Williams', 'full_name': 'Robert Williams'}
            ]
        
        return test_names
    finally:
        db.close()


def load_example_scraper(record_type):
    """Load an example scraper from your existing Dallas scrapers"""
    
    # Map record types to your existing files
    example_files = {
        'tax': PROJECT_ROOT / 'scrapers' / 'texas' / 'dallas' / 'tax.py',
        'probate': PROJECT_ROOT / 'scrapers' / 'texas' / 'dallas' / 'probate.py',
        'property': PROJECT_ROOT / 'scrapers' / 'texas' / 'dallas' / 'property.py'
    }
    
    example_file = example_files.get(record_type)
    
    if example_file and example_file.exists():
        with open(example_file, 'r', encoding='utf-8') as f:
            return f.read()
    
    return None


# ============================================================================
# CORE AGENT FUNCTIONS
# ============================================================================

def interactive_exploration(page, website_url, record_type, county_name, test_names):
    """
    Have Claude INTERACTIVELY explore the website by actually performing searches.
    Claude will navigate, search, click, and document the entire user journey.
    
    For PROBATE searches, includes special logic:
    - Test with jargon to see "no results" pattern
    - Compare probate filing dates to death dates
    - Check for OPEN status
    - Handle multiple results (pick closest to death date)
    
    Returns: Detailed analysis including actual search results and data structure
    """
    print(f"\n🔍 INTERACTIVE EXPLORATION - {county_name} {record_type} website")
    print(f"   URL: {website_url}")
    print(f"   Claude will now navigate the site and perform real searches...")
    
    # For probate, add special test case for "no results"
    if record_type == 'probate':
        print(f"   📋 PROBATE MODE: Will test for 'no results' pattern and OPEN status validation")
        # Add a jargon test name to see what "no results" looks like
        test_names_with_jargon = test_names + [
            {'first_name': 'ZZZZZ', 'last_name': 'TESTNORESULTS', 'full_name': 'ZZZZZ TESTNORESULTS'}
        ]
    else:
        test_names_with_jargon = test_names
    
    conversation_history = []
    exploration_log = []
    
    try:
        # Initial navigation
        print(f"\n   Step 1: Navigating to website...")
        page.goto(website_url, wait_until='networkidle', timeout=30000)
        time.sleep(2)
        
        # Start interactive exploration loop
        max_interactions = 15  # Max steps Claude can take
        
        for step in range(1, max_interactions + 1):
            print(f"\n   --- Step {step}/{max_interactions} ---")
            
            try:
                # Safely get page info (might fail if page is navigating)
                current_url = page.url
                try:
                    page_title = page.title()
                except:
                    page_title = "Unknown"
                
                print(f"   Current URL: {current_url}")
                
                # Wait for page to be ready after navigation
                try:
                    page.wait_for_load_state('domcontentloaded', timeout=5000)
                except:
                    pass  # Continue anyway
                
                # Extract ONLY targeted HTML (forms, buttons, inputs)
                print(f"   Extracting targeted HTML...")
                targeted_html = extract_targeted_html(page)
                targeted_html_str = format_targeted_html_for_prompt(targeted_html)
                
                # Only take screenshot on first step and when extracting data
                should_screenshot = (step == 1 or 
                                    (exploration_log and 
                                     exploration_log[-1].get('action_plan', {}).get('action') == 'click'))
                
                if should_screenshot:
                    try:
                        screenshot_b64 = take_screenshot_base64(page)
                        print(f"   ✓ Screenshot taken")
                    except:
                        screenshot_b64 = None
                        print(f"   ⚠️ Screenshot failed (page may be loading)")
                else:
                    screenshot_b64 = None
                    print(f"   Skipping screenshot (saves tokens)")
                
                # Get only essential visible text (very limited)
                try:
                    visible_text = page.inner_text('body')[:2000]
                except:
                    visible_text = ""
                
                print(f"   Targeted HTML: {len(targeted_html_str)} chars")
                print(f"   Visible text: {len(visible_text)} chars")
                
            except Exception as e:
                print(f"   ⚠️ Error getting page state: {e}")
                # Try to recover
                try:
                    page.wait_for_load_state('networkidle', timeout=10000)
                    continue  # Retry this step
                except:
                    print(f"   ✗ Cannot recover, moving to next step")
                    continue
            
            # Build MINIMAL context for Claude
            if step == 1:
                test_names_list = [{'first': n['first_name'], 'last': n['last_name']} for n in test_names_with_jargon]
                test_names_json = json.dumps(test_names_list, indent=2)
                
                # Add probate-specific instructions
                probate_instructions = ""
                if record_type == 'probate':
                    probate_instructions = f"""

PROBATE-SPECIFIC REQUIREMENTS:
1. Search real name: {test_names[0]['first_name']} {test_names[0]['last_name']}
2. Document ALL filing dates and statuses from results
3. Then search jargon "ZZZZZ TESTNORESULTS" to learn "no results" message
4. Look for status field showing "OPEN", "CLOSED", "PENDING"
5. Identify "no results" message text/element"""
                
                prompt = f"""Search {county_name} {record_type} for: {test_names[0]['first_name']} {test_names[0]['last_name']}
{probate_instructions}

ELEMENTS:
{targeted_html_str}

TEXT:
{visible_text}

IMPORTANT: Return MULTIPLE actions as an array if you can batch them!
Example: Fill first name, last name, then click submit = 3 actions.

JSON response - can be single action OR array of actions:
{{
    "actions": [
        {{"action": "fill_form", "selector": "#firstName", "value": "John"}},
        {{"action": "fill_form", "selector": "#lastName", "value": "Smith"}},
        {{"action": "click", "selector": "#searchBtn"}}
    ],
    "observations": "brief note",
    "next_step": "what happens after these actions"
}}

OR single action:
{{
    "action": "fill_form"|"click"|"wait"|"extract_data"|"done",
    "selector": "exact selector",
    "value": "text if filling",
    "observations": "brief",
    "next_step": "what next"
}}"""
            
            else:
                # Continuing exploration
                last_action = exploration_log[-1] if exploration_log else {}
                searches_completed = len([e for e in exploration_log if e.get('action_plan', {}).get('action') == 'extract_data'])
                
                # Check if we just got search results and haven't extracted data yet
                has_extracted = any('probate_table_structure' in e for e in exploration_log) if record_type == 'probate' else False
                
                extraction_reminder = ""
                if record_type == 'probate' and not has_extracted and searches_completed == 0:
                    extraction_reminder = "\nIMPORTANT: If you see results, use 'extract_data' to capture the table structure!"
                
                prompt = f"""Step {step}. Previous: {last_action.get('action_plan', {}).get('action', 'none')}
{extraction_reminder}

ELEMENTS:
{targeted_html_str}

TEXT:
{visible_text[:1000]}

Searches done: {searches_completed}

JSON - single OR batched actions:
{{
    "actions": [...] OR "action": "...",
    "observations": "brief",
    "next_step": "what next"
}}"""
            
            # Ask Claude what to do - minimal context
            # Only send screenshot on first step to save tokens
            message_content = []
            
            if screenshot_b64:
                message_content.append({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": screenshot_b64
                    }
                })
            
            message_content.append({
                "type": "text",
                "text": prompt
            })
            
            # Add delay to avoid rate limits (stagger requests)
            if step > 1:
                time.sleep(2)  # Wait 2 seconds between API calls
            
            # Try API call with retry on rate limit
            max_retries = 3
            for retry in range(max_retries):
                try:
                    response = client.messages.create(
                        model="claude-sonnet-4-20250514",
                        max_tokens=1000,  # Reduced from 2000
                        messages=conversation_history + [{
                            "role": "user",
                            "content": message_content
                        }]
                    )
                    break  # Success, exit retry loop
                    
                except anthropic.RateLimitError as e:
                    if retry < max_retries - 1:
                        wait_time = 10 * (retry + 1)  # 10s, 20s, 30s
                        print(f"   ⚠️ Rate limited. Waiting {wait_time}s before retry {retry+1}/{max_retries}...")
                        time.sleep(wait_time)
                    else:
                        print(f"   ✗ Rate limit exceeded after {max_retries} retries")
                        raise
            
            # Parse Claude's response
            response_text = response.content[0].text
            print(f"   Claude's response: {response_text[:200]}...")
            
            # Extract JSON
            try:
                import re
                json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                if json_match:
                    action_plan = json.loads(json_match.group())
                else:
                    action_plan = {"action": "done", "observations": response_text}
            except:
                print(f"   ⚠️ Could not parse JSON, assuming done")
                action_plan = {"action": "done", "observations": response_text}
            
            # Check if this is a batched action or single action
            if "actions" in action_plan and isinstance(action_plan["actions"], list):
                # Batched actions!
                actions_to_execute = action_plan["actions"]
                print(f"   📦 Batched: {len(actions_to_execute)} actions")
            else:
                # Single action
                actions_to_execute = [action_plan]
            
            # Log the action(s)
            exploration_log.append({
                "step": step,
                "url": current_url,
                "action_plan": action_plan,
                "batched": len(actions_to_execute) > 1
            })
            
            # Execute all actions in the batch
            for action_idx, single_action in enumerate(actions_to_execute):
                action = single_action.get('action', 'done')
                
                if len(actions_to_execute) > 1:
                    print(f"   [{action_idx+1}/{len(actions_to_execute)}] Action: {action}")
                else:
                    print(f"   Action: {action}")
                
                print(f"   Observations: {single_action.get('observations', 'none')[:150]}...")
                
                # Execute the action
                if action == 'done':
                    print(f"\n   ✅ Claude says exploration is complete!")
                    exploration_complete = True
                    break
                # Execute the action
                if action == 'done':
                    print(f"\n   ✅ Claude says exploration is complete!")
                    exploration_complete = True
                    break
                
                elif action == 'fill_form':
                    selector = single_action.get('selector')
                    value = single_action.get('value')
                    try:
                        print(f"   Filling field {selector} with '{value}'")
                        page.fill(selector, value, timeout=5000)
                        time.sleep(0.3)  # Short delay
                    except Exception as e:
                        print(f"   ⚠️ Error filling form: {e}")
                        try:
                            page.locator(selector).fill(value)
                            time.sleep(0.3)
                        except:
                            print(f"   ✗ Could not fill {selector}")
                
                elif action == 'click':
                    selector = single_action.get('selector')
                    try:
                        print(f"   Clicking {selector}")
                        page.click(selector, timeout=5000)
                        # Wait for navigation or ajax
                        try:
                            page.wait_for_load_state('networkidle', timeout=10000)
                        except:
                            time.sleep(3)
                    except Exception as e:
                        print(f"   ⚠️ Error clicking: {e}")
                        try:
                            page.locator(selector).click()
                            time.sleep(3)
                        except:
                            print(f"   ✗ Could not click {selector}")
                
                elif action == 'wait':
                    wait_time = single_action.get('seconds', 3)
                    print(f"   Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                
                elif action == 'extract_data':
                    print(f"   📊 Extracting data structure...")
                    
                    # For probate, explicitly extract the result structure
                    if record_type == 'probate':
                        print(f"   🔍 PROBATE: Extracting filing dates, statuses, case numbers...")
                        
                        try:
                            # Try to extract table structure
                            table_html = ""
                            tables = page.locator('table').all()
                            
                            for table in tables[:3]:  # Check first 3 tables
                                try:
                                    # Get table headers
                                    headers = []
                                    header_cells = table.locator('th').all()
                                    for th in header_cells:
                                        try:
                                            headers.append(th.inner_text().strip())
                                        except:
                                            pass
                                    
                                    # Get first 5 data rows as examples
                                    sample_rows = []
                                    rows = table.locator('tbody tr').all()[:5]
                                    
                                    for row in rows:
                                        cells = row.locator('td').all()
                                        row_data = []
                                        for cell in cells:
                                            try:
                                                text = cell.inner_text().strip()[:100]  # Limit cell text
                                                row_data.append(text)
                                            except:
                                                row_data.append("")
                                        
                                        if row_data:
                                            sample_rows.append(row_data)
                                    
                                    if headers and sample_rows:
                                        # Found a results table!
                                        table_structure = {
                                            'headers': headers,
                                            'sample_rows': sample_rows,
                                            'row_count': len(rows)
                                        }
                                        
                                        exploration_log[-1]['probate_table_structure'] = table_structure
                                        
                                        print(f"      ✓ Found results table with {len(headers)} columns:")
                                        print(f"        Headers: {', '.join(headers[:6])}")
                                        
                                        # Print first sample row
                                        if sample_rows:
                                            print(f"        Sample row 1: {sample_rows[0][:4]}")
                                            if len(sample_rows) > 1:
                                                print(f"        Sample row 2: {sample_rows[1][:4]}")
                                        
                                        # Look for status and date columns
                                        status_col_idx = None
                                        date_col_idx = None
                                        
                                        for i, header in enumerate(headers):
                                            header_lower = header.lower()
                                            if 'status' in header_lower or 'case status' in header_lower:
                                                status_col_idx = i
                                                print(f"      ✓ Status column found at index {i}: '{header}'")
                                            
                                            if 'date' in header_lower or 'filed' in header_lower:
                                                date_col_idx = i
                                                print(f"      ✓ Date column found at index {i}: '{header}'")
                                        
                                        # Extract actual values
                                        if status_col_idx is not None and sample_rows:
                                            statuses = [row[status_col_idx] if status_col_idx < len(row) else '' for row in sample_rows]
                                            print(f"      ✓ Sample statuses: {statuses[:3]}")
                                        
                                        if date_col_idx is not None and sample_rows:
                                            dates = [row[date_col_idx] if date_col_idx < len(row) else '' for row in sample_rows]
                                            print(f"      ✓ Sample dates: {dates[:3]}")
                                        
                                        break  # Found the right table
                                    
                                except Exception as e:
                                    print(f"      ⚠️ Error extracting table: {e}")
                                    continue
                            
                            # Also capture raw HTML snippet
                            html_snippet = page.content()[:50000]
                            exploration_log[-1]['results_html'] = html_snippet[:5000]
                            
                        except Exception as e:
                            print(f"      ✗ Error during probate extraction: {e}")
                            exploration_log[-1]['results_html'] = "Could not capture"
                    else:
                        # Non-probate: just capture HTML
                        try:
                            html_snippet = page.content()[:50000]
                            exploration_log[-1]['results_html'] = html_snippet[:5000]
                        except:
                            exploration_log[-1]['results_html'] = "Could not capture"
                
                elif action == 'navigate_back':
                    print(f"   ⬅️ Going back...")
                    try:
                        page.go_back()
                        page.wait_for_load_state('domcontentloaded', timeout=5000)
                        time.sleep(2)
                    except Exception as e:
                        print(f"   ⚠️ Error going back: {e}")
            
            # Check if we should break after batch
            if 'exploration_complete' in locals() and exploration_complete:
                break
            
            # Update conversation history
            conversation_history.append({
                "role": "user",
                "content": prompt
            })
            conversation_history.append({
                "role": "assistant",
                "content": response_text
            })
        
        # Final analysis - ask Claude to summarize what it learned
        print(f"\n   📝 Asking Claude to summarize findings...")
        
        final_screenshot = take_screenshot_base64(page)
        
        # Create a simplified exploration log for the prompt
        exploration_summary = []
        for e in exploration_log:
            exploration_summary.append({
                'step': e['step'],
                'url': e['url'],
                'action': e['action_plan'].get('action'),
                'observations': e['action_plan'].get('observations', '')[:200]
            })
        
        # Build probate-specific requirements string
        probate_requirements = ""
        if record_type == 'probate':
            probate_requirements = '''
The scraper must:
1. Extract filing_date from each result
2. Extract case_status (OPEN/CLOSED/PENDING) from each result
3. When multiple results: Compare filing_date to deceased.death_date from database
4. Select the case with filing_date CLOSEST to death_date
5. Check if that case status is OPEN
6. Handle "no results found" gracefully (not an error)
7. Return structured data including:
   - case_status: OPEN/CLOSED/PENDING
   - filing_date: YYYY-MM-DD
   - months_after_death: calculated difference
   - has_probate: true/false (false if no results)
'''
        
        summary_response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4000,
            messages=[{
                "role": "user",
                "content": f"""You explored {county_name} {record_type} search by performing real searches.

EXPLORATION LOG:
{json.dumps(exploration_summary, indent=2)}

Provide COMPLETE technical summary for generating a scraper.

{"PROBATE-SPECIFIC REQUIREMENTS:" if record_type == 'probate' else ""}
{probate_requirements}

Technical summary JSON:
{{
    "workflow_summary": "step by step",
    "search_form": {{
        "url": "exact URL",
        "input_fields": [{{"selector": "...", "field_name": "...", "field_type": "..."}}],
        "submit_selector": "button selector"
    }},
    "results_handling": {{
        "results_container_selector": "where results appear",
        "result_item_selector": "each result",
        "no_results_indicator": "text when nothing found",
        "filing_date_selector": "how to extract filing date",
        "case_status_selector": "how to extract OPEN/CLOSED status"
    }},
    "data_extraction": {{
        "fields_found": [{{"field_name": "...", "selector": "...", "data_type": "..."}}]
    }}
}}"""
            }]
        )
        
        analysis_text = summary_response.content[0].text
        
        # Parse final analysis
        try:
            import re
            json_match = re.search(r'\{.*\}', analysis_text, re.DOTALL)
            if json_match:
                final_analysis = json.loads(json_match.group())
            else:
                final_analysis = {"raw_summary": analysis_text}
        except:
            final_analysis = {"raw_summary": analysis_text}
        
        # Add exploration log to analysis
        final_analysis['exploration_log'] = exploration_log
        final_analysis['searches_performed'] = len([e for e in exploration_log if e.get('action_plan', {}).get('action') == 'extract_data'])
        
        print(f"\n   ✅ Exploration complete!")
        print(f"   - Steps taken: {len(exploration_log)}")
        print(f"   - Searches performed: {final_analysis['searches_performed']}")
        
        return final_analysis
        
    except Exception as e:
        print(f"   ✗ Error during exploration: {e}")
        traceback.print_exc()
        return None


def generate_scraper_code(county_name, state, record_type, website_url, site_analysis, example_code=None):
    """
    Ask Claude to generate a complete scraper based on site analysis
    """
    print(f"\n🤖 Generating scraper code...")
    
    # Load example if available
    if not example_code:
        example_code = load_example_scraper(record_type)
    
    example_section = ""
    if example_code:
        example_section = f"""
# EXAMPLE SCRAPER (Dallas County {record_type.title()})
Here's a working scraper I built for Dallas County as reference for patterns and structure:

```python
{example_code[:10000]}  # First 10k chars
```

Use similar patterns but adapt to the new county's website structure.
"""
    
    prompt = f"""You are an expert web scraper. Generate a complete, production-ready Python scraper.

# COUNTY INFORMATION
- County: {county_name}, {state}
- Record Type: {record_type}
- Website URL: {website_url}

# DETAILED EXPLORATION FINDINGS
You just performed REAL searches on this website. Here's what you learned:

## Workflow Summary
{site_analysis.get('workflow_summary', 'See exploration log')}

## Complete Technical Analysis
{json.dumps(site_analysis, indent=2, default=str)[:5000]}

## Searches Performed
You successfully searched {site_analysis.get('searches_performed', 0)} names and documented the complete flow.

{example_section}

# REQUIREMENTS
Generate a Python script that REPLICATES THE EXACT WORKFLOW you just performed:

1. **Search Function**: `search(first_name, last_name)` that returns a list of records
2. **Uses Playwright**: For browser automation (already imported)
3. **Follows Exact Steps**: Use the selectors and workflow you documented during exploration
4. **Returns Structured Data**: Dictionary matching our database schema
5. **Error Handling**: Graceful failures, returns empty list on no results
6. **Rate Limiting**: 1-2 second delay between requests
7. **Clean Code**: Well commented, follows the example structure

# CRITICAL
- Use the EXACT selectors you discovered during interactive exploration
- Follow the EXACT navigation flow you documented
- Handle the results display method you observed (table/divs/detail page)
- Include wait strategies that worked during exploration

1. **Search Function**: `search(first_name, last_name)` that returns a list of records
2. **Uses Playwright**: For browser automation (already imported)
3. **Returns Structured Data**: Dictionary matching our database schema
4. **Error Handling**: Graceful failures, returns empty list on no results
5. **Rate Limiting**: 1-2 second delay between requests
6. **Clean Code**: Well commented, follows the example structure

# OUTPUT SCHEMA FOR {record_type.upper()}
"""

    if record_type == 'property':
        prompt += """
Return format:
```python
{{
    'account_number': str,
    'parcel_id': str,
    'owner_name': str,
    'address': str,
    'city': str,
    'state': str,
    'zip_code': str,
    'market_value': float,
    'assessed_value': float,
    'property_type': str,
    'year_built': int,
    'raw_data': dict  # Store all scraped data
}}
```"""
    
    elif record_type == 'tax':
        prompt += """
Return format:
```python
{{
    'account_number': str,
    'owner_name': str,
    'address': str,
    'market_value': float,
    'is_delinquent': bool,
    'total_owed': float,
    'delinquency_years': int,
    'year_data': dict,  # {{2024: amount, 2023: amount, ...}}
    'raw_data': dict
}}
```"""
    
    elif record_type == 'probate':
        prompt += """
Return format (PROBATE-SPECIFIC):
```python
{{
    'case_number': str,
    'case_type': str,
    'case_status': str,  # "OPEN", "CLOSED", "PENDING"
    'filing_date': str,  # 'YYYY-MM-DD'
    'decedent_name': str,
    'executor_name': str or None,
    'attorney_name': str or None,
    'estate_value': float or None,
    'is_closed': bool,  # True if status is CLOSED
    'has_probate': bool,  # False if no results found
    'death_date': str,  # From database input (YYYY-MM-DD)
    'months_after_death': int,  # filing_date - death_date in months
    'raw_data': dict
}}
```

CRITICAL PROBATE LOGIC:
The search() function signature must be:
```python
def search(first_name, last_name, death_date=None):
    \"\"\"
    Search for probate cases.
    
    Args:
        first_name: First name to search
        last_name: Last name to search  
        death_date: Death date from SSDI (YYYY-MM-DD string or datetime)
    
    Returns:
        List of dicts. If no results, returns [{{
            'has_probate': False,
            'decedent_name': f'{{first_name}} {{last_name}}'
        }}]
    \"\"\"
```

MUST IMPLEMENT:
1. If no results found: Return [{{'has_probate': False, 'decedent_name': '...'}}]
2. If single result: Extract status, filing_date, calculate months_after_death
3. If multiple results:
   - Extract filing_date from ALL results
   - Compare each filing_date to death_date
   - Select the case with filing_date CLOSEST to death_date
   - Return only that case's data
4. Calculate months_after_death:
   ```python
   from datetime import datetime
   filing = datetime.strptime(filing_date, '%Y-%m-%d')
   death = datetime.strptime(death_date, '%Y-%m-%d')
   months = (filing.year - death.year) * 12 + (filing.month - death.month)
   ```
5. Always check if case_status contains "OPEN" (case-insensitive)

Example usage:
```python
results = search('John', 'Smith', '2020-03-15')
# Returns: [{{'case_status': 'OPEN', 'filing_date': '2020-05-20', 'months_after_death': 2, ...}}]
```"""

    prompt += """

# SCRAPER STRUCTURE
```python
from playwright.sync_api import sync_playwright
import time
import re

def search(first_name, last_name):
    \"\"\"
    Search for records by name.
    Returns: List of dicts matching the schema above
    \"\"\"
    results = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            # Your scraping logic here
            # 1. Navigate to site
            # 2. Fill search form
            # 3. Submit
            # 4. Parse results
            # 5. Return structured data
            
            pass
            
        except Exception as e:
            print(f"Error: {{e}}")
            return []
        finally:
            browser.close()
    
    return results

if __name__ == "__main__":
    # Test the scraper
    import sys
    if len(sys.argv) >= 3:
        results = search(sys.argv[1], sys.argv[2])
        print(f"Found {{len(results)}} results")
        for r in results:
            print(r)
```

Now generate the COMPLETE scraper code. Return ONLY the Python code, no explanation."""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=8000,
            messages=[{
                "role": "user",
                "content": prompt
            }]
        )
        
        code = extract_code_from_response(response.content[0].text)
        print("   ✓ Code generated")
        return code
        
    except Exception as e:
        print(f"   ✗ Error generating code: {e}")
        traceback.print_exc()
        return None


def test_scraper(scraper_code, test_names, county_name, record_type='property'):
    """
    Test the generated scraper with sample names.
    For probate, also tests with death dates.
    
    Returns: (success: bool, results: dict, errors: list)
    """
    print(f"\n🧪 Testing scraper with {len(test_names)} test names...")
    
    # Write code to temp file
    temp_file = PROJECT_ROOT / 'temp_scraper.py'
    with open(temp_file, 'w', encoding='utf-8') as f:
        f.write(scraper_code)
    
    results = []
    errors = []
    
    for i, name in enumerate(test_names, 1):
        first = name['first_name']
        last = name['last_name']
        
        # For probate, use a test death date (2 years ago)
        if record_type == 'probate':
            from datetime import datetime, timedelta
            test_death_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
            print(f"   Test {i}/{len(test_names)}: {first} {last} (death: {test_death_date})")
            cmd = ['python', str(temp_file), first, last, test_death_date]
        else:
            print(f"   Test {i}/{len(test_names)}: {first} {last}")
            cmd = ['python', str(temp_file), first, last]
        
        try:
            # Run the scraper
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                print(f"      ✓ Success")
                results.append({
                    'name': name,
                    'success': True,
                    'output': result.stdout
                })
            else:
                error_msg = result.stderr or result.stdout
                print(f"      ✗ Error: {error_msg[:200]}")
                errors.append({
                    'name': name,
                    'error': error_msg
                })
                results.append({
                    'name': name,
                    'success': False,
                    'error': error_msg
                })
        
        except subprocess.TimeoutExpired:
            print(f"      ✗ Timeout")
            errors.append({
                'name': name,
                'error': 'Timeout after 60 seconds'
            })
        except Exception as e:
            print(f"      ✗ Exception: {str(e)}")
            errors.append({
                'name': name,
                'error': str(e)
            })
    
    # Cleanup
    if temp_file.exists():
        temp_file.unlink()
    
    success_count = len([r for r in results if r.get('success')])
    success_rate = success_count / len(test_names) if test_names else 0
    
    print(f"\n   Results: {success_count}/{len(test_names)} successful ({success_rate*100:.1f}%)")
    
    return success_rate >= 0.5, results, errors  # 50% success threshold


def fix_scraper_code(broken_code, errors, attempt_number):
    """
    Ask Claude to fix the broken scraper based on errors
    """
    print(f"\n🔧 Asking Claude to fix errors (attempt {attempt_number})...")
    
    # Prepare error summary
    error_summary = "\n".join([
        f"Test: {err['name']['full_name']}\nError: {err['error'][:500]}"
        for err in errors[:3]  # Show first 3 errors
    ])
    
    prompt = f"""The scraper code has errors. Fix them.

# ERRORS ENCOUNTERED
{error_summary}

# BROKEN CODE
```python
{broken_code}
```

# INSTRUCTIONS
1. Analyze the errors carefully
2. Fix the issues (likely selector problems, timing issues, or parsing errors)
3. Return the COMPLETE fixed code
4. Make sure to handle edge cases (no results, timeouts, missing fields)

Return ONLY the fixed Python code, no explanation."""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=8000,
            messages=[{
                "role": "user",
                "content": prompt
            }]
        )
        
        fixed_code = extract_code_from_response(response.content[0].text)
        print("   ✓ Fixed code generated")
        return fixed_code
        
    except Exception as e:
        print(f"   ✗ Error fixing code: {e}")
        return broken_code  # Return original if fix fails


def save_scraper(code, county_name, state, record_type):
    """Save the working scraper to the scrapers directory"""
    
    # Create directory structure: scrapers/state/county/
    state_dir = SCRAPERS_DIR / state.lower()
    county_dir = state_dir / county_name.lower().replace(' ', '_')
    
    state_dir.mkdir(exist_ok=True)
    county_dir.mkdir(exist_ok=True)
    
    # Create __init__.py files
    (state_dir / '__init__.py').touch()
    (county_dir / '__init__.py').touch()
    
    # Save the scraper
    filename = f"{record_type}.py"
    filepath = county_dir / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(f"""# Auto-generated scraper for {county_name} County, {state}
# Record Type: {record_type}
# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
# DO NOT EDIT - Regenerate using scout/agent.py if website changes

{code}
""")
    
    print(f"\n💾 Scraper saved to: {filepath}")
    return str(filepath)


# ============================================================================
# MAIN AGENT WORKFLOW
# ============================================================================

def generate_scraper_for_county(county_id=None, county_name=None, state=None, record_type=None):
    """
    Main function: Generate a working scraper for a county
    
    Args:
        county_id: Database ID of county
        OR
        county_name, state, record_type: Manual specification
    
    Returns:
        scraper_path: Path to generated scraper file
    """
    
    print("\n" + "="*80)
    print("🚀 SCOUT AGENT - AUTONOMOUS SCRAPER GENERATOR")
    print("="*80)
    
    db = SessionLocal()
    
    try:
        # Load county from database
        if county_id:
            county = db.query(County).filter(County.id == county_id).first()
            if not county:
                print(f"✗ County ID {county_id} not found in database")
                return None
            
            county_name = county.name
            state = county.state
            
            # Determine which record type to generate
            if record_type is None:
                # Choose based on what's configured but not generated
                if county.property_search_url and not county.property_scraper_generated:
                    record_type = 'property'
                    website_url = county.property_search_url
                elif county.tax_search_url and not county.tax_scraper_generated:
                    record_type = 'tax'
                    website_url = county.tax_search_url
                elif county.probate_search_url and not county.probate_scraper_generated:
                    record_type = 'probate'
                    website_url = county.probate_search_url
                else:
                    print("✗ No unconfigured record types found for this county")
                    return None
            else:
                # Use specified record type
                url_map = {
                    'property': county.property_search_url,
                    'tax': county.tax_search_url,
                    'probate': county.probate_search_url
                }
                website_url = url_map.get(record_type)
                
                if not website_url:
                    print(f"✗ No URL configured for {record_type} in database")
                    return None
        
        else:
            # Manual mode - need all parameters
            if not all([county_name, state, record_type]):
                print("✗ Must provide either county_id OR (county_name, state, record_type)")
                return None
            
            # Need to get URL somehow - for now, prompt user
            print(f"\n📝 Manual mode: {county_name}, {state} - {record_type}")
            website_url = input(f"Enter the {record_type} search URL: ").strip()
        
        print(f"\n📍 County: {county_name}, {state}")
        print(f"📋 Record Type: {record_type}")
        print(f"🔗 Website: {website_url}")
        
        # Get test names
        test_names = get_test_names_from_db(county_name, state)
        print(f"🧪 Test names: {[n['full_name'] for n in test_names]}")
        
        # Start Playwright browser for interactive exploration
        print(f"\n{'='*80}")
        print("PHASE 1: INTERACTIVE WEBSITE EXPLORATION")
        print(f"{'='*80}")
        print("Claude will now:")
        print("  1. Navigate the website like a human")
        print("  2. Perform actual searches with test names")
        print("  3. Click through results and detail pages")
        print("  4. Document the complete workflow")
        print("  5. Extract real data to understand structure")
        print(f"{'='*80}\n")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)  # Visible so you can watch
            page = browser.new_page()
            page.set_viewport_size({"width": 1920, "height": 1080})
            
            # Step 1: Interactive exploration with real searches
            site_analysis = interactive_exploration(page, website_url, record_type, county_name, test_names)
            
            if not site_analysis:
                print("✗ Failed to explore website")
                browser.close()
                return None
            
            browser.close()
        
        print(f"\n{'='*80}")
        print("PHASE 2: CODE GENERATION")
        print(f"{'='*80}")
        print("Now that Claude understands the workflow, generating scraper code...")
        print(f"{'='*80}\n")
        
        # Step 2: Generate initial scraper
        scraper_code = generate_scraper_code(
            county_name, state, record_type, website_url, site_analysis
        )
        
        if not scraper_code:
            print("✗ Failed to generate code")
            return None
        
        # Step 3: Iterative testing and fixing
        max_attempts = 5
        
        for attempt in range(1, max_attempts + 1):
            print(f"\n{'='*80}")
            print(f"ATTEMPT {attempt}/{max_attempts}")
            print(f"{'='*80}")
            
            success, results, errors = test_scraper(scraper_code, test_names, county_name, record_type)
            
            if success:
                print("\n✅ SCRAPER WORKING!")
                break
            
            if attempt < max_attempts:
                # Fix and retry
                scraper_code = fix_scraper_code(scraper_code, errors, attempt)
            else:
                print(f"\n❌ Failed to create working scraper after {max_attempts} attempts")
                return None
        
        # Step 4: Save the working scraper
        scraper_path = save_scraper(scraper_code, county_name, state, record_type)
        
        # Step 5: Update database
        if county_id:
            if record_type == 'property':
                county.property_scraper_generated = True
                county.property_scraper_path = scraper_path
                county.property_last_tested = datetime.utcnow()
            elif record_type == 'tax':
                county.tax_scraper_generated = True
                county.tax_scraper_path = scraper_path
                county.tax_last_tested = datetime.utcnow()
            elif record_type == 'probate':
                county.probate_scraper_generated = True
                county.probate_scraper_path = scraper_path
                county.probate_last_tested = datetime.utcnow()
            
            county.scouted_at = datetime.utcnow()
            county.scout_confidence = 0.85  # Could calculate based on test results
            
            db.commit()
            print(f"✓ Database updated")
        
        print("\n" + "="*80)
        print("🎉 SCRAPER GENERATION COMPLETE!")
        print("="*80)
        print(f"📁 Scraper: {scraper_path}")
        print(f"✓ Ready to use in production")
        
        return scraper_path
        
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        traceback.print_exc()
        return None
        
    finally:
        db.close()


# ============================================================================
# CLI INTERFACE
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate scrapers for county websites')
    parser.add_argument('--county-id', type=int, help='County ID from database')
    parser.add_argument('--county-name', type=str, help='County name (manual mode)')
    parser.add_argument('--state', type=str, help='State code (manual mode)')
    parser.add_argument('--record-type', type=str, choices=['property', 'tax', 'probate'], 
                       help='Record type to scrape')
    
    args = parser.parse_args()
    
    if args.county_id:
        generate_scraper_for_county(county_id=args.county_id, record_type=args.record_type)
    elif args.county_name and args.state and args.record_type:
        generate_scraper_for_county(
            county_name=args.county_name,
            state=args.state,
            record_type=args.record_type
        )
    else:
        print("Usage:")
        print("  python agent.py --county-id 1")
        print("  python agent.py --county-id 1 --record-type property")
        print("  python agent.py --county-name 'Harris' --state 'TX' --record-type probate")