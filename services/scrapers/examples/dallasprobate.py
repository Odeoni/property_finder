from playwright.sync_api import sync_playwright
import time
import requests
import re
import os
import json
import csv
import multiprocessing
from multiprocessing import Manager, Queue, Lock
from datetime import datetime
from queue import Empty

# ==============================================================================
# 🛠️ CONFIGURATION
# ==============================================================================

# FILE PATHS
NAMES_FILE = r"C:\Users\KISFECO\Documents\Real Estate Automations\tax\qualified_properties_20251103_154233.txt"
OUTPUT_FOLDER = r"C:\Users\KISFECO\Documents\Real Estate Automations\probate"
LOG_FILE_NAME = "probate_results_detailed.txt"
CSV_FILE_NAME = "probate_results.csv"

# PROCESSING RANGE (1-indexed, inclusive)
START_FROM_ROW = 1
END_AT_ROW = 10

# PARALLEL PROCESSING CONFIGURATION
NUM_PARALLEL_INSTANCES = 10  # Number of parallel browser instances
HEADLESS_MODE = True  # Set to False to see browsers (useful for debugging)
SLOW_MO = 0  # Milliseconds delay between actions

# API KEYS AND URLS
CAPSOLVER_API_KEY = "CAP-351E10005140E7F03927FDE897DF2F84C88C3683C8ACE13EC31CF71AB63647B9"
URL = "https://courtsportal.dallascounty.org/DALLASPROD/Home/Dashboard/29"

# SELECTORS
SEARCH_INPUT_SELECTOR = '#caseCriteria_SearchCriteria'
SUBMIT_BUTTON_SELECTOR = '#btnSSSubmit'
ADVANCED_OPTIONS_BUTTON = '#AdvOptions'
SMART_SEARCH_TAB_SELECTOR = '#tcControllerLink_0'

# ==============================================================================
# ⚙️ HELPER FUNCTIONS (CAPTCHA & LOGIC)
# ==============================================================================

def detect_captcha_type(page):
    """Detect which type of CAPTCHA is present on the page"""
    if page.locator('.g-recaptcha').count() > 0:
        site_key = page.locator('.g-recaptcha').get_attribute('data-sitekey')
        return "ReCaptchaV2TaskProxyLess", site_key
    
    if page.locator('[data-action]').count() > 0 or 'grecaptcha.execute' in page.content():
        match = re.search(r'grecaptcha\.execute\(["\']([^"\']+)["\']', page.content())
        site_key = match.group(1) if match else None
        return "ReCaptchaV3TaskProxyLess", site_key
    
    if page.locator('.h-captcha').count() > 0:
        site_key = page.locator('.h-captcha').get_attribute('data-sitekey')
        return "HCaptchaTaskProxyLess", site_key
    
    return None, None

def solve_captcha(api_key, captcha_type, site_key, url):
    """Solve CAPTCHA using CapSolver API"""
    create_payload = {
        "clientKey": api_key,
        "task": {
            "type": captcha_type,
            "websiteURL": url,
            "websiteKey": site_key
        }
    }
    
    headers = {"Content-Type": "application/json"}
    response = requests.post("https://api.capsolver.com/createTask", json=create_payload, headers=headers)
    result = response.json()
    
    if result.get("errorId") != 0:
        raise Exception(f"CapSolver error: {result.get('errorDescription')}")
    
    task_id = result["taskId"]
    
    get_payload = {"clientKey": api_key, "taskId": task_id}
    
    for attempt in range(60):
        time.sleep(3)
        result = requests.post("https://api.capsolver.com/getTaskResult", json=get_payload, headers=headers)
        data = result.json()
        
        if data.get("status") == "ready":
            return data["solution"]["gRecaptchaResponse"]
    
    raise Exception("CAPTCHA solving timeout")

def parse_owner_name(raw_owner_string):
    """Parse owner name according to specific court search requirements"""
    cleanup_phrases = ['EST OF', 'ET AL', 'ESTATE OF', 'ESTATE', 'EST']
    cleaned = raw_owner_string.upper().strip()
    
    for phrase in cleanup_phrases:
        cleaned = cleaned.replace(phrase, ' ')
    
    cleaned = ' '.join(cleaned.split())
    
    if '&' in cleaned:
        parts = cleaned.split('&')
        owners_list = []
        
        first_part = parts[0].strip()
        second_part = parts[1].strip() if len(parts) > 1 else ''
        
        for phrase in cleanup_phrases:
            first_part = first_part.replace(phrase, ' ').strip()
            second_part = second_part.replace(phrase, ' ').strip()
        
        first_owner_words = first_part.split()
        if len(first_owner_words) >= 2:
            last1 = first_owner_words[0]
            first1 = first_owner_words[1]
            middle1 = ' '.join(first_owner_words[2:]) if len(first_owner_words) > 2 else ''
            search1 = f"{last1}, {first1}"
            owners_list.append((first1, middle1, last1, search1))
        
        second_words = second_part.split()
        
        if len(second_words) == 1 and len(first_owner_words) >= 1:
            last2 = first_owner_words[0]
            first2 = second_words[0]
            middle2 = ''
            search2 = f"{last2}, {first2}"
            owners_list.append((first2, middle2, last2, search2))
            
        elif len(second_words) >= 2:
            last2 = second_words[0]
            first2 = second_words[1]
            middle2 = ' '.join(second_words[2:]) if len(second_words) > 2 else ''
            search2 = f"{last2}, {first2}"
            owners_list.append((first2, middle2, last2, search2))
        
        return owners_list
    
    else:
        words = cleaned.split()
        
        if ',' in cleaned:
            parts = cleaned.split(',')
            last = parts[0].strip()
            rest = parts[1].strip() if len(parts) > 1 else ''
            rest_words = rest.split()
            first = rest_words[0] if rest_words else ''
            middle = ' '.join(rest_words[1:]) if len(rest_words) > 1 else ''
            search_format = f"{last}, {first}"
            
        elif len(words) >= 2:
            last = words[0]
            first = words[1]
            middle = ' '.join(words[2:]) if len(words) > 2 else ''
            search_format = f"{last}, {first}"
            
        else:
            last = words[0] if words else ''
            first = ''
            middle = ''
            search_format = last
        
        return [(first, middle, last, search_format)]

def wait_for_results(page):
    """
    Wait for search results to load using JavaScript polling.
    Returns: (success, party_card_count)
    """
    try:
        page.wait_for_selector('.k-loading-mask', state='hidden', timeout=15000)
    except:
        pass
    
    result = page.evaluate('''() => {
        return new Promise((resolve) => {
            let attempts = 0;
            const maxAttempts = 20;
            
            const checkResults = () => {
                attempts++;
                
                if (!document.body) {
                    if (attempts < maxAttempts) {
                        setTimeout(checkResults, 1000);
                    } else {
                        resolve({ success: false, count: 0 });
                    }
                    return;
                }
                
                const partyCards = document.querySelectorAll('div.party-card');
                if (partyCards.length > 0) {
                    resolve({ success: true, count: partyCards.length });
                    return;
                }
                
                const bodyText = document.body.textContent || '';
                if (bodyText.includes('No cases match your search') || 
                    bodyText.includes('No records') ||
                    bodyText.includes('no results found')) {
                    resolve({ success: true, count: 0 });
                    return;
                }
                
                if (attempts < maxAttempts) {
                    setTimeout(checkResults, 1000);
                } else {
                    resolve({ success: false, count: 0 });
                }
            };
            
            checkResults();
        });
    }''')
    
    return result['success'], result['count']

def go_back_to_search(page):
    """Click the Smart Search tab to return to search page."""
    try:
        page.locator(SMART_SEARCH_TAB_SELECTOR).click()
        time.sleep(1)
        page.wait_for_selector(SEARCH_INPUT_SELECTOR, state='visible', timeout=5000)
        return True
    except Exception as e:
        return False

def parse_property_data(content):
    """Parse property data from the input file."""
    properties = {}
    
    property_blocks = re.split(r'Property #\d+.*?\n-+\n', content)
    
    for block in property_blocks:
        if not block.strip():
            continue
        
        owner_match = re.search(r'Owner:\s*(.+?)(?=\n|$)', block)
        account_match = re.search(r'Account Number:\s*(.+?)(?=\n|$)', block)
        address_match = re.search(r'Address:\s*(.+?)(?=\n|$)', block)
        market_value_match = re.search(r'Market Value:\s*\$?([\d,]+\.?\d*)(?=\n|$)', block)
        total_tax_match = re.search(r'Total Tax Owed:\s*\$?([\d,]+\.?\d*)(?=\n|$)', block)
        tax_ratio_match = re.search(r'Tax to Value Ratio:\s*([\d.]+)%(?=\n|$)', block)
        prior_year_match = re.search(r'Prior Year Due:\s*\$?([\d,]+\.?\d*)(?=\n|$)', block)
        current_levy_match = re.search(r'Current Levy:\s*\$?([\d,]+\.?\d*)(?=\n|$)', block)
        unpaid_years_match = re.search(r'Unpaid Years:\s*\[(.+?)\]', block)
        
        if owner_match:
            owner_name = owner_match.group(1).strip()
            
            properties[owner_name] = {
                'owner': owner_name,
                'account_number': account_match.group(1).strip() if account_match else 'N/A',
                'address': address_match.group(1).strip() if address_match else 'N/A',
                'market_value': market_value_match.group(1).strip() if market_value_match else 'N/A',
                'total_tax_owed': total_tax_match.group(1).strip() if total_tax_match else 'N/A',
                'tax_to_value_ratio': tax_ratio_match.group(1).strip() if tax_ratio_match else 'N/A',
                'prior_year_due': prior_year_match.group(1).strip() if prior_year_match else 'N/A',
                'current_levy': current_levy_match.group(1).strip() if current_levy_match else 'N/A',
                'unpaid_years': unpaid_years_match.group(1).strip() if unpaid_years_match else 'N/A'
            }
    
    return properties

def extract_owners_from_file(filepath):
    """Extract owner names and property data from the configured input file"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            
            property_data = parse_property_data(content)
            matches = re.findall(r'Owner:\s*(.+?)(?=\n|$)', content)
            owner_names = [match.strip() for match in matches if match.strip()]
            
            return owner_names, property_data
            
    except FileNotFoundError:
        print(f"ERROR: Input file not found at {filepath}")
        return [], {}

def process_search_results(page, first_name, middle_name, last_name):
    """
    Determine if a specific individual owner has a disqualifying probate case.
    """
    disqualifying_case_found = False
    
    first_upper = first_name.upper().strip()
    middle_upper = middle_name.upper().strip() if middle_name else ""
    last_upper = last_name.upper().strip()
    
    try:
        party_cards = page.locator('div.party-card').all()
        
        expected_exact = f"{last_upper}, {first_upper}"
        expected_with_middle = f"{last_upper}, {first_upper} {middle_upper[0]}." if middle_upper else None
        
        for party_card in party_cards:
            try:
                card_full_text = party_card.text_content().upper()
                
                name_matches = False
                owner_text = None
                
                lines = card_full_text.split('\n')
                for line in lines:
                    line_clean = ' '.join(line.strip().split())
                    if ',' in line_clean and 5 < len(line_clean) < 100:
                        if expected_exact in line_clean or (expected_with_middle and expected_with_middle in line_clean):
                            owner_text = line_clean
                            break
                
                if not owner_text:
                    if expected_exact in card_full_text:
                        owner_text = expected_exact
                    elif expected_with_middle and expected_with_middle in card_full_text:
                        owner_text = expected_with_middle
                
                if not owner_text:
                    continue
                
                owner_normalized = ' '.join(owner_text.split())
                
                if owner_normalized == expected_exact:
                    name_matches = True
                elif expected_with_middle and owner_normalized == expected_with_middle:
                    name_matches = True
                elif expected_exact in owner_normalized:
                    remaining = owner_normalized.replace(expected_exact, '').strip()
                    remaining_words = [w for w in remaining.split() if w]
                    if len(remaining_words) <= 1:
                        name_matches = True
                
                if not name_matches:
                    continue
                
                case_tables = party_card.locator('table.kgrid-card-table').all()
                
                for case_table in case_tables:
                    try:
                        all_tds = case_table.locator('td.card-data').all()
                        
                        case_type = None
                        case_status = None
                        
                        for td in all_tds:
                            td_classes = td.get_attribute('class') or ''
                            td_text = td.text_content().strip().upper()
                            
                            if 'party-case-type' in td_classes:
                                case_type = td_text
                            elif 'party-case-status' in td_classes:
                                case_status = td_text
                        
                        if not case_type:
                            continue
                        
                        is_disqualifying_type = (
                            case_type.startswith("DECEDENT - WILL") or
                            case_type.startswith("HEIRSHIP") or
                            "HEIRSHIP" in case_type
                        )
                        
                        if not is_disqualifying_type:
                            continue
                        
                        if case_status == "OPEN":
                            disqualifying_case_found = True
                            break
                        
                    except:
                        continue
                
                if disqualifying_case_found:
                    break
                    
            except:
                continue
        
    except Exception as e:
        pass
    
    return disqualifying_case_found

# ==============================================================================
# 🚀 PARALLEL PROCESSING WORKER FUNCTION
# ==============================================================================

def worker_process(worker_id, work_queue, results_queue, stats_dict, stats_lock, 
                   txt_file_lock, csv_file_lock, txt_output_file, csv_output_file, 
                   property_data_dict, headless, slow_mo, total_tasks):
    """
    Worker process that continuously pulls tasks from shared queue.
    """
    
    local_processed = 0
    local_qualified = 0
    start_time = time.time()
    
    print(f"\n[WORKER {worker_id}] Starting up...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=slow_mo)
        page = browser.new_page()
        
        # INITIAL SETUP (only once per worker)
        try:
            print(f"[WORKER {worker_id}] ⚙️ Initial setup...")
            page.goto(URL)
            page.wait_for_load_state('networkidle')
            
            page.locator(ADVANCED_OPTIONS_BUTTON).click()
            time.sleep(1)
            
            # Location dropdown
            location_input_selector = '#AdvOptionsMask > div:nth-child(1) > div > div > div:nth-child(2) > div > span > span > input'
            location_input = page.locator(location_input_selector)
            location_input.click()
            location_input.clear()
            location_input.type("County Courts - Probate", delay=50)
            time.sleep(0.5)
            
            try:
                filtered_option = page.locator('.k-list-container.k-popup .k-item:has-text("County Courts - Probate")').first
                if filtered_option.is_visible():
                    filtered_option.click()
                else:
                    location_input.press('Enter')
            except:
                location_input.press('Enter')
            
            time.sleep(1)
            
            # Case Type dropdown
            case_type_input_selector = '#caseCriteria_SearchCases_Section > fieldset:nth-child(2) > span > span > input'
            case_type_input = page.locator(case_type_input_selector)
            case_type_input.click()
            case_type_input.clear()
            case_type_input.type("All Available Probate Case Types", delay=50)
            time.sleep(0.5)
            
            try:
                filtered_option = page.locator('.k-list-container.k-popup .k-item:has-text("All Available Probate Case Types")').first
                if filtered_option.is_visible():
                    filtered_option.click()
                else:
                    case_type_input.press('Enter')
            except:
                case_type_input.press('Enter')
            
            time.sleep(1)
            print(f"[WORKER {worker_id}] ✓ Setup complete\n")
            
        except Exception as e:
            print(f"[WORKER {worker_id}] ✗ Setup failed: {str(e)}")
            browser.close()
            return
        
        # Process tasks from queue
        while True:
            try:
                owner_task = work_queue.get(timeout=2)
                
                if owner_task is None:  # Poison pill
                    print(f"[WORKER {worker_id}] Received shutdown signal")
                    break
                
                original_row, raw_owner, parsed_owners = owner_task
                
                with stats_lock:
                    stats_dict['in_progress'] += 1
                    current_progress = stats_dict['completed']
                
                print(f"\n[WORKER {worker_id}] {'='*80}")
                print(f"[WORKER {worker_id}] Row #{original_row}: {raw_owner}")
                print(f"[WORKER {worker_id}] Global Progress: {current_progress}/{total_tasks} completed")
                print(f"[WORKER {worker_id}] Queue remaining: ~{work_queue.qsize()}")
                print(f"[WORKER {worker_id}] {'='*80}")
                
                owners_failed_filter = False
                prop_data = property_data_dict.get(raw_owner, {})
                
                for owner_idx, (first, middle, last, search_term) in enumerate(parsed_owners):
                    owner_label = f"{original_row}" if len(parsed_owners) == 1 else f"{original_row}.{owner_idx+1}"
                    
                    print(f"[WORKER {worker_id}] [{owner_label}] {search_term}")
                    
                    log_entry = {
                        'row': owner_label,
                        'raw_owner': raw_owner,
                        'first_name': first,
                        'middle_name': middle,
                        'last_name': last,
                        'search_term': search_term,
                        'account_number': prop_data.get('account_number', 'N/A'),
                        'address': prop_data.get('address', 'N/A'),
                        'market_value': prop_data.get('market_value', 'N/A'),
                        'total_tax_owed': prop_data.get('total_tax_owed', 'N/A'),
                        'tax_to_value_ratio': prop_data.get('tax_to_value_ratio', 'N/A'),
                        'prior_year_due': prop_data.get('prior_year_due', 'N/A'),
                        'current_levy': prop_data.get('current_levy', 'N/A'),
                        'unpaid_years': prop_data.get('unpaid_years', 'N/A')
                    }
                    
                    try:
                        # Clear and fill search
                        search_input = page.locator(SEARCH_INPUT_SELECTOR)
                        search_input.clear()
                        search_input.fill(search_term)
                        
                        # Solve CAPTCHA
                        print(f"[WORKER {worker_id}]   Solving CAPTCHA...")
                        captcha_type, site_key = detect_captcha_type(page)
                        
                        if captcha_type:
                            token = solve_captcha(CAPSOLVER_API_KEY, captcha_type, site_key, URL)
                            
                            page.evaluate(f'''() => {{
                                const textarea = document.getElementById('g-recaptcha-response');
                                if (textarea) {{
                                    textarea.innerHTML = "{token}";
                                    textarea.value = "{token}";
                                }}
                            }}''')
                        
                        # Submit
                        page.locator(SUBMIT_BUTTON_SELECTOR).first.click()
                        
                        # Wait for results
                        success, row_count = wait_for_results(page)
                        
                        if not success:
                            print(f"[WORKER {worker_id}]   ⚠️ Timeout")
                            log_entry.update({
                                'status': 'TIMEOUT',
                                'count': 0,
                                'disqualifying_probate_found': False,
                                'overall_property_failed': owners_failed_filter
                            })
                            results_queue.put(log_entry)
                            go_back_to_search(page)
                            continue
                        
                        if row_count > 0:
                            print(f"[WORKER {worker_id}]   ✓ Found {row_count} result(s)")
                            
                            owner_disqualified = process_search_results(page, first, middle, last)
                            
                            if owner_disqualified:
                                owners_failed_filter = True
                            
                            log_entry.update({
                                'status': 'DISQUALIFIED' if owner_disqualified else 'FOUND_CLEAN',
                                'count': row_count,
                                'disqualifying_probate_found': owner_disqualified,
                                'overall_property_failed': owners_failed_filter
                            })
                            
                            if owner_disqualified:
                                print(f"[WORKER {worker_id}]   ⚠️ DISQUALIFIED")
                            else:
                                print(f"[WORKER {worker_id}]   ✓ CLEAN")
                        else:
                            print(f"[WORKER {worker_id}]   ✓ No results")
                            log_entry.update({
                                'status': 'NOT_FOUND',
                                'count': 0,
                                'disqualifying_probate_found': False,
                                'overall_property_failed': owners_failed_filter
                            })
                        
                        go_back_to_search(page)
                    
                    except Exception as e:
                        print(f"[WORKER {worker_id}]   ✗ Error: {str(e)[:100]}")
                        log_entry.update({
                            'status': 'ERROR',
                            'count': 0,
                            'error': str(e),
                            'disqualifying_probate_found': False,
                            'overall_property_failed': owners_failed_filter
                        })
                        try:
                            go_back_to_search(page)
                        except:
                            pass
                    
                    results_queue.put(log_entry)
                
                local_processed += 1
                
                # Update global stats
                with stats_lock:
                    stats_dict['completed'] += 1
                    stats_dict['in_progress'] -= 1
                
                # Progress update
                elapsed = time.time() - start_time
                rate = (local_processed / elapsed * 60) if elapsed > 0 else 0
                
                with stats_lock:
                    global_completed = stats_dict['completed']
                    global_qualified = stats_dict['qualified']
                
                remaining = total_tasks - global_completed
                global_rate = (global_completed / elapsed * 60) if elapsed > 0 else 0
                eta_seconds = (remaining / global_rate * 60) if global_rate > 0 else 0
                
                print(f"\n[WORKER {worker_id}] Local: {local_processed} processed")
                print(f"[WORKER {worker_id}] Local Rate: {rate:.2f} owners/min")
                print(f"[WORKER {worker_id}] GLOBAL: {global_completed}/{total_tasks} | "
                      f"Qualified: {global_qualified} | Rate: {global_rate:.2f}/min | ETA: {eta_seconds/60:.1f}min\n")
            
            except Empty:
                with stats_lock:
                    if stats_dict['in_progress'] == 0:
                        print(f"[WORKER {worker_id}] Queue empty, shutting down.")
                        break
                continue
            except Exception as e:
                print(f"[WORKER {worker_id}] ✗ Unexpected error: {str(e)}")
                continue
        
        browser.close()
    
    elapsed_total = time.time() - start_time
    
    print(f"\n[WORKER {worker_id}] {'='*80}")
    print(f"[WORKER {worker_id}] SHUTDOWN COMPLETE")
    print(f"[WORKER {worker_id}] Processed: {local_processed}")
    print(f"[WORKER {worker_id}] Time: {elapsed_total/60:.1f} minutes")
    print(f"[WORKER {worker_id}] {'='*80}\n")

# ==============================================================================
# RESULT WRITING FUNCTION
# ==============================================================================

def parse_address(address_string):
    """
    Parse address into components: street, city, state, zip
    Handles formats like: "123 MAIN ST, DALLAS, TX 75001"
    """
    if not address_string or address_string == 'N/A':
        return '', '', '', ''
    
    # Clean up the address
    address = address_string.strip()
    
    # Try to split by commas
    parts = [p.strip() for p in address.split(',')]
    
    if len(parts) >= 3:
        # Format: STREET, CITY, STATE ZIP
        street = parts[0]
        city = parts[1]
        
        # Parse state and zip from last part
        state_zip = parts[2].split()
        state = state_zip[0] if len(state_zip) > 0 else ''
        zip_code = state_zip[1] if len(state_zip) > 1 else ''
        
        return street, city, state, zip_code
    
    elif len(parts) == 2:
        # Format: STREET, CITY STATE ZIP
        street = parts[0]
        rest = parts[1].split()
        
        # Last item is likely zip, second to last is state
        if len(rest) >= 3:
            city = ' '.join(rest[:-2])
            state = rest[-2]
            zip_code = rest[-1]
        elif len(rest) == 2:
            city = ''
            state = rest[0]
            zip_code = rest[1]
        else:
            city = parts[1]
            state = ''
            zip_code = ''
        
        return street, city, state, zip_code
    
    else:
        # Can't parse properly, return full address as street
        return address, '', '', ''

def extract_property_address(full_address_string):
    """
    Extract property address from strings like:
    - "ALEXANDER OLLIE MAE EST OF 3147 MCDERMOTT AVE DALLAS, TX 75215-0000"
    - "CANTU MARK 1909 LEROY RD DALLAS, TX 75217-0000"
    
    Returns everything starting from the first sequence of digits (house number).
    """
    if not full_address_string or full_address_string == 'N/A':
        return 'N/A'
    
    # Find the first occurrence of a sequence of digits (house number)
    # This regex finds the position of the first digit sequence
    match = re.search(r'\d+', full_address_string)
    
    if match:
        # Extract everything from the start of the house number onward
        property_address = full_address_string[match.start():].strip()
        return property_address
    
    # If no digits found, return the original string
    return full_address_string

def result_writer_process(results_queue, txt_output_file, csv_output_file, stats_dict, stats_lock, total_tasks):
    """
    Dedicated process for writing results to files.
    Continuously reads from results queue and writes to both TXT and CSV files.
    """
    
    print(f"\n[WRITER] Starting result writer process...")
    print(f"[WRITER] TXT file: {txt_output_file}")
    print(f"[WRITER] CSV file: {csv_output_file}")
    
    # CSV file headers - new format
    csv_headers = [
        'First Name',
        'Last Name',
        'Middle Name',
        'Property Address',
        'Property City',
        'Property State',
        'Property Zip'
    ]
    
    # Initialize CSV file with headers
    with open(csv_output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
        writer.writeheader()
    
    qualified_count = 0
    total_processed = 0
    
    while True:
        try:
            entry = results_queue.get(timeout=5)
            
            if entry is None:  # Poison pill
                print(f"[WRITER] Received shutdown signal")
                break
            
            # Only write FOUND_CLEAN and NOT_FOUND to output files
            if entry.get('status') in ['FOUND_CLEAN', 'NOT_FOUND']:
                qualified_count += 1
                
                # Write to TXT file
                with open(txt_output_file, 'a', encoding='utf-8') as txt_file:
                    txt_file.write(f"Row: {entry['row']}\n")
                    txt_file.write(f"Owner: {entry['raw_owner']}\n")
                    txt_file.write(f"Search Term: {entry['search_term']}\n")
                    txt_file.write(f"Account Number: {entry['account_number']}\n")
                    txt_file.write(f"Address: {entry['address']}\n")
                    txt_file.write(f"Market Value: ${entry['market_value']}\n")
                    txt_file.write(f"Total Tax Owed: ${entry['total_tax_owed']}\n")
                    txt_file.write(f"Tax to Value Ratio: {entry['tax_to_value_ratio']}%\n")
                    txt_file.write(f"Prior Year Due: ${entry['prior_year_due']}\n")
                    txt_file.write(f"Current Levy: ${entry['current_levy']}\n")
                    txt_file.write(f"Unpaid Years: {entry['unpaid_years']}\n")
                    txt_file.write(f"-" * 50 + "\n")
                    txt_file.write(f"Probate Search Status: {entry['status']}\n")
                    txt_file.write(f"Result Count: {entry['count']}\n")
                    txt_file.write("="*100 + "\n\n")
                    txt_file.flush()
                
                # Write to CSV file - new format
                # First extract the property address (everything from house number onward)
                property_address = extract_property_address(entry['address'])
                
                # Then parse the property address into components
                street, city, state, zip_code = parse_address(property_address)
                
                with open(csv_output_file, 'a', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
                    writer.writerow({
                        'First Name': entry['first_name'],
                        'Last Name': entry['last_name'],
                        'Middle Name': entry['middle_name'],
                        'Property Address': street,
                        'Property City': city,
                        'Property State': state,
                        'Property Zip': zip_code
                    })
                    csvfile.flush()
                
                # Update qualified count
                with stats_lock:
                    stats_dict['qualified'] = qualified_count
            
            total_processed += 1
            
            if total_processed % 10 == 0:
                print(f"[WRITER] Processed {total_processed} results, {qualified_count} qualified")
        
        except Empty:
            # Check if all workers are done
            with stats_lock:
                if stats_dict['completed'] >= total_tasks and stats_dict['in_progress'] == 0:
                    print(f"[WRITER] All work complete, shutting down")
                    break
            continue
        except Exception as e:
            print(f"[WRITER] ✗ Error writing result: {str(e)}")
            continue
    
    print(f"[WRITER] Final stats: {total_processed} total results, {qualified_count} qualified")
    print(f"[WRITER] Shutdown complete\n")

# ==============================================================================
# 🚀 MAIN EXECUTION
# ==============================================================================

def main():
    """Main execution function with parallel processing"""
    
    print("\n" + "="*100)
    print("PARALLEL PROBATE SEARCH - DYNAMIC WORK QUEUE")
    print("="*100)
    print(f"Configuration:")
    print(f"  - Parallel Workers: {NUM_PARALLEL_INSTANCES}")
    print(f"  - Starting from row: {START_FROM_ROW}")
    print(f"  - Ending at row: {END_AT_ROW}")
    print(f"  - Headless mode: {HEADLESS_MODE}")
    print(f"  - Names file: {NAMES_FILE}")
    print(f"  - Output folder: {OUTPUT_FOLDER}")
    print("="*100 + "\n")
    
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    # Load owners and property data
    all_owners, property_data = extract_owners_from_file(NAMES_FILE)
    
    start_idx = max(0, START_FROM_ROW - 1)
    end_idx = min(len(all_owners), END_AT_ROW)
    
    owners_to_process = all_owners[start_idx:end_idx]
    
    if not owners_to_process:
        print("No owners to process!")
        return
    
    total_tasks = len(owners_to_process)
    
    print(f"Total owners to process: {total_tasks}")
    print(f"Processing range: rows {START_FROM_ROW} to {END_AT_ROW}\n")
    
    # Record start time
    overall_start = time.time()
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    
    # Create output files
    txt_output_file = os.path.join(OUTPUT_FOLDER, f"{LOG_FILE_NAME.replace('.txt', '')}_{timestamp}.txt")
    csv_output_file = os.path.join(OUTPUT_FOLDER, f"{CSV_FILE_NAME.replace('.csv', '')}_{timestamp}.csv")
    
    # Create shared resources
    manager = Manager()
    work_queue = manager.Queue()
    results_queue = manager.Queue()
    stats_lock = manager.Lock()
    txt_file_lock = manager.Lock()
    csv_file_lock = manager.Lock()
    stats_dict = manager.dict()
    
    # Convert property_data to manager dict
    property_data_dict = manager.dict(property_data)
    
    # Initialize stats
    stats_dict['completed'] = 0
    stats_dict['qualified'] = 0
    stats_dict['in_progress'] = 0
    
    # Create TXT file header
    with open(txt_output_file, 'w', encoding='utf-8') as f:
        f.write("="*100 + "\n")
        f.write("PROBATE SEARCH RESULTS - QUALIFIED PROPERTIES ONLY\n")
        f.write("="*100 + "\n")
        f.write(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total owners to process: {total_tasks}\n")
        f.write(f"Processing range: rows {START_FROM_ROW} to {END_AT_ROW}\n")
        f.write(f"Number of workers: {NUM_PARALLEL_INSTANCES}\n")
        f.write("="*100 + "\n\n")
    
    print(f"Created output files:")
    print(f"  TXT: {txt_output_file}")
    print(f"  CSV: {csv_output_file}\n")
    
    # Populate work queue
    print("Populating work queue...")
    for idx, raw_owner in enumerate(owners_to_process, start=start_idx+1):
        parsed_owners = parse_owner_name(raw_owner)
        work_queue.put((idx, raw_owner, parsed_owners))
    
    print(f"Work queue populated with {work_queue.qsize()} tasks")
    
    # Add poison pills
    for _ in range(NUM_PARALLEL_INSTANCES):
        work_queue.put(None)
    
    print(f"\nStarting {NUM_PARALLEL_INSTANCES} worker processes...\n")
    
    # Start result writer process
    writer_process = multiprocessing.Process(
        target=result_writer_process,
        args=(results_queue, txt_output_file, csv_output_file, stats_dict, stats_lock, total_tasks)
    )
    writer_process.start()
    print(f"Started Writer Process (PID: {writer_process.pid})")
    
    # Start worker processes
    processes = []
    for i in range(NUM_PARALLEL_INSTANCES):
        p = multiprocessing.Process(
            target=worker_process,
            args=(i+1, work_queue, results_queue, stats_dict, stats_lock,
                  txt_file_lock, csv_file_lock, txt_output_file, csv_output_file,
                  property_data_dict, HEADLESS_MODE, SLOW_MO, total_tasks)
        )
        p.start()
        processes.append(p)
        print(f"Started Worker {i+1} (PID: {p.pid})")
    
    print(f"\nAll {NUM_PARALLEL_INSTANCES} workers running...\n")
    
    # Wait for all worker processes to complete
    for i, p in enumerate(processes, 1):
        p.join()
        print(f"Worker {i} has finished")
    
    # Send poison pill to writer and wait
    results_queue.put(None)
    writer_process.join()
    print(f"Writer process has finished")
    
    overall_elapsed = time.time() - overall_start
    
    # Write final summary to TXT file
    with open(txt_output_file, 'a', encoding='utf-8') as f:
        f.write("\n" + "="*100 + "\n")
        f.write("FINAL SUMMARY\n")
        f.write("="*100 + "\n")
        f.write(f"Total Owners Processed: {stats_dict['completed']}\n")
        f.write(f"Total Qualified Properties: {stats_dict['qualified']}\n")
        f.write(f"Disqualified: {stats_dict['completed'] - stats_dict['qualified']}\n")
        if stats_dict['completed'] > 0:
            f.write(f"Qualification Rate: {stats_dict['qualified']/stats_dict['completed']*100:.1f}%\n")
        f.write(f"Total Processing Time: {overall_elapsed/60:.1f} minutes\n")
        f.write(f"Average Speed: {stats_dict['completed']/(overall_elapsed/60):.1f} owners/minute\n")
        f.write(f"Number of Workers: {NUM_PARALLEL_INSTANCES}\n")
        f.write(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Display final results
    print("\n" + "="*100)
    print("ALL WORKERS COMPLETE!")
    print("="*100)
    
    total_processed = stats_dict['completed']
    total_qualified = stats_dict['qualified']
    
    print(f"\nOverall Statistics:")
    print(f"  Total Owners Processed: {total_processed}")
    print(f"  Total Qualified Properties: {total_qualified}")
    print(f"  Disqualified: {total_processed - total_qualified}")
    if total_processed > 0:
        print(f"  Qualification Rate: {total_qualified/total_processed*100:.1f}%")
    print(f"  Total Processing Time: {overall_elapsed/60:.1f} minutes")
    print(f"  Average Speed: {total_processed/(overall_elapsed/60):.1f} owners/minute")
    
    print(f"\n{'='*100}")
    print(f"Results saved to:")
    print(f"  TXT: {txt_output_file}")
    print(f"  CSV: {csv_output_file}")
    print(f"{'='*100}\n")

if __name__ == "__main__":
    multiprocessing.set_start_method('spawn', force=True)
    main()