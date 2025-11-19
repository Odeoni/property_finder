from playwright.sync_api import sync_playwright
import time
import re
import os
import multiprocessing
from multiprocessing import Manager, Queue, Lock
from datetime import datetime
import sys

# ============================================================================
# CONFIGURATION SECTION
# ============================================================================

# FILTER CRITERIA
MIN_CONSECUTIVE_YEARS = 3  # At least 3 consecutive years unpaid
CURRENT_YEAR = 2025
MIN_ANNUAL_TAX_RATE = 0.015  # 1.5% of market value per year
MAX_TOTAL_TAX_RATIO = 0.70  # Total tax cannot exceed 70% of market value

# PARALLEL PROCESSING CONFIGURATION
NUM_PARALLEL_INSTANCES = 50  # Number of parallel browser instances
START_FROM_ROW = 900  # Resume from this row (1-indexed, use 1 to start from beginning)
END_AT_ROW = 1000  # Stop at this row (None = process all remaining rows, or specify a number like 2000)

# FILE PATHS
NAMES_FILE = r"C:\Users\KISFECO\Documents\Real Estate Automations\outputlist\unique_estate_owners.txt"
OUTPUT_FOLDER = r"C:\Users\KISFECO\Documents\Real Estate Automations\tax"

# PERFORMANCE SETTINGS
HEADLESS_MODE = True  # Set to False to see browsers (useful for debugging)
SLOW_MO = 0  # Milliseconds delay between actions (0 = fastest, 500 = slower for debugging)

# ============================================================================
# CORE SCRAPING FUNCTIONS (unchanged from original)
# ============================================================================

def check_consecutive_unpaid_years(page, market_value):
    """Check for consecutive unpaid years where NO payments were made"""
    
    try:
        print("    Clicking on tax details...") 
        
        # Click on "Taxes Due Detail by Year and Jurisdiction"
        page.click('a:has-text("Taxes Due Detail by Year and Jurisdiction")')
        page.wait_for_load_state("networkidle")
        
        # Get all table rows
        rows = page.locator('table tr').all()
        
        year_data = {}
        
        for row in rows:
            row_text = row.inner_text()
            
            # Look for year (4 digits at start of row)
            year_match = re.search(r'^(\d{4})\s', row_text)
            if year_match:
                year = int(year_match.group(1))
                
                # Extract all dollar amounts in the row
                amounts = re.findall(r'\$?([\d,]+\.\d{2})', row_text)
                if amounts:
                    # Last amount is typically "Total Due"
                    total_due = float(amounts[-1].replace(',', ''))
                    year_data[year] = total_due
                    print(f"    Year {year}: ${total_due:.2f}")
        
        if not year_data:
            print("    ✗ No year data found")
            return False, {}, 0.0
        
        # Calculate total tax owed across ALL years
        total_tax_all_years = sum(year_data.values())
        tax_ratio = total_tax_all_years / market_value if market_value > 0 else 0
        
        print(f"    Total tax: ${total_tax_all_years:,.2f}, Ratio: {tax_ratio:.1%}")
        
        # Check if total tax exceeds 70% of market value
        if tax_ratio > MAX_TOTAL_TAX_RATIO:
            print(f"    ✗ Tax ratio too high ({tax_ratio:.1%} > 70%)")
            return False, year_data, total_tax_all_years
        
        # Check for consecutive unpaid years
        consecutive_years = []
        expected_annual_tax = market_value * MIN_ANNUAL_TAX_RATE
        
        print(f"    Expected annual tax: ${expected_annual_tax:.2f}")
        
        # Start from 2024 and go backwards
        for year in range(CURRENT_YEAR - 1, CURRENT_YEAR - 10, -1):
            if year not in year_data:
                print(f"    Year {year}: Not in records - breaking streak")
                break
            
            amount_due = year_data[year]
            
            # Check if the amount due is substantial (indicating no payment was made)
            if amount_due >= expected_annual_tax:
                consecutive_years.append(year)
                print(f"    Year {year}: ✓ Unpaid")
            else:
                print(f"    Year {year}: ✗ Payment detected - breaking streak")
                break
        
        print(f"    Consecutive unpaid years: {consecutive_years}")
        
        # Check if we have enough consecutive years
        if len(consecutive_years) < MIN_CONSECUTIVE_YEARS:
            print(f"    ✗ Only {len(consecutive_years)} consecutive years (need {MIN_CONSECUTIVE_YEARS})")
            return False, year_data, total_tax_all_years
        
        print(f"    ✓ Has {len(consecutive_years)} consecutive unpaid years!")
        return True, year_data, total_tax_all_years
        
    except Exception as e:
        print(f"    ✗ Error checking consecutive years: {e}")
        import traceback
        traceback.print_exc()
        return False, {}, 0.0

def extract_property_data(page):
    """Extract key data from property details page"""
    
    try:
        page_content = page.inner_text("body")
        
        # Extract Market Value
        market_value_match = re.search(r'Market Value:\s*\$?([\d,]+\.?\d*)', page_content)
        if market_value_match:
            market_value = float(market_value_match.group(1).replace(',', ''))
        else:
            print("    ✗ Could not find Market Value")
            return None
        
        # Extract Current Tax Levy
        current_levy_match = re.search(r'Current Tax Levy:\s*\$?([\d,]+\.?\d*)', page_content)
        current_levy = float(current_levy_match.group(1).replace(',', '')) if current_levy_match else 0
        
        # Extract Prior Year Amount Due
        prior_year_match = re.search(r'Prior Year Amount Due:\s*\$?([\d,]+\.?\d*)', page_content)
        prior_year_due = float(prior_year_match.group(1).replace(',', '')) if prior_year_match else 0
        
        print(f"    Current Tax Levy: ${current_levy:,.2f}")
        print(f"    Prior Year Amount Due: ${prior_year_due:,.2f}")
        
        # Check if Prior Year Amount Due is less than Current Tax Levy
        if prior_year_due < current_levy:
            print(f"    ✗ Prior year due (${prior_year_due:,.2f}) < Current levy (${current_levy:,.2f}) - skipping")
            return None
        
        # Extract Account Number
        account_match = re.search(r'Account Number:\s*(\S+)', page_content)
        account_number = account_match.group(1) if account_match else "Unknown"
        
        # Extract Address
        address_match = re.search(r'Address:\s*(.+?)(?=Property Site Address:|$)', page_content, re.DOTALL)
        if address_match:
            address = address_match.group(1).strip().replace('\n', ' ')
        else:
            address = "Unknown"
        
        print(f"    Account: {account_number}")
        print(f"    Address: {address}")
        print(f"    Market Value: ${market_value:,.2f}")
        print(f"    ✓ Prior year check passed")
        
        property_data = {
            'account_number': account_number,
            'address': address,
            'market_value': market_value,
            'current_levy': current_levy,
            'prior_year_due': prior_year_due,
        }
        
        return property_data
        
    except Exception as e:
        print(f"    ✗ Error extracting data: {e}")
        import traceback
        traceback.print_exc()
        return None

def search_and_extract(page, last_name, first_name):
    """Search owner and extract/filter property data"""
    
    try:
        print(f"  Navigating to search page...")
        # Search for owner
        page.goto("https://www.dallasact.com/act_webdev/dallas/index.jsp", wait_until="domcontentloaded")
        page.fill('input[name="criteria"]', last_name)
        page.fill('input[name="criteria2"]', first_name)
        
        print(f"  Searching for: {last_name}, {first_name}")
        
        page.click('input[value="Search"]')
        page.wait_for_load_state("networkidle")
        
        # Build search pattern
        search_pattern = f"{last_name} {first_name}"
        print(f"  Looking for names starting with: {search_pattern}")
        
        # Find matching row
        rows = page.locator('table tr[valign="top"]').all()
        print(f"  Found {len(rows)} result rows")
        
        for i, row in enumerate(rows):
            owner_cell = row.locator('td').nth(1)
            owner_text = owner_cell.inner_text().strip()
            
            print(f"  Row {i+1} owner text: {owner_text[:100]}...")
            
            # Check if it starts with LAST FIRST and contains EST OF
            if owner_text.startswith(search_pattern) and "EST OF" in owner_text:
                
                # SIMPLIFIED CHECK: Nothing can come after "EST OF"
                # Find the position of "EST OF" and check if there's any text after it (excluding whitespace and address)
                est_of_pos = owner_text.find("EST OF")
                after_est_of = owner_text[est_of_pos + 6:].strip()  # Everything after "EST OF"
                
                # Split by newline - first line should be the owner name
                first_line = owner_text.split('\n')[0].strip()
                
                # Check if anything comes after EST OF in the first line (the owner name line)
                if "EST OF" in first_line:
                    est_of_pos_in_line = first_line.find("EST OF")
                    after_est_of_in_line = first_line[est_of_pos_in_line + 6:].strip()
                    
                    if after_est_of_in_line:
                        print(f"  ✗ Text found after 'EST OF': '{after_est_of_in_line}' - skipping")
                        continue
                
                print(f"  ✓ Found match in row {i+1} with nothing after EST OF!")
                
                # Click account link
                print(f"  Clicking account link...")
                account_link = row.locator('td').first.locator('a')
                account_link.click()
                page.wait_for_load_state("networkidle")
                
                # Extract property data (includes prior year check)
                print(f"  Extracting property data...")
                property_data = extract_property_data(page)
                
                if not property_data:
                    print(f"  ✗ Failed prior year check or data extraction")
                    # Go back to search results
                    page.goto("https://www.dallasact.com/act_webdev/dallas/index.jsp", wait_until="domcontentloaded")
                    page.fill('input[name="criteria"]', last_name)
                    page.fill('input[name="criteria2"]', first_name)
                    page.click('input[value="Search"]')
                    page.wait_for_load_state("networkidle")
                    continue
                
                # Check consecutive unpaid years and total tax
                print(f"  Checking tax criteria...")
                meets_criteria, year_data, total_tax = check_consecutive_unpaid_years(page, property_data['market_value'])
                
                if meets_criteria:
                    # Store the actual full owner name from the search results
                    owner_lines = owner_text.split('\n')
                    full_owner_name = owner_lines[0].strip() if owner_lines else owner_text[:50]
                    
                    property_data['owner_name'] = full_owner_name
                    property_data['year_data'] = year_data
                    property_data['total_tax_owed'] = total_tax
                    property_data['tax_to_value_ratio'] = total_tax / property_data['market_value']
                    print(f"  ✓✓✓ QUALIFIED PROPERTY! ✓✓✓")
                    return property_data
                else:
                    print(f"  ✗ Does not meet criteria")
                    # Go back to search results to check next property
                    page.goto("https://www.dallasact.com/act_webdev/dallas/index.jsp", wait_until="domcontentloaded")
                    page.fill('input[name="criteria"]', last_name)
                    page.fill('input[name="criteria2"]', first_name)
                    page.click('input[value="Search"]')
                    page.wait_for_load_state("networkidle")
                    continue
        
        print(f"  ✗ No qualifying match found in any rows")
        return None
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

# ============================================================================
# DYNAMIC WORK QUEUE FUNCTIONS
# ============================================================================

def worker_process(worker_id, work_queue, results_queue, stats_dict, stats_lock, file_lock,
                   output_file, headless, slow_mo, total_tasks):
    """
    Worker process that continuously pulls tasks from shared queue.
    Runs until queue is empty. Writes to shared output file with locking.
    """
    
    local_qualified = 0
    local_processed = 0
    start_time = time.time()
    
    print(f"\n[WORKER {worker_id}] Starting up...")
    print(f"[WORKER {worker_id}] Writing to shared file: {output_file}")
    
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless, slow_mo=slow_mo)
        page = browser.new_page()
        
        # Continuously pull from queue until empty
        while True:
            try:
                # Non-blocking get with timeout
                owner_data = work_queue.get(timeout=2)
                
                if owner_data is None:  # Poison pill to signal shutdown
                    print(f"[WORKER {worker_id}] Received shutdown signal")
                    break
                
                original_row, last_name, first_name = owner_data
                
                # Update global stats
                with stats_lock:
                    stats_dict['in_progress'] += 1
                    current_progress = stats_dict['completed']
                
                print(f"\n[WORKER {worker_id}] {'='*80}")
                print(f"[WORKER {worker_id}] Row #{original_row}: {first_name} {last_name} EST OF")
                print(f"[WORKER {worker_id}] Global Progress: {current_progress}/{total_tasks} completed")
                print(f"[WORKER {worker_id}] Queue remaining: ~{work_queue.qsize()}")
                print(f"[WORKER {worker_id}] {'='*80}")
                
                try:
                    property_data = search_and_extract(page, last_name, first_name)
                    
                    local_processed += 1
                    
                    if property_data:
                        local_qualified += 1
                        
                        # Write to SHARED file with locking
                        with file_lock:
                            with open(output_file, 'a', encoding='utf-8') as f:
                                # Get current property number
                                current_property_num = stats_dict['qualified']
                                
                                f.write(f"Property #{current_property_num} (Original Row #{original_row}) [Worker {worker_id}]\n")
                                f.write("-"*100 + "\n")
                                f.write(f"Owner: {property_data['owner_name']}\n")
                                f.write(f"Account Number: {property_data['account_number']}\n")
                                f.write(f"Address: {property_data['address']}\n")
                                f.write(f"Market Value: ${property_data['market_value']:,.2f}\n")
                                f.write(f"Total Tax Owed: ${property_data['total_tax_owed']:,.2f}\n")
                                f.write(f"Tax to Value Ratio: {property_data['tax_to_value_ratio']:.1%}\n")
                                f.write(f"Prior Year Due: ${property_data['prior_year_due']:,.2f}\n")
                                f.write(f"Current Levy: ${property_data['current_levy']:,.2f}\n")
                                f.write(f"Unpaid Years: {list(property_data['year_data'].keys())}\n")
                                f.write("\n")
                                
                                # Flush to disk immediately
                                f.flush()
                                os.fsync(f.fileno())
                        
                        # Send qualified property to results queue
                        results_queue.put({
                            'worker_id': worker_id,
                            'row': original_row,
                            'data': property_data
                        })
                    
                    # Update global stats
                    with stats_lock:
                        stats_dict['completed'] += 1
                        stats_dict['qualified'] += (1 if property_data else 0)
                        stats_dict['in_progress'] -= 1
                    
                    # Progress update
                    elapsed = time.time() - start_time
                    rate = (local_processed / elapsed * 60) if elapsed > 0 else 0
                    
                    with stats_lock:
                        global_completed = stats_dict['completed']
                        global_qualified = stats_dict['qualified']
                        qualification_rate = (global_qualified/global_completed*100) if global_completed > 0 else 0
                    
                    remaining = total_tasks - global_completed
                    global_rate = (global_completed / elapsed * 60) if elapsed > 0 else 0
                    eta_seconds = (remaining / global_rate * 60) if global_rate > 0 else 0
                    
                    print(f"\n[WORKER {worker_id}] Local: {local_processed} processed, {local_qualified} qualified")
                    print(f"[WORKER {worker_id}] Local Rate: {rate:.2f} owners/min")
                    print(f"[WORKER {worker_id}] GLOBAL: {global_completed}/{total_tasks} | "
                          f"Qualified: {global_qualified} ({qualification_rate:.1f}%) | "
                          f"Rate: {global_rate:.2f}/min | ETA: {eta_seconds/60:.1f}min\n")
                
                except Exception as e:
                    print(f"[WORKER {worker_id}] ✗ Error processing row {original_row}: {e}")
                    import traceback
                    traceback.print_exc()
                    
                    local_processed += 1
                    
                    # Update global stats
                    with stats_lock:
                        stats_dict['completed'] += 1
                        stats_dict['in_progress'] -= 1
                    
                    continue
            
            except multiprocessing.queues.Empty:
                # Queue is empty, check if we're really done
                with stats_lock:
                    if stats_dict['in_progress'] == 0:
                        # No one else is working, we're done
                        print(f"[WORKER {worker_id}] Queue empty and no work in progress. Shutting down.")
                        break
                # Otherwise, wait a bit more in case more work appears
                print(f"[WORKER {worker_id}] Queue empty but work in progress elsewhere, waiting...")
                continue
        
        browser.close()
    
    elapsed_total = time.time() - start_time
    
    print(f"\n[WORKER {worker_id}] {'='*80}")
    print(f"[WORKER {worker_id}] SHUTDOWN COMPLETE")
    print(f"[WORKER {worker_id}] Processed: {local_processed} | Qualified: {local_qualified}")
    print(f"[WORKER {worker_id}] Time: {elapsed_total/60:.1f} minutes")
    print(f"[WORKER {worker_id}] {'='*80}\n")
    
    # Return stats for final summary
    return {
        'worker_id': worker_id,
        'processed': local_processed,
        'qualified': local_qualified,
        'elapsed_time': elapsed_total
    }

def load_owners_from_file(names_file, start_from_row=1, end_at_row=None):
    """Load owners from file, optionally starting from a specific row and ending at another"""
    
    print(f"Reading names from: {names_file}")
    print(f"Starting from row: {start_from_row}")
    if end_at_row:
        print(f"Ending at row: {end_at_row}")
    else:
        print(f"Ending at row: [last row in file]")
    
    all_owners = []
    
    with open(names_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
        # Skip header lines (first 2 lines)
        data_lines = lines[2:]
        
        for idx, line in enumerate(data_lines, start=1):  # 1-indexed
            parts = line.split()
            if len(parts) >= 2:
                last_name = parts[0]
                first_name = parts[1]
                all_owners.append((idx, last_name, first_name))
    
    print(f"Total owners in file: {len(all_owners)}")
    
    # Filter to start from specific row
    if start_from_row > 1:
        all_owners = [owner for owner in all_owners if owner[0] >= start_from_row]
        print(f"Owners after start filter (row {start_from_row}+): {len(all_owners)}")
    
    # Filter to end at specific row
    if end_at_row is not None:
        all_owners = [owner for owner in all_owners if owner[0] <= end_at_row]
        print(f"Owners after end filter (row <={end_at_row}): {len(all_owners)}")
    
    return all_owners

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function with dynamic work queue"""
    
    print("\n" + "="*100)
    print("PARALLEL ESTATE PROPERTY SCRAPER - DYNAMIC WORK QUEUE")
    print("="*100)
    print(f"Configuration:")
    print(f"  - Parallel Workers: {NUM_PARALLEL_INSTANCES}")
    print(f"  - Starting from row: {START_FROM_ROW}")
    print(f"  - Ending at row: {END_AT_ROW if END_AT_ROW else '[last row]'}")
    print(f"  - Headless mode: {HEADLESS_MODE}")
    print(f"  - Slow motion: {SLOW_MO}ms")
    print(f"  - Names file: {NAMES_FILE}")
    print(f"  - Output folder: {OUTPUT_FOLDER}")
    print(f"  - Load Balancing: DYNAMIC (workers pull from shared queue)")
    print(f"  - Output Mode: SINGLE FILE (all workers write to one file)")
    print("="*100 + "\n")
    
    # Make sure output folder exists
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    # Load owners from file
    all_owners = load_owners_from_file(NAMES_FILE, START_FROM_ROW, END_AT_ROW)
    
    if not all_owners:
        print("No owners to process!")
        return
    
    total_tasks = len(all_owners)
    
    print(f"\nTotal owners to process: {total_tasks}")
    if END_AT_ROW:
        print(f"Processing range: rows {START_FROM_ROW} to {END_AT_ROW}")
    else:
        print(f"Processing range: rows {START_FROM_ROW} to end of file")
    print(f"Workers will dynamically pull tasks from shared queue")
    print(f"Expected benefit: Perfect load balancing, no idle workers\n")
    
    # Record start time
    overall_start = time.time()
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    
    # Create single shared output file
    output_file = os.path.join(OUTPUT_FOLDER, f"qualified_properties_{timestamp}.txt")
    
    # Create shared resources using Manager
    manager = Manager()
    work_queue = manager.Queue()
    results_queue = manager.Queue()
    stats_lock = manager.Lock()
    file_lock = manager.Lock()  # Lock for writing to shared file
    stats_dict = manager.dict()
    
    # Initialize stats
    stats_dict['completed'] = 0
    stats_dict['qualified'] = 0
    stats_dict['in_progress'] = 0
    
    # Create output file with header
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("QUALIFIED ESTATE PROPERTIES - DYNAMIC WORK QUEUE\n")
        f.write("="*100 + "\n")
        f.write(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total owners to process: {total_tasks}\n")
        f.write(f"Processing range: rows {START_FROM_ROW} to {END_AT_ROW if END_AT_ROW else 'end'}\n")
        f.write(f"Number of workers: {NUM_PARALLEL_INSTANCES}\n")
        f.write("="*100 + "\n\n")
    
    print(f"Created shared output file: {output_file}\n")
    
    # Populate work queue
    print("Populating work queue...")
    for owner in all_owners:
        work_queue.put(owner)
    
    print(f"Work queue populated with {work_queue.qsize()} tasks")
    
    # Add poison pills (None) to signal workers to shut down when queue is empty
    for _ in range(NUM_PARALLEL_INSTANCES):
        work_queue.put(None)
    
    print(f"\nStarting {NUM_PARALLEL_INSTANCES} worker processes...\n")
    
    # Start worker processes
    processes = []
    for i in range(NUM_PARALLEL_INSTANCES):
        p = multiprocessing.Process(
            target=worker_process,
            args=(i+1, work_queue, results_queue, stats_dict, stats_lock, file_lock,
                  output_file, HEADLESS_MODE, SLOW_MO, total_tasks)
        )
        p.start()
        processes.append(p)
        print(f"Started Worker {i+1} (PID: {p.pid})")
    
    print(f"\nAll {NUM_PARALLEL_INSTANCES} workers running...\n")
    
    # Wait for all processes to complete
    for i, p in enumerate(processes, 1):
        p.join()
        print(f"Worker {i} has finished")
    
    overall_elapsed = time.time() - overall_start
    
    # Write final summary to file
    with open(output_file, 'a', encoding='utf-8') as f:
        f.write("\n" + "="*100 + "\n")
        f.write("FINAL SUMMARY\n")
        f.write("="*100 + "\n")
        f.write(f"Total Owners Processed: {stats_dict['completed']}\n")
        f.write(f"Total Qualified Properties: {stats_dict['qualified']}\n")
        if stats_dict['completed'] > 0:
            f.write(f"Overall Qualification Rate: {stats_dict['qualified']/stats_dict['completed']*100:.1f}%\n")
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
    if total_processed > 0:
        print(f"  Overall Qualification Rate: {total_qualified/total_processed*100:.1f}%")
    print(f"  Total Processing Time: {overall_elapsed/60:.1f} minutes")
    print(f"  Average Speed: {total_processed/(overall_elapsed/60):.1f} owners/minute")
    print(f"  Load Balancing: DYNAMIC (perfect utilization)")
    
    print(f"\n{'='*100}")
    print(f"Results saved to: {output_file}")
    print(f"{'='*100}\n")

if __name__ == "__main__":
    # Use multiprocessing spawn method for cross-platform compatibility
    multiprocessing.set_start_method('spawn', force=True)
    main()