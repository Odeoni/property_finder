"""
Update Harris County with Tax and Probate URLs

This script updates the existing Harris County record in the database
with the correct search URLs for tax and probate records.
"""

import sys
from pathlib import Path

# Determine project root based on script location
script_dir = Path(__file__).parent.resolve()

# Check if we're in the database folder or project root
if script_dir.name == 'database':
    # Script is in database folder, go up one level to project root
    project_root = script_dir.parent
else:
    # Script is in project root
    project_root = script_dir

sys.path.insert(0, str(project_root))

# Now import - if script is IN database folder, use relative import
if script_dir.name == 'database':
    from models import SessionLocal, County
else:
    from database.models import SessionLocal, County

from datetime import datetime


def update_harris_county():
    """Update Harris County with tax and probate search URLs"""
    
    db = SessionLocal()
    
    try:
        # Find Harris County in database
        harris = db.query(County).filter(
            County.name == 'Harris',
            County.state == 'TX'
        ).first()
        
        if not harris:
            print("❌ Harris County not found in database!")
            print("Creating new Harris County record...")
            
            # Create Harris County if it doesn't exist
            harris = County(
                name='Harris',
                state='TX',
                fips_code='48201',  # Harris County, TX FIPS code
                population=4731145,
                is_active=True
            )
            db.add(harris)
            db.flush()  # Get the ID
            print(f"✓ Created Harris County (ID: {harris.id})")
        else:
            print(f"✓ Found Harris County (ID: {harris.id})")
        
        # Update Tax Search URL
        harris.tax_search_url = 'https://harriscountyga.governmentwindow.com/select-map-code.html?mode=&lang=&name=Smith&tax_year=2024&map_type='
        harris.tax_scraper_generated = False
        harris.tax_last_tested = None
        
        print(f"✓ Updated Tax Search URL:")
        print(f"  {harris.tax_search_url}")
        
        # Update Probate Search URL
        harris.probate_search_url = 'https://www.cclerk.hctx.net/applications/websearch/CourtSearch.aspx?CaseType=Probate'
        harris.probate_scraper_generated = False
        harris.probate_last_tested = None
        
        print(f"✓ Updated Probate Search URL:")
        print(f"  {harris.probate_search_url}")
        
        # Update metadata
        harris.updated_at = datetime.utcnow()
        
        # Commit changes
        db.commit()
        
        print(f"\n{'='*80}")
        print("✅ SUCCESS! Harris County updated in database")
        print(f"{'='*80}")
        print(f"\nCounty ID: {harris.id}")
        print(f"Name: {harris.name}, {harris.state}")
        print(f"Tax URL: {harris.tax_search_url}")
        print(f"Probate URL: {harris.probate_search_url}")
        print(f"\nReady to generate scrapers!")
        print(f"\nNext steps:")
        print(f"  # Generate tax scraper:")
        print(f"  python scout/agent.py --county-id {harris.id} --record-type tax")
        print(f"\n  # Generate probate scraper:")
        print(f"  python scout/agent.py --county-id {harris.id} --record-type probate")
        
        return harris.id
        
    except Exception as e:
        print(f"\n❌ Error updating Harris County: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        return None
        
    finally:
        db.close()


def verify_harris_county():
    """Verify Harris County configuration in database"""
    
    db = SessionLocal()
    
    try:
        harris = db.query(County).filter(
            County.name == 'Harris',
            County.state == 'TX'
        ).first()
        
        if not harris:
            print("❌ Harris County not found")
            return False
        
        print(f"\n{'='*80}")
        print("HARRIS COUNTY CONFIGURATION")
        print(f"{'='*80}")
        print(f"ID: {harris.id}")
        print(f"Name: {harris.name}, {harris.state}")
        print(f"FIPS: {harris.fips_code}")
        print(f"Population: {harris.population:,}")
        print(f"Active: {harris.is_active}")
        print(f"\n--- TAX SEARCH ---")
        print(f"URL: {harris.tax_search_url or 'Not configured'}")
        print(f"Scraper Generated: {harris.tax_scraper_generated}")
        print(f"Scraper Path: {harris.tax_scraper_path or 'N/A'}")
        print(f"\n--- PROBATE SEARCH ---")
        print(f"URL: {harris.probate_search_url or 'Not configured'}")
        print(f"Scraper Generated: {harris.probate_scraper_generated}")
        print(f"Scraper Path: {harris.probate_scraper_path or 'N/A'}")
        print(f"\n--- PROPERTY SEARCH ---")
        print(f"URL: {harris.property_search_url or 'Not configured'}")
        print(f"Scraper Generated: {harris.property_scraper_generated}")
        print(f"Scraper Path: {harris.property_scraper_path or 'N/A'}")
        print(f"\n--- METADATA ---")
        print(f"Created: {harris.created_at}")
        print(f"Updated: {harris.updated_at}")
        print(f"Scouted: {harris.scouted_at or 'Never'}")
        print(f"{'='*80}\n")
        
        return True
        
    finally:
        db.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Update Harris County URLs')
    parser.add_argument('--verify', action='store_true', help='Verify current configuration')
    
    args = parser.parse_args()
    
    if args.verify:
        verify_harris_county()
    else:
        update_harris_county()