# database/models.py

"""
Complete database schema for heir-finder system
Includes all tables needed for the full pipeline from SSDI data to heir discovery
"""

from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Text, DECIMAL, Date, JSON, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()


# ============================================================================
# CORE DATA TABLES
# ============================================================================

class DeceasedIndividual(Base):
    """
    Core table: Stores individuals from SSDI dataset
    This is the starting point for all research
    """
    __tablename__ = 'deceased_individuals'
    
    id = Column(Integer, primary_key=True)
    
    # SSDI data fields
    ssn_full = Column(String(9), unique=True, index=True)  # Full 9-digit SSN
    ssn_last_4 = Column(String(4), index=True)  # Last 4 digits (for privacy)
    first_name = Column(String(100), index=True)
    middle_initial = Column(String(5))  # Single letter
    last_name = Column(String(100), index=True)
    name_suffix = Column(String(10))  # Jr, Sr, III, etc (the 4th name part)
    verified = Column(Boolean, default=False)

    birth_date = Column(Date)
    death_date = Column(Date, index=True)

    # Location data (as it actually appears in SSDI)
    last_residence_zip = Column(String(10), index=True)  # This is the primary location data
    last_residence_state_code = Column(String(10))  # Could be 'TX', '44', or blank
    last_residence_state_normalized = Column(String(2), index=True)  # We'll normalize to 'TX' format

    # Derived/enriched location data (we'll look up from ZIP) 
    last_residence_city = Column(String(100))  # Looked up from ZIP code
    last_residence_county = Column(String(100))  # Looked up from ZIP code
    last_residence_state = Column(String(2), index=True)  # Normalized state code

    # SSN state derivation
    ssn_area_number = Column(String(3))  # First 3 digits of SSN
    ssn_issued_state = Column(String(2))  # Derived from area number (yes, it's encoded!)
    
    # Processing metadata
    processing_status = Column(String(50), default='queued', index=True)  
    # Status: queued, property_search, tax_check, probate_check, genealogy, skiptracing, completed, failed
    
    priority = Column(Integer, default=0, index=True)  # Higher = process first
    
    # Research results summary
    has_property = Column(Boolean, default=False)
    property_is_delinquent = Column(Boolean, default=False)
    has_probate_case = Column(Boolean, default=False)
    heirs_found = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at = Column(DateTime)
    
    # Relationships
    properties = relationship("Property", back_populates="deceased")
    probate_cases = relationship("ProbateCase", back_populates="deceased")
    genealogy_tree = relationship("GenealogyTree", back_populates="deceased", uselist=False)
    heirs = relationship("Heir", back_populates="deceased_individual")
    jobs = relationship("Job", back_populates="deceased")


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
    fips_code = Column(String(5), unique=True)  # Federal Information Processing Standards code
    population = Column(Integer)
    
    # Property search configuration
    property_search_url = Column(Text)
    property_search_selectors = Column(JSON)  # Stores the CSS selectors discovered by Scout
    property_scraper_generated = Column(Boolean, default=False)
    property_scraper_path = Column(Text)  # Path to generated scraper file
    property_last_tested = Column(DateTime)
    
    # Proxy requirements for property search
    property_requires_proxy = Column(Boolean, default=False)
    property_requires_us_ip = Column(Boolean, default=False)  # Blocks non-US
    property_blocks_datacenter_ips = Column(Boolean, default=False)  # Needs residential
    
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
    
    # Global proxy settings (if all sources need proxy)
    requires_proxy = Column(Boolean, default=False)  # Any source needs proxy
    proxy_type = Column(String(20))  # 'residential', 'datacenter', 'mobile'
    proxy_provider = Column(String(50))  # 'brightdata', 'oxylabs', 'smartproxy', etc.
    proxy_cost_per_request = Column(DECIMAL(6, 4))  # Track costs
    
    # Scout metadata
    scouted_at = Column(DateTime)
    scout_confidence = Column(DECIMAL(3, 2))  # 0.00 to 1.00
    scout_notes = Column(Text)
    
    # Operational flags
    is_active = Column(Boolean, default=True)  # Can we scrape this county?
    requires_captcha = Column(Boolean, default=False)
    requires_authentication = Column(Boolean, default=False)
    blocks_vpn = Column(Boolean, default=False)  # Site detects/blocks VPNs
    
    # Rate limiting info
    rate_limit_requests_per_minute = Column(Integer)  # Max requests before block
    rate_limit_cooldown_seconds = Column(Integer)  # Wait time after limit hit
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    properties = relationship("Property", back_populates="county")
    probate_cases = relationship("ProbateCase", back_populates="county")


# ============================================================================
# PROPERTY & TAX TABLES
# ============================================================================

class Property(Base):
    """
    Discovered properties owned by deceased individuals
    Includes tax delinquency status
    """
    __tablename__ = 'properties'
    
    id = Column(Integer, primary_key=True)
    deceased_id = Column(Integer, ForeignKey('deceased_individuals.id'), index=True)
    county_id = Column(Integer, ForeignKey('counties.id'), index=True)
    
    # Property identification
    parcel_id = Column(String(50), index=True)
    apn = Column(String(50))  # Assessor's Parcel Number (alternative to parcel_id)
    
    # Property details
    address = Column(Text)
    city = Column(String(100))
    state = Column(String(2))
    zip_code = Column(String(10))
    
    owner_name = Column(Text)  # Name as it appears on records
    owner_matches_deceased = Column(Boolean, default=False)
    
    # Property value
    assessed_value = Column(DECIMAL(12, 2))
    market_value = Column(DECIMAL(12, 2))
    land_value = Column(DECIMAL(12, 2))
    improvement_value = Column(DECIMAL(12, 2))
    
    # Property type
    property_type = Column(String(50))  # residential, commercial, land, etc.
    square_footage = Column(Integer)
    year_built = Column(Integer)
    
    # Tax delinquency information
    is_delinquent = Column(Boolean, default=False, index=True)
    delinquency_years = Column(Integer)
    total_owed = Column(DECIMAL(12, 2))
    last_payment_date = Column(Date)
    last_payment_amount = Column(DECIMAL(12, 2))
    
    # Tax sale information
    in_foreclosure = Column(Boolean, default=False)
    tax_sale_date = Column(Date)
    redemption_period_ends = Column(Date)
    
    # Evidence of activity
    has_recent_activity = Column(Boolean, default=False)
    last_activity_date = Column(Date)
    activity_notes = Column(Text)
    
    # Discovery metadata
    discovered_at = Column(DateTime, default=datetime.utcnow)
    data_source = Column(Text)
    raw_data = Column(JSON)  # Store complete scraped data
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    deceased = relationship("DeceasedIndividual", back_populates="properties")
    county = relationship("County", back_populates="properties")
    liens = relationship("Lien", back_populates="property")


class Lien(Base):
    """
    Liens and judgments against properties
    """
    __tablename__ = 'liens'
    
    id = Column(Integer, primary_key=True)
    property_id = Column(Integer, ForeignKey('properties.id'), index=True)
    
    lien_type = Column(String(50))  # tax, mortgage, judgment, mechanic, etc.
    lien_holder = Column(Text)
    amount = Column(DECIMAL(12, 2))
    filing_date = Column(Date)
    status = Column(String(50))  # active, released, foreclosed
    
    document_number = Column(String(50))
    book_page = Column(String(50))
    
    discovered_at = Column(DateTime, default=datetime.utcnow)
    data_source = Column(Text)
    raw_data = Column(JSON)
    
    # Relationships
    property = relationship("Property", back_populates="liens")


# ============================================================================
# PROBATE TABLES
# ============================================================================

class ProbateCase(Base):
    """
    Probate court cases for deceased individuals
    """
    __tablename__ = 'probate_cases'
    
    id = Column(Integer, primary_key=True)
    deceased_id = Column(Integer, ForeignKey('deceased_individuals.id'), index=True)
    county_id = Column(Integer, ForeignKey('counties.id'), index=True)
    
    has_probate = Column(Boolean, default=False, index=True)

    # Case identification
    case_number = Column(String(50), unique=True, index=True)
    case_type = Column(String(50))  # probate, administration, small estate, etc.
    case_status = Column(String(50), index=True)  # open, closed, pending, etc.
    
    # Case details
    filing_date = Column(Date)
    closing_date = Column(Date)
    
    # Parties involved
    executor_name = Column(Text)
    executor_address = Column(Text)
    executor_phone = Column(String(20))
    
    attorney_name = Column(Text)
    attorney_firm = Column(Text)
    attorney_phone = Column(String(20))
    attorney_email = Column(String(255))
    
    # Estate information
    estate_value = Column(DECIMAL(12, 2))
    estate_description = Column(Text)
    
    # Heirs listed in probate documents
    heirs_listed = Column(JSON)  # Array of heir objects from court docs
    
    # Case closure
    is_closed = Column(Boolean, default=False, index=True)
    distribution_completed = Column(Boolean, default=False)
    
    # Documents
    has_will = Column(Boolean)
    will_contents = Column(Text)
    document_urls = Column(JSON)  # Links to court documents
    
    # Discovery metadata
    discovered_at = Column(DateTime, default=datetime.utcnow)
    data_source = Column(Text)
    raw_data = Column(JSON)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    deceased = relationship("DeceasedIndividual", back_populates="probate_cases")
    county = relationship("County", back_populates="probate_cases")


# ============================================================================
# GENEALOGY TABLES
# ============================================================================

class GenealogyTree(Base):
    """
    Complete family tree for a deceased individual
    RAW DATA from all genealogy sources - everything we found, unfiltered
    """
    __tablename__ = 'genealogy_trees'
    
    id = Column(Integer, primary_key=True)
    deceased_id = Column(Integer, ForeignKey('deceased_individuals.id'), unique=True, index=True)
    
    # Complete tree structure (nested JSON with ALL relatives found)
    tree_data = Column(JSON)  
    # Structure: {
    #   "spouse": {...},
    #   "children": [{name, birth_date, sources, ...}, ...],
    #   "grandchildren": [...],
    #   "siblings": [...],
    #   "parents": {...},
    #   "step_children": [...],
    #   "former_spouses": [...]
    # }
    
    # Research metadata
    sources_used = Column(JSON)  # ["obituary_legacy_com", "familysearch", "ancestry", etc.]
    tier_reached = Column(Integer)  # 1=obituary, 2=familysearch, 3=deep_dive
    confidence_score = Column(DECIMAL(3, 2))  # Overall confidence in research
    
    # Quick summary fields (derived from tree_data for easy querying)
    total_relatives_found = Column(Integer, default=0)
    total_children_found = Column(Integer, default=0)
    total_living_children_found = Column(Integer, default=0)
    total_spouses_found = Column(Integer, default=0)
    
    # Spouse summary (can have multiple from different sources/marriages)
    spouses_data = Column(JSON)
    # Structure: [
    #   {
    #     "name": "Mary Smith", 
    #     "married_at_death": true,
    #     "is_alive": false,
    #     "age": 67,
    #     "sources": ["obituary", "death_certificate"],
    #     "confidence": 0.95
    #   },
    #   {...}  # Former spouse
    # ]
    
    # Children data (dynamic list - as many as found)
    children_data = Column(JSON)
    # Structure: [
    #   {
    #     "full_name": "John Doe Jr",
    #     "first_name": "John",
    #     "last_name": "Doe",
    #     "relationship": "biological_son",
    #     "birth_date": "1965-03-15",
    #     "birth_place": "Dallas, TX",
    #     "is_alive": true,
    #     "current_age": 58,
    #     "sources": ["obituary", "census_1970", "birth_record"],
    #     "confidence": 0.92,
    #     "skiptrace_results": {
    #       "addresses": [...],
    #       "phones": [...],
    #       "emails": [...]
    #     }
    #   },
    #   {...}  # Another child
    # ]
    
    # Grandchildren data (if relevant for intestate succession)
    grandchildren_data = Column(JSON)
    
    # Siblings data (backup heirs if no spouse/children)
    siblings_data = Column(JSON)
    
    # All skiptrace results (raw data from multiple services)
    skiptrace_raw_data = Column(JSON)
    # Structure: {
    #   "beenverified": {...},
    #   "tloxp": {...},
    #   "tracers": {...},
    #   "idi": {...}
    # }
    
    # Conflicts and gaps
    conflicts_found = Column(JSON)  
    # Structure: [
    #   {
    #     "field": "child_name",
    #     "source1": {"value": "John", "source": "obituary"},
    #     "source2": {"value": "Johnny", "source": "census"},
    #     "resolved": false
    #   }
    # ]
    
    gaps_identified = Column(JSON)
    # Structure: ["missing_spouse_death_date", "child_2_current_address", ...]
    
    # Research notes
    research_notes = Column(Text)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    research_completed_at = Column(DateTime)
    
    # Relationship
    deceased = relationship("DeceasedIndividual", back_populates="genealogy_tree")


class Heir(Base):
    """
    VALIDATED, ACTIONABLE heirs - the final answer of who inherits and who to contact
    This is populated by a synthesis script that analyzes GenealogyTree data
    """
    __tablename__ = 'heirs'
    
    id = Column(Integer, primary_key=True)
    deceased_id = Column(Integer, ForeignKey('deceased_individuals.id'), index=True)
    genealogy_tree_id = Column(Integer, ForeignKey('genealogy_trees.id'), index=True)
    
    # Heir identification (single validated identity)
    full_name = Column(Text, index=True)
    first_name = Column(String(100))
    middle_name = Column(String(100))
    last_name = Column(String(100))
    maiden_name = Column(String(100))
    
    # Relationship to deceased
    relationship_type = Column(String(50), index=True)  # spouse, child, grandchild, sibling
    is_biological = Column(Boolean)
    is_adopted = Column(Boolean)
    is_step_relation = Column(Boolean)
    
    # Heir details
    birth_date = Column(Date)
    birth_place = Column(String(255))
    current_age = Column(Integer)
    
    heir_is_deceased = Column(Boolean, default=False, index=True)
    death_date = Column(Date)
    
    # If heir is deceased, who inherits in their place (per stirpes)
    substitute_heirs = Column(JSON)  # Their children who step into their shoes
    
    # LEGAL STATUS (this is the key difference from GenealogyTree)
    is_legal_heir = Column(Boolean, default=False, index=True)  # Do they actually inherit?
    inheritance_percentage = Column(DECIMAL(5, 2))  # % of estate (e.g., 33.33 for 1 of 3 children)
    intestacy_order = Column(Integer)  # Priority: 1=spouse, 2=children, 3=grandchildren, etc.
    intestacy_class = Column(String(50))  # "Class 1 - Spouse", "Class 2 - Children", etc.
    
    # VALIDATED contact information (best info from multiple skiptrace sources)
    current_address = Column(Text)
    current_city = Column(String(100))
    current_state = Column(String(2))
    current_zip = Column(String(10))
    address_confidence = Column(DECIMAL(3, 2))  # How sure are we this is current?
    address_last_verified = Column(Date)
    
    phone_numbers = Column(JSON)  
    # Structure: [
    #   {"number": "214-555-1234", "type": "mobile", "confidence": 0.95, "source": "tloxp"},
    #   {"number": "214-555-5678", "type": "landline", "confidence": 0.80, "source": "beenverified"}
    # ]
    
    email_addresses = Column(JSON)
    # Structure: [{"email": "john@example.com", "confidence": 0.85, "source": "beenverified"}]
    
    social_media = Column(JSON)
    # Structure: [{"platform": "facebook", "url": "...", "confidence": 0.90}]
    
    # Employment (helps with validation)
    employer = Column(String(255))
    occupation = Column(String(100))
    
    # Known relatives (cross-validation)
    known_relatives = Column(JSON)
    # Structure: [
    #   {"name": "Jane Doe", "relationship": "spouse", "matches_genealogy": true},
    #   {"name": "Mary Doe", "relationship": "mother", "matches_genealogy": true}
    # ]
    
    # VALIDATION & CONFIDENCE (why we believe this is the right person)
    confidence_score = Column(DECIMAL(3, 2), index=True)  # Overall confidence
    validation_sources = Column(JSON)  
    # Structure: ["obituary_match", "skiptrace_relatives_match", "age_match", "location_match"]
    
    validation_notes = Column(Text)  # Why we're confident or not
    
    # Outreach tracking
    contacted = Column(Boolean, default=False)
    contact_method = Column(String(50))  # "mail", "phone", "email"
    contact_attempts = Column(Integer, default=0)
    last_contact_date = Column(Date)
    response_received = Column(Boolean, default=False)
    response_date = Column(Date)
    response_notes = Column(Text)
    
    # Legal status
    affidavit_signed = Column(Boolean, default=False)
    affidavit_date = Column(Date)
    willing_to_sell = Column(Boolean)
    
    # Timestamps
    identified_at = Column(DateTime, default=datetime.utcnow)
    validated_at = Column(DateTime)
    skiptraced_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship
    deceased_individual = relationship("DeceasedIndividual", back_populates="heirs")


# ============================================================================
# JOB QUEUE TABLE
# ============================================================================

class Job(Base):
    """
    Job queue for tracking processing status
    Each deceased individual goes through multiple job stages
    """
    __tablename__ = 'jobs'
    
    id = Column(Integer, primary_key=True)
    
    # Job details
    job_type = Column(String(50), index=True)  
    # Types: property_search, tax_check, probate_check, genealogy_tier1, genealogy_tier2, etc., skiptrace
    
    deceased_id = Column(Integer, ForeignKey('deceased_individuals.id'), index=True)
    
    # Job status
    status = Column(String(20), default='queued', index=True)  
    # Status: queued, processing, completed, failed, retrying
    
    priority = Column(Integer, default=0, index=True)
    
    # Execution details
    worker_id = Column(String(50))  # Which worker processed this
    attempt_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    
    # Results
    result = Column(JSON)  # Structured result data
    error_message = Column(Text)
    error_trace = Column(Text)
    
    # Timestamps
    queued_at = Column(DateTime, default=datetime.utcnow, index=True)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    failed_at = Column(DateTime)
    
    # Performance metrics
    execution_time_seconds = Column(Integer)
    
    # Relationships
    deceased = relationship("DeceasedIndividual", back_populates="jobs")


# ============================================================================
# INDEXES FOR PERFORMANCE
# ============================================================================

# Composite indexes for common queries
Index('idx_deceased_state_status', DeceasedIndividual.last_residence_state, DeceasedIndividual.processing_status)
Index('idx_deceased_priority_status', DeceasedIndividual.priority, DeceasedIndividual.processing_status)
Index('idx_property_delinquent', Property.is_delinquent, Property.deceased_id)
Index('idx_jobs_status_priority', Job.status, Job.priority, Job.queued_at)
Index('idx_heirs_legal_confidence', Heir.is_legal_heir, Heir.confidence_score)


# ============================================================================
# DATABASE CONNECTION & INITIALIZATION
# ============================================================================

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/property_finder')
engine = create_engine(DATABASE_URL, echo=False)  # Set echo=True for SQL debugging
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    """Create all tables in the database"""
    Base.metadata.create_all(bind=engine)
    print("✅ Database schema created successfully!")
    print(f"   Created {len(Base.metadata.tables)} tables:")
    for table_name in Base.metadata.tables.keys():
        print(f"   - {table_name}")


def get_db():
    """Get database session for use in application code"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def drop_all():
    """WARNING: Drops all tables. Use only for development reset."""
    response = input("⚠️  This will delete ALL data. Type 'yes' to confirm: ")
    if response.lower() == 'yes':
        Base.metadata.drop_all(bind=engine)
        print("🗑️  All tables dropped.")
    else:
        print("Cancelled.")


if __name__ == "__main__":
    init_db()