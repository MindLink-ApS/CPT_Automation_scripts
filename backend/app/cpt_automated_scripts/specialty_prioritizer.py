"""
Specialty Prioritization Script - Phase 5 Milestone 6
Implements multi-factor weighted scoring to determine specialty priority hierarchy
"""

import pandas as pd
import logging
from typing import List, Dict, Optional
from pathlib import Path
import sys
from supabase import create_client, Client
import os
import dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

logger = logging.getLogger(__name__)
dotenv.load_dotenv()

class SpecialtyPrioritizer:
    """Calculate priority scores for CPT code specialty mappings"""
    
    # Medical hierarchy levels (1=general, 2=specialty, 3=subspecialty, 4=super-subspecialty)
    SPECIALTY_HIERARCHY = {
        # Level 1: General/Broad
        'Family Medicine': 1,
        'Internal Medicine': 1,
        'General Surgery': 1,
        
        # Level 2: Specialty
        'Surgery': 2,
        'Orthopedic Surgery': 2,
        'Cardiology': 2,
        'Urology': 2,
        'Ophthalmology Surgery': 2,
        'Neurosurgery': 2,
        'Vascular Surgery': 2,
        'Plastic & Reconstructive Surgery': 2,
        'OBGYN – GYN': 2,
        'OBGYN – OB': 2,
        'Pain Management': 2,
        
        # Level 3: Subspecialty
        'Hand Surgery': 3,
        'Interventional Cardiology': 3,
        'Pediatric Cardiology': 3,
        'Cardiothoracic Surgery – Cardiovascular': 3,
        'Cardiothoracic Surgery – Mediastinum': 3,
        'Cardiothoracic Surgery – Respiratory': 3,
        'GI Endoscopy': 3,
        'Interventional Radiology': 3,
        'IR – Drainage': 3,
        'Gynecologic Oncology': 3,
        'Plastic Surgery – Oculoplastics': 3,
        'ENT / Otolaryngology – Ear': 3,
        'ENT / Otolaryngology – Nose/Throat': 3,
        'ENT / Otolaryngology – Oral': 3,
        
        # Level 4: Super-subspecialty
        'Pediatric Hand Surgery': 4,
    }
    
    def __init__(self, supabase_client: Optional[Client] = None):
        """Initialize prioritizer with optional Supabase client"""
        if supabase_client:
            self.client = supabase_client
        else:
            # Initialize Supabase client
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_KEY")
            if not supabase_url or not supabase_key:
                raise ValueError("Missing Supabase credentials")
            self.client = create_client(supabase_url, supabase_key)
    
    def get_hierarchy_score(self, specialty: str) -> int:
        """Get hierarchy level for specialty (lower number = higher priority)"""
        return self.SPECIALTY_HIERARCHY.get(specialty, 2)  # Default to level 2
    
    def calculate_range_specificity(self, cpt_start: str, cpt_end: str, total_possible_codes: int = 99999) -> float:
        """
        Calculate range specificity score.
        Narrower ranges = more specialized = higher priority
        """
        try:
            start_num = int(cpt_start.replace('T', '').replace('U', '').replace('M', ''))
            end_num = int(cpt_end.replace('T', '').replace('U', '').replace('M', ''))
            range_width = end_num - start_num + 1
            
            if range_width <= 0:
                return 1.0
            
            # Specificity = 1 / (normalized_range_width)
            # Smaller ranges get higher scores
            normalized_width = range_width / total_possible_codes
            specificity = 1 / (normalized_width + 0.0001)  # Add small epsilon to avoid division by zero
            
            # Cap at 10 for very narrow ranges
            return min(specificity, 10.0)
        except (ValueError, AttributeError):
            # If we can't parse, return neutral score
            return 1.0
    
    def calculate_frequency_weighting(self, specialty: str, specialty_size: int, avg_specialty_size: float) -> float:
        """
        Calculate frequency weighting factor.
        Smaller specialties get slight boost (more specialized)
        """
        if avg_specialty_size == 0:
            return 1.0
        
        ratio = specialty_size / avg_specialty_size
        
        if ratio < 0.5:
            return 1.5  # Boost for specialized fields
        elif ratio > 2.0:
            return 0.8  # Slight penalty for very broad fields
        else:
            return 1.0  # Neutral
    
    def calculate_priority_score(
        self,
        cpt_code: str,
        specialty: str,
        range_start: str,
        range_end: str,
        specialty_size: int,
        avg_specialty_size: float
    ) -> float:
        """
        Calculate total priority score using multi-factor weighted algorithm.
        
        Weights:
        - 30% Range specificity
        - 25% Medical hierarchy
        - 20% Frequency weighting
        - 15% Overlap resolution (placeholder)
        - 10% Usage validation (placeholder)
        """
        # Range specificity (30%)
        range_score = self.calculate_range_specificity(range_start, range_end)
        
        # Medical hierarchy (25%) - invert so higher hierarchy = higher score
        hierarchy_level = self.get_hierarchy_score(specialty)
        hierarchy_score = 11 - hierarchy_level  # Invert: level 1 -> score 10, level 4 -> score 7
        
        # Frequency weighting (20%)
        frequency_score = self.calculate_frequency_weighting(specialty, specialty_size, avg_specialty_size)
        
        # Overlap resolution (15%) - placeholder for now
        overlap_score = 1.0
        
        # Usage validation (10%) - placeholder for now
        usage_score = 1.0
        
        # Weighted combination
        total_score = (
            range_score * 0.30 +
            hierarchy_score * 0.25 +
            frequency_score * 0.20 +
            overlap_score * 0.15 +
            usage_score * 0.10
        )
        
        return round(total_score, 2)
    
    def prioritize_specialties_for_cpt(
        self,
        cpt_code: str,
        specialty_ranges: pd.DataFrame,
        specialty_sizes: Dict[str, int]
    ) -> List[Dict]:
        """
        Calculate priority scores for all specialties that include this CPT code.
        
        Returns list of specialties with priority levels assigned.
        """
        applicable_specialties = []
        avg_specialty_size = sum(specialty_sizes.values()) / len(specialty_sizes) if specialty_sizes else 1000
        
        for _, specialty_row in specialty_ranges.iterrows():
            range_start = str(specialty_row.get('cpt_start', ''))
            range_end = str(specialty_row.get('cpt_end', range_start))
            specialty = str(specialty_row.get('specialty', ''))
            
            # Check if CPT code falls within this range
            if self._cpt_in_range(cpt_code, range_start, range_end):
                specialty_size = specialty_sizes.get(specialty, 1000)
                
                score = self.calculate_priority_score(
                    cpt_code=cpt_code,
                    specialty=specialty,
                    range_start=range_start,
                    range_end=range_end,
                    specialty_size=specialty_size,
                    avg_specialty_size=avg_specialty_size
                )
                
                applicable_specialties.append({
                    'specialty': specialty,
                    'score': score,
                    'range_start': range_start,
                    'range_end': range_end,
                    'hierarchy_level': self.get_hierarchy_score(specialty),
                    'evidence_basis': 'range_specificity'  # Can be enhanced
                })
        
        # Sort by score (descending) and assign priority levels
        applicable_specialties.sort(key=lambda x: x['score'], reverse=True)
        
        for i, specialty in enumerate(applicable_specialties):
            if i == 0:
                specialty['priority_level'] = 'primary'
            elif i == 1:
                specialty['priority_level'] = 'secondary'
            elif i == 2:
                specialty['priority_level'] = 'tertiary'
            else:
                specialty['priority_level'] = 'other'
        
        return applicable_specialties
    
    def _cpt_in_range(self, cpt_code: str, range_start: str, range_end: str) -> bool:
        """Check if CPT code falls within the given range"""
        try:
            # Extract numeric part from CPT code
            code_num = int(cpt_code.replace('T', '').replace('U', '').replace('M', '').replace('G', '').replace('J', ''))
            start_num = int(range_start.replace('T', '').replace('U', '').replace('M', '').replace('G', '').replace('J', ''))
            end_num = int(range_end.replace('T', '').replace('U', '').replace('M', '').replace('G', '').replace('J', ''))
            
            return start_num <= code_num <= end_num
        except (ValueError, AttributeError):
            # If parsing fails, do string comparison
            return range_start <= cpt_code <= range_end
    
    def process_all_cpt_specialties(self) -> List[Dict]:
        """
        Process all CPT codes and assign specialty priorities.
        Returns list of records ready for database insertion.
        """
        logger.info("Loading specialty ranges from database...")
        
        # Load specialty ranges (assuming they're in a table or file)
        # For now, we'll query from cpt_specialty_mapping if it exists
        try:
            result = self.client.table('cpt_specialty_mapping').select('*').execute()
            specialty_ranges_df = pd.DataFrame(result.data)
            
            # If table has cpt_start but no cpt_end, assume single code range
            if 'cpt_end' not in specialty_ranges_df.columns:
                specialty_ranges_df['cpt_end'] = specialty_ranges_df['cpt_start']
        except Exception as e:
            logger.warning(f"Could not load from cpt_specialty_mapping: {e}")
            logger.info("You may need to provide specialty ranges manually")
            return []
        
        # Calculate specialty sizes
        specialty_sizes = specialty_ranges_df.groupby('specialty').size().to_dict()
        
        # Get all unique CPT codes from main table
        logger.info("Fetching all CPT codes from database...")
        cpt_result = self.client.table('new_updated_medical_benchmarking_data').select('code').execute()
        unique_codes = list(set([row['code'] for row in cpt_result.data if row.get('code')]))
        
        logger.info(f"Processing {len(unique_codes)} unique CPT codes...")
        
        results = []
        for i, cpt_code in enumerate(unique_codes):
            if i % 1000 == 0:
                logger.info(f"Processed {i}/{len(unique_codes)} codes...")
            
            priorities = self.prioritize_specialties_for_cpt(
                cpt_code=cpt_code,
                specialty_ranges=specialty_ranges_df,
                specialty_sizes=specialty_sizes
            )
            
            for p in priorities:
                results.append({
                    'cpt_code': cpt_code,
                    'specialty': p['specialty'],
                    'priority_level': p['priority_level'],
                    'priority_score': p['score'],
                    'range_start': p['range_start'],
                    'range_end': p['range_end'],
                    'hierarchy_level': p['hierarchy_level'],
                    'evidence_basis': p['evidence_basis']
                })
        
        logger.info(f"Generated {len(results)} specialty priority records")
        return results
    
    def insert_specialty_priorities(self, records: List[Dict]) -> dict:
        """Insert specialty priorities into enhanced mapping table"""
        if not records:
            logger.warning("No records to insert")
            return {"status": "no_records", "records_inserted": 0}
        
        logger.info(f"Inserting {len(records)} specialty priority records...")
        
        # Insert in chunks
        chunk_size = 1000
        total_inserted = 0
        
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            try:
                result = self.client.table('cpt_specialty_mapping_enhanced').upsert(
                    chunk,
                    on_conflict='cpt_code,specialty'
                ).execute()
                total_inserted += len(chunk)
                logger.info(f"Inserted chunk {i//chunk_size + 1} ({len(chunk)} records)")
            except Exception as e:
                logger.error(f"Error inserting chunk {i//chunk_size + 1}: {e}")
                continue
        
        logger.info(f"✅ Successfully inserted {total_inserted} records")
        return {"status": "success", "records_inserted": total_inserted}


def main():
    """Main execution function"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        logger.info("=" * 50)
        logger.info("SPECIALTY PRIORITIZATION - PHASE 5 MILESTONE 6")
        logger.info("=" * 50)
        
        prioritizer = SpecialtyPrioritizer()
        
        # Process all CPT codes
        results = prioritizer.process_all_cpt_specialties()
        
        if results:
            # Insert into database
            result = prioritizer.insert_specialty_priorities(results)
            logger.info(f"Final result: {result}")
        else:
            logger.warning("No results to insert")
        
        logger.info("=" * 50)
        logger.info("SPECIALTY PRIORITIZATION COMPLETED")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        logger.exception("Full traceback:")
        raise


if __name__ == "__main__":
    main()
