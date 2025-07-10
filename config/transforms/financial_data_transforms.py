# File: /config/transforms/financial_data_transforms.py
"""
SEC filing transformation functions for Finnhub data pipeline
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

def enhance_financial_data(df, add_fiscal_year=True, calculate_ratios=True, 
                          market_cap_threshold=1000000, **kwargs):
    """
    Enhance SEC filing data with additional calculated fields
    
    Args:
        df: Input DataFrame with SEC filing data
        add_fiscal_year: Whether to add fiscal year calculation
        calculate_ratios: Whether to calculate financial ratios
        market_cap_threshold: Threshold for market cap categorization
        **kwargs: Additional parameters
    
    Returns:
        DataFrame
    """
    logger.info("Starting SEC filing data enhancement")
    result_df = df.copy()
    
    try:
        # Add fiscal year if requested and date field exists
        if add_fiscal_year and 'filedDate' in result_df.columns:
            result_df['filed_date_parsed'] = pd.to_datetime(result_df['filedDate'], errors='coerce')
            result_df['fiscal_year'] = result_df['filed_date_parsed'].dt.year
            
            # Add quarter information
            result_df['fiscal_quarter'] = result_df['filed_date_parsed'].dt.quarter
            
            # Calculate filing age
            result_df['filing_age_days'] = (datetime.now() - result_df['filed_date_parsed']).dt.days
            result_df['is_recent_filing'] = result_df['filing_age_days'] <= 90
        
        # Calculate financial metrics and scores
        if calculate_ratios:
            # Filing frequency score (example metric)
            result_df['filing_frequency_score'] = np.random.uniform(0.7, 1.0, len(result_df))
            
            # Compliance score based on filing timeliness
            if 'filing_age_days' in result_df.columns:
                result_df['compliance_score'] = np.where(
                    result_df['filing_age_days'] <= 30, 1.0,
                    np.where(result_df['filing_age_days'] <= 90, 0.8, 0.6)
                )
            
            # Risk score (example calculation)
            result_df['risk_score'] = np.random.uniform(0.1, 0.9, len(result_df))
        
        # SEC form categorization
        if 'form' in result_df.columns:
            result_df['form_category'] = result_df['form'].apply(categorize_sec_form)
            result_df['is_annual_report'] = result_df['form'].isin(['10-K', '10-K/A'])
            result_df['is_quarterly_report'] = result_df['form'].isin(['10-Q', '10-Q/A'])
            result_df['is_current_report'] = result_df['form'].str.startswith('8-K')
        
        # Add enhancement metadata
        result_df['enhanced_at'] = datetime.now()
        result_df['enhancement_version'] = '1.2.0'
        result_df['processing_batch_id'] = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"SEC filing data enhancement completed for {len(result_df)} records")
        return result_df
        
    except Exception as e:
        logger.error(f"Error in SEC filing data enhancement: {e}")
        # Return original dataframe with error column
        result_df['enhancement_error'] = str(e)
        result_df['enhanced_at'] = datetime.now()
        return result_df


def categorize_sec_form(form_type):
    """Categorize SEC form types"""
    if pd.isna(form_type):
        return 'Unknown'
    
    form = str(form_type).upper()
    
    if form.startswith('10-K'):
        return 'Annual Report'
    elif form.startswith('10-Q'):
        return 'Quarterly Report'
    elif form.startswith('8-K'):
        return 'Current Report'
    elif form.startswith('DEF'):
        return 'Proxy Statement'
    elif form.startswith('S-'):
        return 'Registration Statement'
    elif form in ['3', '4', '5']:
        return 'Insider Trading'
    elif form.startswith('13'):
        return 'Beneficial Ownership'
    else:
        return 'Other'


def enhance_sec_filing_data(df, categorize_forms=True, add_compliance_metrics=True, 
                           calculate_filing_patterns=True, **kwargs):
    """
    SEC filing analysis with additional insights
    
    Args:
        df: Input DataFrame
        categorize_forms: Whether to categorize form types
        add_compliance_metrics: Whether to add compliance metrics
        calculate_filing_patterns: Whether to analyze filing patterns
        **kwargs: Additional parameters
    
    Returns:
        DataFrame with SEC analysis
    """
    logger.info("Starting SEC filing analysis")
    result_df = df.copy()
    
    try:
        # Form type analysis
        if categorize_forms and 'form' in result_df.columns:
            result_df['form_category'] = result_df['form'].apply(categorize_sec_form)
            result_df['is_annual_report'] = result_df['form'].isin(['10-K', '10-K/A'])
            result_df['is_quarterly_report'] = result_df['form'].isin(['10-Q', '10-Q/A'])
            result_df['is_proxy_statement'] = result_df['form'].str.startswith('DEF')
        
        # Filing compliance scoring
        if add_compliance_metrics:
            # Initialize compliance score
            result_df['regulatory_compliance_score'] = 100.0
            
            # Calculate filing timeliness if dates are available
            if 'filedDate' in result_df.columns and 'acceptedDate' in result_df.columns:
                filed_dates = pd.to_datetime(result_df['filedDate'], errors='coerce')
                accepted_dates = pd.to_datetime(result_df['acceptedDate'], errors='coerce')
                
                # Calculate delay between accepted and filed
                result_df['filing_delay_days'] = (filed_dates - accepted_dates).dt.days
                result_df['is_timely_filing'] = result_df['filing_delay_days'] <= 1
                
                # Adjust compliance score based on delays
                late_penalty = np.where(
                    result_df['filing_delay_days'] > 5, 
                    np.minimum(result_df['filing_delay_days'] * 2, 20),
                    0
                )
                result_df['regulatory_compliance_score'] -= late_penalty
        
        # Filing pattern analysis
        if calculate_filing_patterns and 'symbol' in result_df.columns:
            # Sort by symbol and filing date for pattern analysis
            result_df = result_df.sort_values(['symbol', 'filedDate'])
            
            # Calculate filing sequence per company
            result_df['filing_sequence'] = result_df.groupby('symbol').cumcount() + 1
            
            # Calculate days between filings for same company
            result_df['days_since_previous_filing'] = (
                result_df.groupby('symbol')['filedDate']
                .apply(lambda x: pd.to_datetime(x).diff().dt.days)
                .values
            )
            
            # Company filing frequency metrics
            symbol_counts = result_df['symbol'].value_counts()
            result_df['company_total_filings'] = result_df['symbol'].map(symbol_counts)
            result_df['is_frequent_filer'] = result_df['company_total_filings'] > 10
        
        # Add analysis metadata
        result_df['sec_analysis_version'] = '2.1.0'
        result_df['analyzed_at'] = datetime.now()
        
        logger.info(f"SEC analysis completed for {len(result_df)} records")
        return result_df
        
    except Exception as e:
        logger.error(f"Error in SEC analysis: {e}")
        result_df['analysis_error'] = str(e)
        return result_df


def data_quality_checks(df, required_columns=None, **kwargs):
    """
    Perform data quality checks on SEC filing data
    
    Args:
        df: Input DataFrame
        required_columns: List of required columns
        **kwargs: Additional parameters
    
    Returns:
        DataFrame with quality metrics
    """
    logger.info("Starting SEC filing data quality checks")
    result_df = df.copy()
    
    try:
        # Check for required columns
        if required_columns:
            missing_cols = set(required_columns) - set(result_df.columns)
            result_df['missing_required_columns'] = ', '.join(missing_cols) if missing_cols else None
        
        # Calculate completeness metrics
        result_df['data_completeness_score'] = (
            result_df.notna().sum(axis=1) / len(result_df.columns)
        )
        
        # SEC-specific quality checks
        if 'accessNumber' in result_df.columns:
            # Check accession number format
            accession_pattern = r'^[0-9]{10}-[0-9]{2}-[0-9]{6}$'
            result_df['valid_accession_format'] = (
                result_df['accessNumber'].str.match(accession_pattern, na=False)
            )
        
        if 'cik' in result_df.columns:
            # Check CIK format (should be numeric)
            result_df['valid_cik_format'] = (
                result_df['cik'].str.match(r'^[0-9]+$', na=False)
            )
        
        # Add quality flags
        result_df['high_quality'] = result_df['data_completeness_score'] >= 0.8
        result_df['quality_check_timestamp'] = datetime.now()
        
        # Record count validation
        result_df['record_count_in_batch'] = len(result_df)
        
        logger.info(f"SEC filing data quality checks completed for {len(result_df)} records")
        return result_df
        
    except Exception as e:
        logger.error(f"Error in SEC filing data quality checks: {e}")
        result_df['quality_check_error'] = str(e)
        return result_df