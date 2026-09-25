"""
Transform KSJ Link Riders Data
Merges rider data with hub mapping and prepares for database insertion.
"""
import pandas as pd
import logging


def transform_ksj_link(df_rider_list, df_missing, df_hub, updated_by):
    """
    Transform KSJ Link rider data by merging with hub information.
    
    Args:
        df_rider_list: Full rider list from KSJ Link
        df_missing: Missing riders detected
        df_hub: Hub ID mapping
        updated_by: User identifier for tracking
    
    Returns:
        DataFrame ready for insertion
    """
    logging.info("Starting KSJ Link transformation...")
    
    # Get list of missing rider codes
    list_missing_rider = df_missing['rider_code'].unique().tolist()
    logging.info(f"Processing {len(list_missing_rider)} missing riders")
    
    # Filter rider list to only missing riders
    df_mapping = df_rider_list[df_rider_list['rider_code'].isin(list_missing_rider)].copy()
    
    # Merge with hub mapping
    df_mapping = df_mapping.merge(
        df_hub,
        how='left',
        left_on='hub_code',
        right_on='hub_id'
    )
    
    # Handle testing hubs
    df_mapping.loc[df_mapping['hub_code'] == 'HB-TESTING', 'hub_id'] = 'HB-TESTING'
    df_mapping.loc[df_mapping['hub_code'] == 'HB-TESTING', 'jilid'] = 'JILID-TESTING'
    df_mapping.loc[df_mapping['hub_code'] == 'HB-TESTING', 'hub_name'] = 'HB-TESTING'
    
    # Map columns
    df_mapping['rider_id'] = df_mapping['rider_code']
    df_mapping['cart_id'] = df_mapping['rider_code']
    df_mapping['rider_name'] = df_mapping['name']
    
    # Status mapping
    df_mapping['status'] = df_mapping['is_active'].apply(
        lambda x: 'ACTIVE' if x is True else 'INACTIVE'
    )
    
    df_mapping['scheme'] = 'COMMISSION'
    
    # Clean phone numbers (remove dummy values)
    df_mapping['phone_number'] = df_mapping['phone_number'].apply(
        lambda x: pd.NA if x == '628123456789' else x
    )
    
    # Generate cashier_id
    df_mapping['cashier_id'] = (
        'ksj_link.' + df_mapping['rider_code'].str.lower() + '@hotmail.com'
    )
    
    # Select final columns
    df_mapping = df_mapping[[
        'hub_id', 'jilid', 'hub_name', 'rider_id', 'cart_id', 
        'rider_name', 'status', 'scheme', 'phone_number', 'cashier_id'
    ]]
    
    # Add metadata
    df_mapping['updated_by'] = updated_by
    
    # Fill NaN with pd.NA
    df_mapping = df_mapping.fillna(pd.NA)
    
    # Remove duplicates
    df_mapping = (
        df_mapping
        .drop_duplicates(subset=['hub_id', 'rider_id'], keep='first')
        .reset_index(drop=True)
    )
    
    logging.info(f"Transformation complete. Final records: {len(df_mapping)}")
    
    return df_mapping

