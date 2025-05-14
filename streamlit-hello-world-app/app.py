import streamlit as st
import pandas as pd
from datetime import datetime
import io

st.set_page_config(page_title="DWH Table Deployment Helper", layout="wide")

st.title("Data Warehouse Table Deployment Helper")

# Initialize session state for SQL generation...
if 'sql_generated' not in st.session_state:
    st.session_state.sql_generated = False
    st.session_state.all_sql = ""
    st.session_state.timestamp = ""
    st.session_state.table_suffix = ""

with st.sidebar:
    st.header("Table Information")
    
    # User Initials
    user_initials = st.text_input("Your Initials (e.g., skg)", "").lower()
    
    # Source Table Info
    st.subheader("Source Table")
    source_system_initial = st.selectbox("Source System (Initial)", ["Replicate_Full"], key="source_system_initial")
    source_system_daily = st.selectbox("Source System (Daily)", ["Replicate_CDC"], key="source_system_daily")
    
    src_schema_name = st.text_input("Source Schema Name", "TIA")
    src_table_name = st.text_input("Source Table Name")
    src_table_name_ct = f"{src_table_name}__ct" if src_table_name else ""
    
    # Target Table Info
    st.subheader("Target Table")
    tgt_schema_name_st = "ST"
    tgt_table_name_st = f"ST_{src_table_name}" if src_table_name else ""
    
    tgt_schema_name_hs = "HS"
    tgt_table_name_hs = f"HS_{src_table_name}" if src_table_name else ""
    
    # Business Key
    st.subheader("Key Columns")
    business_key = st.text_input("Business Key (comma-separated)", "")
    
    # SCD2 columns for HS
    st.subheader("SCD2 Columns for HS")
    scd2_columns = st.text_area("SCD2 Columns (comma-separated)", "")
    
    # Delete Configuration
    st.subheader("Delete Configuration")
    delete_type = st.selectbox("Delete Type", [None, "SOFT"], key="delete_type")
    src_delete_column = st.text_input("Source Delete Column", "DELETED_FLAG")
    src_delete_value = st.text_input("Source Delete Value", "Y")
    
    # Skip Options
    st.subheader("Skip Options")
    skip_st_table = st.checkbox("ST Table Already Exists")
    skip_hs_table = st.checkbox("HS Table Already Exists")
    
    # Generate SQL button in sidebar
    if st.button("Generate SQL Script", type="primary"):
        if not src_table_name or not business_key or not scd2_columns:
            st.error("Please fill in all required fields: Source Table Name, Business Key, and SCD2 Columns")
        elif not user_initials:
            st.error("Please enter your initials")
        else:
            # Generate unique timestamp for table names
            current_date = datetime.now().strftime("%Y%m%d")
            st.session_state.timestamp = current_date
            st.session_state.table_suffix = f"{user_initials}_{current_date}"
            st.session_state.sql_generated = True
            st.success("SQL Generated Successfully! Check the tabs below.")

# Main content
st.header("Generated SQL Deployment Script")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "1. Control Tables Backup", 
    "2. ST Control Table", 
    "3. HS Control Table", 
    "4. Job Control Table",
    "5. Create HS Table",
    "6. Cleanup"
])

if st.session_state.sql_generated:
    table_suffix = st.session_state.table_suffix
    
    # Tab 1: Control Tables Backup
    with tab1:
        st.subheader("Step 1: Create Temporary Control Tables")
        
        control_table_stage_sql = f"""-- Make a copy of DWH.CONTROL_TABLE_STAGE
WITH cte AS ( 	
    SELECT TOP 1 * FROM DWH.CONTROL_TABLE_STAGE WHERE job_name IN ('ST_Full_Initial') 	
    UNION ALL 	
    SELECT TOP 1 * FROM DWH.CONTROL_TABLE_STAGE WHERE job_name IN ('ST_Full_Daily') 	
) 	
SELECT *	
INTO sandbox.temp_control_table_st_{table_suffix} FROM cte;
"""
        
        control_table_hs_sql = f"""-- Make a copy of DWH.CONTROL_TABLE_HS
SELECT TOP 1 * 
INTO sandbox.temp_control_table_hs_{table_suffix} 
FROM DWH.CONTROL_TABLE_HS;
"""
        
        job_control_sql = f"""-- Make a copy of DWH.JOB_CONTROL
SELECT * 
INTO sandbox.temp_control_table_job_{table_suffix} 
FROM DWH.JOB_CONTROL 
WHERE job_name IN ('ST_Full_Daily','ST_Full_Initial','HS_Full_Daily','HS_Full_Daily_Control','ST_Placeholder');
"""
        
        tab1_sql = control_table_stage_sql + "\n" + control_table_hs_sql + "\n" + job_control_sql
        st.code(tab1_sql)
    
    # Tab 2: Update ST Control Table
    with tab2:
        st.subheader("Step 2: Update ST Control Table")
        
        st_initial_sql = f"""-- Update temporary control table for stage to reflect Initial Load values
UPDATE sandbox.temp_control_table_st_{table_suffix}
SET 
    job_name = 'ST_Full_Initial',
    source_system = '{source_system_initial}',
    src_schema_name = '{src_schema_name}',
    src_table_name = '{src_table_name}',
    tgt_schema_name = '{tgt_schema_name_st}',
    tgt_table_name = '{tgt_table_name_st}',
    business_key = '{business_key}',
    initial_load_valid_from_column = '__lowDate',
    incremental_filter_column = '__fullLoad',
    incremental_filter_column_timezone = 'UTC',
    skip = 0,
    priority = 0,
    delete_type = NULL,
    src_delete_column = NULL,
    src_delete_value = NULL 
WHERE job_name = 'ST_Full_Initial';
"""
        
        st_daily_sql = f"""-- Update temporary control table for stage to reflect Daily load values
UPDATE sandbox.temp_control_table_st_{table_suffix}
SET 
    job_name = 'ST_Full_Daily',
    source_system = '{source_system_daily}',
    src_schema_name = '{src_schema_name}',
    src_table_name = '{src_table_name_ct}',
    tgt_schema_name = '{tgt_schema_name_st}',
    tgt_table_name = '{tgt_table_name_st}',
    business_key = '{business_key}',
    initial_load_valid_from_column = '__lowDate',
    incremental_filter_column = 'header__timestamp',
    incremental_filter_column_timezone = 'UTC',
    skip = 0,
    priority = 0,
    delete_type = '{delete_type}',
    src_delete_column = '{src_delete_column}',
    src_delete_value = '{src_delete_value}'
WHERE job_name = 'ST_Full_Daily';
"""
        
        tab2_sql = st_initial_sql + "\n" + st_daily_sql
        st.code(tab2_sql)
    
    # Tab 3: Update HS Control Table
    with tab3:
        st.subheader("Step 3: Update HS Control Table")
        
        hs_sql = f"""-- Update temporary control table for historic stage to reflect daily load values
UPDATE sandbox.temp_control_table_hs_{table_suffix}
SET job_name = 'HS_Full_Daily',
    src_schema_name = '{tgt_schema_name_st}',
    src_table_name = '{tgt_table_name_st}', 
    tgt_schema_name = '{tgt_schema_name_hs}',
    tgt_table_name = '{tgt_table_name_hs}', 
    business_key = '{business_key}',
    primary_key = 'TC_ROW_ID',
    incremental_filter_column = '__fullLoad',
    incremental_filter_column_timezone = 'UTC',
    scd_type = 'SCD2',
    scd2_columns = '{scd2_columns}',
    skip = 0,
    priority = 0,
    prescript = '',
    postscript = '',
    partitions = 1,
    use_source_column_for_valid_dates = 1,
    source_column_for_valid_from_date = 'header__timestamp';
"""
        
        tab3_sql = hs_sql
        st.code(tab3_sql)
    
    # Tab 4: Update Job Control
    with tab4:
        st.subheader("Step 4: Update Job Control Table")
        
        job_control_update_sql = f"""-- Update the control table so that the jobs are set to STATUS='SUCCESS'
UPDATE sandbox.temp_control_table_job_{table_suffix}
SET 
    STATUS = 'SUCCESS',
    LAST_LOAD_DATE = '1970-01-01',
    JOB_INTERVAL_IN_MINUTES = 0
WHERE job_name IN ('HS_Full_Daily','ST_Full_Daily','HS_Full_Daily_Control','ST_Full_Initial');
"""
        
        tab4_sql = job_control_update_sql
        st.code(tab4_sql)
        
        st.markdown("""
        ### Pipeline Setup Notes
        
        1. Clone pipeline **Scheduling / pl_StageAndHistoricStageDailyLoad** in a separate branch
        2. Add parameters to match your temporary tables
        3. For initial load:
           - Set STJobName to **ST_Full_Initial**
           - Set HSJobName to **HS_Full_Daily**
           - Set pInitialLoad to **true**
        4. For daily loads:
           - Set STJobName to **ST_Full_Daily**
           - Set HSJobName to **HS_Full_Daily**
           - Set pInitialLoad to **false**
        """)
    
    # Tab 5: Create HS Tables
    with tab5:
        st.subheader("Step 5: Create HS Table")
        
        hs_create_sql = ""
        if not skip_hs_table:
            hs_create_sql = f"""-- Create the HS table with technical columns
SELECT * INTO {tgt_schema_name_hs}.{tgt_table_name_hs} FROM {tgt_schema_name_st}.{tgt_table_name_st} WHERE 1 = 0;

ALTER TABLE {tgt_schema_name_hs}.{tgt_table_name_hs}
ADD TC_CURRENT_FLAG VARCHAR(1), 
    TC_VALID_FROM_DATE DATETIME2(0), 
    TC_VALID_TO_DATE DATETIME2(0), 
    TC_CHECKSUM_BUSKEY VARCHAR(32), 
    TC_CHECKSUM_SCD VARCHAR(32), 
    TC_DELETED_FLAG VARCHAR(1), 
    TC_DELETED_DATETIME DATETIME2(0),
    TC_INSERTED_DATE DATETIME2(0),
    TC_ROW_ID BIGINT IDENTITY(1,1) PRIMARY KEY;

ALTER TABLE {tgt_schema_name_hs}.{tgt_table_name_hs}
DROP COLUMN TC_INITIAL_LOAD_VALID_FROM_DATE;
"""
        else:
            hs_create_sql = f"-- HS Table creation skipped as per user selection"
        
        tab5_sql = hs_create_sql
        st.code(tab5_sql)
        
        st.markdown("""
        **Note:** If you want a quicker way to get to the HS tables, you can run the initial load with an invalid HS job name. 
        This will only run the stage part of the job and then fail. Then create the HS table with this script.
        """)
    
    # Tab 6: Cleanup steps
    with tab6:
        st.subheader("Step 6: Cleanup - Add to Production Control Tables")
        
        cleanup_stage_sql = f"""-- Add the stage job definition to DWH.CONTROL_TABLE_STAGE
-- Backup control table first
SELECT * INTO sandbox.CONTROL_TABLE_STAGE_backup_{table_suffix} FROM DWH.CONTROL_TABLE_STAGE;

-- Option to drop and recreate with new entries (commented out for safety)
/*
DROP TABLE DWH.CONTROL_TABLE_STAGE;

WITH cte AS (
    SELECT * FROM sandbox.temp_control_table_st_{table_suffix}
    UNION ALL
    SELECT * FROM sandbox.CONTROL_TABLE_STAGE_backup_{table_suffix} 
    WHERE job_name+'|'+source_system+'|'+src_schema_name+'|'+src_table_name+'|'+tgt_schema_name+'|'+tgt_table_name 
          NOT IN (SELECT job_name+'|'+source_system+'|'+src_schema_name+'|'+src_table_name+'|'+tgt_schema_name+'|'+tgt_table_name 
                  FROM sandbox.temp_control_table_st_{table_suffix})
)
SELECT * INTO DWH.CONTROL_TABLE_STAGE FROM cte;
*/
"""
        
        cleanup_hs_sql = f"""-- Add the HS job definition to DWH.CONTROL_TABLE_HS
-- Backup control table first
SELECT * INTO sandbox.CONTROL_TABLE_HS_backup_{table_suffix} FROM DWH.CONTROL_TABLE_HS;

-- Option to drop and recreate with new entries (commented out for safety)
/*
DROP TABLE DWH.CONTROL_TABLE_HS;

WITH cte AS (
    SELECT * FROM sandbox.temp_control_table_hs_{table_suffix}
    UNION ALL
    SELECT * FROM sandbox.CONTROL_TABLE_HS_backup_{table_suffix} 
    WHERE job_name+'|'+src_schema_name+'|'+src_table_name+'|'+tgt_schema_name+'|'+tgt_table_name 
          NOT IN (SELECT job_name+'|'+src_schema_name+'|'+src_table_name+'|'+tgt_schema_name+'|'+tgt_table_name 
                  FROM sandbox.temp_control_table_hs_{table_suffix})
)
SELECT * INTO DWH.CONTROL_TABLE_HS FROM cte;
*/
"""
        
        cleanup_temp_sql = f"""-- Drop the temporary tables
/*
DROP TABLE sandbox.temp_control_table_hs_{table_suffix};
DROP TABLE sandbox.temp_control_table_st_{table_suffix};
DROP TABLE sandbox.temp_control_table_job_{table_suffix};
DROP TABLE sandbox.CONTROL_TABLE_HS_backup_{table_suffix};
DROP TABLE sandbox.CONTROL_TABLE_STAGE_backup_{table_suffix};
*/
"""
        
        tab6_sql = cleanup_stage_sql + "\n" + cleanup_hs_sql + "\n" + cleanup_temp_sql
        st.code(tab6_sql)
        
    # Prepare complete SQL script for download
    complete_sql = f"""-- Generated SQL Deployment Script for {src_table_name}
-- Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
-- This script contains all steps needed for deploying {src_table_name} to the data warehouse.
-- Created by: {user_initials.upper()}

---------------------------------------------------------
-- STEP 1: CREATE TEMPORARY CONTROL TABLES
---------------------------------------------------------
{tab1_sql}

---------------------------------------------------------
-- STEP 2: UPDATE ST CONTROL TABLE
---------------------------------------------------------
{tab2_sql}

---------------------------------------------------------
-- STEP 3: UPDATE HS CONTROL TABLE
---------------------------------------------------------
{tab3_sql}

---------------------------------------------------------
-- STEP 4: UPDATE JOB CONTROL TABLE
---------------------------------------------------------
{tab4_sql}

---------------------------------------------------------
-- STEP 5: CREATE HS TABLE
---------------------------------------------------------
{tab5_sql}

---------------------------------------------------------
-- STEP 6: CLEANUP - ADD TO PRODUCTION CONTROL TABLES
---------------------------------------------------------
{tab6_sql}

-- End of script
"""
    
    # Store the complete SQL in session state
    st.session_state.all_sql = complete_sql
    
    # Create a download button for the complete SQL script
    st.markdown("---")
    st.subheader("Download Complete SQL Script")
    
    # Create a buffer for the SQL content
    sql_file = io.StringIO()
    sql_file.write(complete_sql)
    
    # Download button
    st.download_button(
        label="Download SQL Script",
        data=sql_file.getvalue(),
        file_name=f"deploy_{src_table_name}_{table_suffix}.sql",
        mime="text/plain",
        key="download_sql",
    )
else:
    st.info("Fill in the required fields in the sidebar and click 'Generate SQL Script' to see the deployment steps.")

# Footer
st.markdown("---")
st.caption("DWH Table Deployment Helper - Streamlit App") 