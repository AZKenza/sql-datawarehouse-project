CREATE OR REPLACE PROCEDURE bronze.load_source_data()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count bigint;
    v_start_time timestamp;
    v_duration_seconds numeric;
    v_batch_start_time timestamp;
    v_batch_seconds numeric;
BEGIN
    -- Overall start
    RAISE NOTICE '=================================================================';
    RAISE NOTICE 'Loading bronze layer';
    RAISE NOTICE '=================================================================';

    v_batch_start_time := clock_timestamp();

    BEGIN
        ----------------------------------------------------------------
        -- CRM TABLES
        ----------------------------------------------------------------
        RAISE NOTICE '-----------------------------------------------------------------';
        RAISE NOTICE 'Loading CRM tables';
        RAISE NOTICE '-----------------------------------------------------------------';

        -- crm_cust_info
        v_start_time := clock_timestamp();
        TRUNCATE TABLE bronze.crm_cust_info;
        COPY bronze.crm_cust_info
        FROM '/import/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
        DELIMITER ',' CSV HEADER;
        SELECT count(*) INTO v_count FROM bronze.crm_cust_info;
        v_duration_seconds :=
            ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time))::numeric, 3);
        RAISE NOTICE 'crm_cust_info: % rows loaded in % seconds',
                     v_count,
                     v_duration_seconds::text;

        -- crm_prd_info
        v_start_time := clock_timestamp();
        TRUNCATE TABLE bronze.crm_prd_info;
        COPY bronze.crm_prd_info
        FROM '/import/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
        DELIMITER ',' CSV HEADER;
        SELECT count(*) INTO v_count FROM bronze.crm_prd_info;
        v_duration_seconds :=
            ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time))::numeric, 3);
        RAISE NOTICE 'crm_prd_info: % rows loaded in % seconds',
                     v_count,
                     v_duration_seconds::text;

        -- crm_sales_details
        v_start_time := clock_timestamp();
        TRUNCATE TABLE bronze.crm_sales_details;
        COPY bronze.crm_sales_details
        FROM '/import/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
        DELIMITER ',' CSV HEADER;
        SELECT count(*) INTO v_count FROM bronze.crm_sales_details;
        v_duration_seconds :=
            ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time))::numeric, 3);
        RAISE NOTICE 'crm_sales_details: % rows loaded in % seconds',
                     v_count,
                     v_duration_seconds::text;

        ----------------------------------------------------------------
        -- ERP TABLES
        ----------------------------------------------------------------
        RAISE NOTICE '-----------------------------------------------------------------';
        RAISE NOTICE 'Loading ERP tables';
        RAISE NOTICE '-----------------------------------------------------------------';

        -- erp_loc_a101
        v_start_time := clock_timestamp();
        TRUNCATE TABLE bronze.erp_loc_a101;
        COPY bronze.erp_loc_a101
        FROM '/import/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
        DELIMITER ',' CSV HEADER;
        SELECT count(*) INTO v_count FROM bronze.erp_loc_a101;
        v_duration_seconds :=
            ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time))::numeric, 3);
        RAISE NOTICE 'erp_loc_a101: % rows loaded in % seconds',
                     v_count,
                     v_duration_seconds::text;

        -- erp_cust_az12
        v_start_time := clock_timestamp();
        TRUNCATE TABLE bronze.erp_cust_az12;
        COPY bronze.erp_cust_az12
        FROM '/import/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
        DELIMITER ',' CSV HEADER;
        SELECT count(*) INTO v_count FROM bronze.erp_cust_az12;
        v_duration_seconds :=
            ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time))::numeric, 3);
        RAISE NOTICE 'erp_cust_az12: % rows loaded in % seconds',
                     v_count,
                     v_duration_seconds::text;

        -- erp_px_cat_g1v2
        v_start_time := clock_timestamp();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        COPY bronze.erp_px_cat_g1v2
        FROM '/import/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
        DELIMITER ',' CSV HEADER;
        SELECT count(*) INTO v_count FROM bronze.erp_px_cat_g1v2;
        v_duration_seconds :=
            ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time))::numeric, 3);
        RAISE NOTICE 'erp_px_cat_g1v2: % rows loaded in % seconds',
                     v_count,
                     v_duration_seconds::text;

    EXCEPTION
       WHEN OTHERS THEN
        RAISE NOTICE 'Error loading data: %', SQLERRM;
    END;

    -- Batch total time
    v_batch_seconds :=
        ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - v_batch_start_time))::numeric, 3);

    RAISE NOTICE '=================================================================';
    RAISE NOTICE 'Bronze layer loading completed in % seconds', v_batch_seconds::text;
    RAISE NOTICE '=================================================================';

END;
$$;
