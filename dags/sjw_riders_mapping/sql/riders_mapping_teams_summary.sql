SELECT
    omd.sejuta_jiwa_type,
    COUNT(*) AS total_riders
FROM sejutajiwa_riders_hubs_mapping srhm
JOIN outlet_mapping_directory omd ON srhm.jilid = omd.jilid
WHERE DATE_FORMAT(srhm.load_data_at, '%Y-%m-%d') = CURRENT_DATE()
GROUP BY 1;
