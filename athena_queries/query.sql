-- Check Glue catalog
SHOW DATABASES;

-- Use gold database
USE cms_gold;

-- List tables
SHOW TABLES;

-- Query dimensional model
SELECT 
    p.gender,
    COUNT(*) as patient_count,
    AVG(ps.total_healthcare_cost) as avg_cost
FROM cms_gold.dim_patient p
JOIN cms_gold.patient_summary ps ON p.patient_key = ps.patient_key
WHERE p.is_current = true
GROUP BY p.gender;

-- Query fact tables
SELECT 
    claim_year,
    claim_type,
    COUNT(*) as claim_count,
    SUM(payment_amount) as total_payment
FROM fact_claims
GROUP BY claim_year, claim_type
ORDER BY claim_year, claim_type;