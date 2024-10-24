-- models/silver/silver_census.sql
{{ config(materialized='table') }}

WITH cleaned_census_g01 AS (
    SELECT
        TRIM("LGA_CODE_2016") AS lga_code_2016,
        CAST("Tot_P_M" AS BIGINT) AS total_population_male,
        CAST("Tot_P_F" AS BIGINT) AS total_population_female,
        CAST("Tot_P_P" AS BIGINT) AS total_population,
        CAST("Age_0_4_yr_M" AS BIGINT) AS age_0_4_male,
        CAST("Age_0_4_yr_F" AS BIGINT) AS age_0_4_female,
        CAST("Age_0_4_yr_P" AS BIGINT) AS age_0_4_total,
        CAST("Age_5_14_yr_M" AS BIGINT) AS age_5_14_male,
        CAST("Age_5_14_yr_F" AS BIGINT) AS age_5_14_female,
        CAST("Age_5_14_yr_P" AS BIGINT) AS age_5_14_total,
        CAST("Age_15_19_yr_M" AS BIGINT) AS age_15_19_male,
        CAST("Age_15_19_yr_F" AS BIGINT) AS age_15_19_female,
        CAST("Age_15_19_yr_P" AS BIGINT) AS age_15_19_total,
        CAST("Age_20_24_yr_M" AS BIGINT) AS age_20_24_male,
        CAST("Age_20_24_yr_F" AS BIGINT) AS age_20_24_female,
        CAST("Age_20_24_yr_P" AS BIGINT) AS age_20_24_total,
        CAST("Age_25_34_yr_M" AS BIGINT) AS age_25_34_male,
        CAST("Age_25_34_yr_F" AS BIGINT) AS age_25_34_female,
        CAST("Age_25_34_yr_P" AS BIGINT) AS age_25_34_total,
        CAST("Age_35_44_yr_M" AS BIGINT) AS age_35_44_male,
        CAST("Age_35_44_yr_F" AS BIGINT) AS age_35_44_female,
        CAST("Age_35_44_yr_P" AS BIGINT) AS age_35_44_total,
        CAST("Age_45_54_yr_M" AS BIGINT) AS age_45_54_male,
        CAST("Age_45_54_yr_F" AS BIGINT) AS age_45_54_female,
        CAST("Age_45_54_yr_P" AS BIGINT) AS age_45_54_total,
        CAST("Age_55_64_yr_M" AS BIGINT) AS age_55_64_male,
        CAST("Age_55_64_yr_F" AS BIGINT) AS age_55_64_female,
        CAST("Age_55_64_yr_P" AS BIGINT) AS age_55_64_total,
        CAST("Age_65_74_yr_M" AS BIGINT) AS age_65_74_male,
        CAST("Age_65_74_yr_F" AS BIGINT) AS age_65_74_female,
        CAST("Age_65_74_yr_P" AS BIGINT) AS age_65_74_total,
        CAST("Age_75_84_yr_M" AS BIGINT) AS age_75_84_male,
        CAST("Age_75_84_yr_F" AS BIGINT) AS age_75_84_female,
        CAST("Age_75_84_yr_P" AS BIGINT) AS age_75_84_total,
        CAST("Age_85ov_M" AS BIGINT) AS age_85_plus_male,
        CAST("Age_85ov_F" AS BIGINT) AS age_85_plus_female,
        CAST("Age_85ov_P" AS BIGINT) AS age_85_plus_total,
        CAST("Counted_Census_Night_home_M" AS BIGINT) AS counted_census_night_home_male,
        CAST("Counted_Census_Night_home_F" AS BIGINT) AS counted_census_night_home_female,
        CAST("Counted_Census_Night_home_P" AS BIGINT) AS counted_census_night_home_total,
        CAST("Count_Census_Nt_Ewhere_Aust_M" AS BIGINT) AS count_census_elsewhere_australia_male,
        CAST("Count_Census_Nt_Ewhere_Aust_F" AS BIGINT) AS count_census_elsewhere_australia_female,
        CAST("Count_Census_Nt_Ewhere_Aust_P" AS BIGINT) AS count_census_elsewhere_australia_total,
        CAST("Indigenous_psns_Aboriginal_M" AS BIGINT) AS indigenous_aboriginal_male,
        CAST("Indigenous_psns_Aboriginal_F" AS BIGINT) AS indigenous_aboriginal_female,
        CAST("Indigenous_psns_Aboriginal_P" AS BIGINT) AS indigenous_aboriginal_total,
        CAST("Indig_psns_Torres_Strait_Is_M" AS BIGINT) AS indigenous_torres_strait_islander_male,
        CAST("Indig_psns_Torres_Strait_Is_F" AS BIGINT) AS indigenous_torres_strait_islander_female,
        CAST("Indig_psns_Torres_Strait_Is_P" AS BIGINT) AS indigenous_torres_strait_islander_total,
        CAST("Indig_Bth_Abor_Torres_St_Is_M" AS BIGINT) AS indigenous_both_aboriginal_tsi_male,
        CAST("Indig_Bth_Abor_Torres_St_Is_F" AS BIGINT) AS indigenous_both_aboriginal_tsi_female,
        CAST("Indig_Bth_Abor_Torres_St_Is_P" AS BIGINT) AS indigenous_both_aboriginal_tsi_total,
        CAST("Indigenous_P_Tot_M" AS BIGINT) AS indigenous_population_total_male,
        CAST("Indigenous_P_Tot_F" AS BIGINT) AS indigenous_population_total_female,
        CAST("Indigenous_P_Tot_P" AS BIGINT) AS indigenous_population_total,
        CAST("Birthplace_Australia_M" AS BIGINT) AS birthplace_australia_male,
        CAST("Birthplace_Australia_F" AS BIGINT) AS birthplace_australia_female,
        CAST("Birthplace_Australia_P" AS BIGINT) AS birthplace_australia_total,
        CAST("Birthplace_Elsewhere_M" AS BIGINT) AS birthplace_elsewhere_male,
        CAST("Birthplace_Elsewhere_F" AS BIGINT) AS birthplace_elsewhere_female,
        CAST("Birthplace_Elsewhere_P" AS BIGINT) AS birthplace_elsewhere_total,
        CAST("Lang_spoken_home_Eng_only_M" AS BIGINT) AS lang_spoken_home_english_only_male,
        CAST("Lang_spoken_home_Eng_only_F" AS BIGINT) AS lang_spoken_home_english_only_female,
        CAST("Lang_spoken_home_Eng_only_P" AS BIGINT) AS lang_spoken_home_english_only_total,
        CAST("Lang_spoken_home_Oth_Lang_M" AS BIGINT) AS lang_spoken_home_other_lang_male,
        CAST("Lang_spoken_home_Oth_Lang_F" AS BIGINT) AS lang_spoken_home_other_lang_female,
        CAST("Lang_spoken_home_Oth_Lang_P" AS BIGINT) AS lang_spoken_home_other_lang_total,
        CAST("Australian_citizen_M" AS BIGINT) AS australian_citizen_male,
        CAST("Australian_citizen_F" AS BIGINT) AS australian_citizen_female,
        CAST("Australian_citizen_P" AS BIGINT) AS australian_citizen_total,
        CAST("Count_Persons_other_dwgs_P" AS BIGINT) AS count_persons_other_dwgs_total
    FROM {{ ref('bronze_census_g01') }}
    WHERE "LGA_CODE_2016" IS NOT NULL
),

cleaned_census_g02 AS (
    SELECT
        TRIM("LGA_CODE_2016") AS lga_code_2016,
        CAST("Median_age_persons" AS BIGINT) AS median_age_persons,
        CAST("Median_mortgage_repay_monthly" AS BIGINT) AS median_mortgage_repay_monthly,
        CAST("Median_tot_prsnl_inc_weekly" AS BIGINT) AS median_total_personal_income_weekly,
        CAST("Median_rent_weekly" AS BIGINT) AS median_rent_weekly,
        CAST("Median_tot_fam_inc_weekly" AS BIGINT) AS median_total_family_income_weekly,
        CAST("Average_num_psns_per_bedroom" AS DOUBLE PRECISION) AS average_persons_per_bedroom,
        CAST("Median_tot_hhd_inc_weekly" AS BIGINT) AS median_total_household_income_weekly,
        CAST("Average_household_size" AS DOUBLE PRECISION) AS average_household_size
    FROM {{ ref('bronze_census_g02') }}
    WHERE "LGA_CODE_2016" IS NOT NULL
)

SELECT
    g01.*,
    g02.median_age_persons,
    g02.median_mortgage_repay_monthly,
    g02.median_total_personal_income_weekly,
    g02.median_rent_weekly,
    g02.median_total_family_income_weekly,
    g02.average_persons_per_bedroom,
    g02.median_total_household_income_weekly,
    g02.average_household_size
FROM cleaned_census_g01 AS g01
LEFT JOIN cleaned_census_g02 AS g02
ON g01.lga_code_2016 = g02.lga_code_2016