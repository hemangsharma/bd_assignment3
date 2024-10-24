{{ config(materialized='table') }}

SELECT * FROM {{ source('raw', 'airbnb_listings') }}