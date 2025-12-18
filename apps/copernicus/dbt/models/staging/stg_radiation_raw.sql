{{ config(
    materialized='view'
) }}

with source as (

    select * 
    from {{ source('radiation', 'cams_solar_radiation_timeseries') }}

)

, cleaned as (
    select
        -- Top-of-atmosphere irradiance [W/m²]
        -- Incoming solar radiation at the edge of Earth's atmosphere (no atmosphere effects)
        nullif(toa, 'nan')::float               as toa,

        -- --- Clear-sky (theoretical, no clouds) radiation components ---
        -- Global Horizontal Irradiance under clear sky [W/m²]
        -- Direct + diffuse radiation on a flat horizontal surface
        nullif(clear_sky_ghi, 'nan')::float     as clear_sky_ghi,

        -- Beam (direct) Horizontal Irradiance under clear sky [W/m²]
        -- Sunbeam projected onto a horizontal plane
        nullif(clear_sky_bhi, 'nan')::float     as clear_sky_bhi,

        -- Diffuse Horizontal Irradiance under clear sky [W/m²]
        -- Scattered light from the sky dome (excluding direct beam)
        nullif(clear_sky_dhi, 'nan')::float     as clear_sky_dhi,

        -- Beam Normal Irradiance under clear sky [W/m²]
        -- Direct solar irradiance on a surface aimed directly at the sun
        nullif(clear_sky_bni, 'nan')::float     as clear_sky_bni,

        -- --- Actual modeled radiation (with atmosphere, clouds, aerosols) ---
        -- Global Horizontal Irradiance [W/m²]
        nullif(ghi, 'nan')::float               as ghi,

        -- Beam (direct) Horizontal Irradiance [W/m²]
        nullif(bhi, 'nan')::float               as bhi,

        -- Diffuse Horizontal Irradiance [W/m²]
        nullif(dhi, 'nan')::float               as dhi,

        -- Beam Normal Irradiance [W/m²]
        nullif(bni, 'nan')::float               as bni,

        -- Reliability score [0–1]
        -- Model confidence in the radiation estimates (1.0 = high confidence)
        nullif(reliability, 'nan')::float       as reliability,

        -- Start datetime (UTC) of the measurement/forecast
        date_start
    from source
)

select * from cleaned
