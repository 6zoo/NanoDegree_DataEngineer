usc_demograhics = """
SELECT
    city
    , state
    , median_age
    , male_population
    , female_population
    , total_population
    , number_of_veterans
    , foreign_born
    , average_household_size
    , state_code
    , race
    , count
FROM uscdemo
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
"""

immigrations = """
SELECT
    cicid
    , i94yr
    , i94mon
    , i94cit
    , i94res
    , i94port
    , arrdate
    , i94mode
    , i94addr
    , depdate
    , i94bir
    , i94visa
    , dtadfile
    , visapost
    , entdepa
    , entdepd
    , matflag
    , biryear
    , dtaddto
    , gender
    , airline
    , admnum
    , fltno
    , visatype
    , count
FROM immg
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25
"""
airports = """
SELECT
    iata_code
    , ident
    , type
    , name
    , elevation_ft
    , continent
    , iso_country
    , iso_region
    , municipality
    , gps_code
    , local_code
    , coordinates
FROM airports
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
"""


portinfo = """
SELECT
    port_code
    , city
    , state_code
FROM portinfo
GROUP BY 1, 2, 3
"""
