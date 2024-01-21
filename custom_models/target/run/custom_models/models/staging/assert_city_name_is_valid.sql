

  create or replace view `data-eng-practice007`.`dbt_core_models`.`assert_city_name_is_valid`
  OPTIONS()
  as -- "country_coordinates_population" is a table which contains the geo coordinates of different countries. This table is seeded by dbt before the actual models are generated. 

-- Verifying if the models contain only valid countries


--select;

