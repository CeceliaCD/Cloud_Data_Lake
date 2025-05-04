CREATE EXTERNAL TABLE IF NOT EXISTS pokemon_curated.entire_orig_pokemon_info (
  ID INT,
  Pokemon_Name STRING,
  HP INT,
  Attack INT,
  Defense INT,
  Sp_Attack INT,
  Sp_Defense INT,
  Speed INT,
  Base_Stats INT,
  normal_weakness FLOAT,
  fire_weakness FLOAT,
  water_weakness FLOAT,
  electric_weakness FLOAT,
  grass_weakness FLOAT,
  ice_weakness FLOAT,
  fighting_weakness FLOAT,
  poison_weakness FLOAT,
  ground_weakness FLOAT,
  flying_weakness FLOAT,
  psychic_weakness FLOAT,
  bug_weakness FLOAT,
  rock_weakness FLOAT,
  ghost_weakness FLOAT,
  dragon_weakness FLOAT,
  dark_weakness FLOAT,
  steel_weakness FLOAT,
  fairy_weakness FLOAT,
  height_inches INT,
  height_meters FLOAT,
  weight_pounds FLOAT,
  weight_kilograms FLOAT,
  capturing_rate INT,
  gender_male_ratio FLOAT,
  egg_steps INT,
  egg_cycles INT,
  abilities STRING,
  Type_1 STRING,
  Type_2 STRING,
  Classification_info STRING,
  generation INT,
  Is_Legendary INT,
  Is_Mythical INT,
  Is_Ultra_Beast INT,
  number_immune INT,
  number_not_effective INT,
  number_normal INT,
  number_super_effective INT,
  curr_HP FLOAT,
  curr_status STRING,
  curr_status_value INT,
  pokeball_capture FLOAT,
  greatball_capture FLOAT,
  other_balls_capture FLOAT,
  masterball_capture FLOAT
)
STORED AS PARQUET
LOCATION 's3://cecepersonal.datalake/curated/'
TBLPROPERTIES (
  'classification'='parquet'
);

CREATE EXTERNAL TABLE IF NOT EXISTS pokemon_curated.pokemon_stats (
  ID INT,
  Pokemon_Name STRING,
  HP INT,
  Attack INT,
  Defense INT,
  Sp_Attack INT,
  Sp_Defense INT,
  Speed INT,
  Base_Stats INT,
)
STORED AS PARQUET
LOCATION 's3://cecepersonal.datalake/curated/'
TBLPROPERTIES (
  'classification'='parquet'
);

CREATE EXTERNAL TABLE IF NOT EXISTS pokemon_curated.pokemon_health_measurements (
  ID INT,
  Pokemon_Name STRING,
  HP INT,
  gender_male_ratio FLOAT,
  height_inches INT,
  height_meters FLOAT,
  weight_pounds FLOAT,
  weight_kilograms FLOAT,
)
STORED AS PARQUET
LOCATION 's3://cecepersonal.datalake/curated/'
TBLPROPERTIES (
  'classification'='parquet'
);

CREATE EXTERNAL TABLE IF NOT EXISTS pokemon_curated.pokemon_weak_immune (
  ID INT,
  Pokemon_Name STRING,
  normal_weakness FLOAT,
  fire_weakness FLOAT,
  water_weakness FLOAT,
  electric_weakness FLOAT,
  grass_weakness FLOAT,
  ice_weakness FLOAT,
  fighting_weakness FLOAT,
  poison_weakness FLOAT,
  ground_weakness FLOAT,
  flying_weakness FLOAT,
  psychic_weakness FLOAT,
  bug_weakness FLOAT,
  rock_weakness FLOAT,
  ghost_weakness FLOAT,
  dragon_weakness FLOAT,
  dark_weakness FLOAT,
  steel_weakness FLOAT,
  fairy_weakness FLOAT,
  number_immune INT,
  number_not_effective INT,
  number_normal INT,
  number_super_effective INT,
)
STORED AS PARQUET
LOCATION 's3://cecepersonal.datalake/curated/'
TBLPROPERTIES (
  'classification'='parquet'
);

CREATE TABLE pokemon_curated.legendary_pokemon
WITH (
  format = 'PARQUET',
  external_location = 's3://cecepersonal.datalake/curated/',
  partitioned_by = ARRAY['generation']
) AS
SELECT ID, Pokemon_Name, HP, Attack, generation
FROM pokemon_curated.entire_orig_pokemon_info
WHERE Is_Legendary = 1;

CREATE TABLE pokemon_curated.mythical_pokemon
WITH (
  format = 'PARQUET',
  external_location = 's3://cecepersonal.datalake/curated/',
  partitioned_by = ARRAY['generation']
) AS
SELECT ID, Pokemon_Name, HP, Attack, generation
FROM pokemon_curated.entire_orig_pokemon_info
WHERE Is_Mythical = 1;

CREATE TABLE pokemon_curated.ultra_beast_pokemon
WITH (
  format = 'PARQUET',
  external_location = 's3://cecepersonal.datalake/curated/',
  partitioned_by = ARRAY['generation']
) AS
SELECT ID, Pokemon_Name, HP, Attack, generation
FROM pokemon_curated.entire_orig_pokemon_info
WHERE Is_Ultra_Beast = 1;

CREATE TABLE pokemon_curated.legendary_pokemon
WITH (
  format = 'PARQUET',
  external_location = 's3://cecepersonal.datalake/curated/',
  partitioned_by = ARRAY['generation']
) AS
SELECT * FROM pokemon_curated.pokemon_stats WHERE HP >= 100;
 
CREATE EXTERNAL TABLE IF NOT EXISTS pokemon_curated.pokemon_battle (
  ID INT,
  Pokemon_Name STRING,
  generation INT,
  HP INT,
  Attack INT,
  Defense INT,
  Sp_Attack INT,
  Sp_Defense INT,
  Speed INT,
  Classification_info STRING,
  abilities STRING,
  capturing_rate INT,
  normal_weakness FLOAT,
  fire_weakness FLOAT,
  water_weakness FLOAT,
  electric_weakness FLOAT,
  grass_weakness FLOAT,
  ice_weakness FLOAT,
  fighting_weakness FLOAT,
  poison_weakness FLOAT,
  ground_weakness FLOAT,
  flying_weakness FLOAT,
  psychic_weakness FLOAT,
  bug_weakness FLOAT,
  rock_weakness FLOAT,
  ghost_weakness FLOAT,
  dragon_weakness FLOAT,
  dark_weakness FLOAT,
  steel_weakness FLOAT,
  fairy_weakness FLOAT,
  number_immune INT,
  number_not_effective INT,
  number_normal INT,
  number_super_effective INT,
  curr_HP FLOAT,
  curr_status STRING,
  curr_status_value INT,
  pokeball_capture FLOAT,
  greatball_capture FLOAT,
  other_balls_capture FLOAT,
  masterball_capture FLOAT
)
STORED AS PARQUET
LOCATION 's3://cecepersonal.datalake/curated/'
TBLPROPERTIES (
  'classification'='parquet'
);

  