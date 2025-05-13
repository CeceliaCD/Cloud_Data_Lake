CREATE EXTERNAL TABLE IF NOT EXISTS pokemon_curated_indexdb.entire_orig_pokemon_info (
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
LOCATION 's3://cecepersonal.datalake/curated/entire_orig_pokemon_info/'
TBLPROPERTIES (
  'classification'='parquet'
);

CREATE EXTERNAL TABLE IF NOT EXISTS pokemon_curated_indexdb.pokemon_stats (
  ID INT,
  Pokemon_Name STRING,
  HP INT,
  Attack INT,
  Defense INT,
  Sp_Attack INT,
  Sp_Defense INT,
  Speed INT,
  Base_Stats INT
)
STORED AS PARQUET
LOCATION 's3://cecepersonal.datalake/curated/pokemon_stats/'
TBLPROPERTIES (
  'classification'='parquet'
);

INSERT INTO pokemon_curated_indexdb.pokemon_stats
SELECT
  ID,
  Pokemon_Name,
  HP,
  Attack,
  Defense,
  Sp_Attack,
  Sp_Defense,
  Speed,
  Base_Stats
FROM pokemon_curated.entire_orig_pokemon_info;

MSCK REPAIR TABLE pokemon_curated_indexdb.pokemon_stats;

CREATE EXTERNAL TABLE IF NOT EXISTS pokemon_curated_indexdb.pokemon_health_measurements (
  ID INT,
  Pokemon_Name STRING,
  HP INT,
  gender_male_ratio FLOAT,
  height_inches INT,
  height_meters FLOAT,
  weight_pounds FLOAT,
  weight_kilograms FLOAT
)
STORED AS PARQUET
LOCATION 's3://cecepersonal.datalake/curated/pokemon_health_measurements/'
TBLPROPERTIES (
  'classification'='parquet'
);

INSERT INTO pokemon_curated_indexdb.pokemon_health_measurements
SELECT
  ID,
  Pokemon_Name,
  HP,
  gender_male_ratio,
  height_inches,
  height_meters,
  weight_pounds,
  weight_kilograms
FROM pokemon_curated.entire_orig_pokemon_info;

MSCK REPAIR TABLE pokemon_curated_indexdb.pokemon_health_measurements;

CREATE EXTERNAL TABLE IF NOT EXISTS pokemon_curated_indexdb.pokemon_weaknesses_immunity (
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
  number_super_effective INT
)
STORED AS PARQUET
LOCATION 's3://cecepersonal.datalake/curated/pokemon_weaknesses_immunity/'
TBLPROPERTIES (
  'classification'='parquet'
);

CREATE EXTERNAL TABLE IF NOT EXISTS pokemon_curated_indexdb.pokemon_battle (
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
LOCATION 's3://cecepersonal.datalake/curated/pokemon_battle/'
TBLPROPERTIES (
  'classification'='parquet'
);

CREATE EXTERNAL TABLE IF NOT EXISTS pokemon_curated_indexdb.strong_pokemon (
  ID INT,
  Pokemon_Name STRING,
  HP INT,
  Attack INT,
  Defense INT,
  Sp_Attack INT,
  Sp_Defense INT,
  Speed INT,
  Base_Stats INT
)
PARTITIONED BY (generation INT)
STORED AS PARQUET
LOCATION 's3://cecepersonal.datalake/curated/strong_pokemon/'
TBLPROPERTIES (
  'classification' = 'parquet'
);

INSERT INTO pokemon_curated_indexdb.strong_pokemon
SELECT *, generation
FROM pokemon_curated.pokemon_stats
JOIN pokemon_curated.entire_orig_pokemon_info USING (ID, Pokemon_Name)
WHERE HP >= 100;

MSCK REPAIR TABLE pokemon_curated_indexdb.strong_pokemon;

CREATE EXTERNAL TABLE IF NOT EXISTS pokemon_curated_indexdb.legendary_pokemon (
  ID INT,
  Pokemon_Name STRING,
  HP INT,
  Attack INT,
  generation INT
) 
STORED AS PARQUET
LOCATION 's3://cecepersonal.datalake/curated/legendary_pokemon/'
TBLPROPERTIES (
  'classification' = 'parquet'
);

INSERT INTO pokemon_curated_indexdb.legendary_pokemon
SELECT ID, Pokemon_Name, HP, Attack, generation
FROM pokemon_curated.strong_pokemon
JOIN pokemon_curated.entire_orig_pokemon_info USING (ID, Pokemon_Name)
WHERE Is_Legendary = 1;

MSCK REPAIR TABLE pokemon_curated_indexdb.legendary_pokemon;

CREATE EXTERNAL TABLE IF NOT EXISTS pokemon_curated_indexdb.mythical_pokemon (
  ID INT,
  Pokemon_Name STRING,
  HP INT,
  Attack INT,
  generation INT
)
STORED AS PARQUET
LOCATION 's3://cecepersonal.datalake/curated/mythical_pokemon/'
TBLPROPERTIES (
  'classification' = 'parquet'
);

INSERT INTO pokemon_curated_indexdb.mythical_pokemon
SELECT ID, Pokemon_Name, HP, Attack, generation
FROM pokemon_curated.strong_pokemon
JOIN pokemon_curated.entire_orig_pokemon_info USING (ID, Pokemon_Name)
WHERE Is_Mythical = 1;

MSCK REPAIR TABLE pokemon_curated_indexdb.mythical_pokemon;

CREATE EXTERNAL TABLE IF NOT EXISTS pokemon_curated_indexdb.ultra_beast_pokemon (
  ID INT,
  Pokemon_Name STRING,
  HP INT,
  Attack INT,
  generation INT
)
STORED AS PARQUET
LOCATION 's3://cecepersonal.datalake/curated/ultra_beast_pokemon/'
TBLPROPERTIES (
  'classification' = 'parquet'
);

INSERT INTO pokemon_curated_indexdb.ultra_beast_pokemon
SELECT ID, Pokemon_Name, HP, Attack, generation
FROM pokemon_curated.strong_pokemon
JOIN pokemon_curated.entire_orig_pokemon_info USING (ID, Pokemon_Name)
WHERE Is_Ultra_Beast = 1;

MSCK REPAIR TABLE pokemon_curated_indexdb.ultra_beast_pokemon;