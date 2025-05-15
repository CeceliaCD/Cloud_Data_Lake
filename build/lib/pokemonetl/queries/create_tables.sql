SELECT
  TRY_CAST(normal_weakness AS DOUBLE) AS normal_weakness,
  TRY_CAST(fire_weakness AS DOUBLE) AS fire_weakness,
  TRY_CAST(water_weakness AS DOUBLE) AS water_weakness,
  TRY_CAST(electric_weakness AS DOUBLE) AS electric_weakness,
  TRY_CAST(grass_weakness AS DOUBLE) AS grass_weakness,
  TRY_CAST(ice_weakness AS DOUBLE) AS ice_weakness,
  TRY_CAST(fighting_weakness AS DOUBLE) AS fighting_weakness,
  TRY_CAST(poison_weakness AS DOUBLE) AS poison_weakness,
  TRY_CAST(ground_weakness AS DOUBLE) AS ground_weakness,
  TRY_CAST(flying_weakness AS DOUBLE) AS flying_weakness,
  TRY_CAST(psychic_weakness AS DOUBLE) AS psychic_weakness,
  TRY_CAST(bug_weakness AS DOUBLE) AS bug_weakness,
  TRY_CAST(rock_weakness AS DOUBLE) AS rock_weakness,
  TRY_CAST(ghost_weakness AS DOUBLE) AS ghost_weakness,
  TRY_CAST(dragon_weakness AS DOUBLE) AS dragon_weakness,
  TRY_CAST(dark_weakness AS DOUBLE) AS dark_weakness,
  TRY_CAST(steel_weakness AS DOUBLE) AS steel_weakness,
  TRY_CAST(fairy_weakness AS DOUBLE) AS fairy_weakness,
  TRY_CAST(height_meters AS DOUBLE) AS height_meters,
  TRY_CAST(weight_pounds AS DOUBLE) AS weight_pounds,
  TRY_CAST(weight_kilograms AS DOUBLE) AS weight_kilograms,
  TRY_CAST(gender_male_ratio AS DOUBLE) AS gender_male_ratio,
  TRY_CAST(curr_HP AS DOUBLE) AS curr_HP,
  TRY_CAST(pokeball_capture AS DOUBLE) AS pokeball_capture,
  TRY_CAST(greatball_capture AS DOUBLE) AS greatball_capture,
  TRY_CAST(other_balls_capture AS DOUBLE) AS other_balls_capture,
  TRY_CAST(masterball_capture AS DOUBLE) AS masterball_capture
FROM pokemon_index_db.processed;

CREATE TABLE pokemon_curated_indexdb.pokemon_stats
WITH (
  format = 'PARQUET',
  external_location = 's3://cecepersonal.datalake/curated/pokemon_stats/',
  write_compression = 'SNAPPY',
  partitioned_by = ARRAY['generation']
)
AS
SELECT ID, Pokemon_Name, HP, Attack, Defense, Sp_Attack, Sp_Defense, Speed, Base_Stats, generation
FROM pokemon_index_db.processed;

CREATE TABLE pokemon_curated_indexdb.pokemon_health_measurements
WITH (
  format = 'PARQUET',
  external_location = 's3://cecepersonal.datalake/curated/pokemon_health_measurements/',
  write_compression = 'SNAPPY',
  partitioned_by = ARRAY['generation']
)
AS
SELECT ID, Pokemon_Name, HP, gender_male_ratio, height_inches, height_meters, weight_pounds, weight_kilograms, generation
FROM pokemon_index_db.processed;

CREATE TABLE pokemon_curated_indexdb.pokemon_weaknesses_immunity
WITH (
  format = 'PARQUET',
  external_location = 's3://cecepersonal.datalake/curated/pokemon_weaknesses_immunity/',
  write_compression = 'SNAPPY',
  partitioned_by = ARRAY['generation']
)
AS
SELECT 
  ID,
  Pokemon_Name,
  normal_weakness,
  fire_weakness,
  water_weakness,
  electric_weakness,
  grass_weakness,
  ice_weakness,
  fighting_weakness,
  poison_weakness,
  ground_weakness,
  flying_weakness,
  psychic_weakness,
  bug_weakness,
  rock_weakness,
  ghost_weakness,
  dragon_weakness,
  dark_weakness,
  steel_weakness,
  fairy_weakness,
  number_immune,
  number_not_effective,
  number_normal,
  number_super_effective,
  generation
FROM pokemon_index_db.processed;

CREATE TABLE pokemon_curated_indexdb.strong_pokemon
WITH (
  format = 'PARQUET',
  external_location = 's3://cecepersonal.datalake/curated/strong_pokemon/',
  write_compression = 'SNAPPY',
  partitioned_by = ARRAY['generation']
)
AS
SELECT ID, Pokemon_Name, HP, Attack, Defense, Sp_Attack, Sp_Defense, Speed, Base_Stats, generation
FROM pokemon_index_db.processed
WHERE HP >= 100;

CREATE TABLE pokemon_curated_indexdb.legendary_pokemon
WITH (
  format = 'PARQUET',
  external_location = 's3://cecepersonal.datalake/curated/legendary_pokemon/',
  write_compression = 'SNAPPY'
)
AS
SELECT ID, Pokemon_Name, HP, Attack, generation
FROM pokemon_index_db.processed
WHERE Is_Legendary = 1;

CREATE TABLE pokemon_curated_indexdb.mythical_pokemon
WITH (
  format = 'PARQUET',
  external_location = 's3://cecepersonal.datalake/curated/mythical_pokemon/',
  write_compression = 'SNAPPY'
)
AS
SELECT ID, Pokemon_Name, HP, Attack, generation
FROM pokemon_index_db.processed
WHERE Is_Mythical = 1;

CREATE TABLE pokemon_curated_indexdb.ultra_beast_pokemon
WITH (
  format = 'PARQUET',
  external_location = 's3://cecepersonal.datalake/curated/ultra_beast_pokemon/',
  write_compression = 'SNAPPY'
)
AS
SELECT ID, Pokemon_Name, HP, Attack, generation
FROM pokemon_index_db.processed
WHERE Is_Ultra_Beast = 1;

CREATE TABLE pokemon_curated_indexdb.pokemon_battle
WITH (
  format = 'PARQUET',
  external_location = 's3://cecepersonal.datalake/curated/pokemon_battle/',
  write_compression = 'SNAPPY',
  partitioned_by = ARRAY['generation']
)
AS
SELECT   
  ID,
  Pokemon_Name,
  HP,
  Attack,
  Defense,
  Sp_Attack,
  Sp_Defense,
  Speed,
  Classification_info,
  abilities,
  capturing_rate,
  normal_weakness,
  fire_weakness,
  water_weakness,
  electric_weakness,
  grass_weakness,
  ice_weakness,
  fighting_weakness,
  poison_weakness,
  ground_weakness,
  flying_weakness,
  psychic_weakness,
  bug_weakness,
  rock_weakness,
  ghost_weakness,
  dragon_weakness,
  dark_weakness,
  steel_weakness,
  fairy_weakness,
  number_immune,
  number_not_effective,
  number_normal,
  number_super_effective,
  curr_HP,
  curr_status,
  curr_status_value,
  pokeball_capture,
  greatball_capture,
  other_balls_capture,
  masterball_capture,
  generation
FROM pokemon_index_db.processed;