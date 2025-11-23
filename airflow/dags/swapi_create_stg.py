from config.postgres_config import DB_CONN
from config.logger_config import get_logger
from sqlalchemy import create_engine, text


logger = get_logger()


def get_engine():
    """Возвращает SQLAlchemy engine для Postgres"""
    host=DB_CONN["host"]
    port=DB_CONN["port"]
    user=DB_CONN["user"]
    password=DB_CONN["password"]
    db = DB_CONN["DB_NAME"]

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")
    return engine


def get_create_stg_vehicles():
    """Создаёт таблицу vehicles слоя STG"""
    engine = get_engine()
    full_table = f"stg.vehicles"
    with engine.begin() as conn:
        sql = f"""
        create table if not exists {full_table} as
        with vehicles as (
        select
            id
            ,max_atmosphering_speed
            ,manufacturer
            ,cast(case when cost_in_credits = 'unknown' then null else cost_in_credits end as int) as cost_in_credits
            ,cast(case when crew = 'unknown' then null else crew end as int) as crew
            ,cast(case when length = 'unknown' then null else length end as float) as length
            ,name
            ,model
            ,cast(case when passengers = 'unknown' then null else passengers end as int) as passengers
            ,cast(case when cargo_capacity = 'unknown' then null when cargo_capacity = 'none' then '0' else cargo_capacity end as int) as cargo_capacity
            ,vehicle_class
            ,consumables
            ,cast(created as timestamp) as created_at
            ,cast(edited as timestamp) as edited_at
            ,cast(regexp_replace(url, '.*/vehicles/([0-9]+)/.*', E'\\\\1') as int) as vehicles_id
            ,films
            ,pilots
        from
            swapi.raw.vehicles
        )

        ,film_ids as (
        select 
            id
            ,vehicles_id
            ,string_agg(films_match[1], ',') as film_ids
        from
            vehicles, regexp_matches(films, 'films/(\d+)/', 'g') as films_match
        group by
            1,2
        )

        ,pilot_ids as (
        select
            id
            ,vehicles_id
            ,string_agg(pilots_match[1], ',') as pilot_ids
        from
            vehicles, regexp_matches(pilots, 'people/(\d+)/', 'g') as pilots_match
        group by
            1,2
        )

        select
            vehicles.id
            ,vehicles.vehicles_id
            ,vehicles.name
            ,vehicles.model
            ,vehicles.vehicle_class
            ,vehicles.crew
            ,vehicles.passengers
            ,vehicles.length
            ,vehicles.max_atmosphering_speed
            ,vehicles.cargo_capacity
            ,vehicles.consumables	
            ,vehicles.manufacturer
            ,vehicles.cost_in_credits
            ,film_ids.film_ids
            ,pilot_ids.pilot_ids
            ,vehicles.created_at
            ,vehicles.edited_at
        from
            vehicles
        left join
            film_ids
            on vehicles.id = film_ids.id
            and vehicles.vehicles_id = film_ids.vehicles_id
        left join
            pilot_ids
            on vehicles.id = pilot_ids.id
            and vehicles.vehicles_id = pilot_ids.vehicles_id
        ;
        """
        conn.execute(text(sql))
    logger.info(f"✅ Таблица {full_table} создана")


def get_create_stg_planets():
    """Создаёт таблицу planets слоя STG"""
    engine = get_engine()
    full_table = f"stg.planets"
    with engine.begin() as conn:
        sql = f"""
        create table if not exists {full_table} as
        with planets as (
        select
            id
            ,cast(case when diameter = 'unknown' then null else diameter end as int) as diameter
            ,residents
            ,cast(case when rotation_period = 'unknown' then null else rotation_period end as int) as rotation_period_hours
            ,cast(case when population = 'unknown' then null else population end as bigint) as population
            ,case when terrain = 'unknown' then null else terrain end as terrain
            ,cast(created as timestamp) as created_at
            ,cast(regexp_replace(url, '.*/planets/([0-9]+)/.*', E'\\\\1') as int) as planets_id
            ,cast(case when surface_water = 'unknown' then null else surface_water end as float) as surface_water
            ,cast(edited as timestamp) as edited_at
            ,cast(case when orbital_period = 'unknown' then null else orbital_period end as int) as orbital_period_days
            ,name
            ,case when gravity = 'unknown' then null else gravity end as gravity
            ,films
            ,case when climate = 'unknown' then null else climate end as climate
        from
            raw.planets
        )

        ,resident_ids as (
        select 
            id
            ,planets_id
            ,string_agg(residents_match[1], ',') as resident_ids
        from
            planets, regexp_matches(residents, 'people/(\d+)/', 'g') as residents_match
        group by
            1,2
        )

        ,film_ids as (
        select 
            id
            ,planets_id
            ,string_agg(films_match[1], ',') as film_ids
        from
            planets, regexp_matches(films, 'films/(\d+)/', 'g') as films_match
        group by
            1,2
        )

        select
            planets.id
            ,planets.planets_id
            ,planets.name
            ,planets.diameter
            ,planets.rotation_period_hours
            ,planets.population
            ,planets.terrain
            ,planets.surface_water
            ,planets.orbital_period_days
            ,planets.gravity
            ,planets.climate
            ,resident_ids.resident_ids
            ,film_ids.film_ids
            ,planets.created_at
            ,planets.edited_at
        from
            planets
        left join
            resident_ids
            on planets.id = resident_ids.id
            and planets.planets_id = resident_ids.planets_id
        left join
            film_ids
            on planets.id = film_ids.id
            and planets.planets_id = film_ids.planets_id
        ;
        """
        conn.execute(text(sql))
    logger.info(f"✅ Таблица {full_table} создана")


def get_create_stg_films():
    """Создаёт таблицу films слоя STG"""
    engine = get_engine()
    full_table = f"stg.films"
    with engine.begin() as conn:
        sql = f"""
        create table if not exists {full_table} as
        with films as (
        select
            id
            ,species
            ,planets
            ,cast(regexp_replace(url, '.*/films/([0-9]+)/.*', E'\\\\1') as int) as films_id
            ,title
            ,starships
            ,vehicles
            ,producer
            ,director
            ,cast(release_date as timestamp) as release_at
            ,characters
            ,cast(created as timestamp) as created_at
            ,cast(edited as timestamp) as edited_at
            ,opening_crawl
            ,cast(episode_id as int) as episode_id
        from
            raw.films
        )

        ,species_ids as (
        select 
            id
            ,episode_id
            ,string_agg(species_match[1], ',') as species_ids
        from
            films, regexp_matches(species, 'species/(\d+)/', 'g') as species_match
        group by
            1,2
        )

        ,planet_ids as (
        select 
            id
            ,episode_id
            ,string_agg(planets_match[1], ',') as planet_ids
        from
            films, regexp_matches(planets, 'planets/(\d+)/', 'g') as planets_match
        group by
            1,2
        )

        ,starship_ids as (
        select 
            id
            ,episode_id
            ,string_agg(starships_match[1], ',') as starship_ids
        from
            films, regexp_matches(starships, 'starships/(\d+)/', 'g') as starships_match
        group by
            1,2
        )

        ,vehicle_ids as (
        select 
            id
            ,episode_id
            ,string_agg(vehicles_match[1], ',') as vehicle_ids
        from
            films, regexp_matches(vehicles, 'vehicles/(\d+)/', 'g') as vehicles_match
        group by
            1,2
        )

        ,people_ids as (
        select 
            id
            ,episode_id
            ,string_agg(peoples_match[1], ',') as people_ids
        from
            films, regexp_matches(characters, 'people/(\d+)/', 'g') as peoples_match
        group by
            1,2
        )

        select
            films.id
            ,species_ids.species_ids
            ,planet_ids.planet_ids
            ,films.films_id
            ,films.title
            ,starship_ids.starship_ids
            ,vehicle_ids.vehicle_ids
            ,films.producer
            ,films.director
            ,films.release_at
            ,people_ids.people_ids
            ,films.created_at
            ,films.edited_at
            ,films.opening_crawl
            ,films.episode_id
        from
            films
        left join
            species_ids
            on films.id = species_ids.id
            and films.episode_id = species_ids.episode_id
        left join
            planet_ids
            on films.id = planet_ids.id
            and films.episode_id = planet_ids.episode_id
        left join
            starship_ids
            on films.id = starship_ids.id
            and films.episode_id = starship_ids.episode_id
        left join
            vehicle_ids
            on films.id = vehicle_ids.id
            and films.episode_id = vehicle_ids.episode_id
        left join
            people_ids
            on films.id = people_ids.id
            and films.episode_id = people_ids.episode_id
        ;
        """
        conn.execute(text(sql))
    logger.info(f"✅ Таблица {full_table} создана")


def get_create_stg_people():
    """Создаёт таблицу people слоя STG"""
    engine = get_engine()
    full_table = f"stg.people"
    with engine.begin() as conn:
        sql = f"""
        create table if not exists {full_table} as
        with people as (
        select
            id
            ,species
            ,cast(case when mass = 'unknown' then null else replace(mass, ',', '') end as float) as mass
            ,cast(regexp_replace(url, '.*/people/([0-9]+)/.*', E'\\\\1') as int) as people_id
            ,cast(case when height = 'unknown' then null else height end as int) as height
            ,birth_year
            ,homeworld
            ,starships
            ,films
            ,vehicles
            ,eye_color
            ,skin_color
            ,hair_color
            ,cast(created as timestamp) as created_at
            ,cast(edited as timestamp) as edited_at
            ,gender
            ,name
        from
            raw.people
        )

        ,species_ids as (
        select 
            id
            ,people_id
            ,string_agg(species_match[1], ',') AS species_ids
        from
            people, regexp_matches(species, 'species/(\d+)/', 'g') as species_match
        group by
            1,2
        )

        ,homeworld_ids as (
        select 
            id
            ,people_id
            ,string_agg(homeworlds_match[1], ',') AS homeworld_ids
        from
            people, regexp_matches(homeworld, 'planets/(\d+)/', 'g') as homeworlds_match
        group by
            1,2
        )

        ,starship_ids as (
        select 
            id
            ,people_id
            ,string_agg(starships_match[1], ',') AS starship_ids
        from
            people, regexp_matches(starships, 'starships/(\d+)/', 'g') as starships_match
        group by
            1,2
        )

        ,film_ids as (
        select 
            id
            ,people_id
            ,string_agg(films_match[1], ',') AS film_ids
        from
            people, regexp_matches(films, 'films/(\d+)/', 'g') as films_match
        group by
            1,2
        )

        ,vehicle_ids as (
        select 
            id
            ,people_id
            ,string_agg(vehicles_match[1], ',') AS vehicle_ids
        from
            people, regexp_matches(vehicles, 'vehicles/(\d+)/', 'g') as vehicles_match
        group by
            1,2
        )

        select
            people.id
            ,species_ids.species_ids
            ,people.mass
            ,people.people_id
            ,people.height
            ,people.birth_year
            ,homeworld_ids.homeworld_ids
            ,starship_ids.starship_ids
            ,film_ids.film_ids
            ,vehicle_ids.vehicle_ids
            ,people.eye_color
            ,people.skin_color
            ,people.hair_color
            ,people.created_at
            ,people.edited_at
            ,people.gender
            ,people.name
        from
            people
        left join
            species_ids
            on people.id = species_ids.id
            and people.people_id = species_ids.people_id
        left join
            homeworld_ids
            on people.id = homeworld_ids.id
            and people.people_id = homeworld_ids.people_id
        left join
            starship_ids
            on people.id = starship_ids.id
            and people.people_id = starship_ids.people_id	
        left join
            film_ids
            on people.id = film_ids.id
            and people.people_id = film_ids.people_id
        left join
            vehicle_ids
            on people.id = vehicle_ids.id
            and people.people_id = vehicle_ids.people_id
        ;
        """
        conn.execute(text(sql))
    logger.info(f"✅ Таблица {full_table} создана")


def get_create_stg_species():
    """Создаёт таблицу species слоя STG"""
    engine = get_engine()
    full_table = f"stg.species"
    with engine.begin() as conn:
        sql = f"""
        create table if not exists {full_table} as
        with species as (
        select
            id
            ,skin_colors
            ,people
            ,classification
            ,cast(created as timestamp) as created_at
            ,cast(regexp_replace(url, '.*/species/([0-9]+)/.*', E'\\\\1') as int) as species_id
            ,designation
            ,hair_colors
            ,homeworld
            ,films
            ,cast(case when average_lifespan in ('unknown', 'indefinite') then null else average_lifespan end as int) as average_lifespan
            ,eye_colors
            ,cast(edited as timestamp) as edited_at
            ,name
            ,cast(case when average_height in ('unknown', 'n/a') then null else average_height end as int) as average_height
            ,language
        from
            raw.species
        )

        ,people_ids as (
        select 
            id
            ,species_id
            ,string_agg(peoples_match[1], ',') as people_ids
        from
            species, regexp_matches(people, 'vehicles/(\d+)/', 'g') as peoples_match
        group by
            1,2
        )

        ,homeworld_ids as (
        select 
            id
            ,species_id
            ,string_agg(homeworlds_match[1], ',') as homeworld_ids
        from
            species, regexp_matches(homeworld, 'planets/(\d+)/', 'g') as homeworlds_match
        group by
            1,2
        )

        ,film_ids as (
        select 
            id
            ,species_id
            ,string_agg(films_match[1], ',') AS film_ids
        from
            species, regexp_matches(films, 'films/(\d+)/', 'g') as films_match
        group by
            1,2
        )

        select
            species.id
            ,species.species_id	
            ,species.name
            ,species.classification
            ,species.skin_colors
            ,species.eye_colors
            ,species.hair_colors
            ,species.designation	
            ,species.language
            ,species.average_lifespan	
            ,species.average_height
            ,people_ids.people_ids
            ,homeworld_ids.homeworld_ids
            ,film_ids.film_ids
            ,species.created_at
            ,species.edited_at
        from
            species
        left join
            people_ids
            on species.id = people_ids.id
            and species.species_id = people_ids.species_id
        left join
            homeworld_ids
            on species.id = homeworld_ids.id
            and species.species_id = homeworld_ids.species_id
        left join
            film_ids
            on species.id = film_ids.id
            and species.species_id = film_ids.species_id
        ;
        """
        conn.execute(text(sql))
    logger.info(f"✅ Таблица {full_table} создана")


def get_create_stg_starships():
    """Создаёт таблицу starships слоя STG"""
    engine = get_engine()
    full_table = f"stg.starships"
    with engine.begin() as conn:
        sql = f"""
        create table if not exists {full_table} as
        with starships as (
        select
            id
            ,case when consumables = 'unknown' then null else cast(split_part(consumables, ' ', 1) as int) end as consumables_count
            ,rtrim(case when consumables = 'unknown' then null else split_part(consumables, ' ', 2) end, 's') as consumables_period
            ,'MGLT' as mglt
            ,cast(created as timestamp) as created_at
            ,cast(case when cargo_capacity = 'unknown' then null else cargo_capacity end as bigint) as cargo_capacity
            ,case when crew = 'unknown' then null else cast(replace(split_part(crew, '-', 1), ',', '') as float) end as crew
            ,cast(regexp_replace(url, '.*/starships/([0-9]+)/.*', E'\\\\1') as int) as starships_id
            ,starship_class
            ,manufacturer
            ,films
            ,cast(case when max_atmosphering_speed in ('unknown', 'n/a') then null else rtrim(max_atmosphering_speed, 'km') end as bigint) as max_atmosphering_speed
            ,cast(case when cargo_capacity = 'unknown' then null else hyperdrive_rating end as float) as hyperdrive_rating
            ,cast(case when length = 'unknown' then null else replace(length, ',', '.') end as float) as length
            ,pilots
            ,cast(case when passengers in ('unknown', 'n/a') then null else replace(passengers, ',', '') end as bigint) as passengers
            ,cast(edited as timestamp) as edited_at
            ,cast(case when cost_in_credits = 'unknown' then null else cost_in_credits end as bigint) as cost_in_credits
            ,name
            ,model
        from
            raw.starships
        )

        ,film_ids as (
        select 
            id
            ,starships_id
            ,string_agg(films_match[1], ',') AS film_ids
        from
            starships, regexp_matches(films, 'films/(\d+)/', 'g') as films_match
        group by
            1,2
        )

        ,pilot_ids as (
        select 
            id
            ,starships_id
            ,string_agg(pilots_match[1], ',') AS pilot_ids
        from
            starships, regexp_matches(pilots, 'people/(\d+)/', 'g') as pilots_match
        group by
            1,2
        )

        select
            starships.id
            ,starships.starships_id	
            ,starships.name
            ,starships.model
            ,starships.manufacturer
            ,starships.starship_class
            ,starships.cost_in_credits
            ,starships.consumables_count
            ,starships.consumables_period
            ,starships.cargo_capacity
            ,starships.crew
            ,starships.max_atmosphering_speed
            ,starships.hyperdrive_rating
            ,starships.length
            ,starships.passengers
            ,starships.mglt
            ,film_ids.film_ids
            ,pilot_ids.pilot_ids
            ,starships.created_at
            ,starships.edited_at	
        from
            starships
        left join
            film_ids
            on starships.id = film_ids.id
            and starships.starships_id = film_ids.starships_id
        left join
            pilot_ids
            on starships.id = pilot_ids.id
            and starships.starships_id = pilot_ids.starships_id
        ;
        """
        conn.execute(text(sql))
    logger.info(f"✅ Таблица {full_table} создана")