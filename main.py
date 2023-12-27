import asyncio
import aiohttp
from more_itertools import chunked
from models import People, Session, init_db, get_data

import datetime

MAX_CHUNK = 5

async def get_info(person_id, session):
    http_respons = await session.get(f"https://swapi.py4e.com/api/people/{person_id}/")
    if http_respons.status == 200:
        json_data = await http_respons.json()
        films = []
        for url in json_data.get("films"):
            http_respons_film = await session.get(url)
            json_data_film = await http_respons_film.json()
            films.append(json_data_film.get('title'))

        species = []
        for url in json_data.get("species"):
            http_respons_species = await session.get(url)
            json_data_species = await http_respons_species.json()
            species.append(json_data_species.get('name'))

        starships = []
        for url in json_data.get("starships"):
            http_respons_starships = await session.get(url)
            json_data_starships = await http_respons_starships.json()
            starships.append(json_data_starships.get('name'))

        vehicles = []
        for url in json_data.get("vehicles"):
            http_respons_vehicles = await session.get(url)
            json_data_vehicles = await http_respons_vehicles.json()
            vehicles.append(json_data_vehicles.get('name'))
        

        url = json_data.get("homeworld")
        http_respons_homeworld = await session.get(url)
        json_data_homeworld = await http_respons_homeworld.json()
        homeworld = json_data_homeworld.get('name')

        data = {
            "id": json_data.get("id"),
            "birth_year": json_data.get("birth_year"),
            "eye_color": json_data.get("eye_color"),
            "films": ', '.join(films),
            "gender": json_data.get("gender"),
            "hair_color": json_data.get("hair_color"),
            "height": json_data.get("height"),
            "homeworld": homeworld,
            "mass": json_data.get("mass"),
            "name": json_data.get("name"),
            "skin_color": json_data.get("skin_color"),
            "species": ', '.join(species),
            "starships": ', '.join(starships),
            "vehicles":', '.join(vehicles),
            }
        return data
    else:
        return None
    
   
async def insert_records(records):
    records = [People(**record) for record in records if record is not None]
    async with Session() as session:
        session.add_all(records)
        await session.commit()



async def main():
    await init_db()
    session = aiohttp.ClientSession()
    for chunk in chunked(range(1, 90), MAX_CHUNK):
        coros = [get_info(i, session) for i in chunk]
        result = await asyncio.gather(*coros)
        asyncio.create_task(insert_records(result))
    await session.close()
    all_tasks_set = asyncio.all_tasks() -  {asyncio.current_task()}
    await asyncio.gather(*all_tasks_set)
    await get_data()

start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)