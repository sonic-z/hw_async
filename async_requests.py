import asyncio
import datetime

import aiohttp
from more_itertools import chunked

from models import DbSession, SwapiPeople, close_orm, init_orm

MAX_CHUNK = 5


async def get_people(http_session, people_id):
    response = await http_session.get(f"https://swapi.py4e.com/api/people/{people_id}/")
    if response.status == 200:
        json_data = await response.json()
        if json_data:
            json_data['id'] = people_id
            json_data['films'] = await get_names(http_session, json_data['films'])
            json_data['species'] = await get_names(http_session, json_data['species'])
            json_data['starships'] = await get_names(http_session, json_data['starships'])
            json_data['vehicles'] = await get_names(http_session, json_data['vehicles'])
            json_data['homeworld'] = await get_names(http_session, [json_data['homeworld']])
        return json_data


async def get_names(http_session, urls):
    names = []
    for url in urls:
        async with http_session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                names.append(data.get('title') or data.get('name'))
    return ', '.join(names)

async def insert_people(json_list):
    async with DbSession() as session:
        orm_objects = []
        for item in json_list:
            if item is not None:
                character = SwapiPeople(
                    id=item['id'],
                    name=item['name'],
                    birth_year=item['birth_year'],
                    eye_color=item['eye_color'],
                    films=item['films'],
                    gender=item['gender'],
                    hair_color=item['hair_color'],
                    height=item['height'],
                    homeworld=item['homeworld'],
                    mass=item['mass'],
                    skin_color=item['skin_color'],
                    species=item['species'],
                    starships=item['starships'],
                    vehicles=item['vehicles']
                )
                orm_objects.append(character)
            session.add_all(orm_objects)
            await session.commit()


async def main():
    await init_orm()

    async with aiohttp.ClientSession() as http_session:
        for chunk_i in chunked(range(1, 100), MAX_CHUNK):
            coros = [get_people(http_session, i) for i in chunk_i]
            result = await asyncio.gather(*coros)
            asyncio.create_task(insert_people(result))
    main_task = asyncio.current_task()
    tasks = asyncio.all_tasks()
    tasks.remove(main_task)
    await asyncio.gather(*tasks)
    await close_orm()


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
