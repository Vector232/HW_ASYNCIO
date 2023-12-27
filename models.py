import os

from sqlalchemy import String
from sqlalchemy.future import select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs

import pprint

POSTGRES_USER = os.getenv('POSTGRES_USER', 'user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'secret')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'SWDB')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', '127.0.0.1')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5431')

PG_DSN = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


engine = create_async_engine(PG_DSN)
Session = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase, AsyncAttrs):
    pass

class People(Base):
    __tablename__ = 'persons'

    id: Mapped[int] = mapped_column(primary_key=True)
    birth_year: Mapped[str] = mapped_column(String(20))
    eye_color: Mapped[str] = mapped_column(String(20)) 
    films: Mapped[str] = mapped_column(String(2000))
    gender: Mapped[str] = mapped_column(String(20))
    hair_color: Mapped[str] = mapped_column(String(20))
    height: Mapped[str] = mapped_column(String(20))
    homeworld: Mapped[str] = mapped_column(String(200))
    mass: Mapped[str] = mapped_column(String(20))
    name: Mapped[str] = mapped_column(String(30))
    skin_color: Mapped[str] = mapped_column(String(20))
    species: Mapped[str] = mapped_column(String(2000))
    starships: Mapped[str] = mapped_column(String(2000))
    vehicles: Mapped[str] = mapped_column(String(2000))


async def get_data():
    async with Session() as session:
        q = select(People)
        result = await session.execute(q)
        curr = result.scalars()
        data = {i.id: {
            "name": i.name,
            "birth_year": i.birth_year,
            "eye_color": i.eye_color,
            "films": i.films,
            "gender": i.gender,
            "hair_color": i.hair_color,
            "height": i.height,
            "homeworld": i.homeworld,
            "mass": i.mass,
            "skin_color": i.skin_color,
            "species": i.species,
            "starships": i.starships,
            "vehicles": i.vehicles,
        } for i in curr}

    
        pprint.pprint(data)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)