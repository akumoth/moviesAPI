import sys

from fastapi import FastAPI, HTTPException, Response
import peewee as pw
import playhouse.reflection as pl
import pandas as pd
import json

version = f"{sys.version_info.major}.{sys.version_info.minor}"

app = FastAPI()

db = pw.SqliteDatabase("movies.db")
db.connect()
models = pl.generate_models(db)


@app.get("/")
async def read_root():
    message = f"Hello world! From FastAPI running on Uvicorn with Gunicorn. Using Python {version}"
    return {"message": message}


@app.get("/movieAPI_v1/get_listed_in/")
async def get_listed_in(genre):
    genres = models["genre"].select
    movie_by_genre = (
        models["moviegenre"]
        .select(
            models["genre"].name,
            models["platform"].name.alias("platform"),
            pw.fn.COUNT(models["movie"].id).alias("count"),
        )
        .where(models["genre"].name == genre)
        .join(models["genre"], on=(models["moviegenre"].genre_id == models["genre"].id))
        .join(models["movie"], on=(models["moviegenre"].movie_id == models["movie"].id))
        .join(models["platform"])
        .group_by(models["genre"].name, models["platform"].name)
        .order_by(pw.fn.COUNT(models["movie"].id).desc())
        .dicts()
    )
    if not movie_by_genre.exists():
        raise HTTPException(status_code=404, detail="Item not found")
    return movie_by_genre[0]


@app.get("/movieAPI_v1/get_count_platform/")
async def get_count_platform(platform):
    movie_by_platform = (
        models["movie"]
        .select(models["platform"].name, pw.fn.COUNT(models["movie"].id))
        .join(models["platform"])
        .group_by(models["platform"].name)
        .dicts()
    )
    movie_by_platform = {
        tuple(i.values())[0]: tuple(i.values())[1] for i in movie_by_platform
    }
    if platform not in movie_by_platform:
        raise HTTPException(status_code=404, detail="Item not found")
    return {platform: movie_by_platform[platform]}


@app.get("/movieAPI_v1/get_max_duration/")
async def get_max_duration(year, platform, mtype):
    movie_by_genre = (
        models["movie"]
        .select(
            models["movie"].year,
            models["movie"].title,
            models["platform"].name.alias("platform"),
            models["movie"].mtype,
            models["movie"].duration,
        )
        .where(
            (models["movie"].year == year)
            & (models["movie"].mtype == mtype)
            & (models["platform"].name == platform)
        )
        .join(models["platform"])
        .order_by(models["movie"].duration.desc())
        .dicts()
    )
    if not movie_by_genre.exists():
        raise HTTPException(status_code=404, detail="Item not found")
    return movie_by_genre[0]


@app.get("/movieAPI_v1/get_actor/")
async def get_actor(year, platform):
    actor_by_platform = (
        models["cast"]
        .select(
            models["actor"].name.alias("actor"),
            models["platform"].name.alias("platform"),
            pw.fn.COUNT(models["cast"].movie_id).alias("count")
        )
        .where((models["movie"].year == year) & (models["platform"].name == platform))
        .join(models["actor"], on=(models["cast"].actor_id == models["actor"].id))
        .join(models["movie"], on=(models["cast"].movie_id == models["movie"].id))
        .join(models["platform"])
        .group_by(models["cast"].actor_id)
        .order_by(pw.fn.COUNT(models["cast"].movie_id).desc())
        .dicts()
    )
    if not actor_by_platform.exists():
        raise HTTPException(status_code=404, detail="Item not found")
    return actor_by_platform[0]
