import sys

from fastapi import FastAPI
import peewee as pw
import playhouse.reflection as pl

version = f"{sys.version_info.major}.{sys.version_info.minor}"

app = FastAPI()

db = pw.SqliteDatabase("movies.db")
db.connect()
models = pl.generate_models(db)


@app.get("/")
async def read_root():
    message = f"Hello world! From FastAPI running on Uvicorn with Gunicorn. Using Python {version}"
    return {"message": message}


@app.get("/movieAPI_v1/get_count_platform/{platform}")
async def get_count_platform(platform):
    movie_by_platform = (
        models["movie"]
        .select(models["platform"].name, pw.fn.COUNT(models["movie"].id))
        .join(models["platform"])
        .group_by(models["platform"].name)
        .dicts()
    )
    print(platform)
    movie_by_platform = {tuple(i.values())[0]:tuple(i.values())[1] for i in movie_by_platform}
    return {platform : movie_by_platform[platform]}