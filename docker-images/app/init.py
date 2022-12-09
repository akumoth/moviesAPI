import pandas as pd
import numpy as np
import peewee as pw

# Inicializamos todos los dataframes iniciales

amazonDf = pd.read_csv("/app/Datasets/amazon_prime_titles.csv")
disneyDf = pd.read_csv("/app/Datasets/disney_plus_titles.csv")
huluDf = pd.read_csv("/app/Datasets/hulu_titles.csv")
netflixDf = pd.read_json("/app/Datasets/netflix_titles.json")

# Inicializamos la database

db = pw.SqliteDatabase("movies.db")

# Creamos las funciones necesarias


def concatStrings(loc, x, y):
    a = loc.split(", ")
    if (x or y) in a:
        a.append(x + " & " + y)
    if x in a:
        a.remove(x)
    if y in a:
        a.remove(y)
    return ", ".join(a)


def eitherStrings(loc, x, y, z):
    a = loc.split(", ")
    if (x or y) in a:
        a.append(z)
    if x in a:
        a.remove(x)
    if y in a:
        a.remove(y)
    return ", ".join(a)


# Soltamos todos los valores duplicados

amazonDf.drop_duplicates(inplace=True)
disneyDf.drop_duplicates(inplace=True)
huluDf.drop_duplicates(inplace=True)
netflixDf.drop_duplicates(inplace=True)

# Estandarizamos las tablas

# Empezando por limpiar generos, y igualarlos donde haga falta

# Amazon

amazonDf.listed_in = amazonDf.listed_in.str.replace("Arthouse, ", "")
amazonDf.listed_in = amazonDf.listed_in.str.replace("Arthouse", "")
amazonDf.listed_in = amazonDf.listed_in.str.replace("and Culture, ", "")
amazonDf.listed_in = amazonDf.listed_in.str.replace("and Culture", "")
amazonDf.listed_in = amazonDf.listed_in.str.replace(
    "Music Videos and Concerts", "Concert Film"
)
amazonDf.listed_in = amazonDf.listed_in.str.replace("Young Adult Audience", "Teen")
amazonDf.listed_in = amazonDf.listed_in.str.replace("and", "&")
amazonDf.listed_in = [
    concatStrings(loc, "Science Fiction", "Fantasy") for loc in amazonDf.listed_in
]
amazonDf.listed_in = [
    concatStrings(loc, "Action", "Adventure") for loc in amazonDf.listed_in
]
amazonDf.listed_in = amazonDf.listed_in.str.replace(r"^[\W_]+|[\W_]+$", "")
amazonDf.duration = amazonDf.duration.str.extract('(\d+)', expand=False).fillna(0)

# Disney

disneyDf.listed_in = disneyDf.listed_in.str.replace(
    "Action-Adventure", "Action & Adventure"
)
disneyDf.listed_in = disneyDf.listed_in.str.replace(
    "Animals & Nature", "Science & Nature"
)
disneyDf.listed_in = disneyDf.listed_in.str.replace("Soap Opera / Melodrama", "Drama")
disneyDf.listed_in = disneyDf.listed_in.str.replace("Docuseries", "Documentary")
disneyDf.listed_in = disneyDf.listed_in.str.replace(
    "Science Fiction", "Science Fiction & Fantasy"
)
disneyDf.listed_in = disneyDf.listed_in.str.replace(
    "Game Show / Competition", "Game Shows"
)
disneyDf.listed_in = [
    concatStrings(loc, "Music", "Musical") for loc in disneyDf.listed_in
]
disneyDf.listed_in = [
    concatStrings(loc, "Talk Show", "Variety") for loc in disneyDf.listed_in
]
disneyDf.listed_in = disneyDf.listed_in.str.replace(", Movies", "")
disneyDf.listed_in = disneyDf.listed_in.str.replace("Movies", "")
disneyDf.listed_in = disneyDf.listed_in.str.replace(", Series", "")
disneyDf.listed_in = disneyDf.listed_in.str.replace("Series", "")
disneyDf.listed_in = disneyDf.listed_in.str.replace("Police/Cop", "Crime")
disneyDf.listed_in = disneyDf.listed_in.str.replace(", Romantic Comedy", "")
disneyDf.listed_in = disneyDf.listed_in.str.replace(r"^[\W_]+|[\W_]+$", "")
disneyDf.duration = disneyDf.duration.str.extract('(\d+)', expand=False).fillna(0)

# Hulu

huluDf.listed_in = huluDf.listed_in.str.replace("History", "Historical")
huluDf.listed_in = huluDf.listed_in.str.replace("Documentaries", "Documentary")
huluDf.listed_in = huluDf.listed_in.str.replace("Music", "Music & Musical")
huluDf.listed_in = huluDf.listed_in.str.replace(
    "Science Fiction", "Science Fiction & Fantasy"
)
huluDf.listed_in = huluDf.listed_in.str.replace(
    "Science & Technology", "Science & Nature"
)
huluDf.listed_in = huluDf.listed_in.str.replace("Lifestyle & Culture", "Lifestyle")
huluDf.listed_in = [
    eitherStrings(loc, "Classics", "Cult", "Classic & Cult") for loc in huluDf.listed_in
]

huluDf.listed_in = [
    concatStrings(loc, "Action", "Adventure") for loc in huluDf.listed_in
]
huluDf.listed_in = [
    eitherStrings(loc, "Adult Animation", "Cartoons", "Animation")
    for loc in huluDf.listed_in
]
huluDf.listed_in = huluDf.listed_in.str.replace(r"^[\W_]+|[\W_]+$", "")
huluDf.listed_in = huluDf.listed_in.str.replace("  ", " ")
huluDf.listed_in = huluDf.listed_in.str.replace(" , ", ", ")
huluDf.listed_in = huluDf.listed_in.str.replace("LGBTQ+", "LGBTQ", regex=False)
huluDf.duration = huluDf.duration.str.extract('(\d+)', expand=False).fillna(0)

# Netflix

netflixDf.listed_in = netflixDf.listed_in.str.replace("Kids' TV", "Kids")
netflixDf.listed_in = netflixDf.listed_in.str.replace("Dramas", "Drama")
netflixDf.listed_in = netflixDf.listed_in.str.replace("TV Shows", "")
netflixDf.listed_in = netflixDf.listed_in.str.replace("TV", "")
netflixDf.listed_in = netflixDf.listed_in.str.replace("LGBTQ+", "LGBTQ", regex=False)
netflixDf.listed_in = netflixDf.listed_in.str.replace("Movies", "")
netflixDf.listed_in = netflixDf.listed_in.str.replace("Thrillers", "Thriller")
netflixDf.listed_in = netflixDf.listed_in.str.replace("Musicals", "Musical")
netflixDf.listed_in = netflixDf.listed_in.str.replace(
    "Stand-Up Comedy & Talk Shows", "Stand Up, Talk Show & Variety"
)
netflixDf.listed_in = netflixDf.listed_in.str.replace("Stand-Up Comedy", "Stand Up")
netflixDf.listed_in = netflixDf.listed_in.str.replace("Mysteries", "Mystery")
netflixDf.listed_in = netflixDf.listed_in.str.replace("Comedies", "Comedy")
netflixDf.listed_in = netflixDf.listed_in.str.replace("Children & Family", "Family")

netflixDf.listed_in = netflixDf.listed_in.str.replace(
    "Sci-Fi & Fantasy", "Science Fiction & Fantasy"
)
netflixDf.listed_in = [
    eitherStrings(loc, "Anime Features", "Anime Series", "Anime")
    for loc in netflixDf.listed_in
]
netflixDf.listed_in = [
    eitherStrings(loc, "Documentaries", "Docuseries", "Documentary")
    for loc in netflixDf.listed_in
]
netflixDf.listed_in = [
    eitherStrings(loc, "Horror Movies", "TV Horror", "Horror")
    for loc in netflixDf.listed_in
]
netflixDf.listed_in = netflixDf.listed_in.str.replace(r"^[\W_]+|[\W_]+$", "")
netflixDf.listed_in = netflixDf.listed_in.str.replace("  ", " ")
netflixDf.listed_in = netflixDf.listed_in.str.replace(" , ", ", ")
netflixDf.listed_in = [
    eitherStrings(loc, "Classic", "Cult", "Classic & Cult")
    for loc in netflixDf.listed_in
]
netflixDf.duration = netflixDf.duration.str.extract('(\d+)', expand=False).fillna(0)

# Creando un dataframe que contenga solo los generos

amazonGenres = amazonDf.listed_in.str.get_dummies(sep=", ").columns
netflixGenres = netflixDf.listed_in.str.get_dummies(sep=", ").columns
huluGenres = huluDf.listed_in.str.get_dummies(sep=", ").columns
disneyGenres = disneyDf.listed_in.str.get_dummies(sep=", ").columns

genresSeries = (
    amazonGenres.join(netflixGenres, how="outer")
    .join(huluGenres, how="outer")
    .join(disneyGenres, how="outer")
    .to_series()
)

genresDf = pd.DataFrame(genresSeries.reset_index(drop=True), columns=["name"])

# Creando la tabla principal que contiene las peliculas

amazonDf = amazonDf.assign(platform=0)
disneyDf = disneyDf.assign(platform=1)
huluDf = huluDf.assign(platform=2)
netflixDf = netflixDf.assign(platform=3)

moviesDf = pd.concat([amazonDf, disneyDf, huluDf, netflixDf], axis=0)
moviesDf.reset_index(inplace=True)


class BaseModel(pw.Model):
    class Meta:
        database = db


class Genre(BaseModel):
    name = pw.CharField()


class Actor(BaseModel):
    name = pw.CharField()


class Platform(BaseModel):
    name = pw.CharField()


class Movie(BaseModel):
    year = pw.IntegerField()
    title = pw.CharField()
    mtype = pw.CharField()
    description = pw.CharField()
    duration = pw.IntegerField()
    platform = pw.ForeignKeyField(Platform, backref="movies")


class MovieGenre(BaseModel):
    movie = pw.ForeignKeyField(Movie)
    genre = pw.ForeignKeyField(Genre)


class Cast(BaseModel):
    movie = pw.ForeignKeyField(Movie)
    actor = pw.ForeignKeyField(Actor)


db.connect()


db.create_tables([Genre, Actor, Platform, Movie, MovieGenre, Cast])

genresSeries = (
    amazonGenres.join(netflixGenres, how="outer")
    .join(huluGenres, how="outer")
    .join(disneyGenres, how="outer")
    .to_series()
)
# Crear tabla de generos
Genre.delete().execute()
genresList = [dict(id=i, name=genresDf.name[i]) for i in genresDf.index]
Genre.insert_many(genresList).execute()

Actor.delete().execute()
def to_list(x):
    return [i.strip() for i in x.split(",")]

moviesDf.cast = moviesDf.cast.dropna().apply(lambda x: [i.strip() for i in x.split(",")])
actors = moviesDf.cast.explode()
actors = actors.unique()
actorsDF = pd.DataFrame({"name": actors})
actorsDF["id"] = actorsDF.index
Actor.insert_many(actorsDF.to_dict(orient="records")).execute()

Platform.delete().execute()
platformsList = [
    {"id": 0, "name": "Amazon Prime Video"},
    {"id": 1, "name": "Disney Plus"},
    {"id": 2, "name": "Hulu TV"},
    {"id": 3, "name": "Netflix"},
]
Platform.insert_many(platformsList).execute()

Movie.delete().execute()
moviesDB = moviesDf.loc[:, ["release_year", "title", "type", "description", "duration", "platform"]]
moviesDB.columns = ["year", "title", "mtype", "description", "duration", "platform_id"]
moviesDB.year = moviesDB.year.astype(int)
moviesDB["id"] = moviesDB.index
Movie.insert_many(moviesDB.to_dict(orient="records")).execute()

def get_actor_id(x):
    return actorsDF.index[actorsDF.name == x]

Cast.delete().execute()
moviesDf["movie_id"] = moviesDf.index
castDF = moviesDf.loc[:, ["title", "cast", "movie_id"]].dropna()
castDF = castDF.explode("cast")
castDF.columns = ["title", "actor", "movie_id"]
castDF = castDF.merge(actorsDF, left_on="actor", right_on="name", how="left")
castDF = castDF.loc[:, ["movie_id", "id"]].rename(columns={"id": "actor_id"})

Cast.insert_many(castDF.to_dict(orient="records")).execute()

MovieGenre.delete().execute()
genresDf["genre_id"] = genresDf.index
movieGenreDf = moviesDf[["title", "movie_id", "listed_in"]].dropna()
movieGenreDf.listed_in = movieGenreDf.listed_in.apply(lambda x: [i.strip() for i in x.split(",")])
movieGenreDf = movieGenreDf.explode("listed_in")
movieGenreDf.columns = ["title", "movie_id", "genre"]
movieGenreDf = movieGenreDf.merge(genresDf, left_on="genre", right_on="name", how="left")
movieGenreDf = movieGenreDf.loc[:, ["movie_id", "genre_id"]]    
movieGenreDf = movieGenreDf.dropna()
MovieGenre.insert_many(movieGenreDf.to_dict(orient="records")).execute()
