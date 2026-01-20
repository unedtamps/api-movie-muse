import asyncio
from logging import debug

import aiohttp
from flasgger import Swagger
from flask import Flask, jsonify, request
from flask_cors import CORS
from src.film import get_film_by_id
from src.get_list import get_list as fetch_list
from src.recomender import (
    cache,
    cache_slow,
    get_ranked_by_seeds_cached,
    get_ranked_cached,
)
from src.search import get_film_by_name
from src.users import get_user_diary_page

app = Flask(__name__)
swagger = Swagger(app)
cors = CORS(app)
# app.config["CORS_HEADERS"] = "Content-Type"
cache.init_app(app)
cache_slow.init_app(app)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
}


@app.route("/film/<string:id>", methods=["GET"])
async def get_film(id):
    """
    Get film details by ID
    ---
    tags:
      - Film
    parameters:
      - name: id
        in: path
        type: string
        required: true
        description: The ID of the film
    responses:
      200:
        description: Film data retrieved successfully
    """
    key = f"film:{id}"
    if cache_slow.get(key):
        data = cache_slow.get(key)
        return jsonify(data)
    data = await get_film_by_id(f"/film/{id}")
    cache_slow.set(key, data)
    return jsonify(data)


@app.route("/diary/<string:user_id>", methods=["GET"])
async def get_dialy_user(user_id):
    """
    Get user diary entries
    ---
    tags:
      - Users
    parameters:
      - name: user_id
        in: path
        type: string
        required: true
      - name: page
        in: query
        type: integer
        default: 1
        description: Page number for pagination
    responses:
      200:
        description: User diary data
    """
    page = request.args.get("page", default=1, type=int)
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        data = await get_user_diary_page(session, user_id, page)
    return jsonify(data)


@app.route("/recommend/personalize/<string:user_id>", methods=["GET"])
async def get_recommend_user(user_id):
    """
    Get personalized recommendations for a user
    ---
    tags:
      - Recommendations
    parameters:
      - name: user_id
        in: path
        type: string
        required: true
      - name: k
        in: query
        type: integer
        default: 1
        description: Number of recommendations to return
    responses:
      200:
        description: List of recommended films
    """
    k = request.args.get("k", default=1, type=int)

    data = await get_ranked_cached(user_id, k)
    return jsonify(data)


@app.route("/recommend/seed", methods=["POST"])
async def get_recommend_seed():
    """
    POST recommendations based on seed films
    ---
    tags:
      - Recommendations
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          properties:
            seed_film_ids:
              type: array
              items:
                type: string
              example: ["film_1", "film_2"]
            k:
              type: integer
              default: 1
    responses:
      200:
        description: List of recommended films based on seeds
    """
    body = request.get_json()
    seed_film_ids = body.get("seed_film_ids", [])
    k = body.get("k", 1)

    data = await get_ranked_by_seeds_cached(seed_film_ids, k)
    return jsonify(data)


@app.route("/get_list", methods=["GET"])
async def get_list():
    """
    Fetch a list from Letterboxd
    ---
    tags:
      - Lists
    parameters:
      - name: list_url
        in: query
        type: string
        default: "official-top-250-narrative-feature-films"
        description: The Letterboxd list slug or full URL
    responses:
      200:
        description: The fetched list data
    """
    list_id = request.args.get(
        "list_url", default="official-top-250-narrative-feature-films", type=str
    )
    if list_id.startswith("https://letterboxd.com"):
        list_id = list_id.replace("https://letterboxd.com", "")
        list_id = list_id.split("/")[1:4]
    else:
        list_id = list_id.split("/")[0:3]

    list_id = "/".join(list_id)

    data = await fetch_list(f"https://letterboxd.com/{list_id}")
    return jsonify(data)


@app.route("/search", methods=["GET"])
async def search_films():
    """
    Search for films by name
    ---
    tags:
      - search
    parameters:
      - name: query
        in: query
        type: string
        required: true
        description: The search query for the film name
    responses:
      200:
        description: List of films matching the search query
    """
    query = request.args.get("query", default="", type=str)
    if not query:
        return jsonify([])

    key = f"search:{query}"

    if cache_slow.get(key):
        data = cache_slow.get(key)
        return jsonify(data)

    data = await get_film_by_name(query)
    cache_slow.set(key, data)
    return jsonify(data)


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5000)
