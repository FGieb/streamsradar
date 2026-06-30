"""
StreamsRadar - Proof of Concept
Find where movies/series stream across countries, with ratings.
"""

import os
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import httpx
import json

load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
OMDB_API_KEY = os.getenv("OMDB_API_KEY", "")

TMDB_BASE = "https://api.themoviedb.org/3"
OMDB_BASE = "https://www.omdbapi.com"
TMDB_IMG = "https://image.tmdb.org/t/p"

app = FastAPI(title="StreamsRadar")

# Country code to flag emoji + name mapping
COUNTRIES = {
    "AR": ("🇦🇷", "Argentina"), "AT": ("🇦🇹", "Austria"), "AU": ("🇦🇺", "Australia"),
    "BE": ("🇧🇪", "Belgium"), "BR": ("🇧🇷", "Brazil"), "CA": ("🇨🇦", "Canada"),
    "CH": ("🇨🇭", "Switzerland"), "CL": ("🇨🇱", "Chile"), "CO": ("🇨🇴", "Colombia"),
    "CZ": ("🇨🇿", "Czechia"), "DE": ("🇩🇪", "Germany"), "DK": ("🇩🇰", "Denmark"),
    "EC": ("🇪🇨", "Ecuador"), "EE": ("🇪🇪", "Estonia"), "ES": ("🇪🇸", "Spain"),
    "FI": ("🇫🇮", "Finland"), "FR": ("🇫🇷", "France"), "GB": ("🇬🇧", "United Kingdom"),
    "GR": ("🇬🇷", "Greece"), "HK": ("🇭🇰", "Hong Kong"), "HR": ("🇭🇷", "Croatia"),
    "HU": ("🇭🇺", "Hungary"), "ID": ("🇮🇩", "Indonesia"), "IE": ("🇮🇪", "Ireland"),
    "IL": ("🇮🇱", "Israel"), "IN": ("🇮🇳", "India"), "IT": ("🇮🇹", "Italy"),
    "JP": ("🇯🇵", "Japan"), "KR": ("🇰🇷", "South Korea"), "LT": ("🇱🇹", "Lithuania"),
    "LV": ("🇱🇻", "Latvia"), "MX": ("🇲🇽", "Mexico"), "MY": ("🇲🇾", "Malaysia"),
    "NL": ("🇳🇱", "Netherlands"), "NO": ("🇳🇴", "Norway"), "NZ": ("🇳🇿", "New Zealand"),
    "PE": ("🇵🇪", "Peru"), "PH": ("🇵🇭", "Philippines"), "PL": ("🇵🇱", "Poland"),
    "PT": ("🇵🇹", "Portugal"), "RO": ("🇷🇴", "Romania"), "RS": ("🇷🇸", "Serbia"),
    "RU": ("🇷🇺", "Russia"), "SA": ("🇸🇦", "Saudi Arabia"), "SE": ("🇸🇪", "Sweden"),
    "SG": ("🇸🇬", "Singapore"), "SK": ("🇸🇰", "Slovakia"), "TH": ("🇹🇭", "Thailand"),
    "TR": ("🇹🇷", "Turkey"), "TW": ("🇹🇼", "Taiwan"), "US": ("🇺🇸", "United States"),
    "VE": ("🇻🇪", "Venezuela"), "ZA": ("🇿🇦", "South Africa"),
}


async def search_tmdb(query: str, media_type: str = "multi") -> list:
    """Search TMDB for movies or TV shows."""
    async with httpx.AsyncClient() as client:
        if media_type == "multi":
            resp = await client.get(
                f"{TMDB_BASE}/search/multi",
                params={"api_key": TMDB_API_KEY, "query": query, "language": "en-US"},
            )
        else:
            resp = await client.get(
                f"{TMDB_BASE}/search/{media_type}",
                params={"api_key": TMDB_API_KEY, "query": query, "language": "en-US"},
            )
        data = resp.json()
        # Filter to only movies and TV shows
        results = []
        for item in data.get("results", []):
            mt = item.get("media_type", media_type)
            if mt in ("movie", "tv"):
                results.append(item)
        return results[:10]


async def get_watch_providers(tmdb_id: int, media_type: str) -> dict:
    """Get watch providers for all countries from TMDB."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{TMDB_BASE}/{media_type}/{tmdb_id}/watch/providers",
            params={"api_key": TMDB_API_KEY},
        )
        data = resp.json()
        return data.get("results", {})


async def get_tmdb_details(tmdb_id: int, media_type: str) -> dict:
    """Get detailed info from TMDB including external IDs."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{TMDB_BASE}/{media_type}/{tmdb_id}",
            params={"api_key": TMDB_API_KEY, "append_to_response": "external_ids"},
        )
        return resp.json()


async def get_omdb_ratings(imdb_id: str) -> dict:
    """Get ratings from OMDb (IMDb + Rotten Tomatoes)."""
    if not imdb_id or not OMDB_API_KEY:
        return {}
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            OMDB_BASE,
            params={"apikey": OMDB_API_KEY, "i": imdb_id},
        )
        data = resp.json()
        if data.get("Response") == "True":
            ratings = {}
            ratings["imdb_rating"] = data.get("imdbRating", "N/A")
            ratings["imdb_votes"] = data.get("imdbVotes", "N/A")
            # Extract Rotten Tomatoes from Ratings array
            for r in data.get("Ratings", []):
                if r["Source"] == "Rotten Tomatoes":
                    ratings["rotten_tomatoes"] = r["Value"]
                if r["Source"] == "Metacritic":
                    ratings["metacritic"] = r["Value"]
            return ratings
    return {}


def organise_providers(raw_providers: dict) -> dict:
    """Organise provider data by platform across countries."""
    # Group by provider name, then list countries
    platforms = {}  # provider_name -> {logo, type -> [countries]}

    for country_code, country_data in raw_providers.items():
        if country_code not in COUNTRIES:
            continue

        flag, country_name = COUNTRIES[country_code]

        for access_type in ["flatrate", "free", "ads"]:
            for provider in country_data.get(access_type, []):
                pname = provider["provider_name"]
                if pname not in platforms:
                    platforms[pname] = {
                        "logo": f"{TMDB_IMG}/original{provider['logo_path']}",
                        "countries": {},
                        "provider_id": provider["provider_id"],
                    }
                if access_type not in platforms[pname]["countries"]:
                    platforms[pname]["countries"][access_type] = []
                platforms[pname]["countries"][access_type].append({
                    "code": country_code,
                    "flag": flag,
                    "name": country_name,
                })

    # Also collect rent/buy separately
    rent_buy = {}
    for country_code, country_data in raw_providers.items():
        if country_code not in COUNTRIES:
            continue
        flag, country_name = COUNTRIES[country_code]
        for access_type in ["rent", "buy"]:
            for provider in country_data.get(access_type, []):
                pname = provider["provider_name"]
                if pname not in rent_buy:
                    rent_buy[pname] = {
                        "logo": f"{TMDB_IMG}/original{provider['logo_path']}",
                        "countries": {},
                    }
                if access_type not in rent_buy[pname]["countries"]:
                    rent_buy[pname]["countries"][access_type] = []
                rent_buy[pname]["countries"][access_type].append({
                    "code": country_code,
                    "flag": flag,
                    "name": country_name,
                })

    # Sort platforms by number of countries (most available first)
    sorted_platforms = dict(
        sorted(
            platforms.items(),
            key=lambda x: sum(len(v) for v in x[1]["countries"].values()),
            reverse=True,
        )
    )

    return {"streaming": sorted_platforms, "rent_buy": rent_buy}


# ─── API Endpoints ───────────────────────────────────────────────

@app.get("/api/search")
async def api_search(q: str = Query(..., min_length=1)):
    """Search for movies/TV shows."""
    results = await search_tmdb(q)
    cleaned = []
    for item in results:
        mt = item.get("media_type", "movie")
        title = item.get("title") or item.get("name", "Unknown")
        year = (item.get("release_date") or item.get("first_air_date") or "")[:4]
        cleaned.append({
            "id": item["id"],
            "title": title,
            "year": year,
            "media_type": mt,
            "poster": f"{TMDB_IMG}/w200{item['poster_path']}" if item.get("poster_path") else None,
            "overview": (item.get("overview") or "")[:200],
        })
    return cleaned


@app.get("/api/details/{media_type}/{tmdb_id}")
async def api_details(media_type: str, tmdb_id: int):
    """Get full details: providers by country + ratings."""
    # Fetch TMDB details, providers, and ratings in parallel
    details = await get_tmdb_details(tmdb_id, media_type)
    raw_providers = await get_watch_providers(tmdb_id, media_type)

    # Get IMDb ID for OMDb lookup
    imdb_id = None
    if "external_ids" in details:
        imdb_id = details["external_ids"].get("imdb_id")
    elif "imdb_id" in details:
        imdb_id = details["imdb_id"]

    ratings = await get_omdb_ratings(imdb_id) if imdb_id else {}

    # Organise providers
    providers = organise_providers(raw_providers)

    title = details.get("title") or details.get("name", "Unknown")
    year = (details.get("release_date") or details.get("first_air_date") or "")[:4]

    return {
        "title": title,
        "year": year,
        "media_type": media_type,
        "overview": details.get("overview", ""),
        "poster": f"{TMDB_IMG}/w300{details['poster_path']}" if details.get("poster_path") else None,
        "backdrop": f"{TMDB_IMG}/w1280{details['backdrop_path']}" if details.get("backdrop_path") else None,
        "imdb_id": imdb_id,
        "ratings": ratings,
        "providers": providers,
        "total_countries_available": len(raw_providers),
    }


# ─── Frontend ────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def homepage():
    """Serve the main page."""
    with open("templates/index.html", "r") as f:
        return f.read()
