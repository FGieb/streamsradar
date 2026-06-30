"""
StreamsRadar - Proof of Concept
Find where movies/series stream across countries, with ratings.
Multi-source verification for accuracy.
"""

import os
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import httpx
import json
import asyncio

load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
OMDB_API_KEY = os.getenv("OMDB_API_KEY", "")
RAPID_API_KEY = os.getenv("RAPID_API_KEY", "")

TMDB_BASE = "https://api.themoviedb.org/3"
OMDB_BASE = "https://www.omdbapi.com"
TMDB_IMG = "https://image.tmdb.org/t/p"
STREAMING_AVAIL_BASE = "https://streaming-availability.p.rapidapi.com"

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

# ─── Community verification store (in-memory for now, DB later) ──
# Structure: { "tmdb_id:media_type:country:provider" -> {"yes": N, "no": N} }
community_votes = {}


# ─── TMDB Functions ──────────────────────────────────────────────

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


# ─── Streaming Availability API (2nd source) ─────────────────────

async def get_streaming_availability(imdb_id: str) -> dict:
    """Get streaming data from Streaming Availability API (RapidAPI).
    Returns: { country_code: { provider_name: [access_types] } }
    """
    if not RAPID_API_KEY or not imdb_id:
        return {}

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{STREAMING_AVAIL_BASE}/shows/{imdb_id}",
                headers={
                    "X-RapidAPI-Key": RAPID_API_KEY,
                    "X-RapidAPI-Host": "streaming-availability.p.rapidapi.com",
                },
            )
            if resp.status_code != 200:
                return {}

            data = resp.json()

            # Parse the response into a simple structure
            # { "BE": {"Netflix": ["subscription"], "Disney+": ["subscription"]}, ... }
            result = {}
            streaming_options = data.get("streamingOptions", {})

            for country_code, options in streaming_options.items():
                country_upper = country_code.upper()
                if country_upper not in COUNTRIES:
                    continue
                if country_upper not in result:
                    result[country_upper] = {}

                for option in options:
                    service_name = option.get("service", {}).get("name", "Unknown")
                    access_type = option.get("type", "unknown")
                    if service_name not in result[country_upper]:
                        result[country_upper][service_name] = []
                    if access_type not in result[country_upper][service_name]:
                        result[country_upper][service_name].append(access_type)

            return result
    except Exception as e:
        print(f"Streaming Availability API error: {e}")
        return {}


# ─── OMDb Ratings ────────────────────────────────────────────────

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
            for r in data.get("Ratings", []):
                if r["Source"] == "Rotten Tomatoes":
                    ratings["rotten_tomatoes"] = r["Value"]
                if r["Source"] == "Metacritic":
                    ratings["metacritic"] = r["Value"]
            return ratings
    return {}


# ─── Confidence Scoring ─────────────────────────────────────────

def calculate_confidence(
    tmdb_says: bool,
    sa_says: bool | None,  # None = no data from this source
    community_yes: int = 0,
    community_no: int = 0,
) -> dict:
    """Calculate confidence score based on multiple sources.

    Returns: { "score": 0-100, "level": "high"|"medium"|"low"|"conflict", "sources": {...} }
    """
    sources_agree = 0
    sources_disagree = 0
    sources_total = 0

    # TMDB source
    sources_total += 1
    if tmdb_says:
        sources_agree += 1

    # Streaming Availability source
    if sa_says is not None:
        sources_total += 1
        if sa_says:
            sources_agree += 1
        else:
            sources_disagree += 1

    # Community votes
    total_votes = community_yes + community_no
    if total_votes > 0:
        sources_total += 1
        if community_yes > community_no:
            sources_agree += 1
        elif community_no > community_yes:
            sources_disagree += 1

    # Calculate score
    if sources_total == 0:
        score = 50
    else:
        base_score = (sources_agree / sources_total) * 100

        # Bonus for community verification
        if total_votes >= 3:
            community_ratio = community_yes / total_votes
            base_score = base_score * 0.7 + community_ratio * 100 * 0.3

        score = round(base_score)

    # Determine level
    if sources_disagree > 0 and sources_agree > 0:
        level = "conflict"
    elif score >= 80:
        level = "high"
    elif score >= 50:
        level = "medium"
    else:
        level = "low"

    return {
        "score": score,
        "level": level,
        "sources_checked": sources_total,
        "sources_agree": sources_agree,
        "tmdb": tmdb_says,
        "streaming_availability": sa_says,
        "community_yes": community_yes,
        "community_no": community_no,
    }


# ─── Organise & Merge Providers ──────────────────────────────────

def normalise_provider_name(name: str) -> str:
    """Normalise provider names for matching across sources."""
    # Different APIs use slightly different names
    mappings = {
        "netflix": "Netflix",
        "amazon prime video": "Amazon Prime Video",
        "prime video": "Amazon Prime Video",
        "disney plus": "Disney+",
        "disney+": "Disney+",
        "hbo max": "HBO Max",
        "max": "HBO Max",
        "apple tv plus": "Apple TV+",
        "apple tv+": "Apple TV+",
        "paramount plus": "Paramount+",
        "paramount+": "Paramount+",
    }
    return mappings.get(name.lower().strip(), name)


def organise_providers_with_confidence(
    tmdb_providers: dict,
    sa_data: dict,
) -> dict:
    """Organise provider data with confidence scoring from multiple sources."""
    platforms = {}

    for country_code, country_data in tmdb_providers.items():
        if country_code not in COUNTRIES:
            continue

        flag, country_name = COUNTRIES[country_code]

        for access_type in ["flatrate", "free", "ads"]:
            for provider in country_data.get(access_type, []):
                pname = normalise_provider_name(provider["provider_name"])
                raw_pname = provider["provider_name"]

                if pname not in platforms:
                    platforms[pname] = {
                        "logo": f"{TMDB_IMG}/original{provider['logo_path']}",
                        "countries": {},
                        "provider_id": provider["provider_id"],
                    }
                if access_type not in platforms[pname]["countries"]:
                    platforms[pname]["countries"][access_type] = []

                # Check if Streaming Availability API agrees
                sa_confirms = None  # None = no data
                if sa_data:
                    sa_country = sa_data.get(country_code, {})
                    # Check if any provider name in SA data matches
                    sa_confirms = False
                    for sa_provider_name in sa_country:
                        if normalise_provider_name(sa_provider_name) == pname:
                            sa_confirms = True
                            break

                # Get community votes
                vote_key = f"{country_code}:{pname}"
                votes = community_votes.get(vote_key, {"yes": 0, "no": 0})

                confidence = calculate_confidence(
                    tmdb_says=True,
                    sa_says=sa_confirms,
                    community_yes=votes["yes"],
                    community_no=votes["no"],
                )

                platforms[pname]["countries"][access_type].append({
                    "code": country_code,
                    "flag": flag,
                    "name": country_name,
                    "confidence": confidence,
                })

    # Check for entries in SA data that TMDB doesn't have
    if sa_data:
        for country_code, sa_providers in sa_data.items():
            if country_code not in COUNTRIES:
                continue
            flag, country_name = COUNTRIES[country_code]

            for sa_pname, access_types in sa_providers.items():
                norm_name = normalise_provider_name(sa_pname)

                # Check if this country+provider combo already exists from TMDB
                already_listed = False
                if norm_name in platforms:
                    for at, countries in platforms[norm_name]["countries"].items():
                        for c in countries:
                            if c["code"] == country_code:
                                already_listed = True
                                break

                if not already_listed and any(t in ["subscription", "free", "addon"] for t in access_types):
                    # SA has it but TMDB doesn't — add with lower confidence
                    if norm_name not in platforms:
                        platforms[norm_name] = {
                            "logo": "",
                            "countries": {},
                            "provider_id": 0,
                        }
                    at = "flatrate"
                    if at not in platforms[norm_name]["countries"]:
                        platforms[norm_name]["countries"][at] = []

                    vote_key = f"{country_code}:{norm_name}"
                    votes = community_votes.get(vote_key, {"yes": 0, "no": 0})

                    confidence = calculate_confidence(
                        tmdb_says=False,
                        sa_says=True,
                        community_yes=votes["yes"],
                        community_no=votes["no"],
                    )

                    platforms[norm_name]["countries"][at].append({
                        "code": country_code,
                        "flag": flag,
                        "name": country_name,
                        "confidence": confidence,
                    })

    # Collect rent/buy (keep simple — no cross-referencing needed)
    rent_buy = {}
    for country_code, country_data in tmdb_providers.items():
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
    """Get full details: providers by country + ratings, with confidence scoring."""
    # Fetch TMDB details first to get IMDb ID
    details = await get_tmdb_details(tmdb_id, media_type)

    # Get IMDb ID for OMDb + Streaming Availability lookups
    imdb_id = None
    if "external_ids" in details:
        imdb_id = details["external_ids"].get("imdb_id")
    elif "imdb_id" in details:
        imdb_id = details["imdb_id"]

    # Fetch providers, ratings, and streaming availability in parallel
    tmdb_providers_task = get_watch_providers(tmdb_id, media_type)
    ratings_task = get_omdb_ratings(imdb_id) if imdb_id else asyncio.coroutine(lambda: {})()
    sa_task = get_streaming_availability(imdb_id) if imdb_id else asyncio.coroutine(lambda: {})()

    tmdb_providers, ratings, sa_data = await asyncio.gather(
        tmdb_providers_task,
        ratings_task,
        sa_task,
    )

    # Organise providers with confidence from both sources
    providers = organise_providers_with_confidence(tmdb_providers, sa_data)

    title = details.get("title") or details.get("name", "Unknown")
    year = (details.get("release_date") or details.get("first_air_date") or "")[:4]

    # Count sources used
    sources_used = ["TMDB"]
    if sa_data:
        sources_used.append("Streaming Availability")

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
        "total_countries_available": len(tmdb_providers),
        "sources_used": sources_used,
    }


@app.post("/api/vote")
async def api_vote(country: str, provider: str, available: bool):
    """Community verification vote."""
    vote_key = f"{country}:{provider}"
    if vote_key not in community_votes:
        community_votes[vote_key] = {"yes": 0, "no": 0}

    if available:
        community_votes[vote_key]["yes"] += 1
    else:
        community_votes[vote_key]["no"] += 1

    return {"status": "ok", "votes": community_votes[vote_key]}


# ─── Frontend ────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def homepage():
    """Serve the main page."""
    with open("templates/index.html", "r") as f:
        return f.read()
