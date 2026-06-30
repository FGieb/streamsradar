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
UNOGS_BASE = "https://unogsng.p.rapidapi.com"

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

# uNoGS country IDs mapping (uNoGS uses numeric IDs)
UNOGS_COUNTRY_IDS = {
    "AR": 21, "AU": 23, "BE": 26, "BR": 29, "CA": 33,
    "CZ": 307, "FR": 45, "DE": 39, "GR": 327, "HK": 331,
    "HU": 334, "IS": 265, "IN": 337, "IE": 270, "IL": 336,
    "IT": 269, "JP": 267, "KR": 348, "LT": 357, "MX": 65,
    "NL": 67, "NZ": 392, "PL": 392, "PT": 268, "RO": 400,
    "SG": 408, "SK": 412, "ZA": 447, "ES": 270, "SE": 73,
    "CH": 34, "TH": 425, "TR": 432, "GB": 46, "US": 78,
    "CO": 36, "EE": 309, "FI": 310, "HR": 321, "ID": 338,
    "LV": 354, "MY": 378, "NO": 379, "PE": 391, "PH": 390,
    "RS": 401, "SA": 307, "TW": 429, "VE": 445,
}

# ─── Community verification store (in-memory for now, DB later) ──
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
                print(f"Streaming Availability API returned {resp.status_code}")
                return {}

            data = resp.json()
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


# ─── uNoGS API (3rd source — Netflix-specific) ──────────────────

async def get_unogs_netflix_countries(title: str, imdb_id: str = None) -> set:
    """Get Netflix country availability from uNoGS (independent of JustWatch).
    Returns: set of country codes where title is on Netflix, e.g. {"US", "GB", "NL"}
    """
    if not RAPID_API_KEY:
        return set()

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Search by title on uNoGS
            resp = await client.get(
                f"{UNOGS_BASE}/search",
                params={
                    "query": title,
                    "limit": "5",
                },
                headers={
                    "X-RapidAPI-Key": RAPID_API_KEY,
                    "X-RapidAPI-Host": "unogsng.p.rapidapi.com",
                },
            )

            if resp.status_code != 200:
                print(f"uNoGS search returned {resp.status_code}: {resp.text[:200]}")
                return set()

            data = resp.json()
            results = data.get("results", [])

            if not results:
                return set()

            # Find matching title — prefer IMDB ID match
            netflix_id = None
            for item in results:
                if imdb_id and item.get("imdbid") == imdb_id:
                    netflix_id = item.get("nfid") or item.get("netflix_id") or item.get("id")
                    break

            # Fallback to first result if no IMDB match
            if not netflix_id and results:
                netflix_id = results[0].get("nfid") or results[0].get("netflix_id") or results[0].get("id")

            if not netflix_id:
                return set()

            # Get country availability for this Netflix title
            country_resp = await client.get(
                f"{UNOGS_BASE}/title/countries",
                params={"netflix_id": netflix_id},
                headers={
                    "X-RapidAPI-Key": RAPID_API_KEY,
                    "X-RapidAPI-Host": "unogsng.p.rapidapi.com",
                },
            )

            if country_resp.status_code != 200:
                print(f"uNoGS countries returned {country_resp.status_code}: {country_resp.text[:200]}")
                return set()

            country_data = country_resp.json()

            # Extract country codes
            countries = set()
            # uNoGS returns country list in various formats depending on version
            items = country_data if isinstance(country_data, list) else country_data.get("results", country_data.get("countries", []))

            for item in items:
                if isinstance(item, dict):
                    # Could be {"country_code": "BE", ...} or {"cc": "BE", ...} or {"id": 26, ...}
                    cc = item.get("country_code") or item.get("cc") or item.get("countrycode", "")
                    cc = cc.upper().strip()
                    if cc and cc in COUNTRIES:
                        countries.add(cc)
                elif isinstance(item, str):
                    cc = item.upper().strip()
                    if cc in COUNTRIES:
                        countries.add(cc)

            return countries

    except Exception as e:
        print(f"uNoGS API error: {e}")
        return set()


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
    sa_says: bool | None,       # None = no data from this source
    unogs_says: bool | None,    # None = no data / not Netflix
    community_yes: int = 0,
    community_no: int = 0,
) -> dict:
    """Calculate confidence score based on multiple sources."""
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

    # uNoGS source (Netflix-specific, independent)
    if unogs_says is not None:
        sources_total += 1
        if unogs_says:
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

        # Bonus for community verification with enough votes
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
        "unogs": unogs_says,
        "community_yes": community_yes,
        "community_no": community_no,
    }


# ─── Organise & Merge Providers ──────────────────────────────────

def normalise_provider_name(name: str) -> str:
    """Normalise provider names for matching across sources."""
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


def is_netflix(name: str) -> bool:
    """Check if a provider name is Netflix (any variant)."""
    return "netflix" in name.lower()


def organise_providers_with_confidence(
    tmdb_providers: dict,
    sa_data: dict,
    unogs_countries: set,
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

                if pname not in platforms:
                    platforms[pname] = {
                        "logo": f"{TMDB_IMG}/original{provider['logo_path']}",
                        "countries": {},
                        "provider_id": provider["provider_id"],
                    }
                if access_type not in platforms[pname]["countries"]:
                    platforms[pname]["countries"][access_type] = []

                # Check Streaming Availability API
                sa_confirms = None
                if sa_data:
                    sa_country = sa_data.get(country_code, {})
                    sa_confirms = False
                    for sa_provider_name in sa_country:
                        if normalise_provider_name(sa_provider_name) == pname:
                            sa_confirms = True
                            break

                # Check uNoGS (only for Netflix providers)
                unogs_confirms = None
                if is_netflix(pname) and unogs_countries is not None:
                    unogs_confirms = country_code in unogs_countries

                # Get community votes
                vote_key = f"{country_code}:{pname}"
                votes = community_votes.get(vote_key, {"yes": 0, "no": 0})

                confidence = calculate_confidence(
                    tmdb_says=True,
                    sa_says=sa_confirms,
                    unogs_says=unogs_confirms,
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

                already_listed = False
                if norm_name in platforms:
                    for at, countries in platforms[norm_name]["countries"].items():
                        for c in countries:
                            if c["code"] == country_code:
                                already_listed = True
                                break

                if not already_listed and any(t in ["subscription", "free", "addon"] for t in access_types):
                    if norm_name not in platforms:
                        platforms[norm_name] = {
                            "logo": "",
                            "countries": {},
                            "provider_id": 0,
                        }
                    at = "flatrate"
                    if at not in platforms[norm_name]["countries"]:
                        platforms[norm_name]["countries"][at] = []

                    unogs_confirms = None
                    if is_netflix(norm_name) and unogs_countries is not None:
                        unogs_confirms = country_code in unogs_countries

                    vote_key = f"{country_code}:{norm_name}"
                    votes = community_votes.get(vote_key, {"yes": 0, "no": 0})

                    confidence = calculate_confidence(
                        tmdb_says=False,
                        sa_says=True,
                        unogs_says=unogs_confirms,
                        community_yes=votes["yes"],
                        community_no=votes["no"],
                    )

                    platforms[norm_name]["countries"][at].append({
                        "code": country_code,
                        "flag": flag,
                        "name": country_name,
                        "confidence": confidence,
                    })

    # Collect rent/buy
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
    # Fetch TMDB details first to get IMDb ID and title
    details = await get_tmdb_details(tmdb_id, media_type)

    imdb_id = None
    if "external_ids" in details:
        imdb_id = details["external_ids"].get("imdb_id")
    elif "imdb_id" in details:
        imdb_id = details["imdb_id"]

    title = details.get("title") or details.get("name", "Unknown")

    # Fetch all data sources in parallel
    async def safe_ratings():
        return await get_omdb_ratings(imdb_id) if imdb_id else {}

    async def safe_sa():
        return await get_streaming_availability(imdb_id) if imdb_id else {}

    async def safe_unogs():
        return await get_unogs_netflix_countries(title, imdb_id)

    tmdb_providers, ratings, sa_data, unogs_countries = await asyncio.gather(
        get_watch_providers(tmdb_id, media_type),
        safe_ratings(),
        safe_sa(),
        safe_unogs(),
    )

    # Organise providers with confidence from all three sources
    providers = organise_providers_with_confidence(tmdb_providers, sa_data, unogs_countries)

    year = (details.get("release_date") or details.get("first_air_date") or "")[:4]

    # Count sources used
    sources_used = ["TMDB"]
    if sa_data:
        sources_used.append("Streaming Availability")
    if unogs_countries is not None:
        sources_used.append("uNoGS (Netflix)")

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
