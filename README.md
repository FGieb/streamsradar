# StreamsRadar 📡

**Find where to stream any movie or series — across every country.**

Unlike JustWatch (which shows you one country at a time), StreamsRadar shows you **all countries at once**, ranked by availability. Built for VPN users, expats, travellers, and anyone who wants the full picture.

## Features

- 🔍 Search any movie or TV series
- 🌍 See streaming availability across 50+ countries in one view
- ⭐ IMDb + 🍅 Rotten Tomatoes ratings
- 📡 Grouped by platform (Netflix, Disney+, Prime, etc.)
- 💳 Rent/buy options with country breakdown

## Quick Start

### 1. Get API Keys (free)

- **TMDB**: Sign up at [themoviedb.org](https://www.themoviedb.org/signup) → Settings → API
- **OMDb**: Get key at [omdbapi.com/apikey.aspx](https://www.omdbapi.com/apikey.aspx)

### 2. Configure

```bash
cp .env.example .env
# Edit .env and add your API keys
```

### 3. Run

```bash
pip install fastapi uvicorn httpx python-dotenv
python -m uvicorn app:app --reload --port 8000
```

### 4. Open

Go to [http://localhost:8000](http://localhost:8000)

## Tech Stack

- **Backend**: Python / FastAPI
- **Frontend**: Vanilla HTML/CSS/JS (no framework needed for MVP)
- **APIs**: TMDB (streaming data) + OMDb (ratings)

## Attribution

- Streaming availability data by [JustWatch](https://www.justwatch.com/) via [TMDB](https://www.themoviedb.org/)
- Ratings from [IMDb](https://www.imdb.com/) & [Rotten Tomatoes](https://www.rottentomatoes.com/) via [OMDb](https://www.omdbapi.com/)

## License

MIT
