# Letterboxd Scraper & API

* **User Discovery:** Scrape lists of currently popular Letterboxd curators.
* **Data Extraction:** Pull comprehensive movie diaries and written reviews from specific profiles.
* **REST API:** Access scraped film details and user activity through structured endpoints.


## Scraping 

Run the scripts in the following order to populate your database:

### 1. Discover Popular Users

Fetches a list of trending profiles from the Letterboxd "Popular" section.

```bash
python3 scrap/get_users.py
```

### 2. Extract User Diaries & Reviews

Crawls the diaries of the users identified in the first step.

```bash
python3 scrap/user_reviews.py
```

### 3. Fetch Film Details

Enriches the data by fetching specific metadata (genres, cast, etc.) for movies found in diaries.

```bash
python3 scrap/get_film_details.py
```



## API Usage

Start the local server to serve your scraped data:

```bash
python3 api/main.py
```

### Endpoints
| Endpoint | Description | Parameters |
| --- | --- | --- |
| `GET /film/<id>` | Retrieve detailed metadata for a specific film. | `id`: Letterboxd film slug |
| `GET /diary/<user_id>?page=` | Get a paginated list of a user's diary entries. | `page`: Page number (optional) |
| `GET /recommend/<user_id>?k=` | Get a paginated list movie recommendation for users | `k`: Page number (optional) |
| `GET /get_list` | Get List of movie from url | None |



**Example Request:**
`GET /diary/official?page=2`

---

##  Installation

1. Clone the repository.
2. Install dependencies:
```bash
pip install -r requirements.txt
```
