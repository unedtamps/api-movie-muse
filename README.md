# Letterboxd Scraper & API

* **User Discovery:** Scrape lists of currently popular Letterboxd curators.
* **Data Extraction:** Pull comprehensive movie diaries and written reviews from specific profiles.
* **REST API:** Access scraped film details and user activity through structured endpoints and get film recommendations based on user preferences and seed films.


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

Start the local server to serve  scraped data and provide recommendations:

```bash
python3 api/main.py
```

### Endpoints
Access the following endpoints and documentation via Swagger UI at `http://localhost:5000/api/docs`.

##  Installation

1. Clone the repository.
2. Install dependencies:
```bash
pip install -r requirements.txt
```
