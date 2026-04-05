import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.basketball-reference.com"

class BRefAdvancedScraper:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36"
        }
        self.session = requests.Session()

    def player_season_stats(self, year):
        url = f"https://www.basketball-reference.com/leagues/NBA_{year}_advanced.html"

        print("Requesting:", url)
        response = self.session.get(url)

        if response.status_code != 200:
            print(f"Failed to fetch {year}: HTTP {response.status_code}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        # table = soup.find("table", {"id": "advanced_stats"})

        # if table is None:
        #     print(f"No advanced stats table found for {year}")
        #     return []

        all_players_data = []
        rows = soup.find_all('tr', class_=lambda x: x != 'thead')

        for row in rows:
            cells = row.find_all(['th', 'td'])
            
            if not cells:
                continue
            player_stats = {cell['data-stat']: cell.get_text(strip=True) for cell in cells if cell.has_attr('data-stat')}
            if player_stats:
                player_stats["season"] = year
                all_players_data.append(player_stats)

        all_players_data = all_players_data[1:]

        print(f"{year} scraped: {len(all_players_data)} players")
        return all_players_data
    
    def extract_birthplace_links(self, url):

        response = self.session.get(url)
        if response.status_code != 200:
            print(f"Failed to fetch: HTTP {response.status_code}")
            return []
        

        soup = BeautifulSoup(response.text, "html.parser")

        birthplaces = []

        for box in soup.find_all("div", class_="data_grid_box"):
            section = box.find("div", class_="gridtitle").get_text(strip=True)

            for a in box.find_all("a"):
                name = a.get_text(strip=True)
                href = a.get("href")

                if not href:
                    continue

                birthplaces.append({
                    "section": section,   # United States / Other Countries
                    "place_name": name,
                    "url": BASE_URL + href
                })

        return birthplaces
    
    def scrape_birthplace_players(self, url, place_name):
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Failed: {url}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")

        table = soup.find("table")

        if not table:
            return []

        players = []

        for row in table.find("tbody").find_all("tr"):
            cells = row.find_all(["th", "td"])

            if not cells:
                continue

            row_data = {}

            for cell in cells:
                if cell.has_attr("data-stat"):
                    row_data[cell["data-stat"]] = cell.get_text(strip=True)

                    # capture player link
                    if cell["data-stat"] == "player":
                        link = cell.find("a")
                        if link:
                            row_data["player_url"] = BASE_URL + link["href"]

            if row_data:
                row_data["birthplace"] = place_name
                players.append(row_data)

        return players
    