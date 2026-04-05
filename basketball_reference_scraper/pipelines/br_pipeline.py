import os
import time
import random
import pandas as pd
from scraper.scraper_script import BRefAdvancedScraper

def run_player_pipeline(start_year, end_year):
    print("Starting NBA Advanced Stats Pipeline...")
    print(f"Pulling seasons {start_year} through {end_year}")

    scraper = BRefAdvancedScraper()
    all_data = []

    for year in range(start_year, end_year + 1):
        print(f"\n=== Fetching {year} ===")
        season_data = scraper.player_season_stats(year)
        all_data.extend(season_data)

        if year != end_year:
            print("Sleeping 20 seconds...")
            time.sleep(20)

    os.makedirs("data", exist_ok=True)
    df = pd.DataFrame(all_data)

    if "player" in df.columns and "tm" in df.columns:
        df = df.sort_values(by=["player", "season", "tm"])
        df = df.drop_duplicates(subset=["player", "season"], keep="last")

    output_path = f"data/nba_advanced_{start_year}_{end_year}.csv"
    df.to_csv(output_path, index=False)

    print("\nPipeline Complete.")
    print(f"Saved {len(df)} rows to {output_path}")

    return df

def run_state_pipieline():
    print("Starting NBA Player Birthplace Pipeline...")

    all_data = []

    scraper = BRefAdvancedScraper()
    birthplaces = scraper.extract_birthplace_links("https://www.basketball-reference.com/friv/birthplaces.fcgi")

    for place in birthplaces: 
        loc = place["place_name"]
        print(f"\n=== Fetching {loc} ===")
        state_data = scraper.scrape_birthplace_players(place["url"], place["place_name"])
        all_data.extend(state_data)
        print("Sleeping 5 seconds...")
        time.sleep(random.uniform(3, 8))

    df = pd.DataFrame(all_data)
    output_path = f"data/nba_state_data.csv"
    df.to_csv(output_path, index=False)

    print("\nPipeline Complete.")
    print(f"Saved {len(df)} rows to {output_path}")


if __name__ == "__main__":
    run_state_pipieline()