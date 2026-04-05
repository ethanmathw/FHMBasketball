import os
import time
import pandas as pd
from tqdm import tqdm
from datetime import datetime


from basketball_reference_web_scraper import client

# -------------------------------
# SETTINGS
# -------------------------------

START_YEAR = 2024
CURRENT_YEAR = datetime.now().year

BASE_DIR = "nba_basketball_reference_1950"
SEASONS_DIR = os.path.join(BASE_DIR, "seasons")

os.makedirs(SEASONS_DIR, exist_ok=True)

all_seasons_data = []

# -------------------------------
# Loop Through Seasons
# -------------------------------

for year in tqdm(range(START_YEAR, CURRENT_YEAR)):
    
    season_str = f"{year}-{str(year+1)[-2:]}"
    print(f"\nFetching season: {season_str}")
    
    try:
        time.sleep(30) 
        stats = client.players_season_totals(season_end_year = season_str)

        
        df = pd.DataFrame(stats)
        
        if df.empty:
            print(f"No data for {season_str}")
            continue
        
        df["SEASON"] = season_str
        
        # Save season file
        season_path = os.path.join(SEASONS_DIR, f"{season_str}.csv")
        df.to_csv(season_path, index=False)
        
        all_seasons_data.append(df)
        

    except Exception as e:
        print(f"Error with {season_str}: {e}")
        continue

# -------------------------------
# Save Master Dataset
# -------------------------------

if all_seasons_data:
    master_df = pd.concat(all_seasons_data, ignore_index=True)
    
    master_path = os.path.join(BASE_DIR, "nba_player_stats_1980_present.csv")
    master_df.to_csv(master_path, index=False)
    
    print("\nMaster dataset saved successfully.")

print("\nDONE.")
