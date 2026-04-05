import os
import time
import pandas as pd
from tqdm import tqdm
from datetime import datetime

from nba_api.stats.endpoints import leaguedashplayerstats

# -------------------------------
# SETTINGS
# -------------------------------

START_YEAR = 1940
CURRENT_YEAR = datetime.now().year

BASE_DIR = "nba_data_1940_present"
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
        stats = leaguedashplayerstats.LeagueDashPlayerStats(
            season=season_str,
            season_type_all_star="Regular Season",
            per_mode_detailed="Totals"  # Change to PerGame if needed
        )

        
        df = stats.get_data_frames()[0]
        
        if df.empty:
            print(f"No data for {season_str}")
            continue
        
        df["SEASON"] = season_str
        
        # Save season file
        season_path = os.path.join(SEASONS_DIR, f"{season_str}.csv")
        df.to_csv(season_path, index=False)
        
        all_seasons_data.append(df)
        
        time.sleep(0.8)  # Avoid rate limiting
        
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
