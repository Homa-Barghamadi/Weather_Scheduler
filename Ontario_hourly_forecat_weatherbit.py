

# Zone and city mapping (name and optional lat/lon)
# CITY_ZONE_MAPPING = {
#     "Northwest": [{"name": "Thunder Bay"}],
#     "Northeast": [{"name": "Greater Sudbury"}, {"name": "Sault Ste. Marie"}],
#     "East": [{"name": "Kingston"}],
#     "Ottawa": [{"name": "Ottawa"}],
#     "Toronto": [
#         {"name": "Toronto_downtown", "lat": 43.6532, "lon": -79.3832},
#         {"name": "Toronto_midtown", "lat": 43.7126, "lon": -79.4000},
#         {"name": "Toronto_north", "lat": 43.7615, "lon": -79.4111},
#         {"name": "Mississauga"},
#         {"name": "Brampton"},
#         {"name": "Markham"},
#         {"name": "Vaughan"},
#     ],
#     "Essa": [{"name": "Barrie"}],
#     "Bruce": [{"name": "Kincardine"}, {"name": "Owen Sound"}],
#     "West": [{"name": "London"}, {"name": "Windsor"}],
#     "Southwest": [{"name": "St. Catharines"}],
#     "Niagara": [{"name": "Niagara Falls"}],
#     "Uncategorized": [{"name": "Kitchener"}, {"name": "Waterloo"}, {"name": "Cambridge"}, {"name": "Guelph"}]
# }
import pandas as pd
import requests
import logging
import os
from constants import API_KEY_FORECAST_WEATHERBIT, WEATHERBIT_FORECAST_URL_HOURLY

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

TORONTO_DOWNTOWN = {"name": "Toronto_downtown", "lat": 43.6532, "lon": -79.3832}

def get_weather_forecast_from_weatherbit(api_key: str, city_name: str, lat: float = None, lon: float = None, hours: int = 337) -> pd.DataFrame:
    try:
        url = WEATHERBIT_FORECAST_URL_HOURLY
        params = {
            "key": api_key,
            "hours": hours,
            "lat": lat,
            "lon": lon
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        return pd.DataFrame(response.json().get("data", []))
    except Exception as e:
        logger.exception("❌ Failed to fetch Weatherbit forecast for %s: %s", city_name, e)
        raise

def main():
    try:
        logger.info("📍 Fetching weather forecast for Toronto_downtown")
        df = get_weather_forecast_from_weatherbit(
            api_key=API_KEY_FORECAST_WEATHERBIT,
            city_name=TORONTO_DOWNTOWN["name"],
            lat=TORONTO_DOWNTOWN["lat"],
            lon=TORONTO_DOWNTOWN["lon"]
        )

        df["fetched_at"] = pd.Timestamp.now()
        df.to_csv("weatherbit_forecast.csv", mode='a', header=not os.path.exists("weatherbit_forecast.csv"), index=False)
        logger.info("✅ Saved forecast to weather_forecast_toronto_downtown.csv")

    except Exception as e:
        logger.error("❌ Failed to fetch forecast: %s", e, exc_info=True)

if __name__ == "__main__":
    main()


# def get_weather_forecast(api_key: str, city_obj: dict) -> pd.DataFrame:
#     try:
#         name = city_obj.get("name")
#         lat = city_obj.get("lat")
#         lon = city_obj.get("lon")
#         df = get_weather_forecast_from_weatherbit(api_key, name, lat, lon)
#         return df
#     except Exception as e:
#         logger.exception("❌ Failed to prepare forecast for %s: %s", city_obj.get("name"), e)
#         raise


# def main():
#     all_dfs = []
#
#     for zone, city_list in CITY_ZONE_MAPPING.items():
#         for city_obj in city_list:
#             try:
#                 city_name = city_obj["name"]
#                 logger.info(f"📍 Fetching weather forecast for {city_name} in {zone}")
#                 df = get_weather_forecast(API_KEY_FORECAST_WEATHERBIT, city_obj)
#                 df["zone"] = zone
#                 df["city_name"] = city_name
#                 all_dfs.append(df)
#             except Exception as e:
#                 logger.warning("⚠️ Skipping city %s due to error: %s", city_obj["name"], e)
#
#     if all_dfs:
#         final_df = pd.concat(all_dfs, ignore_index=True)
#         ordered_cols = ["zone", "city_name"] + [col for col in final_df.columns if col not in ["zone", "city_name"]]
#         final_df = final_df[ordered_cols]
#
#         # Print preview
#         print(final_df.head())
#
#         # Save result
#         final_df.to_csv("weather_forecast_all_cities.csv", index=False)
#         logger.info("✅ Saved forecast to weather_forecast_all_cities.csv")
#     else:
#         logger.warning("❗ No data collected from any city.")

