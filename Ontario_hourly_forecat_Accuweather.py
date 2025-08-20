

# CITY_ZONE_MAPPING = {
#     "Northwest": [
#         {"name": "Thunder Bay", "lat": 48.3809, "lon": -89.2477}
#     ],
#     "Northeast": [
#         {"name": "Greater Sudbury", "lat": 46.4917, "lon": -80.9930},
#         {"name": "Sault Ste. Marie", "lat": 46.5219, "lon": -84.3461}
#     ],
#     "East": [
#         {"name": "Kingston", "lat": 44.2312, "lon": -76.4860}
#     ],
#     "Ottawa": [
#         {"name": "Ottawa", "lat": 45.4215, "lon": -75.6990}
#     ],
#     "Toronto": [
#         {"name": "Toronto_downtown", "lat": 43.6532, "lon": -79.3832},
#         {"name": "Toronto_midtown", "lat": 43.7126, "lon": -79.4000},
#         {"name": "Toronto_north", "lat": 43.7615, "lon": -79.4111},
#         {"name": "Mississauga", "lat": 43.5890, "lon": -79.6441},
#         {"name": "Brampton", "lat": 43.7315, "lon": -79.7624},
#         {"name": "Markham", "lat": 43.8561, "lon": -79.3370},
#         {"name": "Vaughan", "lat": 43.8372, "lon": -79.5083}
#     ],
#     "Essa": [
#         {"name": "Barrie", "lat": 44.3894, "lon": -79.6903}
#     ],
#     "Bruce": [
#         {"name": "Kincardine", "lat": 44.1745, "lon": -81.6363},
#         {"name": "Owen Sound", "lat": 44.5672, "lon": -80.9435}
#     ],
#     "West": [
#         {"name": "London", "lat": 42.9849, "lon": -81.2453},
#         {"name": "Windsor", "lat": 42.3149, "lon": -83.0364}
#     ],
#     "Southwest": [
#         {"name": "St. Catharines", "lat": 43.1594, "lon": -79.2469}
#     ],
#     "Niagara": [
#         {"name": "Niagara Falls", "lat": 43.0896, "lon": -79.0849}
#     ],
#     "Uncategorized": [
#         {"name": "Kitchener", "lat": 43.4516, "lon": -80.4925},
#         {"name": "Waterloo", "lat": 43.4643, "lon": -80.5204},
#         {"name": "Cambridge", "lat": 43.3616, "lon": -80.3144},
#         {"name": "Guelph", "lat": 43.5448, "lon": -80.2482}
#     ]
# }

import pandas as pd
import requests
import logging
from constants import API_KEY_FORECAST_ACCUWEATHER_HOURLY, ACCUWEATHER_FORECAST_URL_HOURLY, ACCUWEATHER_LOCATION_URL

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
DOWNTOWN_TORONTO = {"name": "Toronto_downtown", "lat": 43.6532, "lon": -79.3832}


def get_location_key(api_key: str, lat: float, lon: float) -> str:
    try:
        params = {"apikey": api_key, "q": f"{lat},{lon}"}
        response = requests.get(ACCUWEATHER_LOCATION_URL, params=params)
        response.raise_for_status()
        return response.json()["Key"]
    except Exception as e:
        logger.error(f"❌ Failed to get location key for {lat},{lon}: {e}", exc_info=True)
        raise


def get_hourly_forecast(api_key: str, location_key: str) -> pd.DataFrame:
    try:
        params = {"apikey": api_key, "metric": "true", "details": "true"}
        url = f"{ACCUWEATHER_FORECAST_URL_HOURLY}/{location_key}"
        response = requests.get(url, params=params)
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except Exception as e:
        logger.error(f"❌ Failed to get forecast for location key {location_key}: {e}", exc_info=True)
        raise


def get_weather_forecast(api_key: str, city_obj: dict) -> pd.DataFrame:
    try:
        name = city_obj.get("name")
        lat = city_obj.get("lat")
        lon = city_obj.get("lon")

        if lat is None or lon is None:
            raise ValueError(f"Latitude and longitude required for {name}")

        location_key = get_location_key(api_key, lat, lon)
        df = get_hourly_forecast(api_key, location_key)
        df["city_name"] = name
        return df
    except Exception as e:
        logger.error(f"❌ Error preparing forecast for {city_obj.get('name')}: {e}", exc_info=True)
        raise

def main():
    try:
        logger.info("📍 Fetching weather forecast for Toronto_downtown")
        df = get_weather_forecast(API_KEY_FORECAST_ACCUWEATHER_HOURLY, DOWNTOWN_TORONTO)

        # Print and save
        df["fetched_at"] = pd.Timestamp.now()
        df.to_csv("Accuweather_forecast.csv", mode='a', header=not os.path.exists("Accuweather_forecast.csv"), index=False)
        logger.info("✅ Saved forecast to accuweather_forecast_toronto_downtown.csv")
    except Exception as e:
        logger.error(f"❌ Failed to fetch forecast: {e}", exc_info=True)

if __name__ == "__main__":
    main()

# def main():
#     all_dfs = []
#
#     for zone, city_list in CITY_ZONE_MAPPING.items():
#         for city_obj in city_list:
#             try:
#                 city_name = city_obj["name"]
#                 logger.info(f"📍 Fetching weather forecast for {city_name} in {zone}")
#                 df = get_weather_forecast(API_KEY_FORECAST_ACCUWEATHER, city_obj)
#                 df["zone"] = zone
#                 df["city_name"] = city_name
#                 all_dfs.append(df)
#             except Exception as e:
#                 logger.warning(f"⚠️ Skipping city {city_obj['name']} due to error: {e}")
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
#         final_df.to_csv("accuweather_forecast_all_cities.csv", index=False)
#         logger.info("✅ Saved forecast to accuweather_forecast_all_cities.csv")
#     else:
#         logger.warning("❗ No data collected from any city.")



