# import os
# import logging
# import requests
# import pandas as pd
#
# from constants import API_KEY_WEATHERSOURCE, WEATHERSOURCE_FORECAST_URL
#
# logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO)
#
# CITY_ZONE_MAPPING = {
#     "Northwest": [{"name": "Thunder Bay", "lat": 48.3809, "lon": -89.2477}],
#     "Northeast": [
#         {"name": "Greater Sudbury", "lat": 46.4917, "lon": -80.9930},
#         {"name": "Sault Ste. Marie", "lat": 46.5219, "lon": -84.3461},
#     ],
#     "East": [{"name": "Kingston", "lat": 44.2312, "lon": -76.4860}],
#     "Ottawa": [{"name": "Ottawa", "lat": 45.4215, "lon": -75.6990}],
#     "Toronto": [
#         {"name": "Toronto_downtown", "lat": 43.6532, "lon": -79.3832},
#         {"name": "Toronto_midtown",  "lat": 43.7126, "lon": -79.4000},
#         {"name": "Toronto_north",    "lat": 43.7615, "lon": -79.4111},
#         {"name": "Mississauga",      "lat": 43.5890, "lon": -79.6441},
#         {"name": "Brampton",         "lat": 43.7315, "lon": -79.7624},
#         {"name": "Markham",          "lat": 43.8563, "lon": -79.3370},
#         {"name": "Vaughan",          "lat": 43.8372, "lon": -79.5083},
#     ],
#     "Essa": [{"name": "Barrie", "lat": 44.3894, "lon": -79.6903}],
#     "Bruce": [
#         {"name": "Kincardine", "lat": 44.1775, "lon": -81.6362},
#         {"name": "Owen Sound", "lat": 44.5600, "lon": -80.9435},
#     ],
#     "West": [
#         {"name": "London",  "lat": 42.9849, "lon": -81.2453},
#         {"name": "Windsor", "lat": 42.3149, "lon": -83.0364},
#     ],
#     "Southwest": [{"name": "St. Catharines", "lat": 43.1594, "lon": -79.2469}],
#     "Niagara": [{"name": "Niagara Falls", "lat": 43.0896, "lon": -79.0849}],
#     "Uncategorized": [
#         {"name": "Kitchener", "lat": 43.4516, "lon": -80.4925},
#         {"name": "Waterloo",  "lat": 43.4643, "lon": -80.5204},
#         {"name": "Cambridge", "lat": 43.3616, "lon": -80.3144},
#         {"name": "Guelph",    "lat": 43.5448, "lon": -80.2482},
#     ],
# }
#
#
# def get_weather_forecast_from_weathersource(api_key: str, lat: float, lon: float, hours: int = 240) -> pd.DataFrame:
#     """
#     Fetch hourly forecast for a given lat/lon from Weather Source API.
#     """
#     url = f"{WEATHERSOURCE_FORECAST_URL}/points/{lat},{lon}/hours"
#     headers = {"X-API-KEY": api_key}
#     params = {"limit": hours}  # Some plans limit horizon (check docs)
#
#     try:
#         response = requests.get(url, headers=headers, params=params, timeout=30)
#         response.raise_for_status()
#         data = response.json()
#         df = pd.DataFrame(data)
#         if df.empty:
#             raise ValueError("Empty forecast payload")
#         return df
#     except Exception as e:
#         logger.exception("❌ Failed to fetch Weather Source forecast for (%s, %s): %s", lat, lon, e)
#         raise
#
#
# def get_weather_forecast(api_key: str, city_obj: dict) -> pd.DataFrame:
#     return get_weather_forecast_from_weathersource(api_key, city_obj["lat"], city_obj["lon"])
#
#
# def main():
#     all_dfs = []
#
#     for zone, city_list in CITY_ZONE_MAPPING.items():
#         for city_obj in city_list:
#             city_name = city_obj["name"]
#             try:
#                 logger.info("📍 Fetching forecast for %s (%s)", city_name, zone)
#                 df = get_weather_forecast(API_KEY_WEATHERSOURCE, city_obj)
#                 df["zone"] = zone
#                 df["city_name"] = city_name
#                 all_dfs.append(df)
#             except Exception as e:
#                 logger.warning("⚠️ Skipping %s due to error: %s", city_name, e)
#
#     if not all_dfs:
#         logger.warning("❗ No data collected.")
#         return
#
#     final_df = pd.concat(all_dfs, ignore_index=True)
#     ordered_cols = ["zone", "city_name"] + [c for c in final_df.columns if c not in ("zone", "city_name")]
#     final_df = final_df[ordered_cols]
#
#     print(final_df.head())
#     final_df.to_csv("weather_forecast_all_cities_weathersource.csv", index=False)
#     logger.info("✅ Saved forecast to weather_forecast_all_cities.csv")
#
#
# if __name__ == "__main__":
#     main()


import logging
import requests
import os
import pandas as pd

from constants import API_KEY_WEATHERSOURCE, WEATHERSOURCE_FORECAST_URL

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

TORONTO_DOWNTOWN = {
    "name": "Toronto_downtown",
    "lat": 43.6532,
    "lon": -79.3832,
    "zone": "Toronto"
}


def get_weather_forecast_from_weathersource(api_key: str, lat: float, lon: float, hours: int = 240) -> pd.DataFrame:
    """
    Fetch hourly forecast for a given lat/lon from Weather Source API.
    """
    url = f"{WEATHERSOURCE_FORECAST_URL}/points/{lat},{lon}/hours"
    headers = {"X-API-KEY": api_key}
    params = {
        "fields": "all",  # add other fields you need
        "unitScale": "METRIC"
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Safely extract the actual list of forecast records
        forecast_data = data.get("forecast") or data.get("forecasts") or data.get("data")        # adapt based on API structure
        if not isinstance(forecast_data, list):
            raise ValueError("Unexpected forecast format")

        df = pd.DataFrame(forecast_data)
        if df.empty:
            raise ValueError("Empty forecast payload")

        return df

    except Exception as e:
        logger.exception("❌ Failed to fetch forecast for (%s, %s): %s", lat, lon, e)
        raise


def main():
    city = TORONTO_DOWNTOWN
    logger.info("📍 Fetching forecast for %s (%s)", city["name"], city["zone"])

    try:
        df = get_weather_forecast_from_weathersource(API_KEY_WEATHERSOURCE, city["lat"], city["lon"])
        df["zone"] = city["zone"]
        df["city_name"] = city["name"]
        ordered_cols = ["zone", "city_name"] + [c for c in df.columns if c not in ("zone", "city_name")]
        df = df[ordered_cols]

        df["fetched_at"] = pd.Timestamp.now()
        df.to_csv("WeatherNetwork_forecast.csv", mode='a', header=not os.path.exists("WeatherNetwork_forecast.csv"), index=False)
        logger.info("✅ Saved forecast to weather_forecast_toronto_downtown.csv")
    except Exception as e:
        logger.error("❌ Could not fetch forecast for Toronto downtown: %s", e)


if __name__ == "__main__":
    main()

