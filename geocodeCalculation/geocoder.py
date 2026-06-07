"""Google postcode geocoding with on-disk cache."""

import hashlib
import json
import logging
import time
from pathlib import Path

import requests

logger = logging.getLogger(__name__)


class GeocodeCache:
    """Manages geocoding cache in .cache/geocode (under project/exe root when frozen)."""

    def __init__(self, cache_dir=None):
        if cache_dir is None:
            from migration_support import get_project_root

            cache_dir = get_project_root() / ".cache" / "geocode"
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Cache directory: %s", self.cache_dir.absolute())

    def _get_cache_key(self, postcode):
        normalized = postcode.strip().upper().replace(" ", "")
        return hashlib.md5(normalized.encode()).hexdigest()

    def get(self, postcode):
        cache_key = self._get_cache_key(postcode)
        cache_file = self.cache_dir / f"{cache_key}.json"
        if cache_file.exists():
            try:
                with open(cache_file, "r", encoding="utf-8") as handle:
                    return json.load(handle)
            except Exception as exc:
                logger.warning("Failed to read cache for %s: %s", postcode, exc)
        return None

    def set(self, postcode, geocode_data):
        cache_key = self._get_cache_key(postcode)
        cache_file = self.cache_dir / f"{cache_key}.json"
        try:
            data = {
                "postcode": postcode,
                "full_response": geocode_data,
                "timestamp": time.time(),
            }
            with open(cache_file, "w", encoding="utf-8") as handle:
                json.dump(data, handle, indent=2)
        except Exception as exc:
            logger.warning("Failed to cache %s: %s", postcode, exc)


class GoogleGeocoder:
    """Geocodes postcodes using Google Maps Geocoding API."""

    def __init__(self, api_key, cache, shutdown_check=None):
        self.api_key = api_key
        self.cache = cache
        self.shutdown_check = shutdown_check or (lambda: False)
        self.request_count = 0
        self.cache_hits = 0

    def geocode(self, postcode, country="IE"):
        if not postcode or postcode.strip() == "":
            return None

        cached = self.cache.get(postcode)
        if cached:
            self.cache_hits += 1
            full_response = cached.get("full_response", {})
            if full_response.get("status") == "OK" and full_response.get("results"):
                return full_response
            if full_response.get("status") in ["REQUEST_DENIED", "INVALID_REQUEST", "UNKNOWN_ERROR"]:
                logger.info("Cached error for %s, retrying...", postcode)
            else:
                return None

        if self.shutdown_check():
            logger.warning("Shutdown requested, skipping geocoding for: %s", postcode)
            return None

        try:
            url = "https://maps.googleapis.com/maps/api/geocode/json"
            params = {
                "address": postcode,
                "components": f"country:{country}",
                "key": self.api_key,
            }
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            self.request_count += 1

            if data["status"] == "OK" and len(data["results"]) > 0:
                self.cache.set(postcode, data)
                time.sleep(0.1)
                return data

            logger.warning(
                "Geocoding failed for %s: %s - %s",
                postcode,
                data.get("status"),
                data.get("error_message", "No error message"),
            )
            return None
        except Exception as exc:
            logger.error("Error geocoding %s: %s", postcode, exc)
            return None
