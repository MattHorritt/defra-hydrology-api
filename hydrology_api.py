"""
UK Environment Agency Hydrology API Client

A Python library for accessing historic and recent hydrological data including
river flows, river levels, groundwater levels, rainfall and water quality from
the UK Environment Agency's Hydrology API.

Documentation: https://environment.data.gov.uk/hydrology/doc/reference
"""

import requests
import json
import time
from typing import Optional, List, Dict, Any, Union
from datetime import datetime
from urllib.parse import urlencode
import logging

logger = logging.getLogger(__name__)


class HydrologyAPIError(Exception):
    """Base exception for Hydrology API errors"""
    pass


class HydrologyAPI:
    """
    Client for the UK Environment Agency Hydrology API.
    
    Parameters:
        base_url (str): Base URL for the API. Defaults to official URL.
        timeout (int): Request timeout in seconds. Defaults to 30.
    """
    
    BASE_URL = "http://environment.data.gov.uk/hydrology"
    
    # Valid values for various parameters
    OBSERVED_PROPERTIES = [
        "waterFlow", "waterLevel", "rainfall", "groundwaterLevel",
        "dissolved-oxygen", "fdom", "bga", "turbidity", "chlorophyll",
        "conductivity", "temperature", "ammonium", "nitrate", "ph"
    ]
    
    OBSERVATION_TYPES = ["Qualified", "Measured"]
    STATION_STATUSES = ["Active", "Suspended", "Closed"]
    QUALITY_FLAGS = ["Good", "Estimated", "Suspect", "Unchecked", "Missing"]
    
    def __init__(self, base_url: str = BASE_URL, timeout: int = 30):
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
    
    def _make_request(
        self, 
        endpoint: str, 
        format: str = "json",
        params: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Union[Dict, List, str]:
        """
        Make an HTTP GET request to the API.
        
        Parameters:
            endpoint (str): API endpoint path (without base URL)
            format (str): Response format ('json', 'csv', 'geojson', etc.)
            params (dict): Query parameters
            **kwargs: Additional arguments to pass to requests
            
        Returns:
            Parsed response data (dict, list, or string depending on format)
        """
        url = f"{self.base_url}{endpoint}.{format}"
        
        try:
            response = self.session.get(
                url, 
                params=params, 
                timeout=self.timeout,
                **kwargs
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise HydrologyAPIError(f"API request failed: {e}")
        
        if format == "json":
            try:
                return response.json()
            except json.JSONDecodeError as e:
                raise HydrologyAPIError(f"Failed to parse JSON response: {e}")
        elif format == "csv":
            return response.text
        else:
            return response.text
    
    def get_stations(
        self,
        station_id: Optional[str] = None,
        rloi_id: Optional[str] = None,
        wiski_id: Optional[str] = None,
        station_guid: Optional[str] = None,
        observed_property: Optional[str] = None,
        status: Optional[str] = None,
        search: Optional[str] = None,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        distance: Optional[float] = None,
        easting: Optional[float] = None,
        northing: Optional[float] = None,
        view: str = "default",
        limit: int = 100,
        offset: int = 0,
        format: str = "json"
    ) -> Union[Dict, str]:
        """
        Get monitoring stations.
        
        Parameters:
            station_id (str): Get a specific station by ID
            rloi_id (str): Filter by River Levels On the Internet ID
            wiski_id (str): Filter by WISKI ID
            station_guid (str): Filter by station GUID
            observed_property (str): Filter by observed property (e.g., 'waterFlow', 'waterLevel')
            status (str): Filter by status ('Active', 'Suspended', 'Closed')
            search (str): Text search
            latitude (float): Latitude for geographic search
            longitude (float): Longitude for geographic search
            distance (float): Distance in km for geographic search
            easting (float): Easting for geographic search (British National Grid)
            northing (float): Northing for geographic search (British National Grid)
            view (str): View type ('default' or 'minimal')
            limit (int): Maximum number of results to return
            offset (int): Offset for pagination
            format (str): Response format ('json' or 'csv')
            
        Returns:
            API response containing station data
        """
        params = {"_limit": limit, "_offset": offset, "_view": view}
        
        if station_id:
            endpoint = f"/id/stations/{station_id}"
        else:
            endpoint = "/id/stations"
            
            if rloi_id:
                params["RLOIid"] = rloi_id
            if wiski_id:
                params["wiskiID"] = wiski_id
            if station_guid:
                params["stationGuid"] = station_guid
            if observed_property:
                if observed_property not in self.OBSERVED_PROPERTIES:
                    logger.warning(f"Unknown observed property: {observed_property}")
                params["observedProperty"] = observed_property
            if status:
                if status not in self.STATION_STATUSES:
                    logger.warning(f"Unknown status: {status}")
                params["status.label"] = status
            if search:
                params["search"] = search
            if latitude is not None and longitude is not None:
                params["lat"] = latitude
                params["long"] = longitude
                if distance:
                    params["dist"] = distance
            if easting is not None and northing is not None:
                params["easting"] = easting
                params["northing"] = northing
                if distance:
                    params["dist"] = distance
        
        return self._make_request(endpoint, format=format, params=params if endpoint == "/id/stations" else None)
    
    def get_open_stations(
        self,
        from_date: Union[str, datetime],
        to_date: Union[str, datetime],
        observed_property: Optional[str] = None,
        status: Optional[str] = None,
        search: Optional[str] = None,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        distance: Optional[float] = None,
        limit: int = 100,
        offset: int = 0,
        format: str = "json"
    ) -> Union[Dict, str]:
        """
        Get stations that were open during a specific time period.
        
        Parameters:
            from_date (str or datetime): Start date (YYYY-MM-DD)
            to_date (str or datetime): End date (YYYY-MM-DD)
            observed_property (str): Filter by observed property
            status (str): Filter by status
            search (str): Text search
            latitude (float): Latitude for geographic search
            longitude (float): Longitude for geographic search
            distance (float): Distance in km for geographic search
            limit (int): Maximum number of results
            offset (int): Offset for pagination
            format (str): Response format ('json' or 'csv')
            
        Returns:
            API response containing station data
        """
        from_date_str = from_date.strftime("%Y-%m-%d") if isinstance(from_date, datetime) else from_date
        to_date_str = to_date.strftime("%Y-%m-%d") if isinstance(to_date, datetime) else to_date
        
        params = {
            "from": from_date_str,
            "to": to_date_str,
            "_limit": limit,
            "_offset": offset
        }
        
        if observed_property:
            params["observedProperty"] = observed_property
        if status:
            params["status.label"] = status
        if search:
            params["search"] = search
        if latitude is not None and longitude is not None:
            params["lat"] = latitude
            params["long"] = longitude
            if distance:
                params["dist"] = distance
        
        return self._make_request("/id/open/stations", format=format, params=params)
    
    def get_measures(
        self,
        measure_id: Optional[str] = None,
        station_guid: Optional[str] = None,
        station_wiski_id: Optional[str] = None,
        observed_property: Optional[str] = None,
        observation_type: Optional[str] = None,
        view: str = "default",
        limit: int = 100,
        format: str = "json"
    ) -> Union[Dict, str]:
        """
        Get measurement timeseries available at stations.
        
        Parameters:
            measure_id (str): Get a specific measure by ID
            station_guid (str): Filter by station GUID
            station_wiski_id (str): Filter by station WISKI ID
            observed_property (str): Filter by observed property (e.g., 'waterFlow')
            observation_type (str): Filter by observation type ('Qualified' or 'Measured')
            view (str): View type ('default' or 'full')
            limit (int): Maximum number of results
            format (str): Response format ('json' or 'csv')
            
        Returns:
            API response containing measure/timeseries data
        """
        if measure_id:
            endpoint = f"/id/measures/{measure_id}"
            return self._make_request(endpoint, format=format)
        
        params = {"_limit": limit, "_view": view}
        
        if station_guid:
            params["station"] = station_guid
        if station_wiski_id:
            params["station.wiskiID"] = station_wiski_id
        if observed_property:
            params["observedProperty"] = observed_property
        if observation_type:
            params["observationType"] = observation_type
        
        return self._make_request("/id/measures", format=format, params=params)
    
    def get_station_measures(
        self,
        station_guid: str,
        observed_property: Optional[str] = None,
        observation_type: Optional[str] = None,
        view: str = "default",
        format: str = "json"
    ) -> Union[Dict, str]:
        """
        Get all measurement timeseries for a specific station.
        
        Parameters:
            station_guid (str): Station GUID
            observed_property (str): Filter by observed property
            observation_type (str): Filter by observation type
            view (str): View type ('default' or 'full')
            format (str): Response format ('json' or 'csv')
            
        Returns:
            API response containing measure data
        """
        params = {"_view": view}
        
        if observed_property:
            params["observedProperty"] = observed_property
        if observation_type:
            params["observationType"] = observation_type
        
        endpoint = f"/id/stations/{station_guid}/measures"
        return self._make_request(endpoint, format=format, params=params)
    
    def get_readings(
        self,
        measure_id: Optional[str] = None,
        station_guid: Optional[str] = None,
        station_rloi_id: Optional[str] = None,
        station_wiski_id: Optional[str] = None,
        observed_property: Optional[str] = None,
        observation_type: Optional[str] = None,
        date: Optional[Union[str, datetime]] = None,
        min_date: Optional[Union[str, datetime]] = None,
        max_date: Optional[Union[str, datetime]] = None,
        min_inclusive_date: Optional[Union[str, datetime]] = None,
        max_inclusive_date: Optional[Union[str, datetime]] = None,
        earliest: bool = False,
        latest: bool = False,
        period: Optional[int] = None,
        view: str = "default",
        limit: int = 100,
        format: str = "json"
    ) -> Union[Dict, str]:
        """
        Get readings (data points) for measurement timeseries.
        
        Parameters:
            measure_id (str): Filter by specific measure ID
            station_guid (str): Filter by station GUID
            station_rloi_id (str): Filter by station RLOI ID
            station_wiski_id (str): Filter by station WISKI ID
            observed_property (str): Filter by observed property
            observation_type (str): Filter by observation type ('Qualified' or 'Measured')
            date (str or datetime): Filter to specific date (YYYY-MM-DD)
            min_date (str or datetime): Strictly greater than date
            max_date (str or datetime): Strictly less than date
            min_inclusive_date (str or datetime): Greater than or equal to date
            max_inclusive_date (str or datetime): Less than or equal to date
            earliest (bool): Get earliest reading available
            latest (bool): Get latest reading available
            period (int): Filter by period (in seconds, e.g., 900 for 15 min, 86400 for daily)
            view (str): View type ('default', 'flow', 'full', or 'min')
            limit (int): Maximum number of results (default 100000, hard limit 2000000)
            format (str): Response format ('json' or 'csv')
            
        Returns:
            API response containing readings data
        """
        params = {"_limit": limit, "_view": view}
        
        def _format_date(d):
            if isinstance(d, datetime):
                return d.strftime("%Y-%m-%d")
            return d
        
        if measure_id:
            params["measure"] = measure_id
        if station_guid:
            params["station"] = station_guid
        if station_rloi_id:
            params["station.RLOIid"] = station_rloi_id
        if station_wiski_id:
            params["station.wiskiID"] = station_wiski_id
        if observed_property:
            params["observedProperty"] = observed_property
        if observation_type:
            params["observationType"] = observation_type
        if date:
            params["date"] = _format_date(date)
        if min_date:
            params["min-date"] = _format_date(min_date)
        if max_date:
            params["max-date"] = _format_date(max_date)
        if min_inclusive_date:
            params["mineq-date"] = _format_date(min_inclusive_date)
        if max_inclusive_date:
            params["maxeq-date"] = _format_date(max_inclusive_date)
        if earliest:
            params["earliest"] = ""
        if latest:
            params["latest"] = ""
        if period:
            params["period"] = period
        
        return self._make_request("/data/readings", format=format, params=params)
    
    def get_batch_readings(
        self,
        measure_ids: Optional[List[str]] = None,
        station_guid: Optional[str] = None,
        station_rloi_id: Optional[str] = None,
        station_wiski_id: Optional[str] = None,
        observed_property: Optional[str] = None,
        observation_type: Optional[str] = None,
        min_date: Optional[Union[str, datetime]] = None,
        max_date: Optional[Union[str, datetime]] = None,
        min_inclusive_date: Optional[Union[str, datetime]] = None,
        max_inclusive_date: Optional[Union[str, datetime]] = None,
        format: str = "csv",
        poll_interval: int = 5,
        max_wait_time: int = 3600
    ) -> Dict[str, Any]:
        """
        Submit a batch request for large data downloads.
        
        The batch API allows requesting large datasets without hitting the per-request
        limit. Requests are queued and processed asynchronously.
        
        Parameters:
            measure_ids (list): List of measure IDs to query
            station_guid (str): Filter by station GUID
            station_rloi_id (str): Filter by station RLOI ID
            station_wiski_id (str): Filter by station WISKI ID
            observed_property (str): Filter by observed property
            observation_type (str): Filter by observation type
            min_date (str or datetime): Start date
            max_date (str or datetime): End date
            min_inclusive_date (str or datetime): Start date (inclusive)
            max_inclusive_date (str or datetime): End date (inclusive)
            format (str): Response format (currently only CSV supported)
            poll_interval (int): Seconds to wait between status checks
            max_wait_time (int): Maximum seconds to wait for completion
            
        Returns:
            Dictionary with status and download URL when complete
        """
        def _format_date(d):
            if isinstance(d, datetime):
                return d.strftime("%Y-%m-%d")
            return d
        
        params = {}
        
        if measure_ids:
            for mid in measure_ids:
                if "measure" not in params:
                    params["measure"] = []
                params["measure"].append(mid)
        
        if station_guid:
            params["station"] = station_guid
        if station_rloi_id:
            params["station.RLOIid"] = station_rloi_id
        if station_wiski_id:
            params["station.wiskiID"] = station_wiski_id
        if observed_property:
            params["observedProperty"] = observed_property
        if observation_type:
            params["observationType"] = observation_type
        if min_date:
            params["min-date"] = _format_date(min_date)
        if max_date:
            params["max-date"] = _format_date(max_date)
        if min_inclusive_date:
            params["mineq-date"] = _format_date(min_inclusive_date)
        if max_inclusive_date:
            params["maxeq-date"] = _format_date(max_inclusive_date)
        
        # Submit batch request
        url = f"{self.base_url}/data/batch.json"
        try:
            response = self.session.post(url, data=params, timeout=self.timeout)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise HydrologyAPIError(f"Failed to submit batch request: {e}")
        
        status_url = response.headers.get("Location")
        if not status_url:
            raise HydrologyAPIError("No status URL provided in batch response")
        
        # Poll for completion
        start_time = time.time()
        while True:
            elapsed = time.time() - start_time
            if elapsed > max_wait_time:
                raise HydrologyAPIError(f"Batch request timed out after {max_wait_time} seconds")
            
            try:
                status_response = self.session.get(status_url, timeout=self.timeout)
                status_response.raise_for_status()
                status_data = status_response.json()
            except requests.exceptions.RequestException as e:
                raise HydrologyAPIError(f"Failed to check batch status: {e}")
            
            if status_data.get("status") == "Completed":
                return status_data
            elif status_data.get("status") == "Failed":
                raise HydrologyAPIError(f"Batch request failed: {status_data}")
            
            logger.info(f"Batch request status: {status_data.get('status')} "
                       f"(position: {status_data.get('positionInQueue')})")
            time.sleep(poll_interval)
    
    def download_batch_results(self, download_url: str) -> str:
        """
        Download the results of a completed batch request.
        
        Parameters:
            download_url (str): URL provided in the batch status response
            
        Returns:
            CSV data as string
        """
        try:
            response = self.session.get(download_url, timeout=self.timeout)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            raise HydrologyAPIError(f"Failed to download batch results: {e}")
    
    def close(self):
        """Close the session"""
        self.session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Example 1: Get stations measuring water flow
    print("Example 1: Stations measuring water flow")
    print("-" * 50)
    with HydrologyAPI() as api:
        stations = api.get_stations(observed_property="waterFlow", limit=5)
        if stations.get("items"):
            for station in stations["items"][:2]:
                print(f"Station: {station.get('label')} (ID: {station.get('notation')})")
    
    # Example 2: Find stations near coordinates
    print("\n\nExample 2: Stations within 5km of given coordinates")
    print("-" * 50)
    with HydrologyAPI() as api:
        # Coordinates in London
        stations = api.get_stations(latitude=51.5074, longitude=-0.1278, distance=5, limit=3)
        if stations.get("items"):
            for station in stations["items"]:
                print(f"Station: {station.get('label')}")
    
    # Example 3: Get readings for a specific date
    print("\n\nExample 3: Get readings (requires a real measure ID)")
    print("-" * 50)
    print("To get readings, you need a measure ID from a station's measures.")
    print("See get_station_measures() and get_readings() methods.")
