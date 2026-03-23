"""
Resilient API client for Blue Owls Data API with token management and retry logic.
"""

import os
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class APIClient:
    """Client for interacting with Blue Owls Data API with resilience features."""
    
    def __init__(
        self,
        base_url: str = "http://mock-api:8000",
        username: str = "candidate",
        password: str = "blue-owls-2026",
        token_expiry_minutes: int = 15,
    ):
        """
        Initialize API client.
        
        Args:
            base_url: Base URL of the API
            username: API username
            password: API password
            token_expiry_minutes: Token expiration threshold in minutes
        """
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.token_expiry_minutes = token_expiry_minutes
        
        self.token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None
        
        # Configure session with retry strategy
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry strategy for 500 and 429 errors."""
        session = requests.Session()
        
        # Retry strategy: retry on 429 and 500 errors with exponential backoff
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _is_token_valid(self) -> bool:
        """Check if current token is still valid."""
        if not self.token or not self.token_expires_at:
            return False
        # Consider token invalid if it will expire in the next 2 minutes
        return datetime.utcnow() < (self.token_expires_at - timedelta(minutes=2))
    
    def _get_token(self) -> str:
        """
        Obtain a new authentication token.
        
        Returns:
            Access token
            
        Raises:
            Exception: If authentication fails
        """
        url = f"{self.base_url}/api/v1/auth/token"
        payload = {
            "username": self.username,
            "password": self.password,
        }
        
        try:
            response = self.session.post(url, json=payload, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            self.token = data["access_token"]
            # Set expiry slightly before actual expiry to account for clock skew
            self.token_expires_at = datetime.utcnow() + timedelta(
                minutes=self.token_expiry_minutes - 2
            )
            logger.info(f"Token obtained, expires at {self.token_expires_at}")
            return self.token
        except Exception as e:
            logger.error(f"Failed to obtain token: {e}")
            raise
    
    def _ensure_token(self) -> str:
        """Ensure we have a valid token, refreshing if necessary."""
        if not self._is_token_valid():
            self._get_token()
        return self.token
    
    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        max_retries: int = 5,
    ) -> Dict[str, Any]:
        """
        Make a GET request to the API with automatic token refresh and retry logic.
        
        Args:
            endpoint: API endpoint (without base URL)
            params: Query parameters
            max_retries: Maximum number of retries for 401 responses
            
        Returns:
            JSON response
            
        Raises:
            Exception: If request fails after retries
        """
        if params is None:
            params = {}
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        for attempt in range(max_retries):
            # Ensure we have a valid token
            token = self._ensure_token()
            headers = {"Authorization": f"Bearer {token}"}
            
            try:
                response = self.session.get(url, headers=headers, params=params, timeout=30)
                
                if response.status_code == 401:
                    # Token expired, refresh and retry
                    logger.info(f"Token expired, refreshing (attempt {attempt + 1}/{max_retries})")
                    self.token = None
                    self.token_expires_at = None
                    continue
                
                response.raise_for_status()
                return response.json()
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    # Let the retry strategy handle backoff
                    continue
                raise
        
        raise Exception(f"Failed to fetch {endpoint} after {max_retries} retries")
    
    def fetch_paginated(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_size: int = 1000,
    ) -> List[Dict[str, Any]]: # pyright: ignore[reportInvalidTypeForm]
        """
        Fetch all pages from a paginated endpoint.
        
        Args:
            endpoint: API endpoint
            params: Base query parameters (e.g., date_from, date_to)
            page_size: Number of records per page
            
        Yields:
            Records from all pages
        """
        if params is None:
            params = {}
        
        page = 1
        while True:
            page_params = {**params, "page": page, "page_size": page_size}
            
            try:
                response = self.get(endpoint, params=page_params)
            except Exception as e:
                logger.error(f"Error fetching page {page} from {endpoint}: {e}")
                # Skip this page and continue with next
                page += 1
                continue
            
            # Handle both array and paginated response formats
            if isinstance(response, list):
                records = response
                has_more = len(records) == page_size
            else:
                # Assume paginated format with data array
                records = response.get("data", [])
                pagination = response.get("pagination", {})
                has_more = pagination.get("has_more", False)
            
            if not records:
                break
            
            for record in records:
                yield record
            
            if not has_more or len(records) < page_size:
                break
            
            page += 1
