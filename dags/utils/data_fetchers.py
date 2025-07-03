# File: dags/utils/data_fetchers.py
"""
Data fetching utilities for external sources
"""

import requests
import pandas as pd
import paramiko
from io import StringIO, BytesIO
import json
import xml.etree.ElementTree as ET
from airflow.hooks.base import BaseHook


def fetch_http_data(url, api_key=None, headers=None, params=None, format='json'):
    """
    Fetch data from HTTP API
    
    Args:
        url: API endpoint URL
        api_key: API key for authentication
        headers: Additional headers
        params: Query parameters
        format: Expected response format ('json', 'csv', 'xml')
    
    Returns:
        pandas.DataFrame: Parsed data
    """
    # Set up headers
    request_headers = headers or {}
    if api_key:
        request_headers['Authorization'] = f'Bearer {api_key}'
        # or request_headers['X-API-Key'] = api_key  # depending on API
    
    # Make request
    response = requests.get(url, headers=request_headers, params=params)
    response.raise_for_status()
    
    # Parse based on format
    if format == 'json':
        data = response.json()
        return pd.json_normalize(data)
    elif format == 'csv':
        return pd.read_csv(StringIO(response.text))
    elif format == 'xml':
        root = ET.fromstring(response.text)
        # Simple XML to dict conversion - adjust based on your XML structure
        data = []
        for child in root:
            item = {}
            for subchild in child:
                item[subchild.tag] = subchild.text
            data.append(item)
        return pd.DataFrame(data)
    else:
        raise ValueError(f"Unsupported format: {format}")


def fetch_sftp_data(sftp_conn_id, remote_path, file_format='csv'):
    """
    Fetch data from SFTP server
    
    Args:
        sftp_conn_id: Airflow connection ID for SFTP
        remote_path: Path to file on SFTP server
        file_format: File format ('csv', 'json', 'xml')
    
    Returns:
        pandas.DataFrame: Parsed data
    """
    # Get connection details from Airflow
    connection = BaseHook.get_connection(sftp_conn_id)
    
    # Connect to SFTP
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=connection.host,
        port=connection.port or 22,
        username=connection.login,
        password=connection.password
    )
    
    sftp = ssh.open_sftp()
    
    try:
        # Download file to memory
        file_obj = BytesIO()
        sftp.getfo(remote_path, file_obj)
        file_obj.seek(0)
        
        # Parse based on format
        if file_format == 'csv':
            return pd.read_csv(file_obj)
        elif file_format == 'json':
            data = json.load(file_obj)
            return pd.json_normalize(data)
        elif file_format == 'xml':
            tree = ET.parse(file_obj)
            root = tree.getroot()
            data = []
            for child in root:
                item = {}
                for subchild in child:
                    item[subchild.tag] = subchild.text
                data.append(item)
            return pd.DataFrame(data)
        else:
            raise ValueError(f"Unsupported format: {file_format}")
    
    finally:
        sftp.close()
        ssh.close()