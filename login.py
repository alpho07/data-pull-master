import requests
from requests.auth import HTTPBasicAuth
import os
from dotenv import load_dotenv
import logging
import pprint
# Load environment variables (optional)
load_dotenv()

# DHIS2 API credentials (use your credentials or set them in a .env file)
username = "interagency-api-ken-interagency-9165"
password = "IJH;zkRvns8ZFA6"

# DHIS2 API base URL
dhis2_base_url = 'https://www.datim.org'

# Endpoint to test login
login_url = f"{dhis2_base_url}/api/me"

# Enable logging for requests (this will provide verbose output for requests and responses)
logging.basicConfig(level=logging.DEBUG)

# Make a GET request with Basic Auth to log in
response = requests.get(login_url, auth=HTTPBasicAuth(username, password))

# Check if the login was successful
if response.status_code == 200:
    print("Login successful.")
    print("Response data:", response.json())  # Print user info
else:
    print(f"Login failed with status code: {response.status_code}")
    print(f"Response content: {response.text}")

