import os
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

NYT_API_KEY = os.getenv("NYT_API_KEY")

if NYT_API_KEY is None:
    raise ValueError("NYT_API_KEY is missing. Add it to your .env file.")
