from dotenv import load_dotenv
import os
load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
USER_AGENT = os.getenv("USER_AGENT")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
