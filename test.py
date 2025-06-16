from dotenv import load_dotenv
import os

load_dotenv(override=True)
host = os.getenv("host")

print(host)