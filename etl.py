import requests
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv(override=True)

# =======================
# EXTRACT
# =======================
url = 'https://en.wikipedia.org/wiki/List_of_largest_universities_and_university_networks_by_enrollment'
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

table = soup.find('table', {'class': 'sortable'})
rows = table.find_all('tr')

columns = [th.text.strip() for th in rows[0].find_all('th')]
columns[-1] = 'Link'

def extract_row(tr):
    cells = tr.find_all('td')
    if len(cells) < 6:
        return None
    data = [td.text.strip() for td in cells]
    links = cells[1].find_all('a')
    data[-1] = 'https://en.wikipedia.org/' + links[1]['href'].lstrip('/') if len(links) > 1 else ''
    return data

data = [extract_row(tr) for tr in rows[1:] if extract_row(tr)]
raw_df = pd.DataFrame(data, columns=columns)

# =======================
# STAGING
# =======================
host = os.getenv("host")
port = os.getenv("port")
user = os.getenv("user")
password = os.getenv("password")
database = os.getenv("database")

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
print("Connected to PostgreSQL")

with engine.begin() as conn:
    conn.execute(text("""
        DROP TABLE IF EXISTS staginguniversities CASCADE;
        CREATE TABLE staginguniversities (
            rank TEXT,
            institution TEXT,
            location TEXT,
            continent TEXT,
            founded TEXT,
            affiliation TEXT,
            deliverymode TEXT,
            enrollment TEXT,
            link TEXT
        );
    """))

raw_df.columns = raw_df.columns.str.lower()
raw_df.to_sql('staginguniversities', engine, if_exists='replace', index=False)

# =======================
# TRANSFORMATION
# =======================
df = raw_df.copy()

df['founded'] = df['founded'].str.extract(r'(\d{4})')
df['founded'] = pd.to_numeric(df['founded'], errors='coerce')

df['enrollment'] = df['enrollment'].str.replace(' ', ',')
df['enrollment'] = df['enrollment'].str.extract(r'(\d{1,3}(?:,\d{3})*)')
df['enrollment'] = df['enrollment'].str.replace(',', '')
df['enrollment'] = pd.to_numeric(df['enrollment'], errors='coerce')

def split_location(loc):
    if pd.isna(loc):
        return pd.Series([None, None])
    parts = [p.strip() for p in loc.split(',')]
    if len(parts) == 1:
        return pd.Series([parts[0], parts[0]])
    return pd.Series([', '.join(parts[:-1]), parts[-1]])

df[['city', 'country']] = df['location'].apply(split_location)

def fix_mojibake(val):
    if isinstance(val, str):
        try:
            return val.encode('latin1').decode('utf-8')
        except:
            return val
    return val

df = df.applymap(fix_mojibake)

def get_id(name, id_map={}):
    if name not in id_map:
        id_map[name] = f"IN{len(id_map)+1:04d}"
    return id_map[name]

df['institutionid'] = df['institution'].apply(get_id)
df['year'] = datetime.now().year
df = df.rename(columns={'distance/in-person[a]': 'deliverymode'})


print(df.columns)


institution_df = df[['institutionid', 'institution', 'founded', 'city', 'country', 'continent', 'deliverymode', 'affiliation']].drop_duplicates()

enrollment_df = df[['institutionid', 'enrollment', 'year']]


# =======================
# LOADING
# =======================
with engine.begin() as conn:
    conn.execute(text("""
        DROP TABLE IF EXISTS enrollmentfact CASCADE;
        DROP TABLE IF EXISTS institution CASCADE;

        CREATE TABLE institution (
            institutionid VARCHAR(10) PRIMARY KEY,
            institution TEXT,
            founded INT,
            city TEXT,
            country TEXT,
            continent TEXT,
            deliverymode TEXT,
            affiliation TEXT
        );

        CREATE TABLE enrollmentfact (
            institutionid VARCHAR(10),
            enrollment INT,
            year INT,
            PRIMARY KEY (institutionid, year),
            FOREIGN KEY (institutionid) REFERENCES institution(institutionid)
        );
    """))

institution_df.to_sql('institution', engine, if_exists='append', index=False)
enrollment_df.to_sql('enrollmentfact', engine, if_exists='append', index=False)

print("✅ Full ETL pipeline with staging, transformation, and loading completed successfully")
