import requests
from bs4 import BeautifulSoup
import csv
import json
import pandas as pd
from collections import defaultdict
from sqlalchemy import create_engine
import psycopg2

#Extraction

response = requests.get('https://en.wikipedia.org/wiki/List_of_largest_universities_and_university_networks_by_enrollment') 
soup = BeautifulSoup(response.content)

table = soup.find('table', attrs={'class': 'sortable'})
trs = table.findAll('tr')

columns = list(map(lambda x: x.text.strip(), trs[0].findAll('th')))
columns[-1] = "link"
rows= trs[1:]
rows
rows[0].findAll('td')[1].find_all('a')[1]['href'].lstrip('/')

def extract_row(tr):
    row_soup_list = tr.findAll('td')
    row = list(map(lambda x: x.text.strip(), row_soup_list))
    if len(row) < 6:
        row.append('')
        print("===========================================================================")
        print(row)
    link = row_soup_list[1].find_all('a')[1]['href'].lstrip('/')
    row[-1] = f'https://en.wikipedia.org/{link}' if link else ''
    return row

data = list(map(extract_row, rows))

with open('universities.csv', 'w', newline='', encoding='utf-8') as f:
    csvwriter = csv.writer(f)
    csvwriter.writerow(columns)
    csvwriter.writerows(data)
    
df = pd.DataFrame(data=data, columns=columns)

with open('universities.json', 'w') as f:
    json.dump(data, f, indent=4)
    
#Transformation
df = pd.read_csv('universities.csv', encoding='latin-1')
df['Founded'].values
df['Founded'] = df['Founded'].str.extract(r"(\d{4})")
df['Founded'] = df['Founded'].astype(int)

df['Enrollment'].values
df['Enrollment'] = df['Enrollment'].str.replace(' ', ',')
df['Enrollment'] = df['Enrollment'].str.extract(r"(\d{1,3}(?:,\d{1,3})*)")
df['Enrollment'] = df['Enrollment'].str.replace(',', '')
df['Enrollment'] = df['Enrollment'].astype(int)

def clean_location(row):
    location = row['Location']
    if not isinstance(location, str):
        return pd.Series([location, None])  # Handle non-string gracefully
    
    parts = [part.strip() for part in location.split(',')]
    
    if len(parts) == 1:
        # Only a country is present
        return pd.Series([parts[0], parts[0]])
    else:
        # Separate city and country
        country = parts[-1]
        new_location = ', '.join(parts[:-1])
        return pd.Series([new_location, country])


df[['Location', 'Country']] = df.apply(clean_location, axis=1)
df = df.rename(columns={'Distance/In-Person[a]': 'DeliveryMode'})

def fix_mojibake(val):
    if isinstance(val, str):
        try:
            return val.encode('latin1').decode('utf-8')
        except UnicodeDecodeError:
            return val  # leave unchanged if it wasn't mojibake
    return val

df = df.applymap(fix_mojibake)


# Track ID assignments
id_counters = defaultdict(int)
id_maps = {
    'Continent': {}, 'Country': {}, 'Location': {},
    'Affiliation': {}, 'DeliveryMode': {}, 'Institution': {}
}

def generate_id(entity_prefix, key, store):
    if key not in store:
        id_counters[entity_prefix] += 1
        store[key] = f"{entity_prefix}{id_counters[entity_prefix]:02d}"
    return store[key]

# Generate ContinentID
df['ContinentID'] = df['Continent'].apply(lambda x: generate_id('CO', x, id_maps['Continent']))

# Generate CountryID
df['CountryID'] = df['Country'].apply(lambda x: generate_id('CN', x, id_maps['Country']))

# Generate LocationID (City + CountryID)
df['LocationKey'] = df['Location'] + "_" + df['CountryID']
df['LocationID'] = df['LocationKey'].apply(lambda x: generate_id('LO', x, id_maps['Location']))

# Generate AffiliationID
df['AffiliationID'] = df['Affiliation'].apply(lambda x: generate_id('AF', x, id_maps['Affiliation']))

# Generate DeliveryModeID
df['DeliveryModeID'] = df['DeliveryMode'].apply(lambda x: generate_id('DM', x, id_maps['DeliveryMode']))

# Generate InstitutionID
df['InstitutionID'] = df['Institution'].apply(lambda x: generate_id('IN', x, id_maps['Institution']))

# Generate RankingID
df['RankingID'] = ['RA{:03d}'.format(i+1) for i in range(len(df))]

df.drop(columns=['LocationKey'], inplace=True)

df.rename(columns={'Location': 'city'}, inplace=True)

df.to_csv('universities-clean.csv', index=False)

#Loading
df = pd.read_csv('universities-clean.csv')



try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="amdariuser",
        password="amdariuserpassword",
        database="amdaridb"
    )
    print("✅ Connected to local PostgreSQL successfully!")
    
except Exception as e:
    print("❌ Failed to connect:", e)

cursor = conn.cursor()

create_query = """
-- Drop tables if they exist
DROP TABLE IF EXISTS InstitutionRanking;
DROP TABLE IF EXISTS Institution;
DROP TABLE IF EXISTS Location;
DROP TABLE IF EXISTS Country;
DROP TABLE IF EXISTS Continent;
DROP TABLE IF EXISTS Affiliation;
DROP TABLE IF EXISTS DeliveryMode;

-- Create Continent table
CREATE TABLE Continent (
    ContinentID VARCHAR(4) PRIMARY KEY,
    ContinentName VARCHAR(50) NOT NULL UNIQUE
);

-- Create Country table
CREATE TABLE Country (
    CountryID VARCHAR(4) PRIMARY KEY,
    CountryName VARCHAR(100) NOT NULL UNIQUE,
    ContinentID VARCHAR(4) NOT NULL,
    CONSTRAINT fk_country_continent FOREIGN KEY (ContinentID) REFERENCES Continent(ContinentID)
);

-- Create Location table
CREATE TABLE Location (
    LocationID VARCHAR(4) PRIMARY KEY,
    City VARCHAR(100) NOT NULL,
    CountryID VARCHAR(4) NOT NULL,
    CONSTRAINT fk_location_country FOREIGN KEY (CountryID) REFERENCES Country(CountryID),
    CONSTRAINT unique_city_country UNIQUE (City, CountryID)
);

-- Create Institution table
CREATE TABLE Institution (
    InstitutionID VARCHAR(4) PRIMARY KEY,
    InstitutionName VARCHAR(100) NOT NULL,
    Founded SMALLINT,
    Link VARCHAR(255),
    LocationID VARCHAR(4) NOT NULL,
    CONSTRAINT fk_institution_location FOREIGN KEY (LocationID) REFERENCES Location(LocationID)
);

-- Create Affiliation table
CREATE TABLE Affiliation (
    AffiliationID VARCHAR(4) PRIMARY KEY,
    AffiliationType VARCHAR(50) NOT NULL UNIQUE
);

-- Create DeliveryMode table
CREATE TABLE DeliveryMode (
    DeliveryModeID VARCHAR(4) PRIMARY KEY,
    DeliveryModeName VARCHAR(50) NOT NULL UNIQUE
);

-- Create InstitutionRanking table
CREATE TABLE InstitutionRanking (
    RankingID VARCHAR(5) PRIMARY KEY,
    InstitutionID VARCHAR(4) NOT NULL,
    LocationID VARCHAR(4) NOT NULL,
    AffiliationID VARCHAR(4) NOT NULL,
    DeliveryModeID VARCHAR(4) NOT NULL,
    Enrollment INTEGER,
    Rank INTEGER NOT NULL,
    CONSTRAINT fk_ranking_institution FOREIGN KEY (InstitutionID) REFERENCES Institution(InstitutionID),
    CONSTRAINT fk_ranking_location FOREIGN KEY (LocationID) REFERENCES Location(LocationID),
    CONSTRAINT fk_ranking_affiliation FOREIGN KEY (AffiliationID) REFERENCES Affiliation(AffiliationID),
    CONSTRAINT fk_ranking_deliverymode FOREIGN KEY (DeliveryModeID) REFERENCES DeliveryMode(DeliveryModeID)
);
"""

cursor.execute(create_query)
conn.commit()

# Define UPSERT queries in Python
upsert_queries = {
    'CONTINENT': """
        INSERT INTO Continent (ContinentID, ContinentName)
        VALUES (%s, %s)
        ON CONFLICT (ContinentName) DO UPDATE
        SET ContinentName = EXCLUDED.ContinentName
        RETURNING ContinentID;
    """,
    'COUNTRY': """
        INSERT INTO Country (CountryID, CountryName, ContinentID)
        VALUES (%s, %s, %s)
        ON CONFLICT (CountryName) DO UPDATE
        SET ContinentID = EXCLUDED.ContinentID
        RETURNING CountryID;
    """,
    'LOCATION': """
        INSERT INTO Location (LocationID, City, CountryID)
        VALUES (%s, %s, %s)
        ON CONFLICT (City, CountryID) DO UPDATE
        SET City = EXCLUDED.City
        RETURNING LocationID;
    """,
    'AFFILIATION': """
        INSERT INTO Affiliation (AffiliationID, AffiliationType)
        VALUES (%s, %s)
        ON CONFLICT (AffiliationType) DO UPDATE
        SET AffiliationType = EXCLUDED.AffiliationType
        RETURNING AffiliationID;
    """,
    'DELIVERYMODE': """
        INSERT INTO DeliveryMode (DeliveryModeID, DeliveryModeName)
        VALUES (%s, %s)
        ON CONFLICT (DeliveryModeName) DO UPDATE
        SET DeliveryModeName = EXCLUDED.DeliveryModeName
        RETURNING DeliveryModeID;
    """,
    'INSTITUTION': """
        INSERT INTO Institution (InstitutionID, InstitutionName, Founded, Link, LocationID)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (InstitutionID) DO UPDATE
        SET InstitutionName = EXCLUDED.InstitutionName,
            Founded = EXCLUDED.Founded,
            Link = EXCLUDED.Link,
            LocationID = EXCLUDED.LocationID
        RETURNING InstitutionID;
    """,
    'INSTITUTIONRANKING': """
        INSERT INTO InstitutionRanking (
            RankingID, InstitutionID, LocationID, Enrollment, AffiliationID, DeliveryModeID, rank
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (RankingID) DO UPDATE
        SET InstitutionID = EXCLUDED.InstitutionID,
            LocationID = EXCLUDED.LocationID,
            AffiliationID = EXCLUDED.AffiliationID,
            DeliveryModeID = EXCLUDED.DeliveryModeID,
            rank = EXCLUDED.rank,
            Enrollment = EXCLUDED.Enrollment
        RETURNING RankingID;
    """
}


# Loop through the DataFrame rows and apply the corresponding UPSERT
for row in df.itertuples(index=False):
    cursor.execute(upsert_queries['CONTINENT'], (row.ContinentID, row.Continent))

for row in df.itertuples(index=False):
    cursor.execute(upsert_queries['COUNTRY'], (row.CountryID, row.Country, row.ContinentID))

for row in df.itertuples(index=False):
    cursor.execute(upsert_queries['LOCATION'], (row.LocationID, row.city, row.CountryID))

for row in df.itertuples(index=False):
    cursor.execute(upsert_queries['AFFILIATION'], (row.AffiliationID, row.Affiliation))

for row in df.itertuples(index=False):
    cursor.execute(upsert_queries['DELIVERYMODE'], (row.DeliveryModeID, row.DeliveryMode))

for row in df.itertuples(index=False):
    cursor.execute(upsert_queries['INSTITUTION'], (
        row.InstitutionID, row.Institution, row.Founded, row.link, row.LocationID
    ))

for row in df.itertuples(index=False):
    cursor.execute(upsert_queries['INSTITUTIONRANKING'], (
        row.RankingID, row.InstitutionID, row.LocationID, row.Enrollment,
        row.AffiliationID, row.DeliveryModeID, row.Rank
    ))

# Commit the changes
conn.commit()
cursor.close()

print("✅ ETL script ran successfully and all steps completed.")

