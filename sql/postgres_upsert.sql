
-- Upsert queries for university ranking database

-- Continent
INSERT INTO Continent (ContinentID, ContinentName)
VALUES (%s, %s)
ON CONFLICT (ContinentName) DO UPDATE
SET ContinentName = EXCLUDED.ContinentName
RETURNING ContinentID;

-- Country
INSERT INTO Country (CountryID, CountryName, ContinentID)
VALUES (%s, %s, %s)
ON CONFLICT (CountryName) DO UPDATE
SET ContinentID = EXCLUDED.ContinentID
RETURNING CountryID;

-- Location
INSERT INTO Location (LocationID, City, CountryID)
VALUES (%s, %s, %s)
ON CONFLICT (City, CountryID) DO UPDATE
SET City = EXCLUDED.City
RETURNING LocationID;

-- Affiliation
INSERT INTO Affiliation (AffiliationID, AffiliationType)
VALUES (%s, %s)
ON CONFLICT (AffiliationType) DO UPDATE
SET AffiliationType = EXCLUDED.AffiliationType
RETURNING AffiliationID;

-- DeliveryMode
INSERT INTO DeliveryMode (DeliveryModeID, DeliveryModeName)
VALUES (%s, %s)
ON CONFLICT (DeliveryModeName) DO UPDATE
SET DeliveryModeName = EXCLUDED.DeliveryModeName
RETURNING DeliveryModeID;

-- Institution
-- Institution
-- Institution
INSERT INTO Institution (InstitutionID, InstitutionName, Founded, Link, LocationID)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (InstitutionID) DO UPDATE
SET
    InstitutionName = EXCLUDED.InstitutionName,
    Founded = EXCLUDED.Founded,
    Link = EXCLUDED.Link,
    LocationID = EXCLUDED.LocationID
RETURNING InstitutionID;


-- InstitutionRanking
-- InstitutionRanking
-- InstitutionRanking
INSERT INTO InstitutionRanking (
    RankingID, InstitutionID, LocationID, Enrollment, AffiliationID, DeliveryModeID, rank
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (RankingID) DO UPDATE
SET 
    InstitutionID = EXCLUDED.InstitutionID,
    LocationID = EXCLUDED.LocationID,
    AffiliationID = EXCLUDED.AffiliationID,
    DeliveryModeID = EXCLUDED.DeliveryModeID,
    rank = EXCLUDED.rank,
    Enrollment = EXCLUDED.Enrollment
RETURNING RankingID;

