
-- Create tables for university ranking database in PostgreSQL
-- Database: university_ranking

-- Drop tables if they exist (in reverse order to avoid FK conflicts)
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
