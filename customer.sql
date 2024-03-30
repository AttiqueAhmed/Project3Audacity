CREATE EXTERNAL TABLE IF NOT EXISTS Customer (
    customerName VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(20),
    birthDay DATE,
    serialNumber VARCHAR(50),
    registrationDate BIGINT,
    lastUpdateDate BIGINT,
    shareWithResearchAsOfDate BIGINT,
    shareWithPublicAsOfDate BIGINT,
    shareWithFriendsAsOfDate BIGINT
)

LOCATION 's3://project3parent/customer/landing/';
