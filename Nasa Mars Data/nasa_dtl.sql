USE SCHEMA PUBLIC.RAWZ;

CREATE OR REPLACE TEMPORARY TABLE NASA_1(
SELECT
ACTIVITYID,
catalog,
TO_TIMESTAMP_NTZ(startTime) AS STARTTIME,
sourceLocation,
note as DTL_CMNTS,
cmeAnalyses:latitude :: NUMBER(10,2) AS LATITUDE,
cmeAnalyses:LONGITUDE :: NUMBER(10,2) AS LONGITUDE,
cmeAnalyses:halfangle :: NUMBER(10,2) AS HALFANGLE,
cmeAnalyses:speed :: NUMBER(10,2) AS SPEED
);

INSERT INTO PUBLIC.CNFZ.NASA_MARS_DATA(
LOAD_DATE,
SRC_ID,
activityID,
catalog,
startTime,
sourceLocation,
dtl_cmnts,
latitude,
LONGITUDE,
halfangle,
speed
)
SELECT
CURRENT_TIMESTAMP AS LOAD_DATE,
'NASA' AS SRC_ID,
activityID,
catalog,
startTime,
sourceLocation,
dtl_cmnts,
latitude,
LONGITUDE,
halfangle,
speed
FROM NASA_1;