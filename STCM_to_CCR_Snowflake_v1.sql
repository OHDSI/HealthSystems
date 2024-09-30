----------------------------------------------------------------
/*******************************************************************************
# Copyright 2020 Corewell Health
# http://www.corewellhealth.org
#
# Unless required by applicable law or agreed to in writing, this software
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.
#
********************************************************************************/

/*******************************************************************************
Name: STCM_to_CCR_v1.sql

Authors: Roger Carlson & Matt Phad
		 Corewell Health
		 roger.carlson@corewellhealth.org
		 matthew.phad@corewellhealth.org

Last Revised: 27-Sept-2024

Description: A process for transferring data from the SOURCE_TO_CONCEPT_MAP table to the CONCEPT, CONCEPT_RELATIONSHIP, and VOCABULARY tables.

Structure: (if your structure is different, you will have to modify the code to match)
	Database: CARE_RES_OMOP_DEV2_WKSP
	Schemas: CARE_RES_OMOP_DEV2_WKSP.OMOP

Note: All SOURCE_VOCABULARY_ID's in our SOURCE_TO_CONCEPT_MAP begin with "CH_". That naming convention is integral to ensuring rows are properly updated and deleted in this process.
  We recommend you utilize a naming convention like this, and replace "CH_" with your naming convention in the CUSTOM_SOURCE_VOCABULARY_ID_PREFIX variable below.
  Similarly, replace "CH generated" with your desired value for VOCABULARY_REFERENCE in custom vocabularies in the CUSTOM_VOCABULARY_REFERENCE variable below.

********************************************************************************/
USE  CARE_RES_OMOP_DEV2_WKSP.OMOP;
SET CUSTOM_VOCABULARY_REFERENCE='CH generated'; --your vocabulary reference
SET CUSTOM_SOURCE_VOCABULARY_ID_PREFIX='CH_'; --your custom source vocabulary prefix

/*
1. SEQUENCE: Create a Sequence (e.g., 2B_SEQ).
    if max(concept_id) < 2,000,000,000 then sequence = 2000000000*
    if max(concept_id) > 2,000,000,000 then sequence = max(concept_id) + 1
    *Note: You can start at a higher number if you wish to reserve some 2billionaire values.
 */

CREATE OR REPLACE PROCEDURE CreateSequence()
 RETURNS VARCHAR
   AS
   $$
    DECLARE MAX_CONCEPT NUMBER;
    BEGIN
        SELECT MAX(CONCEPT_ID)+1
        INTO :MAX_CONCEPT
        FROM CARE_RES_OMOP_DEV2_WKSP.OMOP.CONCEPT;

        IF  (MAX_CONCEPT < 2000000000) THEN
            BEGIN
                LET SQL TEXT := 'CREATE OR REPLACE SEQUENCE SEQ_2B START = 2000000000 INCREMENT = 1 ';
                EXECUTE IMMEDIATE SQL;
                RETURN SQL;
            END;
        ELSE
            BEGIN
                LET SQL TEXT := CONCAT('CREATE OR REPLACE SEQUENCE SEQ_2B START = ',
                                        (SELECT MAX(CONCEPT_ID)+1
                                        FROM CARE_RES_OMOP_DEV2_WKSP.OMOP.CONCEPT
                                        ORDER BY CONCEPT_ID DESC));
                EXECUTE IMMEDIATE SQL;
                RETURN SQL;
            END;
        END IF;
    END;

   $$
   ;
   --END CREATE SEQUENCE
 CALL CreateSequence();

--------------------------------------------------------------------------

--------------------------------------------------------------------------
/*
2. VOCABULARY Table: Create a row in Vocabulary for distinct values of the Vocabulary_ids in STCM
    Snapshot existing 2B data and join to STCM
    Delete records from Vocabulary that do not exist in the STCM & “CH_generated”
    Ignore records that do match
    Insert new records that do not exist in the Vocabulary
        VOCABULARY_ID = STCM.SOURCE_VOCABULARY_ID
        VOCABULARY_NAME = STCM.SOURCE_VOCABULARY_ID
        VOCABULARY_REFERENCE = "CH generated"
        VOCABULARY_VERSION = <current_date of the vocab update>
        VOCABULARY_CONCEPT_ID = 0
*/
--------------------------------------------------------------------------
--First drop rows in VOCABULARY that need to be updated:
CREATE OR REPLACE TEMPORARY TABLE VOCABULARY_EXISTING AS (
  SELECT *
  FROM VOCABULARY
);
DELETE FROM CARE_RES_OMOP_DEV2_WKSP.OMOP.VOCABULARY
WHERE VOCABULARY_REFERENCE = $CUSTOM_VOCABULARY_REFERENCE --'CH generated' --WANT TO REMOVE ONLY VOCABULARIES THAT WERE PREVIOUSLY CREATED IN THE STCM->C/CR PROCESS, BUT ARE NO LONGER IN STCM
    AND VOCABULARY_ID NOT IN (
      SELECT VOCABULARY_EXISTING.VOCABULARY_ID
      FROM VOCABULARY_EXISTING
      INNER JOIN SOURCE_TO_CONCEPT_MAP STCM     ON VOCABULARY_EXISTING.VOCABULARY_ID = STCM.SOURCE_VOCABULARY_ID
  );
---------------------------------------------------------------------------

---------------------------------------------------------------------------
--Insert new rows into VOCABULARY:
INSERT INTO CARE_RES_OMOP_DEV2_WKSP.OMOP.VOCABULARY
WITH existing_data as (
  SELECT
    VOCABULARY_ID
    ,VOCABULARY_NAME
    ,VOCABULARY_REFERENCE
    ,VOCABULARY_VERSION
    ,VOCABULARY_CONCEPT_ID
  FROM VOCABULARY
)
, inserts as (
  SELECT DISTINCT
        STCM.SOURCE_VOCABULARY_ID       AS VOCABULARY_ID
        ,STCM.SOURCE_VOCABULARY_ID      AS VOCABULARY_NAME
        ,$CUSTOM_VOCABULARY_REFERENCE   AS VOCABULARY_REFERENCE
        ,CURRENT_DATE()                 AS VOCABULARY_VERSION
        ,0                              AS VOCABULARY_CONCEPT_ID
  FROM SOURCE_TO_CONCEPT_MAP STCM
  LEFT JOIN existing_data ON STCM.SOURCE_VOCABULARY_ID = existing_data.VOCABULARY_ID AND STCM.SOURCE_VOCABULARY_ID = existing_data.VOCABULARY_NAME
  WHERE existing_data.VOCABULARY_ID IS NULL
)
select *
from inserts;
---------------------------------------------------------------------------

---------------------------------------------------------------------------
/*
3. CONCEPT table: Create 1 row in CONCEPT for each row in STCM
    Snapshot existing 2B data (existing_data CTE)
        Delete records from CONCEPT that do not exist in the STCM
    Check for new rows to insert or changed rows to update.
        Place these in a single CTE and MERGE them into existing CONCEPT table.
        INSERT new records that do not exist in the in existing_data (i.e., not matched in the MERGE)
            CONCEPT_ID = 2B_SEQ (becomes <assigned concept_id>)
            CONCEPT_NAME = STCM.SOURCE_CODE_DESCRIPTION
            DOMAIN_ID = CONCEPT.DOMAIN_ID
                                    (STCM.TARGET_CONCEPT_ID >--< CONCEPT.CONCEPT_ID)
            VOCABULARY_ID = STCM.SOURCE_VOCABULARY_ID
            CONCEPT_CLASS_ID = CONCEPT.CONCEPT_CLASS_ID
                                    (STCM.TARGET_CONCEPT_ID >--< CONCEPT.CONCEPT_ID)
            STANDARD_CONCEPT = NULL
            CONCEPT_CODE = STCM.SOURCE_CODE
            VALID_START_DATE = 1970-01-01
            VALID_END_DATE = 2099-12-31
            INVALID_REASON = NULL
        UPDATE any rows in existing_data have changed (potential changes that would require an update would be if the target concept in STCM changed)
            CONCEPT row will only be need updating if the target concept has a different DOMAIN_ID or CONCEPT_CLASS_ID then the existing/previous target concept
            Be sure to preserve the existing CONCEPT_ID for the concept, allowing for the row to be updated and not inserted.
  */
--FIRST DROP ROWS THAT NEED TO BE UPDATED:
-------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE CONCEPT_DROP AS
WITH existing_data as (
  SELECT
        CONCEPT_ID
        ,CONCEPT_NAME
        ,DOMAIN_ID
        ,VOCABULARY_ID
        ,CONCEPT_CLASS_ID
        ,STANDARD_CONCEPT
        ,CONCEPT_CODE
        ,VALID_START_DATE
        ,VALID_END_DATE
        ,INVALID_REASON
  FROM CONCEPT
)
, rows_to_drop as (
    SELECT existing_data.*
    FROM existing_data
    LEFT JOIN SOURCE_TO_CONCEPT_MAP STCM  ON existing_data.CONCEPT_NAME = STCM.SOURCE_CODE_DESCRIPTION
      AND existing_data.VOCABULARY_ID = STCM.SOURCE_VOCABULARY_ID
      AND existing_data.CONCEPT_CODE = STCM.SOURCE_CODE
    WHERE existing_data.vocabulary_id LIKE CONCAT($CUSTOM_SOURCE_VOCABULARY_ID_PREFIX, '%') AND STCM.SOURCE_CODE IS NULL
)
select * from rows_to_drop
;
DELETE FROM CARE_RES_OMOP_DEV2_WKSP.OMOP.CONCEPT
USING CONCEPT_DROP
WHERE CONCEPT.CONCEPT_ID = CONCEPT_DROP.CONCEPT_ID;
-------------------------------------------------------------------------

-------------------------------------------------------------------------
--Insert new and updated rows into CONCEPT:
CREATE OR REPLACE TEMPORARY TABLE CONCEPT_inserts_updates AS
WITH existing_data as (
  SELECT
        CONCEPT_ID
        ,CONCEPT_NAME
        ,DOMAIN_ID
        ,VOCABULARY_ID
        ,CONCEPT_CLASS_ID
        ,STANDARD_CONCEPT
        ,CONCEPT_CODE
        ,VALID_START_DATE
        ,VALID_END_DATE
        ,INVALID_REASON
  FROM CONCEPT
)
, inserts as (
  SELECT
        SEQ_2B.NEXTVAL                AS CONCEPT_ID
        ,SOURCE_CODE_DESCRIPTION      AS CONCEPT_NAME
        ,CONCEPT.DOMAIN_ID            AS DOMAIN_ID
        ,SOURCE_VOCABULARY_ID         AS VOCABULARY_ID
        ,CONCEPT.CONCEPT_CLASS_ID     AS CONCEPT_CLASS_ID
        ,NULL                         AS STANDARD_CONCEPT
        ,SOURCE_CODE                  AS CONCEPT_CODE
        ,TO_DATE('1970-01-01')        AS VALID_START_DATE
        ,TO_DATE('2099-12-31')        AS VALID_END_DATE
        ,NULL                         AS INVALID_REASON
  FROM SOURCE_TO_CONCEPT_MAP STCM
  INNER JOIN CONCEPT      ON STCM.TARGET_CONCEPT_ID = CONCEPT.CONCEPT_ID
  LEFT JOIN existing_data ON STCM.SOURCE_CODE_DESCRIPTION = existing_data.CONCEPT_NAME
    AND STCM.SOURCE_VOCABULARY_ID = existing_data.VOCABULARY_ID
    AND STCM.SOURCE_CODE = existing_data.CONCEPT_CODE
  WHERE existing_data.CONCEPT_ID IS NULL
)
, updates as (
  SELECT DISTINCT
       existing_data.CONCEPT_ID       AS CONCEPT_ID
        ,STCM.SOURCE_CODE_DESCRIPTION AS CONCEPT_NAME
        ,CONCEPT.DOMAIN_ID            AS DOMAIN_ID
        ,STCM.SOURCE_VOCABULARY_ID    AS VOCABULARY_ID
        ,CONCEPT.CONCEPT_CLASS_ID     AS CONCEPT_CLASS_ID
        ,NULL                         AS STANDARD_CONCEPT
        ,STCM.SOURCE_CODE             AS CONCEPT_CODE
        ,TO_DATE('1970-01-01')        AS VALID_START_DATE
        ,TO_DATE('2099-12-31')        AS VALID_END_DATE
        ,NULL                         AS INVALID_REASON
  FROM SOURCE_TO_CONCEPT_MAP STCM
  INNER JOIN CONCEPT       ON STCM.TARGET_CONCEPT_ID = CONCEPT.CONCEPT_ID
  INNER JOIN existing_data ON STCM.SOURCE_CODE_DESCRIPTION = existing_data.CONCEPT_NAME
    AND STCM.SOURCE_VOCABULARY_ID = existing_data.VOCABULARY_ID
    AND STCM.SOURCE_CODE = existing_data.CONCEPT_CODE
  WHERE --check for differences in existing data and current STCM
    existing_data.DOMAIN_ID <> CONCEPT.DOMAIN_ID  --target DOMAIN_ID has changed
    OR existing_data.CONCEPT_CLASS_ID <> CONCEPT.CONCEPT_CLASS_ID --target CONCEPT_CLASS_ID has changed
)
select  * from inserts
union all
select * from updates
;
MERGE INTO CONCEPT USING CONCEPT_inserts_updates c2
  ON CONCEPT.CONCEPT_ID = c2.CONCEPT_ID
  WHEN MATCHED THEN UPDATE SET   CONCEPT.CONCEPT_NAME = c2.CONCEPT_NAME
                                ,CONCEPT.DOMAIN_ID = c2.DOMAIN_ID
                                ,CONCEPT.VOCABULARY_ID = c2.VOCABULARY_ID
                                ,CONCEPT.CONCEPT_CLASS_ID = c2.CONCEPT_CLASS_ID
                                ,CONCEPT.STANDARD_CONCEPT = c2.STANDARD_CONCEPT
                                ,CONCEPT.CONCEPT_CODE = c2.CONCEPT_CODE
                                ,CONCEPT.VALID_START_DATE = c2.VALID_START_DATE
                                ,CONCEPT.VALID_END_DATE = c2.VALID_END_DATE
                                ,CONCEPT.INVALID_REASON = c2.INVALID_REASON
  WHEN NOT MATCHED THEN
    INSERT (CONCEPT_ID, CONCEPT_NAME, DOMAIN_ID, VOCABULARY_ID, CONCEPT_CLASS_ID, STANDARD_CONCEPT, CONCEPT_CODE, VALID_START_DATE, VALID_END_DATE, INVALID_REASON)
    VALUES (c2.CONCEPT_ID, c2.CONCEPT_NAME, c2.DOMAIN_ID, c2.VOCABULARY_ID, c2.CONCEPT_CLASS_ID, c2.STANDARD_CONCEPT, c2.CONCEPT_CODE, c2.VALID_START_DATE, c2.VALID_END_DATE, c2.INVALID_REASON)
;
-------------------------------------------------------------------------
/*
4. CONCEPT_RELATIONSHIP Table Create 2 records in Concept_Relationship for each record in STCM.
    Since we do not need to preserve CONCEPT_IDs, we can DELETE all mappings currently in CONCEPT_RELATIONSHIP that originated from our STCM.
        We do this by DELETING rows where VOCABULARY_ID like 'CH_%', as all our custom vocabularies start with "CH_".
        We also DELETE rows corresponding to concepts that do not currently exist in CONCEPT.
            This accounts for any concepts that were previously in STCM and mapped into CONCEPT/CONCEPT_RELATIONSHIP, but have since been removed from STCM and CONCEPT.
    This allows for mappings that have changed without having to MERGE/UPDATE the existing table.
    Insert STCM mappings
        “Maps to” standard concept.
            CONCEPT_ID_1 = <assigned concept_id>
            CONCEPT_ID_2 = STCM.TARGET_CONCEPT_ID
            RELATIONSHIP_ID = "Maps to"
        “Mapped from” standard concept.
            CONCEPT_ID_1 = STCM.TARGET_CONCEPT_ID
            CONCEPT_ID_2 = <assigned concept_id> S
            RELATIONSHIP_ID = "Mapped from"
    */

--FIRST DROP ROWS THAT NEED TO BE UPDATED:
DELETE FROM CONCEPT_RELATIONSHIP
WHERE CONCEPT_ID_1 IN (
  SELECT
  CONCEPT_ID
  FROM CONCEPT
  WHERE VOCABULARY_ID LIKE CONCAT($CUSTOM_SOURCE_VOCABULARY_ID_PREFIX, '%')
)
OR CONCEPT_ID_2 IN (
  SELECT
  CONCEPT_ID
  FROM CONCEPT
  WHERE VOCABULARY_ID LIKE CONCAT($CUSTOM_SOURCE_VOCABULARY_ID_PREFIX, '%')
)
OR CONCEPT_ID_1 NOT IN (SELECT CONCEPT_ID FROM CONCEPT)
OR CONCEPT_ID_2 NOT IN (SELECT CONCEPT_ID FROM CONCEPT)
;


--INSERT MAPPINGS THAT ARE CURRENTLY IN STCM:
INSERT INTO CONCEPT_RELATIONSHIP
WITH existing_data as (
  SELECT
    CONCEPT_ID_1
    ,CONCEPT_ID_2
    ,RELATIONSHIP_ID
    ,VALID_START_DATE
    ,VALID_END_DATE
    ,INVALID_REASON
  FROM CONCEPT_RELATIONSHIP
  WHERE CONCEPT_ID_1>=2000000000 OR CONCEPT_ID_2>=2000000000
)
, inserts as (
    SELECT
          CONCEPT.CONCEPT_ID          AS CONCEPT_ID_1
          ,STCM_2.TARGET_CONCEPT_ID   AS CONCEPT_ID_2
          ,'Maps to'                  AS RELATIONSHIP_ID
          ,TO_DATE('1970-01-01')      AS VALID_START_DATE
          ,TO_DATE('2099-12-31')      AS VALID_END_DATE
          ,NULL                       AS INVALID_REASON
    FROM CONCEPT
    INNER JOIN (
      SELECT
          SOURCE_CODE
          ,SOURCE_VOCABULARY_ID
          ,TARGET_CONCEPT_ID
      FROM SOURCE_TO_CONCEPT_MAP
    ) AS STCM_2           ON CONCEPT.CONCEPT_CODE = STCM_2.SOURCE_CODE AND CONCEPT.VOCABULARY_ID = STCM_2.SOURCE_VOCABULARY_ID
    UNION ALL
    SELECT
          STCM_2.TARGET_CONCEPT_ID    AS CONCEPT_ID_1
          ,CONCEPT.CONCEPT_ID         AS CONCEPT_ID_2
          ,'Maps from'                AS RELATIONSHIP_ID
          ,TO_DATE('1970-01-01')      AS VALID_START_DATE
          ,TO_DATE('2099-12-31')      AS VALID_END_DATE
          ,NULL                       AS INVALID_REASON
    FROM CONCEPT
    INNER JOIN (
      SELECT
          SOURCE_CODE
          ,SOURCE_VOCABULARY_ID
          ,TARGET_CONCEPT_ID
      FROM SOURCE_TO_CONCEPT_MAP
    ) AS STCM_2           ON CONCEPT.CONCEPT_CODE = STCM_2.SOURCE_CODE AND CONCEPT.VOCABULARY_ID = STCM_2.SOURCE_VOCABULARY_ID
)
select *
from inserts
;

