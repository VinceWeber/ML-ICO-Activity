USE [ICO_Activite]
GO

DECLARE @RC int
DECLARE @CHOICE int
DECLARE @startdate date
DECLARE @enddate date

-- TODO: Set parameter values here.

EXECUTE @RC = [dbo].[ReportCarePathActivtiy_By_actes] 
   2 --PARAMETER 1 :CHOICE
   		-- 0 : REPORT DAYLY turnover from stardate of calendar (1 column = 1 DAY), CURSOR LINES ARE DAYS
		-- 1 : REPORT WEEKLY turnover from stardate of calendar  (1 column = 1 WEEK), CURSOR LINES ARE WEEKS
		-- 2 : REPORT MONTHLY turnover from stardate of calendar  (1 column = 1 month), CURSOR LINES ARE MONTHS
		-- 3 : REPORT TRIMESTER turnover for the @YEAR (1 column = 3 months), CURSOR LINES ARE TRIMESTERS
		-- 4 : REPORT ALL YEAR turnovers (1 column = 1 Year), CURSOR LINES ARE YEARS
  ,'2019-05-01 00:00:00.000' --PARAMETER 2 : @startdate --OK
  ,'2019-05-10 00:00:00.000' --PARAMETER 3 : @enddate --OK
  ,'PARCOURS' --PARAMETER 3: @AggParameter as nvarchar(20)
				--TYPE D'AGGREGATION (PAR DATE CALENDAIRE, PAR DATE PARCOURS)
				--CALENDAR
				--PARCOURS
  ,''
  ,0 --PARAMETER 4 : @AggParameter_min as int (NB jours)
  ,10 --PARAMETER 5 : @AggParameter_max as int (NB jours)
  ,'COUNT'	-- PARAMETER 4 : @AggMeth as nvarchar(20) --OK
		--PRESENCE : -> FUNCTION COUNT
		--COUNT -> FUNCTION SUM
		--DENSITE -> Function SUM / Length of aggregation choice (1, 7, 30.41 , 365.25)
  ,'0'	-- PARAMETER 5  @Filter1_type as nvarchar(20)
		--0-ALL
		--1-SERVICE -> from Listing_UF_V3
		--2-ACTIVITE -> from Listing_UF_V3
		--3-PHASE PARCOURS -> from Listing_UF_V3
		--4-DIMENSION PARCOURS -> from Listing_UF_V3
		--5-POIDS ACTE -> from Listing_UF_V3
		--6-TYPE DE SEQUENCE -> from Tmp_Type_Seq
		--7-TYPE DE PARCOURS-- -> (After Clustering only)
		--8-R_NGAP
		--9-R_CCAM
		--10-Statut
		--11-UFH
		--12-INX

	,'' -- PARAMETER 6  @@Filter1_value as nvarchar(20)
	,'0'	-- PARAMETER 7  @@Filter2_type as nvarchar(20)
		--0-ALL
		--1-SERVICE -> from Listing_UF_V3
		--2-ACTIVITE -> from Listing_UF_V3
		--3-PHASE PARCOURS -> from Listing_UF_V3
		--4-DIMENSION PARCOURS -> from Listing_UF_V3
		--5-POIDS ACTE -> from Listing_UF_V3
		--6-TYPE DE SEQUENCE -> from Tmp_Type_Seq
		--7-TYPE DE PARCOURS-- -> (After Clustering only)
		--8-R_NGAP
		--9-R_CCAM
		--10-Statut
		--11-UFH
		--12-INX

	,'VALUE' -- PARAMETER 8  @@Filter2_type as nvarchar(20)
	,'V3'  --PARAMETER 9 @TypeJ0 as nvarchar(20)
		--V1
		--V2
		--V3
		--V4
GO
