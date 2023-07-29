USE [ICO_Activite]
GO
/****** Object:  StoredProcedure [dbo].[Create_Calendar]    Script Date: 29/07/2023 12:29:49 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

--QUESTION 3 - PROCEDURE WHICH RETURNS A REPORT OF TURNOVER BY CUSTOMER
-- PROCEDURE TAKES 2 ARGUMENTS : CHOICE & YEAR 

CREATE PROCEDURE [dbo].[Create_Calendar]  @startdate DATE, @enddate DATE


AS
SET NOCOUNT ON;
DECLARE 

	--VARIABLES "INTERNES"
		--@startdate DATE,
		--@enddate DATE,
		@currentAggregator int,
		@currentAggregatorName nvarchar(10),

		@Query nvarchar(3000),
		@Insert_Query nvarchar(1000),
		@Numberitteration int,
		@itterator int,
		@subitterator int,

		@MYSELECT nvarchar(500),
		@MYFROM nvarchar(500),
		@MYWHERE nvarchar(500),

		@CHOICE int,
		@YEAR int


--ASSIGN VALUES TO VARIABLES :

		SET @CHOICE=1
		SET @YEAR=2019

	--VARIABLES "INTERNES"
		--SET @startdate=CAST('1900-01-01' as DATE)
		--SET @enddate=CAST('1910-12-31' as DATE)
		--SET @enddate=DATEADD(dd,@nb_day,@startdate) 


PRINT '---STARTING PROCEDURE WITH INPUT PARAMETERS @CHOICE ' + CAST(@CHOICE as varchar) + ' AND @YEAR ' + CAST( @YEAR as varchar) + ' ---'

-- CREATE CALENDAR TABLE TO DEFINE A TEMPLATE OF REPORT (BY month = LIst of all months of the YEAR : Jan, Fev, Mar, Apr, Mai, Jun, Jul, Aug, Sep, Oct, Nov, Dec)
		
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[MYCALENDAR]') AND type in (N'U'))
			BEGIN
				DROP TABLE [dbo].[Tmp_MYCALENDAR]
			END
		
		CREATE TABLE [dbo].[Tmp_MYCALENDAR] (
			[MYDATE][nvarchar](12) NOT NULL,
			[MYDATENAME] [nvarchar](10) NOT NULL,
			[MYYEAR] [int] NOT NULL,
			[MYMONTH] [int] NOT NULL,
			[MYMONTHName] [nvarchar](10) NOT NULL,
			[MYTRIMESTER] [nvarchar](10) NOT NULL,
			[MYWEEK] [int] NOT NULL,
			[MYYEARWEEK] [varchar](10) NOT NULL,
			[MYYEARMONTH] [varchar](10) NOT NULL,
			[MYYEARQUARTER] [varchar](10) NOT NULL
			)

		SET @itterator=0
		--LOOP TO CREATE THE CALENDAR TABLE FROM @STARTYEAR TO @ENDYEAR, all days 
		WHILE @startdate<=@enddate
			BEGIN
					INSERT INTO  [dbo].[Tmp_MYCALENDAR]([MYDATE],[MYDATENAME],[MYYEAR],[MYMONTH],[MYMONTHName],[MYTRIMESTER],[MYWEEK],[MYYEARWEEK],[MYYEARMONTH],[MYYEARQUARTER])
					VALUES	(@startdate,FORMAT(@itterator, '00000#') ,
							DATENAME(YYYY, @startdate),
							
							MONTH(@startdate), LEFT(DATENAME(m, @startdate),3),
							DATENAME(qq,@startdate),
							DATEPART(ww,@startdate),
							CONCAT( CAST(DATENAME(YYYY, @startdate) as nvarchar(4)),'-',FORMAT(DATEPART(ww,@startdate),'0#')),
							CONCAT( CAST(DATENAME(YYYY, @startdate) as nvarchar(4)),'-',FORMAT(MONTH(@startdate),'0#')),
							CONCAT( CAST(DATENAME(YYYY, @startdate) as nvarchar(4)),'-',FORMAT(DATEPART(qq,@startdate),'#'))
							);

					SET @startdate= DATEADD(dd,1,@startdate) 
					SET @itterator+=1;
					--PRINT '@startdate' + CAST(@startdate as varchar) + ' / @endtdate' +  CAST(@enddate as varchar);
					--PRINT CAST(DATEADD(dd,1,@startdate) as varchar);
			END
GO
