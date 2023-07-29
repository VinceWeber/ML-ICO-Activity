
/****** Object:  StoredProcedure [dbo].[ReportCustomerTurnover]    Script Date: 21/07/2023 09:08:26 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

--QUESTION 3 - PROCEDURE WHICH RETURNS A REPORT OF TURNOVER BY CUSTOMER
-- PROCEDURE TAKES 2 ARGUMENTS : CHOICE & YEAR 

CREATE PROCEDURE [dbo].[ReportCarePathActivtiy_By_actes] @CHOICE as int, @startdate as DATE, @enddate as DATE

AS
SET NOCOUNT ON;
DECLARE 

	--VARIABLES "INTERNES"
		--@startdate DATE,
		--@enddate DATE,
		@currentAggregator nvarchar(15),
		@currentAggregatorName nvarchar(10),
		@Mindate DATETIME,
		@Maxdate DATETIME,

		@Query nvarchar(3000),
		@Insert_Query nvarchar(1000),
		@Numberitteration int,
		@itterator int,
		@subitterator int,

		@MYSELECT nvarchar(500),
		@MYFROM nvarchar(500),
		@MYWHERE nvarchar(500),

		--@CHOICE int,
		@YEAR int


--ASSIGN VALUES TO VARIABLES :

		--SET @CHOICE=1
		--SET @YEAR=2019

	--VARIABLES "INTERNES"
		SET @startdate=CAST('2019-09-01' as DATE)
		SET @enddate=CAST('2019-09-30' as DATE)

		EXECUTE dbo.Create_Calendar  @startdate,@enddate



PRINT '---STARTING PROCEDURE WITH INPUT PARAMETERS @CHOICE ' + CAST(@CHOICE as varchar) + ' AND @YEAR ' + CAST( @YEAR as varchar) + ' ---'

--BLOC DE TEST DE CONDITIONS DES PARAMETRES
 
	IF @CHOICE<>4 
		SET @YEAR=COALESCE(@YEAR, 2018)    -- IF YEAR =NULL , YEAR  IS DEFINED TO 2018.
	ELSE
		BEGIN
			SET @CHOICE=COALESCE(@CHOICE, 1)	-- IF  CHOICE=NULL, CHOICE IS DEFINED TO 1, EXCEPT IF .
			SET @YEAR=COALESCE(@YEAR, 2013)
		END

	 IF @CHOICE=0    -- REPORT DAYLY turnover from stardate of calendar (1 column = 1 DAY), CURSOR LINES ARE DAYS
		BEGIN	
			DECLARE MyCursor CURSOR SCROLL FOR
										SELECT	DISTINCT([MYDATE]),  'J-' + CAST([MYDATENAME] as nvarchar), MIN([MYDATE]),MAX([MYDATE]) 
										FROM	[dbo].[MYCALENDAR]
										GROUP BY [MYDATE],'J-' + CAST([MYDATENAME] as nvarchar)
										ORDER BY 'J-' + CAST([MYDATENAME] as nvarchar) asc 
	
				SET @Numberitteration = (SELECT COUNT(DISTINCT([MYDATE])) FROM [dbo].[MYCALENDAR])
				PRINT '@Numberitteration' + CAST(@Numberitteration as varchar)
		END

	IF @CHOICE=1    -- REPORT WEEKLY turnover from stardate of calendar  (1 column = 1 WEEK), CURSOR LINES ARE WEEKS
		BEGIN	
			DECLARE MyCursor CURSOR SCROLL FOR
										SELECT	DISTINCT([MYYEARWEEK]), CAST([MYYEAR] as nvarchar) + '-W' + CAST([MYWEEK] as nvarchar), MIN([MYDATE]),MAX([MYDATE])
										FROM	[dbo].[MYCALENDAR]
										GROUP BY [MYYEARWEEK],CAST([MYYEAR] as nvarchar) + '-W' + CAST([MYWEEK] as nvarchar)
										ORDER BY CAST([MYYEAR] as nvarchar) + '-W' + CAST([MYWEEK] as nvarchar) asc 
	
				SET @Numberitteration = (SELECT COUNT(DISTINCT([MYYEARWEEK])) FROM [dbo].[MYCALENDAR])
				PRINT '@Numberitteration' + CAST(@Numberitteration as varchar)
	END


	 IF @CHOICE=2 OR @CHOICE=NULL   -- REPORT MONTHLY turnover from stardate of calendar  (1 column = 1 month), CURSOR LINES ARE MONTHS
		BEGIN	
			DECLARE MyCursor CURSOR SCROLL FOR
										SELECT	DISTINCT([MYYEARMONTH]), CAST([MYYEAR] as nvarchar) + '-M' + CAST([MYMONTH] as nvarchar), MIN([MYDATE]),MAX([MYDATE])
										FROM	[dbo].[MYCALENDAR]
										GROUP BY [MYYEARMONTH], CAST([MYYEAR] as nvarchar) + '-M' + CAST([MYMONTH] as nvarchar)
										ORDER BY [MYYEARMONTH] asc 
	
				SET @Numberitteration = (SELECT COUNT(DISTINCT([MYYEARMONTH])) FROM [dbo].[MYCALENDAR])
				PRINT '@Numberitteration' + CAST(@Numberitteration as varchar)
		END


	 IF @CHOICE=3				-- REPORT TRIMESTER turnover for the @YEAR (1 column = 3 months), CURSOR LINES ARE TRIMESTERS
		BEGIN	
			DECLARE MyCursor CURSOR SCROLL FOR
										SELECT	DISTINCT([MYYEARQUARTER]), CAST([MYYEAR] as nvarchar) + '-Q' + CAST([MYTRIMESTER] as nvarchar), MIN([MYDATE]),MAX([MYDATE])
										FROM	[dbo].[MYCALENDAR]
										GROUP BY [MYYEARQUARTER], CAST([MYYEAR] as nvarchar) + '-Q' + CAST([MYTRIMESTER] as nvarchar)
										ORDER BY [MYYEARQUARTER] asc 
	
				SET @Numberitteration = (SELECT COUNT(DISTINCT([MYYEARQUARTER])) FROM [dbo].[MYCALENDAR])
				PRINT '@Numberitteration' + CAST(@Numberitteration as varchar)
		END

	 IF @CHOICE=4				-- REPORT ALL YEAR turnovers (1 column = 1 Year), CURSOR LINES ARE YEARS
		BEGIN	
			DECLARE MyCursor CURSOR SCROLL FOR
										SELECT	DISTINCT([MYYEAR]), CAST([MYYEAR] as nvarchar), MIN([MYDATE]),MAX([MYDATE])
										FROM	[dbo].[MYCALENDAR]
										GROUP BY [MYYEAR],CAST([MYYEAR] as nvarchar)
										ORDER BY [MYYEAR] asc 
	
				SET @Numberitteration = (SELECT COUNT(DISTINCT([MYYEAR])) FROM [dbo].[MYCALENDAR])
				PRINT '@Numberitteration' + CAST(@Numberitteration as varchar)
		END

		
				--CREATE TEMPORARY TABLE (MYREPORT) TO STORE THE RESULT OF THE QUERY	
					IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[MYREPORT]') AND type in (N'U'))
						BEGIN
							DROP TABLE [dbo].[MYREPORT]
						END
						
					SET @itterator=1
					SET @Query= 'CREATE TABLE [dbo].[MYREPORT] (
						[NIP] nvarchar(12)  NOT NULL'
				
					WHILE @itterator<=@Numberitteration	
						BEGIN
							SET @Query=	@Query + ',[TURNOVER_' + CAST(@itterator as varchar) + ']  NVARCHAR(max)  NOT NULL'  -- HAS TO BE CREATED ACCORDING TO NUMBER OF ROWS OF THE CURSOR
							SET @itterator+=1
						END
					SET  @Query= @Query+ ')'
					PRINT '@Query Myreport : ' + @Query
					EXEC (@Query);

				--CREATE THE BEGINNING OF THE INSERT QUERY AND SAVE IT INTO @INSRT_QUERY VARIABLE	
					SET @itterator=1;
					SET @Insert_Query= 'INSERT INTO [dbo].[MYREPORT] (
						[NIP]'

					WHILE @itterator<=@Numberitteration	
						BEGIN
							SET @Insert_Query=	@Insert_Query + ',[TURNOVER_' + CAST(@itterator as varchar) + ']'   -- HAS TO BE CREATED ACCORDING TO NUMBER OF ROWS OF THE CURSOR
							SET @itterator+=1
						END
					SET  @Insert_Query= @Insert_Query + ')'

				OPEN MyCursor
				FETCH NEXT FROM MyCursor INTO @currentAggregator
									, @currentAggregatorName,@Mindate,@MaxDate

				PRINT 'START @currentMonth= : ' + CAST(@currentAggregator as nvarchar(10))

				--LOOP WHICH CREATE QUERY FILLING THE CURRENT AGGREGATOR (MONTH,TRIMESTER, YEAR, or other defined by Cursor from the Calendar Table) AND AND PUT ZERO TO OTHERS

				SET @itterator=1
				
				WHILE @@FETCH_STATUS = 0
				BEGIN
					--DEFINE THE SELECT CLAUSE (MAIN QUERY) WITH THE ACTIVE MONTH
						SET @subitterator=1
						PRINT '@itterator : ' + CAST(@itterator as varchar)

						SET @Query= @Insert_Query + 'SELECT NIP as NIP ' 

						WHILE @subitterator<=@Numberitteration
							BEGIN
								IF @itterator=@subitterator
									BEGIN
										SET @Query= @Query+ ',STRING_AGG(Base.Actes_Serie,''-'') ' --as M' + CAST(@subitterator as varchar) + ' ' -- PUT THE ACTIVE (MONTH,TRIMESTER, YEAR)
									END
								ELSE
									BEGIN
										SET @Query= @Query+ ',''''' --+ CAST(@subitterator as varchar) + ' ' -- PUT OTHER (MONTH, TRIMESTER, YEAR)
									END
								SET @subitterator+=1
							END
						PRINT 'SELECT months @Query : ' + @Query
						
					--DEFINITION DE LA CLAUSE -FROM (MAINQUERY) ( + SELECT (SUBQUERY))
						SET @Query= @Query + '
						FROM (
							SELECT Table_Actes.[NIP]
								  --,Table_Actes.[ID_A]
								  ,STRING_AGG(Table_Encoded_Actes.Cle_Acte_Encoded,''-'')  WITHIN GROUP(ORDER BY Table_Actes.DD_A ASC,CONCAT(UFX,R_CCAM,R_NGAP,UFH)  ASC) as Actes_Serie
								  --,Table_Actes.[N_S]
								  --,Table_Encoded_Sejours.Cle_Sejour_Encoded
								  --,Table_Encoded_Sequence.id_Sequence
								  --,Table_Encoded_Sequence.Cle_Sequence_Encoded
								  --,Table_Encoded_Parcours.Cle_Parcours_Encoded
								  --,Table_Actes.[DD_A]
								  --,[UFX]
								  --,[INX]
								  --,[R_NGAP]
								  --,[R_CCAM]
								  --,[UFH]
								  --,[Site]
							  FROM [ICO_Activite].[dbo].[Tmp_A_Actes_Table_Analyse] as Table_Actes,
								   [ICO_Activite].[dbo].[Tmp_Acte_Encoded] as Table_Encoded_Actes,
								   [ICO_Activite].[dbo].[Tmp_Sejour_Encoded] as Table_Encoded_Sejours,
								   [ICO_Activite].[dbo].[Tmp_Sequence_Encoded] as Table_Encoded_Sequence,
								   [ICO_Activite].[dbo].[Tmp_Type_Sequence] as Table_Type_Seq,
								   [ICO_Activite].[dbo].[Tmp_Parcours_Encoded] as Table_Encoded_Parcours

							  WHERE Table_Actes.ID_A=Table_Encoded_Actes.ID_A AND
									Table_Actes.N_S=Table_Encoded_Sejours.N_S AND
									Table_Actes.N_S=Table_Type_Seq.N_S AND
									Table_Type_Seq.id_Sequence=Table_Encoded_Sequence.id_Sequence AND
									Table_Actes.NIP=Table_Encoded_Parcours.NIP '
			
						SET @Query= @Query + 'AND Table_Actes.DD_A BETWEEN ''' + CONVERT(nvarchar(20),@Mindate,120) + ''' AND  ''' + CONVERT(nvarchar(20),@Maxdate,120) + ''''
	 
						SET @Query= @Query + 'GROUP BY Table_Actes.[NIP] ) as Base '
						

					--DEFINITION DE LA CLAUSE GROUPBY (MAIN QUERY) 
						SET @Query= @Query + 'GROUP BY Base.[NIP]'  
						PRINT 'QUERY INSERT TO REPORT N  '+ CAST(@itterator as varchar) + ' @Query : ' + @Query
						
					--EXECUTION DE LA REQUETE	
						EXEC(@Query);
						  
						SET @itterator+=1
						  
						  
						FETCH NEXT FROM MyCursor INTO @currentAggregator
											, @currentAggregatorName,@Mindate,@MaxDate
				END	

	
		--FINAL QUERY TO EXPORT DATAS		
			--RESTART CURSOR
				FETCH FIRST FROM MyCursor INTO @currentAggregator
													, @currentAggregatorName,@Mindate,@MaxDate

				
				SET @itterator=1
				SET @Query= 'SELECT [NIP] AS NIP'   -- SELECT CLAUSE OF THE QUERY
				SET @Insert_Query = ''


				WHILE @@FETCH_STATUS = 0 -- WHILE @Numberitteration-@itterator<=0 
					BEGIN
						SET @Query=	@Query + ',STRING_AGG([TURNOVER_' + CAST(@itterator as varchar) + '],'''') as  [' + CAST(@currentAggregatorName as varchar) + ']'
						SET @Insert_Query= @Insert_Query + ', '''' as  [' + CAST(@currentAggregatorName as varchar) + ']' 
						SET @itterator+=1
					
						FETCH NEXT FROM MyCursor INTO @currentAggregator
														, @currentAggregatorName,@Mindate,@MaxDate
					END


				SET @Query = @Query + ' FROM [ICO_Activite].[dbo].[MYREPORT]  GROUP BY NIP '  --FROM + GROUPBY CLAUSES OF THE QUERY
				
			--ADD THE CUSTOMERS WHO DO NOT HAVE PURCHASE ANYTHING
				SET @Query = @Query + ' UNION 
						SELECT 	C.NIP' + @Insert_Query +
						' FROM [ICO_Activite].[dbo].[Tmp_A_Actes_Table_Analyse] as C
						WHERE C.NIP NOT IN (SELECT [NIP] FROM [ICO_Activite].[dbo].[MYREPORT]) ORDER BY NIP asc'

				PRINT 'FINAL @Query Myreport : ' + @Query
				EXEC (@Query);

			--CLOSE CURSOR
				CLOSE MyCursor
				DEALLOCATE MyCursor
/*
		--DELETE TEMPORARY TABLES
			IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[MYREPORT]') AND type in (N'U'))
			BEGIN
			 	DROP TABLE [dbo].[MYREPORT]
			END
			*/
