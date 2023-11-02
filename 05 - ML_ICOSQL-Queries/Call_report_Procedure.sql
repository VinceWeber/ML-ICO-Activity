USE [ICO_Activite]
GO
/****** Object:  StoredProcedure [dbo].[ReportCarePathActivtiy_By_actes]    Script Date: 30/10/2023 11:28:50 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

--QUESTION 3 - PROCEDURE WHICH RETURNS A REPORT OF TURNOVER BY CUSTOMER
-- PROCEDURE TAKES 2 ARGUMENTS : CHOICE & YEAR 

ALTER PROCEDURE [dbo].[ReportCarePathActivtiy_By_actes_Calendar] 
										@CHOICE as int --OK 
												-- 0 : REPORT DAYLY turnover from stardate of calendar (1 column = 1 DAY), CURSOR LINES ARE DAYS
												-- 1 : REPORT WEEKLY turnover from stardate of calendar  (1 column = 1 WEEK), CURSOR LINES ARE WEEKS
												-- 2 : REPORT MONTHLY turnover from stardate of calendar  (1 column = 1 month), CURSOR LINES ARE MONTHS
												-- 3 : REPORT TRIMESTER turnover for the @YEAR (1 column = 3 months), CURSOR LINES ARE TRIMESTERS
												-- 4 : REPORT ALL YEAR turnovers (1 column = 1 Year), CURSOR LINES ARE YEARS
										,@startdate as DATE --OK
										,@enddate as DATE --OK
										,@AggParameter as nvarchar(20)
												--CONDITION SUR LE TYPE D'AGGREGATION (PAR DATE CALENDAIRE, PAR DATE PARCOURS)
												--CALENDAR
												--PARCOURS
										,@AggParameter_min as int
										,@AggParameter_max as int
										,@AggMeth as nvarchar(20) -- OK
												--PRESENCE : -> FUNCTION COUNT
												--COUNT -> FUNCTION SUM
												--DENSITE -> Function SUM / Length of aggregation choice (1, 7, 30.41 , 365.25)
										,@Filter1_type as nvarchar(20)
												--0-ALL
												--1-SERVICE -> from Listing_UF_V3
												--2-ACTIVITE -> from Listing_UF_V3
												--3-PHASE PARCOURS -> from Listing_UF_V3
												--4-DIMENSION PARCOURS -> from Listing_UF_V3
												--5-POIDS ACTE -> from Listing_UF_V3
												--6-TYPE DE SEQUENCE -> from Tmp_Type_Seq
												--7-TYPE DE PARCOURS-- -> (After Clustering only)
										,@Filter1_value as nvarchar(20)
										,@Filter2_type as nvarchar(40)
												--0-ALL
												--1-SERVICE -> from Listing_UF_V3
												--2-ACTIVITE -> from Listing_UF_V3
												--3-PHASE PARCOURS -> from Listing_UF_V3
												--4-DIMENSION PARCOURS -> from Listing_UF_V3
												--5-POIDS ACTE -> from Listing_UF_V3
												--6-TYPE DE SEQUENCE -> from Tmp_Type_Seq
												--7-TYPE DE PARCOURS-- -> (After Clustering only)
										,@Filter2_value as nvarchar(40)
										,@TypeJ0 as nvarchar(20)
												--V1
												--V2
												--V3
												--V4


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

		@Query nvarchar(max),
		@Insert_Query nvarchar(max),
		@Numberitteration int,
		@itterator int,
		@subitterator int,
		@lenghtagg float,
		@filtre1_query_FROM nvarchar(200),
		@filtre1_query_WHERE nvarchar(200),
		@filtre2_query_FROM nvarchar(200),
		@filtre2_query_WHERE nvarchar(200),

		@MYSELECT nvarchar(500),
		@MYFROM nvarchar(500),
		@MYWHERE nvarchar(500),
		@ErrorMEss nvarchar(200),

		--@CHOICE int,
		@YEAR int


--ASSIGN VALUES TO VARIABLES :

		--SET @CHOICE=1
		--SET @YEAR=2019

	--VARIABLES "INTERNES"
		--SET @startdate=CAST('2019-09-01' as DATE)
		--SET @enddate=CAST('2019-09-30' as DATE)
		IF @CHOICE=0
			BEGIN
				SET @lenghtagg=1.0
			END
		IF @CHOICE=1
			BEGIN
				SET @lenghtagg=7.0
			END
		IF @CHOICE=3
			BEGIN
				SET @lenghtagg=30.41
			END
		IF @CHOICE=4
			BEGIN
				SET @lenghtagg=365.25
			END

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
										FROM	[dbo].[Tmp_MYCALENDAR]
										GROUP BY [MYDATE],'J-' + CAST([MYDATENAME] as nvarchar)
										ORDER BY 'J-' + CAST([MYDATENAME] as nvarchar) asc 
	
				SET @Numberitteration = (SELECT COUNT(DISTINCT([MYDATE])) FROM [dbo].[Tmp_MYCALENDAR])
				PRINT '@Numberitteration' + CAST(@Numberitteration as nvarchar)
		END

	IF @CHOICE=1    -- REPORT WEEKLY turnover from stardate of calendar  (1 column = 1 WEEK), CURSOR LINES ARE WEEKS
		BEGIN	
			DECLARE MyCursor CURSOR SCROLL FOR
										SELECT	DISTINCT([MYYEARWEEK]), CAST([MYYEAR] as nvarchar) + '-W' + CAST([MYWEEK] as nvarchar), MIN([MYDATE]),MAX([MYDATE])
										FROM	[dbo].[Tmp_MYCALENDAR]
										GROUP BY [MYYEARWEEK],CAST([MYYEAR] as nvarchar) + '-W' + CAST([MYWEEK] as nvarchar)
										ORDER BY CAST([MYYEAR] as nvarchar) + '-W' + CAST([MYWEEK] as nvarchar) asc 
	
				SET @Numberitteration = (SELECT COUNT(DISTINCT([MYYEARWEEK])) FROM [dbo].[Tmp_MYCALENDAR])
				PRINT '@Numberitteration' + CAST(@Numberitteration as nvarchar)
	END


	 IF @CHOICE=2 OR @CHOICE=NULL   -- REPORT MONTHLY turnover from stardate of calendar  (1 column = 1 month), CURSOR LINES ARE MONTHS
		BEGIN	
			DECLARE MyCursor CURSOR SCROLL FOR
										SELECT	DISTINCT([MYYEARMONTH]), CAST([MYYEAR] as nvarchar) + '-M' + CAST([MYMONTH] as nvarchar), MIN([MYDATE]),MAX([MYDATE])
										FROM	[dbo].[Tmp_MYCALENDAR]
										GROUP BY [MYYEARMONTH], CAST([MYYEAR] as nvarchar) + '-M' + CAST([MYMONTH] as nvarchar)
										ORDER BY [MYYEARMONTH] asc 
	
				SET @Numberitteration = (SELECT COUNT(DISTINCT([MYYEARMONTH])) FROM [dbo].[Tmp_MYCALENDAR])
				PRINT '@Numberitteration' + CAST(@Numberitteration as nvarchar)
		END


	 IF @CHOICE=3				-- REPORT TRIMESTER turnover for the @YEAR (1 column = 3 months), CURSOR LINES ARE TRIMESTERS
		BEGIN	
			DECLARE MyCursor CURSOR SCROLL FOR
										SELECT	DISTINCT([MYYEARQUARTER]), CAST([MYYEAR] as nvarchar) + '-Q' + CAST([MYTRIMESTER] as nvarchar), MIN([MYDATE]),MAX([MYDATE])
										FROM	[dbo].[Tmp_MYCALENDAR]
										GROUP BY [MYYEARQUARTER], CAST([MYYEAR] as nvarchar) + '-Q' + CAST([MYTRIMESTER] as nvarchar)
										ORDER BY [MYYEARQUARTER] asc 
	
				SET @Numberitteration = (SELECT COUNT(DISTINCT([MYYEARQUARTER])) FROM [dbo].[Tmp_MYCALENDAR])
				PRINT '@Numberitteration' + CAST(@Numberitteration as nvarchar)
		END

	 IF @CHOICE=4				-- REPORT ALL YEAR turnovers (1 column = 1 Year), CURSOR LINES ARE YEARS
		BEGIN	
			DECLARE MyCursor CURSOR SCROLL FOR
										SELECT	DISTINCT([MYYEAR]), CAST([MYYEAR] as nvarchar), MIN([MYDATE]),MAX([MYDATE])
										FROM	[dbo].[Tmp_MYCALENDAR]
										GROUP BY [MYYEAR],CAST([MYYEAR] as nvarchar)
										ORDER BY [MYYEAR] asc 
	
				SET @Numberitteration = (SELECT COUNT(DISTINCT([MYYEAR])) FROM [dbo].[Tmp_MYCALENDAR])
				PRINT '@Numberitteration' + CAST(@Numberitteration as nvarchar)
		END

	-- PREPARE Filter Query to be added
		--AJOUTER ICI LES CONDITIONS DE FILTRE 1
						SET @ErrorMEss='ERROR ON Filter1 Parameter in ReportCarePathActivtiy_By_actes procedure .' + 
													@Filter1_type + ' was given,but an integer from 0 to 7 is expected'
							--0-ALL
							--1-SERVICE -> from Listing_UF_V3
							--2-ACTIVITE -> from Listing_UF_V3
							--3-PHASE PARCOURS -> from Listing_UF_V3
							--4-DIMENSION PARCOURS -> from Listing_UF_V3
							--5-POIDS ACTE -> from Listing_UF_V3
							--6-TYPE DE SEQUENCE -> from Tmp_Type_Seq
							--7-TYPE DE PARCOURS-- -> (After Clustering only)

							IF @Filter1_type='1'
								BEGIN 
									SET @filtre1_query_FROM= ',[ICO_Activite].[dbo].[Listing_UF_V3] as T_UFX '  + ' ' -- ADD A FILTER BY UFX.Service
									SET @filtre1_query_WHERE= 'AND Table_Actes.UFX=T_UFX.UFX_Code AND T_UFX.Service=''' + @Filter1_value + ''' '
								END
							ELSE
								IF @Filter1_type='2'
									BEGIN 
										SET @filtre1_query_FROM= ',[ICO_Activite].[dbo].[Listing_UF_V3] as T_UFX '  + ' ' -- ADD A FILTER BY UFX.Activite
										SET @filtre1_query_WHERE= 'AND Table_Actes.UFX=T_UFX.UFX_Code AND T_UFX.Activite=''' + @Filter1_value+ ''' '
									END
								ELSE
									IF @Filter1_type='3'
										BEGIN 
											SET @filtre1_query_FROM= ',[ICO_Activite].[dbo].[Listing_UF_V3] as T_UFX '  + ' ' -- ADD A FILTER BY UFX.Phase_Parcours
											SET @filtre1_query_WHERE= 'AND Table_Actes.UFX=T_UFX.UFX_Code AND T_UFX.Phase_Parcours=''' + @Filter1_value+ ''' '
										END
									ELSE
										IF @Filter1_type='4'
											BEGIN 
												SET @filtre1_query_FROM= ',[ICO_Activite].[dbo].[Listing_UF_V3] as T_UFX '  + ' ' -- ADD A FILTER BY UFX.Dimension_Parcours
												SET @filtre1_query_WHERE= 'AND Table_Actes.UFX=T_UFX.UFX_Code AND T_UFX.Dimension_Parcours=''' + @Filter1_value+ ''' '
											END
										ELSE
											IF @Filter1_type='5'
												BEGIN 
													SET @filtre1_query_FROM= ',[ICO_Activite].[dbo].[Listing_UF_V3] as T_UFX ' + ' ' -- ADD A FILTER BY UFX.Poids_Acte
													SET @filtre1_query_WHERE= 'AND Table_Actes.UFX=T_UFX.UFX_Code AND T_UFX.Poids_acte=''' + @Filter1_value+ ''' '
												END
											ELSE
												IF @Filter1_type='6'
													BEGIN 
														SET @filtre1_query_FROM= '' -- ADD A FILTER BY UFX.Type_Sequence
														SET @filtre1_query_WHERE= 'AND Table_Type_Seq.Type_Sequence=''' + @Filter1_value+ ''' '
													END
												ELSE
													IF @Filter1_type='7'
														BEGIN 
															SET @filtre1_query_FROM= 'Not Provided yet'  -- ADD A FILTER BY UFX.Type_parcours
															SET @filtre1_query_WHERE= 'Not Provided yet' + @Filter1_value+ ''' '
														END
													ELSE
														IF @Filter1_type='0'
															BEGIN
																SET @filtre1_query_FROM= ''
																SET @filtre1_query_WHERE=''
															END
														ELSE
															RAISERROR(15600,-1,-1,@ErrorMEss)

															PRINT 'My Filter1 FROM ' + @filtre1_query_FROM
															PRINT 'My Filter1 WHERE ' + @filtre1_query_WHERE

		--AJOUTER ICI LES CONDITIONS DE FILTRE 2
						SET @ErrorMEss='ERROR ON Filter2 Parameter in ReportCarePathActivtiy_By_actes procedure .' + 
													@Filter2_type + ' was given,but an integer from 0 to 7 is expected'
							--0-ALL
							--1-SERVICE -> from Listing_UF_V3
							--2-ACTIVITE -> from Listing_UF_V3
							--3-PHASE PARCOURS -> from Listing_UF_V3
							--4-DIMENSION PARCOURS -> from Listing_UF_V3
							--5-POIDS ACTE -> from Listing_UF_V3
							--6-TYPE DE SEQUENCE -> from Tmp_Type_Seq
							--7-TYPE DE PARCOURS-- -> (After Clustering only)

							IF @Filter2_type='1'
								BEGIN 
									SET @filtre2_query_FROM= ',[ICO_Activite].[dbo].[Listing_UF_V3] as T_UFX '  + ' ' -- ADD A FILTER BY UFX.Service
									SET @filtre2_query_WHERE= 'AND Table_Actes.UFX=T_UFX.UFX_Code AND T_UFX.Service=''' + @filter2_value+ ''' '
								END
							ELSE
								IF @filter2_type='2'
									BEGIN 
										SET @filtre2_query_FROM= ',[ICO_Activite].[dbo].[Listing_UF_V3] as T_UFX '  + ' ' -- ADD A FILTER BY UFX.Activite
										SET @filtre2_query_WHERE= 'AND Table_Actes.UFX=T_UFX.UFX_Code AND T_UFX.Activite=''' + @filter2_value+ ''' '
									END
								ELSE
									IF @filter2_type='3'
										BEGIN 
											SET @filtre2_query_FROM= ',[ICO_Activite].[dbo].[Listing_UF_V3] as T_UFX ' +  ' ' -- ADD A FILTER BY UFX.Phase_Parcours
											SET @filtre2_query_WHERE= 'AND Table_Actes.UFX=T_UFX.UFX_Code AND T_UFX.Phase_Parcours=''' + @filter2_value+ ''' '
										END
									ELSE
										IF @filter2_type='4'
											BEGIN 
												SET @filtre2_query_FROM= ',[ICO_Activite].[dbo].[Listing_UF_V3] as T_UFX ' +  ' ' -- ADD A FILTER BY UFX.Dimension_Parcours
												SET @filtre2_query_WHERE= 'AND Table_Actes.UFX=T_UFX.UFX_Code AND T_UFX.Dimension_Parcours=''' + @filter2_value+ ''' '
											END
										ELSE
											IF @filter2_type='5'
												BEGIN 
													SET @filtre2_query_FROM= ',[ICO_Activite].[dbo].[Listing_UF_V3] as T_UFX ' +  ' ' -- ADD A FILTER BY UFX.Poids_Acte
													SET @filtre2_query_WHERE= 'AND Table_Actes.UFX=T_UFX.UFX_Code AND T_UFX.Poids_acte=''' + @filter2_value+ ''' '
												END
											ELSE
												IF @filter2_type='6'
													BEGIN 
														SET @filtre2_query_FROM= '' -- ADD A FILTER BY UFX.Type_Sequence
														SET @filtre2_query_WHERE= 'AND Table_Type_Seq.Type_Sequence=''' + @filter2_value+ ''' '
													END
												ELSE
													IF @filter2_type='7'
														BEGIN 
															SET @filtre2_query_FROM= 'Not Provided yet'  -- ADD A FILTER BY UFX.Type_parcours
															SET @filtre2_query_WHERE= 'Not Provided yet' + @filter2_value+ ''' '
														END
													ELSE
														IF @filter2_type='0'
															BEGIN
																SET @filtre2_query_FROM= ''
																SET @filtre2_query_WHERE=''
															END
														ELSE
															RAISERROR(15600,-1,-1,@ErrorMEss)

															PRINT 'My filter2 FROM ' + @filtre2_query_FROM
															PRINT 'My filter2 WHERE ' + @filtre2_query_WHERE


				--CREATE TEMPORARY TABLE (MYREPORT) TO STORE THE RESULT OF THE QUERY	
					IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Tmp_MYREPORT]') AND type in (N'U'))
						BEGIN
							DROP TABLE [dbo].[Tmp_MYREPORT]
						END
						
					SET @itterator=1
					SET @Query= 'CREATE TABLE [dbo].[Tmp_MYREPORT] (
						[NIP] nvarchar(12)  NOT NULL'
				
					WHILE @itterator<=@Numberitteration	
						BEGIN
							SET @Query=	@Query + ',[TURNOVER_' + CAST(@itterator as nvarchar) + ']  NVARCHAR(max)  NOT NULL'  -- HAS TO BE CREATED ACCORDING TO NUMBER OF ROWS OF THE CURSOR
							SET @itterator+=1
						END
					SET  @Query= @Query+ ')'
					PRINT '@Query Myreport : ' + @Query
					EXEC (@Query);

				--CREATE THE BEGINNING OF THE INSERT QUERY AND SAVE IT INTO @INSRT_QUERY VARIABLE	
					SET @itterator=1;
					SET @Insert_Query= 'INSERT INTO [dbo].[Tmp_MYREPORT] (
						[NIP]'

					WHILE @itterator<=@Numberitteration	
						BEGIN
							SET @Insert_Query=	@Insert_Query + ',[TURNOVER_' + CAST(@itterator as nvarchar) + ']'   -- HAS TO BE CREATED ACCORDING TO NUMBER OF ROWS OF THE CURSOR
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
						PRINT '@itterator : ' + CAST(@itterator as nvarchar)

						SET @Query= @Insert_Query + 'SELECT NIP as NIP ' 

						WHILE @subitterator<=@Numberitteration
							BEGIN
								IF @itterator=@subitterator
									BEGIN
										
										-- CONDITION IF SUR LE PARAMETRE DEFINISSANT LA METHODE D'AGGREGATION (SUM, COUNT, etc..)
										SET @ErrorMEss='ERROR ON AggMeth Parameter in ReportCarePathActivtiy_By_actes procedure .' + 
													@AggMeth + ' was given,but PRESENCE, COUNT OR DENSITE is expected'

										IF @AggMeth='PRESENCE'
											SET @Query= @Query+ ',COUNT(Base.Nb_Actes_Serie) ' --as M' + CAST(@subitterator as varchar) + ' ' -- PUT THE ACTIVE (MONTH,TRIMESTER, YEAR)
										ELSE
											IF @AggMeth='COUNT'
												SET @Query= @Query+ ',SUM(Base.Nb_Actes_Serie) ' --as M' + CAST(@subitterator as varchar) + ' ' -- PUT THE ACTIVE (MONTH,TRIMESTER, YEAR)
											ELSE
												IF @AggMeth='DENSITE'
													SET @Query= @Query+ ',SUM( CAST(Base.Nb_Actes_Serie as float)) /' + CAST(@lenghtagg as varchar) --as M' + CAST(@subitterator as varchar) + ' ' -- PUT THE ACTIVE (MONTH,TRIMESTER, YEAR)
												ELSE
													RAISERROR(15600,-1,-1,@ErrorMEss)
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
								  '
							SET @Query= @Query + '	  
								  , COUNT(Table_Encoded_Actes.Cle_Acte_Encoded) as Nb_Actes_Serie
								  '
								--AJOUTER ICI LES PARAMETRES SUPPLEMENTAIRES DE FILTRE (UF Particulière, Famille d'UF, Dimension, etc..)

							SET @Query= @Query + '  
								  , MAX(TCalendar.MYDATE) as MyDate
								  , MAX(TCalendar.MYWEEK) as MyWeek
								  , MAX(TCalendar.MYMONTH) as MyMonth
							      , MAX(TCalendar.MYTRIMESTER) as MyTrimester
								  , MAX(TCalendar.MYYEAR) as MyYear--,Table_Actes.[N_S]
								  , MAX(TCalendar.MYYEARWEEK) as MyYearWeek
								  , MAX(TCalendar.MYYEARMONTH) as MyYearMonth
							      , MAX(TCalendar.MYYEARQUARTER) as MyYearQuarter
								  , MAX(Table_Type_Seq.id_Sequence) as  MyIDSequence
								  , MAX(Table_Type_Seq.Type_Sequence) as  MyTypeSequence
								  '

								  -- CONDITION IF SUR LE PARAMETRE J0, V1, V2, V3 ou V4
										SET @ErrorMEss='ERROR ON J0_Type Parameter in ReportCarePathActivtiy_By_actes procedure .' + 
													@TypeJ0 + ' was given,but  V1, V2, V3 or V4 is expected'

										IF @TypeJ0='V1'
											BEGIN 
												SET @Query= @Query + ' 
												  , MIN(T_PS.J0_V1) as J0V1
												  , DATEDIFF(D,MIN(T_PS.J0_V1),MIN(TCalendar.MYDATE)) as MyCarePath_V1_Day
												  , ROUND(DATEDIFF(D,MIN(T_PS.J0_V1),MIN(TCalendar.MYDATE))/7,0) as MyCarePath_V1_Week
												  , ROUND(DATEDIFF(D,MIN(T_PS.J0_V1),MIN(TCalendar.MYDATE))/30.41,0) as MyCarePath_V1_Month
												  , ROUND(DATEDIFF(D,MIN(T_PS.J0_V1),MIN(TCalendar.MYDATE))/365.25,0)  as MyCarePath_V1_Year
												  '
												
											END 
										ELSE
											IF @TypeJ0='V2'
												BEGIN 
													SET @Query= @Query + ' 
												    , MIN(T_PS.J0_V2) as J0V2
												    , DATEDIFF(D,MIN(T_PS.J0_V2),MIN(TCalendar.MYDATE)) as MyCarePath_V2_Day
												    , ROUND(DATEDIFF(D,MIN(T_PS.J0_V2),MIN(TCalendar.MYDATE))/7,0) as MyCarePath_V2_Week
												    , ROUND(DATEDIFF(D,MIN(T_PS.J0_V2),MIN(TCalendar.MYDATE))/30.41,0) as MyCarePath_V2_Month
												    , ROUND(DATEDIFF(D,MIN(T_PS.J0_V2),MIN(TCalendar.MYDATE))/365.25,0)  as MyCarePath_V2_Year	
													  '
													 
												END 
											ELSE
												IF @TypeJ0='V3'
													BEGIN 
														SET @Query= @Query + ' 
														, MIN(Table_Type_Seq.J0_V3) as J0V3
														, DATEDIFF(D,MIN(Table_Type_Seq.J0_V3),MIN(TCalendar.MYDATE)) as MyCarePath_V3_Day
														, ROUND(DATEDIFF(D,MIN(Table_Type_Seq.J0_V3),MIN(TCalendar.MYDATE))/7,0) as MyCarePath_V3_Week
														, ROUND(DATEDIFF(D,MIN(Table_Type_Seq.J0_V3),MIN(TCalendar.MYDATE))/30.41,0) as MyCarePath_V3_Month
														, ROUND(DATEDIFF(D,MIN(Table_Type_Seq.J0_V3),MIN(TCalendar.MYDATE))/365.25,0)  as MyCarePath_V3_Year	
														  '
														 
													END 
												ELSE
													IF @TypeJ0='V4'
														BEGIN
															SET @Query= @Query + ' 
															, MIN(Table_Type_Seq.J0_V4) as J0V4
															, DATEDIFF(D,MIN(Table_Type_Seq.J0_V4),MIN(TCalendar.MYDATE)) as MyCarePath_V4_Day
															, ROUND(DATEDIFF(D,MIN(Table_Type_Seq.J0_V4),MIN(TCalendar.MYDATE))/7,0) as MyCarePath_V4_Week
															, ROUND(DATEDIFF(D,MIN(Table_Type_Seq.J0_V4),MIN(TCalendar.MYDATE))/30.41,0) as MyCarePath_V4_Month
															, ROUND(DATEDIFF(D,MIN(Table_Type_Seq.J0_V4),MIN(TCalendar.MYDATE))/365.25,0)  as MyCarePath_V4_Year	
															  '
														END 	
													ELSE
														RAISERROR(15600,-1,-1,@ErrorMEss)
								
							SET @Query= @Query + '

							  FROM [ICO_Activite].[dbo].[Tmp_A_Actes_Table_Analyse] as Table_Actes,
								   [ICO_Activite].[dbo].[Tmp_Acte_Encoded] as Table_Encoded_Actes,
								   [ICO_Activite].[dbo].[Tmp_Sejour_Encoded] as Table_Encoded_Sejours,
								   [ICO_Activite].[dbo].[Tmp_Sequence_Encoded] as Table_Encoded_Sequence,
								   [ICO_Activite].[dbo].[Tmp_Type_Sequence] as Table_Type_Seq,
								   [ICO_Activite].[dbo].[Tmp_Parcours_Encoded] as Table_Encoded_Parcours,
								   [ICO_Activite].[dbo].[Tmp_MYCALENDAR] as TCalendar,
								   [ICO_Activite].[dbo].[Tmp_PS_] as T_PS
								   '
							SET @Query= @Query + @filtre1_query_FROM
							SET @Query= @Query + @filtre2_query_FROM						
						

						SET @Query= @Query + '
							  WHERE 
									Table_Actes.ID_A=Table_Encoded_Actes.ID_A AND
									Table_Actes.N_S=Table_Encoded_Sejours.N_S AND
									Table_Actes.N_S=Table_Type_Seq.N_S AND
									Table_Type_Seq.id_Sequence=Table_Encoded_Sequence.id_Sequence AND
									T_PS.N_S=Table_Actes.N_S AND
									Table_Actes.NIP=Table_Encoded_Parcours.NIP AND 
									Table_Actes.DD_A=TCalendar.MYDATE '

							SET @Query= @Query + @filtre1_query_WHERE
							SET @Query= @Query + @filtre2_query_WHERE


						--AJOUTER ICI UNE CONDITION SUR LE TYPE D'AGGREGATION (PAR DATE CALENDAIRE, PAR DATE PARCOURS)
							SET @ErrorMEss='ERROR ON Agg_Type Parameter in ReportCarePathActivtiy_By_actes procedure .' + 
											@AggParameter + ' was given,but  CALENDAR or PARCOURS is expected'
							IF @AggParameter='CALENDAR'
								BEGIN 
									--FILTRE SUR LES DATES DES ACTES AVANT AGGREGATION
									SET @Query= @Query + 'AND Table_Actes.DD_A BETWEEN ''' + CONVERT(nvarchar(20),@Mindate,120) + ''' AND  ''' + CONVERT(nvarchar(20),@Maxdate,120) + ''''
								END
							ELSE
								IF @AggParameter='PARCOURS'
									BEGIN 
										--FILTRE SUR LES DATES DE JOUR DE PARCOURS AVANT AGGREGATION -> @J0agg
										SET @Query= @Query + 'AND MyCarePath_'+ @TypeJ0 + '_Day BETWEEN ' + CAST(@AggParameter_min as varchar) + ' AND  ' + CAST(@AggParameter_max as varchar) + ' '
									END
								ELSE
									RAISERROR(15600,-1,-1,@ErrorMEss)



						SET @Query= @Query + 'GROUP BY Table_Actes.[NIP] ) as Base '
						

					--DEFINITION DE LA CLAUSE GROUPBY (MAIN QUERY) 
						SET @Query= @Query + 'GROUP BY Base.[NIP]'  
						PRINT 'QUERY INSERT TO REPORT N  '+ CAST(@itterator as nvarchar) + ' @Query : ' + @Query
						
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
						SET @Query=	@Query + ',STRING_AGG([TURNOVER_' + CAST(@itterator as nvarchar) + '],'''') as  [' + CAST(@currentAggregatorName as nvarchar) + ']'
						SET @Insert_Query= @Insert_Query + ', '''' as  [' + CAST(@currentAggregatorName as nvarchar) + ']' 
						SET @itterator+=1
					
						FETCH NEXT FROM MyCursor INTO @currentAggregator
														, @currentAggregatorName,@Mindate,@MaxDate
					END


				SET @Query = @Query + ' FROM [ICO_Activite].[dbo].[Tmp_MYREPORT]  GROUP BY NIP '  --FROM + GROUPBY CLAUSES OF THE QUERY
				
			--ADD THE CUSTOMERS WHO DO NOT HAVE PURCHASE ANYTHING
				SET @Query = @Query + ' UNION 
						SELECT 	C.NIP' + @Insert_Query +
						' FROM [ICO_Activite].[dbo].[Tmp_A_Actes_Table_Analyse] as C
						WHERE C.NIP NOT IN (SELECT [NIP] FROM [ICO_Activite].[dbo].[Tmp_MYREPORT]) ORDER BY NIP asc'

				PRINT 'FINAL @Query Myreport : ' + @Query
				EXEC (@Query);

			--CLOSE CURSOR
				CLOSE MyCursor
				DEALLOCATE MyCursor
/*
		--DELETE TEMPORARY TABLES
			IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Tmp_MYREPORT]') AND type in (N'U'))
			BEGIN
			 	DROP TABLE [dbo].[MYREPORT]
			END
			*/
