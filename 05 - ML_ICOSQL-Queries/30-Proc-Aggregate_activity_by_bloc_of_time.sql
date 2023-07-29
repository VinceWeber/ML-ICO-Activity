
/****** Object:  StoredProcedure [dbo].[ReportCustomerTurnover]    Script Date: 21/07/2023 09:08:26 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

--QUESTION 3 - PROCEDURE WHICH RETURNS A REPORT OF TURNOVER BY CUSTOMER
-- PROCEDURE TAKES 2 ARGUMENTS : CHOICE & YEAR 

--ALTER PROCEDURE [dbo].[ReportCustomerTurnover] @CHOICE int =NULL, @YEAR int =NULL

--AS
--SET NOCOUNT ON;
DECLARE 

	--VARIABLES "INTERNES"
		@startdate DATE,
		@enddate DATE,
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
		SET @startdate=CAST('1900-01-01' as DATE)
		SET @enddate=CAST('1910-12-31' as DATE)

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
										SELECT	DISTINCT([MYDATE]), + 'J-' + CAST([MYDATENAME] as nvarchar) 
										FROM	[dbo].[MYCALENDAR]
										ORDER BY 'J-' + CAST([MYDATENAME] as nvarchar) asc 
	
				SET @Numberitteration = (SELECT COUNT(DISTINCT([MYDATE])) FROM [dbo].[MYCALENDAR])
				--PRINT '@Numberitteration' + CAST(@Numberitteration as varchar)
		END

	IF @CHOICE=1    -- REPORT WEEKLY turnover from stardate of calendar  (1 column = 1 WEEK), CURSOR LINES ARE WEEKS
		BEGIN	
			DECLARE MyCursor CURSOR SCROLL FOR
										SELECT	DISTINCT([MYYEARWEEK]), CAST([MYYEAR] as nvarchar) + '-W' + CAST([MYWEEK] as nvarchar)
										FROM	[dbo].[MYCALENDAR]
										ORDER BY CAST([MYYEAR] as nvarchar) + '-W' + CAST([MYWEEK] as nvarchar) asc 
	
				SET @Numberitteration = (SELECT COUNT(DISTINCT([MYYEARWEEK])) FROM [dbo].[MYCALENDAR])
				--PRINT '@Numberitteration' + CAST(@Numberitteration as varchar)
	END


	 IF @CHOICE=2 OR @CHOICE=NULL   -- REPORT MONTHLY turnover from stardate of calendar  (1 column = 1 month), CURSOR LINES ARE MONTHS
		BEGIN	
			DECLARE MyCursor CURSOR SCROLL FOR
										SELECT	DISTINCT([MYYEARMONTH]), CAST([MYYEAR] as nvarchar) + '-M' + CAST([MYMONTH] as nvarchar)
										FROM	[dbo].[MYCALENDAR]
										ORDER BY [MYYEARMONTH] asc 
	
				SET @Numberitteration = (SELECT COUNT(DISTINCT([MYYEARMONTH])) FROM [dbo].[MYCALENDAR])
				--PRINT '@Numberitteration' + CAST(@Numberitteration as varchar)
		END


	 IF @CHOICE=3				-- REPORT TRIMESTER turnover for the @YEAR (1 column = 3 months), CURSOR LINES ARE TRIMESTERS
		BEGIN	
			DECLARE MyCursor CURSOR SCROLL FOR
										SELECT	DISTINCT([MYYEARQUARTER]), CAST([MYYEAR] as nvarchar) + '-Q' + CAST([MYTRIMESTER] as nvarchar)
										FROM	[dbo].[MYCALENDAR]
										ORDER BY [MYYEARQUARTER] asc 
	
				SET @Numberitteration = (SELECT COUNT(DISTINCT([MYYEARQUARTER])) FROM [dbo].[MYCALENDAR])
				--PRINT '@Numberitteration' + CAST(@Numberitteration as varchar)
		END

	 IF @CHOICE=4				-- REPORT ALL YEAR turnovers (1 column = 1 Year), CURSOR LINES ARE YEARS
		BEGIN	
			DECLARE MyCursor CURSOR SCROLL FOR
										SELECT	DISTINCT([MYYEAR]), CAST([MYYEAR] as nvarchar)
										FROM	[dbo].[MYCALENDAR]
										ORDER BY [MYYEAR] asc 
	
				SET @Numberitteration = (SELECT COUNT(DISTINCT([MYYEAR])) FROM [dbo].[MYCALENDAR])
				--PRINT '@Numberitteration' + CAST(@Numberitteration as varchar)
		END

		
				--CREATE TEMPORARY TABLE (MYREPORT) TO STORE THE RESULT OF THE QUERY	
					IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[MYREPORT]') AND type in (N'U'))
						BEGIN
							DROP TABLE [dbo].[MYREPORT]
						END
						
					SET @itterator=1
					SET @Query= 'CREATE TABLE [dbo].[MYREPORT] (
						[NIP][nvarchar(12)] NOT NULL'
				
					WHILE @itterator<=@Numberitteration	
						BEGIN
							SET @Query=	@Query + ',[TURNOVER_' + CAST(@itterator as varchar) + '] [Float] NOT NULL'  -- HAS TO BE CREATED ACCORDING TO NUMBER OF ROWS OF THE CURSOR
							SET @itterator+=1
						END
					SET  @Query= @Query+ ')'
					PRINT '@Query Myreport : ' + @Query
					EXEC (@Query);

				--CREATE THE BEGINNING OF THE INSERT QUERY AND SAVE IT INTO @INSRT_QUERY VARIABLE	
					SET @itterator=1;
					SET @Insert_Query= 'INSERT INTO [dbo].[MYREPORT] (
						[CUSTID]'

					WHILE @itterator<=@Numberitteration	
						BEGIN
							SET @Insert_Query=	@Insert_Query + ',[TURNOVER_' + CAST(@itterator as varchar) + ']'   -- HAS TO BE CREATED ACCORDING TO NUMBER OF ROWS OF THE CURSOR
							SET @itterator+=1
						END
					SET  @Insert_Query= @Insert_Query + ')'

				OPEN MyCursor
				FETCH NEXT FROM MyCursor INTO @currentAggregator
									, @currentAggregatorName

				--PRINT 'START @currentMonth= : ' + CAST(@currentAggregator as varchar)

				--LOOP WHICH CREATE QUERY FILLING THE CURRENT AGGREGATOR (MONTH,TRIMESTER, YEAR, or other defined by Cursor from the Calendar Table) AND AND PUT ZERO TO OTHERS

				SET @itterator=1
				
				WHILE @@FETCH_STATUS = 0
				BEGIN
					--DEFINE THE SELECT CLAUSE (MAIN QUERY) WITH THE ACTIVE MONTH
						SET @subitterator=1
						--PRINT '@itterator : ' + CAST(@itterator as varchar)

						SET @Query= @Insert_Query + 'SELECT MAX(Base.CustomerID) as CustID,Base.[CustomerName]' 

						WHILE @subitterator<=@Numberitteration
							BEGIN
								IF @itterator=@subitterator
									BEGIN
										SET @Query= @Query+ ',SUM(Base.Total_Value) ' --as M' + CAST(@subitterator as varchar) + ' ' -- PUT THE ACTIVE (MONTH,TRIMESTER, YEAR)
									END
								ELSE
									BEGIN
										SET @Query= @Query+ ', 0.0 ' --+ CAST(@subitterator as varchar) + ' ' -- PUT OTHER (MONTH, TRIMESTER, YEAR)
									END
								SET @subitterator+=1
							END
						--PRINT 'SELECT months @Query : ' + @Query
						
					--DEFINITION DE LA CLAUSE -FROM (MAINQUERY) ( + SELECT (SUBQUERY))
						SET @Query= @Query + '
						FROM (
							SELECT  C.[CustomerID],C.[CustomerName]
								,SUM(IL.[Quantity]*IL.[UnitPrice]) as Total_Value
								, MAX(TCalendar.MYMONTH) as MyMonth
								, MAX(TCalendar.MYTRIMESTER) as MyTrimester
								, MAX(TCalendar.MYYEAR) as MyYear

								FROM	[WideWorldImporters].[Sales].[Invoices] as I,
									[WideWorldImporters].[Sales].[InvoiceLines] as IL,
									[WideWorldImporters].[Sales].[Customers] as C,
									[WideWorldImporters].[dbo].[MYCALENDAR] as TCalendar '
						
					--DEFINITION DE LA CLAUSE  WHERE  (SUBQUERY)
						SET @Query= @Query + ' WHERE  I.InvoiceID=IL.InvoiceID AND C.[CustomerID]=I.[CustomerID] and TCalendar.MYDATE = I.InvoiceDate AND '

						IF @CHOICE=1
							BEGIN
								SET @Query= @Query + 'MyYear=' + CAST(@YEAR as varchar) + ' AND MyMonth= '+ CAST(@currentAggregator as varchar) +''
							END
						IF @CHOICE=2
							BEGIN
								SET @Query= @Query + 'MyYear=' + CAST(@YEAR as varchar) + ' AND MyTrimester= '+ CAST(@currentAggregator as varchar)+ ''
							END
						IF @CHOICE=3
							BEGIN
								SET @Query= @Query + 'MyYear=' + CAST(@currentAggregator as varchar) + ''
							END
						 
					--DEFINITION DE LA CLAUSE GROUPBY (SUB QUERY)	 
						IF @CHOICE=1
							BEGIN
								SET @Query= @Query +' GROUP BY C.[CustomerID],C.[CustomerName], I.[InvoiceDate], I.[InvoiceID],IL.[InvoiceID], MyYear, MYMonth) as Base '
							END
						IF @CHOICE=2
							BEGIN
								SET @Query= @Query +' GROUP BY C.[CustomerID],C.[CustomerName], I.[InvoiceDate], I.[InvoiceID],IL.[InvoiceID], MyYear, MyTrimester) as Base '
							END
						IF @CHOICE=3
							BEGIN
								SET @Query= @Query +' GROUP BY C.[CustomerID],C.[CustomerName], I.[InvoiceDate], I.[InvoiceID],IL.[InvoiceID], MyYear) as Base '
							END

					--DEFINITION DE LA CLAUSE GROUPBY (MAIN QUERY) 
						SET @Query= @Query + 'GROUP BY Base.[CustomerName]'  
						--PRINT 'QUERY INSER TO REPORT N  '+ CAST(@itterator as varchar) + ' @Query : ' + @Query
						
					--EXECUTION DE LA REQUETE	
						EXEC(@Query);
						  
						SET @itterator+=1
						  
						  
						FETCH NEXT FROM MyCursor INTO @currentAggregator
											, @currentAggregatorName
				END	

	
		--FINAL QUERY TO EXPORT DATAS		
			--RESTART CURSOR
				FETCH FIRST FROM MyCursor INTO @currentAggregator
													, @currentAggregatorName

				
				SET @itterator=1
				SET @Query= 'SELECT [CUSTNAME] AS CustomerName'   -- SELECT CLAUSE OF THE QUERY
				SET @Insert_Query = ''


				WHILE @@FETCH_STATUS = 0 -- WHILE @Numberitteration-@itterator<=0 
					BEGIN
						SET @Query=	@Query + ',SUM([TURNOVER_' + CAST(@itterator as varchar) + ']) as  [' + CAST(@currentAggregatorName as varchar) + ']'
						SET @Insert_Query= @Insert_Query + ', 0.0 as  [' + CAST(@currentAggregatorName as varchar) + ']' 
						SET @itterator+=1
					
						FETCH NEXT FROM MyCursor INTO @currentAggregator
														, @currentAggregatorName
					END


				SET @Query = @Query + ' FROM [WideWorldImporters].[dbo].[MYREPORT]  GROUP BY CUSTNAME '  --FROM + GROUPBY CLAUSES OF THE QUERY
				
			--ADD THE CUSTOMERS WHO DO NOT HAVE PURCHASE ANYTHING
				SET @Query = @Query + ' UNION 
						SELECT 	C.CustomerName' + @Insert_Query +
						' FROM [WideWorldImporters].[Sales].[Customers] as C
						WHERE C.CustomerID NOT IN (SELECT [CUSTID] FROM [WideWorldImporters].[dbo].[MYREPORT]) ORDER BY CustomerName asc'

				PRINT 'FINAL @Query Myreport : ' + @Query
				EXEC (@Query);

			--CLOSE CURSOR
				CLOSE MyCursor
				DEALLOCATE MyCursor

		--DELETE TEMPORARY TABLES
			IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[MYREPORT]') AND type in (N'U'))
			BEGIN
				DROP TABLE [dbo].[MYREPORT]
			END

			IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[MYCALENDAR]') AND type in (N'U'))
			DROP TABLE [dbo].[MYCALENDAR]
