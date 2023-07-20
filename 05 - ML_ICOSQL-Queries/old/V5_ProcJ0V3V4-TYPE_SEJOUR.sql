/****** Script for SelectTopNRows command from SSMS  ******/


--PARAMETRES :
	--TABLE DE SORTIE / TABLE SOURCE: [Tmp_J0V3V4_Type_Sejour] -OK
	--REGLE DE CONSTRUCTION DU J0V3 et J0V4 (créer une procédure spécifique)
	--POIDS DE SEJOUR (CUT OFF DE LA PARTIE SOINS) 
		--@Seuil_PS=100000000000 
	--DATE DU DEBUT DU DATA SET (INIT_NEW // INIT_OLD) 01/01/2018
		--@Date_Deb_DATATSET
	--SEUIL DE DEFINITION DU SUIVI_LT (cas d'un NIP qui apparait dans le DAtaSEt)= SI Date dernier TT = Unknown -> J0 - DATE DU SEJOUR <= -80 => SUIVI LT, si non on copie le type de séjour précédent.
		--@Seuil_LT
	--SEUIL DE DEFINITION DU SUIVI_CT = SI Date dernier TT = Unknown -> Datedernier tt - Date du Sejour <= -60 => SUIVI CT, si non SUIVI_LT
		--@Seuil_CT

DECLARE 

	--VARIABLES "INTERNES"
		@Num_sejour nvarchar(14),
		@NIP nvarchar(14),
		@NIP_Precedent nvarchar(14),
		@Date_deb_sejour DATETIME,
		@J0V1 DATETIME,
		@J0V2 DATETIME,
		@J0V3 DATETIME,
		@J0V4 DATETIME,
		@Date_Dernier_TT DATETIME,
		@CURSOR_Prec_Date_Dernier_TT DATETIME,
		@CURSOR_Prec_Type_Sejour nvarchar(10),
		@Date_Deb_DATATSET DATETIME,
		@ANNEE_NIP int,
		@Seuil_PS as float,
		@Seuil_LT as float,
		@Seuil_CT as float,
		@Date_TT_inconnu as DATETIME,
		@PSejourDS as float,
		@PSejourSOS as float,
		@Type_Sejour as nvarchar(10),

		@TableSource nvarchar(50),
		@TableSortie nvarchar(50),
		@Query nvarchar(3000),
		@Itterator int,
		@Max_Itterator int,

		@MYSELECT nvarchar(500),
		@MYFROM nvarchar(500),
		@MYWHERE nvarchar(500)

	
--ASSIGN VALUES TO VARIABLES :
		SET @Seuil_PS=100000000000
		SET @TableSortie='Tmp_J0V3V4_Type_Sejour_2'
		SET @TableSource='Tmp_A1234_Export'
		SET @Date_Deb_DATATSET= CONVERT(VARCHAR, '2018-01-01 00:00:00', 103)
		SET @Seuil_LT=80
		SET @Seuil_CT=60
		SET @Date_TT_inconnu = CONVERT(VARCHAR, '2199-07-01 00:00:00', 103)
		SET @Itterator=1

PRINT '---STARTING A5 PROCEDURE WITH INPUT PARAMETERS @TableSource = ' + @TableSource

--  SORTIE = 
  -- Seq_PARCOURS 
  -- Date_Dernier_TT
  --J0 V3
  --J0_V4
  
--DEFINITION DU CURSEUR SUR LES NIP / NUM_SEJOURS CLASSES PAR ORDRE CROISSANT DE TEMPORALITE

SET @Query= 'DECLARE MyCursor CURSOR SCROLL FOR
							SELECT TOP (20) [NIP_original]
						  ,[Ho_Num_Num_sejour]
						  ,[Ddebsej]
						  ,[J0_V1]
						  ,[J0_V2]
						  ,[Annee_NIP]
						  ,[Poids_Sejour_DS]
						  ,[Poids_Sejour_DSOS]
							FROM [dbo].[' + @TableSource +  ']
							ORDER BY [NIP_original] asc,[Ddebsej] asc' 
EXECUTE(@Query)




--SET @Numberitteration = (SELECT COUNT(DISTINCT([MYTRIMESTER])) FROM [dbo].[MYCALENDAR])

--CREER TABLE DE SORTIE
SET  @Query= 'Delete_Table_if_exists '+ @TableSortie
EXECUTE (@Query)

SET  @Query='CREATE TABLE [dbo].[' + @TableSortie +' ] (
						[NIP_original] varchar(10) NOT NULL,
						[Ho_Num_Num_sejour] varchar(10) NOT NULL,
						[Ddebsej] DATETIME,
						[J0_V3] DATETIME,
						[J0_V4] DATETIME,
						[Poids_Sejour_DS] float,
						[Type_Sejour] varchar(10) NOT NULL
						)'
EXECUTE (@Query)
SET @NIP_Precedent=''

OPEN MyCursor
FETCH NEXT FROM MyCursor INTO @NIP , @Num_sejour, @Date_deb_sejour,@J0V1,@J0V2, @ANNEE_NIP,@PSejourDS,@PSejourSOS

		WHILE @@FETCH_STATUS = 0
		BEGIN
			--PRINT 'STEP ' 
			--PRINT 'LINE ' + CAST(@Itterator as nvarchar(20)) + ' IS  NIP = ' + @NIP  + ' / Num _sejour =' + @Num_sejour +'/ Date debut sejour =' + CAST(@Date_deb_sejour as nvarchar(20))
			
			-- ******* DEFINITION DU J0V3 et J0V4

				  -- REALISATION DU J0V3 et J0V4 -> CREER UNE PROCEDURE A PART ET/OU METTRE A JOUR LA PROCEDURE A2.
				  --SI Année NIP >= ANNEE DU DEBUT DU DATASET D'ENTRAINEMENT => J0V3 = J0V1 // J0V4=J0V2
				  --SINON : J0V3 = 01/07 de l'année en cours / idem pour J0V4.  *****PARAMETER*****
					-- CREER UNE DOC AVEC LES DIFFERENTS STEPS et DEFINITIONS DE VARIABLES
			
			IF @ANNEE_NIP>=2018
				BEGIN
					SET @J0V3 = @J0V1
					SET @J0V4 = @J0V2
				END
			ELSE
				BEGIN
					SET @J0V3 =  CONVERT(VARCHAR, CAST(@ANNEE_NIP as varchar(5)) + '-07-01 00:00:00', 103) 
					SET @J0V4 =  CONVERT(VARCHAR, CAST(@ANNEE_NIP as varchar(5)) + '-07-01 00:00:00', 103) 
				END
			
			--PRINT 'LINE ' + CAST(@Itterator as nvarchar(20)) + ' CALCULATED VALUES :@J0V3 ' + CAST(@J0V3 as nvarchar(20)) + ' @J0V4 ' + CAST(@J0V4 as nvarchar(20))
			
			--******* DEFINTION TYPE DE SEJOUR ET DATE DERNIER TRAITEMENT
			
			IF @PSejourDS >= @Seuil_PS
				BEGIN
																--	SI POIDS SEJOUR DS > 100000000000 ***** PARAMETER*****
																--		ALORS		
																--			-LE TYPE DE SEJOUR EST "TRAIT"
																--			-LA date du Dernier traitement = date debut de séjour
					SET @Type_Sejour='TRAIT'
					SET @Date_Dernier_TT=@Date_deb_sejour
				END
			ELSE
				BEGIN
					IF @NIP=@NIP_Precedent							
						BEGIN										-- CAS DES SEJOURS  SUIVANT LA PREMIERE LIGNE DU NIP CONCERNE (2ND SEJOUR ET SUIVANTS)
																						--		SI LE DERNIER TT EST "Unknown"
																	--			ALORS
																	--				SI J0 - DATE DU SEJOUR <= -80  ****PARAMETER*****
																	--					ALORS
																	--						Type de séjour = Suivi au long terme
																	--					SINON
																	--						Type de séjour = Type de séjour précédent
																	--			SINON
																	--				SI Date_dernier_tt - DATE DU SEJOUR <= -60  ****PARAMETER*****
																	--					ALORS
																	--						Type de séjour = Suivi au long terme
																	--					SINON
																	--						Type de séjour = SUIVI_CT	
							IF @CURSOR_Prec_Date_Dernier_TT=@Date_TT_inconnu
								BEGIN
									IF DATEDIFF(day,@J0V1,@Date_deb_sejour) >= @Seuil_LT
										BEGIN
											SET @Type_Sejour='SUIVI_LT1'
											--PRINT ' @J0V1 (' +CAST(@J0V1 as nvarchar(20)) +') - @Date_deb_sejour (' +CAST(@Date_deb_sejour as nvarchar(20)) +')<= @Seuil_LT (+ ' + CAST(@Seuil_LT  as nvarchar(20))+')'
											--PRINT CAST(DATEDIFF(day,@J0V1,@Date_deb_sejour) as nvarchar(20))
										END
									ELSE
										BEGIN
											SET @Type_Sejour=@CURSOR_Prec_Type_Sejour
											--PRINT ' @J0V1 (' +CAST(@J0V1 as nvarchar(20)) +') - @Date_deb_sejour (' +CAST(@Date_deb_sejour as nvarchar(20)) +')<= @Seuil_LT (+ ' + CAST(@Seuil_LT  as nvarchar(20))+')'
											--PRINT CAST(DATEDIFF(day,@J0V1,@Date_deb_sejour) as nvarchar(20))
										
										END
								END
							ELSE
								BEGIN
									IF DATEDIFF(day,@Date_Dernier_TT,@Date_deb_sejour) >= @Seuil_CT
										BEGIN
											SET @Type_Sejour='SUIVI_LT2'
										END
									ELSE
										BEGIN
											SET @Type_Sejour='SUIVI_CT'
										END
								END
						END
					ELSE																							
							BEGIN
																	-- CAS DE LA PREMIERE LIGNE DU NIP CONCERNE (1ER SEJOUR)		
																	--		SI LE DERNIER TT EST "VIDE / N'EXISTE PAS ENCORE"
																	--			ALORS
																	--				SI ANNEE DU NIP >= DATE DEBUT DU DATASET ******PARAMETER******
																	--					ALORS
																	--						Type de séjour = INIT_NEW
																	--						Date_dernier traitement  = Unknown ***INTERNAL PARAMETER***
																	--					SINON
																	--						Type de séjour = INIT_OLD
																	--						Date_dernier traitement  = Unknown ***INTERNAL PARAMETER***	
								IF CONVERT(VARCHAR, CAST(@ANNEE_NIP as nvarchar(5)) + '-01-01 00:00:00', 103)>=@Date_Deb_DATATSET
									BEGIN
										SET @Type_Sejour='INIT_NEW'
										SET @Date_Dernier_TT=@Date_TT_inconnu
									END
								ELSE
									BEGIN 
										SET @Type_Sejour='INIT_OLD'
										SET @Date_Dernier_TT=@Date_TT_inconnu							
									END
							END
				END
		

			--******* REMPLISSAGE TABLE DE SORTIE
			SET  @Query= 'INSERT INTO [dbo].[' + @TableSortie + '] (
						[NIP_original],
						[Ho_Num_Num_sejour],
						[Ddebsej],
						[J0_V3],
						[J0_V4],
						[Poids_Sejour_DS],
						[Type_Sejour]
						)
			VALUES	(
						''' + @NIP + ''',
						''' + @Num_sejour +''',
						''' + CONVERT(VARCHAR, @Date_deb_sejour, 120) + ''',
						''' + CONVERT(VARCHAR,@J0V3 , 120) +''',
						''' + CONVERT(VARCHAR,@J0V4 , 120) + ''',
						''' + CONVERT(VARCHAR,@PSejourDS , 120) +''',
						''' + @Type_Sejour +'''
						)'
			PRINT @Query
			EXECUTE (@Query)

			SET @NIP_Precedent=@NIP
			SET @CURSOR_Prec_Date_Dernier_TT=@Date_Dernier_TT
			SET @CURSOR_Prec_Type_Sejour=@Type_Sejour
			SET @Itterator+=1
			FETCH NEXT FROM MyCursor INTO @NIP , @Num_sejour, @Date_deb_sejour,@J0V1,@J0V2, @ANNEE_NIP,@PSejourDS,@PSejourSOS
		END
--CLOSE CURSOR
CLOSE MyCursor
DEALLOCATE MyCursor


--ANALYSE DU SEJOUR:  -> CREER UNE PROCEDURE SPECIFIQUE A LA  QUALIFICATION DU SEJOUR
--DANS UNE BOUCLE SUR TOUTES LES LIGNES DE LA TABLE (TRIEE PAR NIP et SEJOURS ascendants)


-- CAS DE LA PREMIERE LIGNE DU NIP CONCERNE (1ER SEJOUR)
--	SI POIDS SEJOUR DS > 100000000000 ***** PARAMETER*****
--		ALORS		     2000000000
--			-LE TYPE DE SEJOUR EST "TRAIT"
--			-LA date du Dernier traitement = date debut de séjour
--	SINON
--		SI LE DERNIER TT EST "VIDE / N'EXISTE PAS ENCORE"
--			ALORS
--				SI ANNEE DU NIP >= DATE DEBUT DU DATASET ******PARAMETER******
--					ALORS
--						Type de séjour = INIT_NEW
--						Date_dernier traitement  = Unknown ***INTERNAL PARAMETER***
--					SINON
--						Type de séjour = INIT_OLD
--						Date_dernier traitement  = Unknown ***INTERNAL PARAMETER***			
--			SINON


-- CAS DES SEJOURS  SUIVANT LA PREMIERE LIGNE DU NIP CONCERNE (2ND SEJOUR ET SUIVANTS)
--	SI POIDS SEJOUR DS > 100000000000 ***** PARAMETER*****
--		ALORS 
--			-LE TYPE DE SEJOUR EST "TRAIT"
--			-LA date du Dernier traitement = date debut séjour
--	SINON
--		SI LE DERNIER TT EST "Unknown"
--			ALORS
--				SI J0 - DATE DU SEJOUR <= -80  ****PARAMETER*****
--					ALORS
--						Type de séjour = Suivi au long terme
--					SINON
--						Type de séjour = Type de séjour précédent
--			SINON
--				SI Date_dernier_tt - DATE DU SEJOUR <= -60  ****PARAMETER*****
--					ALORS
--						Type de séjour = Suivi au long terme
--					SINON
--						Type de séjour = SUIVI_CT			


