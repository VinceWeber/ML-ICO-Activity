USE [ICO_Activite]
GO
/****** Object:  StoredProcedure [dbo].[Preproc_B1_J0V3V4_Type_Sejour_V7]    Script Date: 11/07/2023 16:45:55 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
	PROCEDURE:
		Input : 
			Table source (format from Preproc_A4)
			Table sortie
			Date_Debut_Dataset
			Seuil LT
			Seuil CT

		Objet: attribue une catégorie su séjour fonction de deux critères :
			
			
			
			
			1er critère :
				Si le séjour contient un acte de soins => le séjour est qualifié de Traitement
				On défini une "dernirèe date de traitement"
			
			2nd Critère :
				S'il existe une dernière date de traitement :
					Si la distance entre le séjour et le précédent traitement < Seuil CT
						Alors le séjour est qualifié de SUIVI_CT
						Sinon SUIVI_LT
				Sinon
					Si le J0 du patient est postérieur à la date de début du dataset
						-> INIT_NEW
					Sinon  : le J0 du patient est antérieur à la date du début de dataset
						-> INIT_OLD
					
					Si le début du séjour est supérieur au Seuil LT
						-> SUIVI_LT
					Sinon
						On copie le précédent type de séjour.

*/


CREATE PROCEDURE [dbo].[Preproc_B1_J0V3V4_Type_Sejour_V7] @TableSource nvarchar(50) 
										, @TableSortie nvarchar(50) 
--										, @Seuil_PS float=NULL
										,@Date_Deb_DATATSET DATETIME
										,@Seuil_LT float
										,@Seuil_CT float
AS
SET NOCOUNT ON;


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
		@CURSOR_Prec_Type_Sequence nvarchar(10),
		--@Date_Deb_DATATSET DATETIME,
		@ANNEE_NIP int,
		@Seuil_PS as float,
		--@Seuil_LT as float,
		--@Seuil_CT as float,
		@Date_TT_inconnu as DATETIME,
		@PSejourDS as float,
		@PSejourSOS as float,
		@Type_Sequence as nvarchar(10),
		@Type_Sequence_Precedent as nvarchar(10),
		@ID_Sequence as int,
		--@TableSource nvarchar(50),
		--@TableSortie nvarchar(50),
		@Query nvarchar(3000),
		@Itterator int,
		@Max_Itterator int,

		@MYSELECT nvarchar(500),
		@MYFROM nvarchar(500),
		@MYWHERE nvarchar(500)

	
--ASSIGN VALUES TO VARIABLES :
		--SET @Seuil_PS=100000000000

		--PRINT 'SEUIL DEFINED BY USER AT ' + CAST(@Seuil_PS as nvarchar(50))
			SET @Seuil_PS=100000000000
		PRINT 'SET SEUIL AT ' + CAST(@Seuil_PS as nvarchar(50))

		--SET @TableSortie='Tmp_J0V3V4_Type_Sejour_2'
		--SET @TableSource='Tmp_A1234_Export'
		--SET @Date_Deb_DATATSET= CONVERT(VARCHAR, '2018-01-01 00:00:00', 103)
		--SET @Seuil_LT=80
		--SET @Seuil_CT=60
		SET @Date_TT_inconnu = CONVERT(VARCHAR, '2199-07-01 00:00:00', 103)
		SET @Itterator=1
		SET @ID_Sequence=0

PRINT '---STARTING B1 PROCEDURE WITH INPUT PARAMETERS @TableSource = ' + @TableSource + ' ,@TableSortie =' + @TableSortie + ' ,@Seuil_PS = ' + CAST (@Seuil_PS as nvarchar(20))
													+' ,@Date_Deb_DATATSET = ' + CAST(@Date_Deb_DATATSET as varchar (20)) + ' , @Seuil_LT = ' + CAST (@Seuil_LT as nvarchar(20))
													+' ,@Seuil_CT = ' + CAST (@Seuil_CT as nvarchar(20));


--  SORTIE = 
  -- Seq_PARCOURS 
  -- Date_Dernier_TT
  --J0 V3
  --J0_V4
  
--DEFINITION DU CURSEUR SUR LES NIP / NUM_SEJOURS CLASSES PAR ORDRE CROISSANT DE TEMPORALITE

SET @Query= 'DECLARE MyCursor CURSOR SCROLL FOR
							SELECT [NIP]
						  ,[N_S]
						  ,[Ddebsej]
						  ,[J0_V1]
						  ,[J0_V2]
						  ,[Annee_NIP]
						  ,[Poids_Sejour_DS]
						  ,[Poids_Sejour_DSOS]
							FROM [dbo].[' + @TableSource +  ']
							ORDER BY [NIP] asc,[Ddebsej] asc' 
EXECUTE(@Query)




--SET @Numberitteration = (SELECT COUNT(DISTINCT([MYTRIMESTER])) FROM [dbo].[MYCALENDAR])

--CREER TABLE DE SORTIE
SET  @Query= 'Delete_Table_if_exists '+ @TableSortie
EXECUTE (@Query)

SET  @Query='CREATE TABLE [dbo].[' + @TableSortie +'] (
						[NIP] varchar(10) NOT NULL,
						[id_Sequence] int,
						[N_S] varchar(10) NOT NULL,
						[Ddebsej] DATETIME,
						[J0_V3] DATETIME,
						[J0_V4] DATETIME,
						[Poids_Sejour_DS] float,
						[Type_Sequence] varchar(10) NOT NULL
						)'
EXECUTE (@Query)
SET @NIP_Precedent=''
SET @Type_Sequence_Precedent=''

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
			
			IF @NIP<>@NIP_Precedent
				BEGIN
					SET @Type_Sequence_Precedent=''
					--SET @ID_Sequence+=1
				END


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
					SET @Type_Sequence='TRAIT'
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
											SET @Type_Sequence='SUIVI_LT'
											--PRINT ' @J0V1 (' +CAST(@J0V1 as nvarchar(20)) +') - @Date_deb_sejour (' +CAST(@Date_deb_sejour as nvarchar(20)) +')<= @Seuil_LT (+ ' + CAST(@Seuil_LT  as nvarchar(20))+')'
											--PRINT CAST(DATEDIFF(day,@J0V1,@Date_deb_sejour) as nvarchar(20))
										END
									ELSE
										BEGIN
											SET @Type_Sequence=@CURSOR_Prec_Type_Sequence
											--PRINT ' @J0V1 (' +CAST(@J0V1 as nvarchar(20)) +') - @Date_deb_sejour (' +CAST(@Date_deb_sejour as nvarchar(20)) +')<= @Seuil_LT (+ ' + CAST(@Seuil_LT  as nvarchar(20))+')'
											--PRINT CAST(DATEDIFF(day,@J0V1,@Date_deb_sejour) as nvarchar(20))
										
										END
								END
							ELSE
								BEGIN
									IF DATEDIFF(day,@Date_Dernier_TT,@Date_deb_sejour) >= @Seuil_CT
										BEGIN
											SET @Type_Sequence='SUIVI_LT'
										END
									ELSE
										BEGIN
											SET @Type_Sequence='SUIVI_CT'
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
										SET @Type_Sequence='INIT_NEW'
										SET @Date_Dernier_TT=@Date_TT_inconnu
									END
								ELSE
									BEGIN 
										SET @Type_Sequence='INIT_OLD'
										SET @Date_Dernier_TT=@Date_TT_inconnu							
									END
							END
				END
		

			--******* REMPLISSAGE TABLE DE SORTIE
			IF @Type_Sequence_Precedent<>@Type_Sequence
				BEGIN
					SET @ID_Sequence+=1
				END

			SET  @Query= 'INSERT INTO [dbo].[' + @TableSortie + '] (
						[NIP],
						[id_Sequence],
						[N_S],
						[Ddebsej],
						[J0_V3],
						[J0_V4],
						[Poids_Sejour_DS],
						[Type_Sequence]
						)
			VALUES	(
						''' + @NIP + ''',
						''' + cast(@ID_Sequence as nvarchar(15)) + ''',
						''' + @Num_sejour +''',
						''' + CONVERT(VARCHAR, @Date_deb_sejour, 120) + ''',
						''' + CONVERT(VARCHAR,@J0V3 , 120) +''',
						''' + CONVERT(VARCHAR,@J0V4 , 120) + ''',
						''' + CONVERT(VARCHAR,@PSejourDS , 120) +''',
						''' + @Type_Sequence +'''
						)'
			--PRINT @Query
			EXECUTE (@Query)

			SET @NIP_Precedent=@NIP
			SET @Type_Sequence_Precedent=@Type_Sequence
			SET @CURSOR_Prec_Date_Dernier_TT=@Date_Dernier_TT
			SET @CURSOR_Prec_Type_Sequence=@Type_Sequence
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


