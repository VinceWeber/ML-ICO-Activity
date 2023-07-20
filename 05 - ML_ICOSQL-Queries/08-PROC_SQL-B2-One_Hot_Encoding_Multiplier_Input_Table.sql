USE [ICO_Activite]
GO
/****** Object:  StoredProcedure [dbo].[Preproc_B2_Category_With_Multiplier_V7]    Script Date: 11/07/2023 17:10:12 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/* PROCEDURE : ONE HOT ENCODING WITH MULTIPLIER
	INPUT : 
		Table source : Table listant des catégories (mini 3 colonnes) col ID + Col Label + Col multipliers
		Table Sortie : Table contenant: 
			Colonne 1 : ID
			Colonne 2 : Catégory 1
			Colonne 3 : Catégory 2
			Colonne 4 : ...

			Les valeurs sont positionnées à 0 ou multiplier si l'ID pointe vers cette catégorie ou non.
*/


CREATE PROCEDURE [dbo].[Preproc_B2_Category_With_Multiplier_V7] 
										 @TableSource nvarchar(50) 
										,@TableSortie nvarchar(50) 
 										,@IDColumnName nvarchar(50)
										--,@ID_type nvarchar(50)
										,@CatColumnName nvarchar(50)
										,@MultiplierColumn nvarchar(50)
AS
SET NOCOUNT ON;


DECLARE 
	--VARIABLES "INTERNES"
		--@TableSource nvarchar(50),
		--@TableSortie nvarchar(50),
		--@IDColumnName nvarchar(50),
		--@CatColumnName nvarchar(50),
		@ID nvarchar(50),
		@Category nvarchar(50),
		@CatValue nvarchar(50),
		@Category_type nvarchar(50),
		@ID_type nvarchar(50),
		@Query nvarchar(3000),
		@Itterator int,
		@Colnameitt nvarchar(3000),
		@Valueitt nvarchar(3000),
		@Max_Itterator int,
		@Stop_Loop bit,
		@Mutliplier bigint

--ASSIGN VALUES TO VARIABLES :

		--SET @TableSource='Actes_Test'
		--SET @TableSortie='Table_Category'
		SET @Category_type='nvarchar(50)'
		SET @ID_type='int'
		SET @Category=''
		SET @Id=''
		SET @CatValue=''
		--SET @IDColumnName='idImport-Actes-CCAM'
		--SET @CatColumnName='UFX_UFX_Code'


		PRINT ('---STARTING B2 PROCEDURE WITH INPUT PARAMETERS @TableSource = ' + @TableSource + ' ,@TableSortie =' + @TableSortie + ' ,@IDColumnName = ' + @IDColumnName
													+' ,@CatColumnName = ' +@CatColumnName +' ,@MultiplierColumn = ' +@MultiplierColumn)

--DEFINITION DU CURSEUR SUR LES NIP / NUM_SEJOURS CLASSES PAR ORDRE CROISSANT DE TEMPORALITE

SET @Query= 'DECLARE MyCategoryCursor CURSOR SCROLL FOR
							SELECT DISTINCT [' + @CatColumnName + ']
							FROM [dbo].[' + @TableSource + ']' 
--PRINT(@QUERY)
EXECUTE(@Query) 


--CREER TABLE DE SORTIE
SET  @Query= 'Delete_Table_if_exists '+ @TableSortie
EXECUTE (@Query)

SET  @Query='CREATE TABLE [dbo].[' + @TableSortie +'] (
						[' + @IDColumnName +'] '+ @ID_type + ' NOT NULL'
				--->> PUT MULTIPLE COLUNMN ACCORDING TO MYCATEGORYCURSOR
	OPEN MyCategoryCursor
	FETCH NEXT FROM MyCategoryCursor INTO @Category

	WHILE @@FETCH_STATUS=0
	BEGIN
		SET  @Query+=',[' + @CatColumnName + '_' + @Category + '] ' + @Category_type 
		FETCH NEXT FROM MyCategoryCursor INTO @Category
	END
	SET  @Query+=')'


--PRINT @Query						
EXECUTE(@Query)



SET @Query= 'DECLARE MyIDCursor CURSOR SCROLL FOR
							SELECT DISTINCT [' + @IDColumnName + '],['+ @CatColumnName +'],['+ @MultiplierColumn +']
							FROM [dbo].[' + @TableSource + ']' 
--PRINT(@QUERY)
EXECUTE(@Query) 

OPEN MyIDCursor
FETCH NEXT FROM MyIDCursor INTO @ID, @Catvalue,@Mutliplier
	WHILE @@FETCH_STATUS=0
	BEGIN
			--RESTART CATEGORY CURSOR
			SET @Stop_Loop=0
			SET @Colnameitt = '[' + @IDColumnName +']'
			SET @Valueitt = CAST(@ID as nvarchar(50))

			FETCH FIRST FROM MyCategoryCursor INTO @Category
			WHILE @@FETCH_STATUS=0 AND @Stop_Loop=0
				BEGIN
						SET @Colnameitt += ',[' + @CatColumnName + '_' + @Category + ']'

						IF @CatValue=@Category
							BEGIN
								SET @Valueitt+=',' + CAST(@Mutliplier as nvarchar(15))
							END
						ELSE
							BEGIN
								SET @Valueitt+=',0'

							END
					FETCH NEXT FROM MyCategoryCursor INTO @Category
				END
		--print 'Colonnes :' + @Colnameitt
		--Print 'Valeurs :' + @Valueitt
		--PREPARATION QUERY D'INSERT
		SET  @Query= 'INSERT INTO [dbo].[' + @TableSortie + '] (' + @Colnameitt + ') VALUES ( ' + @Valueitt + ' )'
		--Print @Query
		EXECUTE (@Query)

		FETCH NEXT FROM MyIDCursor INTO @ID, @Catvalue,@Mutliplier
	END
	SET  @Query+=')'







--CLOSE CURSORS
CLOSE MyCategoryCursor
DEALLOCATE MyCategoryCursor

CLOSE MyIDCursor
DEALLOCATE MyIDCursor

/*
EXECUTE (@Query)




OPEN MyCursor
FETCH NEXT FROM MyCursor INTO @NIP , @Num_sejour, @Date_deb_sejour,@J0V1,@J0V2, @ANNEE_NIP,@PSejourDS,@PSejourSOS

		WHILE @@FETCH_STATUS = 0
		BEGIN

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
			--PRINT @Query
			EXECUTE (@Query)

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
--						Type de séjour = SUIVI_CT	"""		*/


