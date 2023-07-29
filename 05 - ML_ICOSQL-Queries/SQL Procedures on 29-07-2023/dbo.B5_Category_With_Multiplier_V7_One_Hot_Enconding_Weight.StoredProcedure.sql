USE [ICO_Activite]
GO
/****** Object:  StoredProcedure [dbo].[B5_Category_With_Multiplier_V7_One_Hot_Enconding_Weight]    Script Date: 29/07/2023 12:29:49 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[B5_Category_With_Multiplier_V7_One_Hot_Enconding_Weight] 
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
		SET @Category_type='bigint'
		SET @ID_type='nvarchar(10)'
		SET @Category=''
		SET @Id=''
		SET @CatValue=''
		--SET @IDColumnName='idImport-Actes-CCAM'
		--SET @CatColumnName='UFX_UFX_Code'


		PRINT ('---STARTING B5 PROC-- WITH INPUT PARAMETERS @TableSource = ' + @TableSource + ' ,@TableSortie =' + @TableSortie + ' ,@IDColumnName = ' + @IDColumnName
													+' ,@CatColumnName = ' +@CatColumnName +' ,@MultiplierColumn = ' +@MultiplierColumn)

--DEFINITION DU CURSEUR SUR LES NIP / NUM_SEJOURS CLASSES PAR ORDRE CROISSANT DE TEMPORALITE

SET @Query= 'DECLARE MyCategoryCursor CURSOR SCROLL FOR
							SELECT DISTINCT [' + @CatColumnName + ']
							FROM [dbo].[' + @TableSource + ']' 
PRINT(@QUERY)
EXECUTE(@Query) 


--CREER TABLE DE SORTIE
SET  @Query= 'Delete_Table_if_exists '+ @TableSortie
PRINT(@QUERY)
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


PRINT @Query						
EXECUTE(@Query)



SET @Query= 'DECLARE MyIDCursor CURSOR SCROLL FOR
							SELECT DISTINCT [' + @IDColumnName + '],['+ @CatColumnName +'],['+ @MultiplierColumn +']
							FROM [dbo].[' + @TableSource + ']' 
PRINT(@QUERY)
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
		Print(@Query)
		EXECUTE (@Query)

		FETCH NEXT FROM MyIDCursor INTO @ID, @Catvalue,@Mutliplier
	END
	SET  @Query+=')'


--CLOSE CURSORS
CLOSE MyCategoryCursor
DEALLOCATE MyCategoryCursor

CLOSE MyIDCursor
DEALLOCATE MyIDCursor
GO
