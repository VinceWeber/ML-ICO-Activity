/****** Script for SelectTopNRows command from SSMS  ******/

USE [ICO_Activite]
GO

--PROCEDURE CREANT DES CATEGORIES POUR LES UF (0 ou 1 par colonne de catégorie ) EN Y INTEGRANT LE POIDS ISSU DE LA TABLE UF (0 ou poids par colonne de catégorie).
-- EN DUR SE TROUVE LES NOMS DES COLONNES ID UF, DIMENSION ET POIDS ISSU DE LA TABLE UF

CREATE PROCEDURE [dbo].[Preproc_B2_Category_UF_With_Weight_V7] 
										 @TABLE_ACT nvarchar(50)
										,@TABLE_UF nvarchar(50)
										,@Table_Sortie nvarchar(50) 
 										--,@IDColumnName nvarchar(50)
										--,@ID_type nvarchar(50)
										--,@CatColumnName nvarchar(50)
										--,@MultiplierColumn nvarchar(50)
AS
SET NOCOUNT ON;



DECLARE
--@TABLE_ACT as nvarchar(50),
--@TABLE_UF as nvarchar(50),
@TABLE_TMP as nvarchar(50),
--@Table_Sortie as nvarchar(50),
@Query as nvarchar(500)

--SET @TABLE_ACT='A_Actes_Table_Analyse'
--SET @TABLE_UF='Listing_UF_V3'
SET @TABLE_TMP='Tmp_C0_Dim_UF'
--SET @Table_Sortie='Tmp_UF_Dim_Table'
SET @Query =''

--EFFACE TABLE TEMPORAIRE
EXECUTE [dbo].[Delete_Table_if_exists] @TABLE_TMP


--CREE UNE TABLE INTERMEDIAIRE AVEC ID_A, Dimension_parcours et poids acte
SET @Query=' SELECT Table_acte.[ID_A]
	   ,Table_UF.idUFX_Ress_Equ
      --,Table_UF.UFX_Code
	  ,Table_UF.Dimension_Parcours
	  --,Table_UF.Phase_Parcours
	  ,Table_UF.Poids_acte'
SET @Query+= ' INTO ' + @TABLE_TMP
SET @Query+= ' FROM [ICO_Activite].[dbo].[' + @TABLE_ACT +'] as Table_acte,'
SET @Query+= '	    [ICO_Activite].[dbo].[' + @TABLE_UF  +'] as Table_UF '
SET @Query+= '  WHERE Table_acte.UFX=Table_UF.UFX_Code ORDER BY ID_A '

--PRINT(@Query)
EXEC(@Query)

EXECUTE [dbo].[Preproc_B2_Category_With_Multiplier_V7] @TABLE_TMP , @Table_Sortie , 'idUFX_Ress_Equ' , 'Dimension_Parcours' , 'Poids_acte'

--EFFACE TABLE TEMPORAIRE
EXECUTE [dbo].[Delete_Table_if_exists] @TABLE_TMP