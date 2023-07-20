/****** Script for SelectTopNRows command from SSMS  ******/

USE [ICO_Activite]
GO

--PROCEDURE DE RESTITUION D'UN DATASET EXPLOITABLE
-- PARAMETRES A AJOUTER OU DUPLIQUER PROCEDURE
-- REGROUPER PAR SEJOUR, SEQUENCE, PARCOURS(NIP), SOMMER LES CATEGORIES, ET RESTITUER LA DUREE D'UN SEJOUR, SEQUENCE, PARCOURS


--CREATE PROCEDURE [dbo].[Preproc_B2_Category_UF_With_Weight_V7] 
--										 @TABLE_ACT nvarchar(50)
--										,@TABLE_UF nvarchar(50)
--										,@Table_Sortie nvarchar(50) 
 										--,@IDColumnName nvarchar(50)
										--,@ID_type nvarchar(50)
										--,@CatColumnName nvarchar(50)
										--,@MultiplierColumn nvarchar(50)
--AS
--SET NOCOUNT ON;

DECLARE
@TABLE_ACT as nvarchar(50),
@TABLE_Seq as nvarchar(50),
@TABLE_UF as nvarchar(50),
@TABLE_Seq_NS as nvarchar(50),
@TABLE_PD_UF as nvarchar(50),
@TABLE_TMP as nvarchar(50),
--@Table_Sortie as nvarchar(50),
@Query as nvarchar(max),
@Col as nvarchar(max),

@J0DATASET as DATETIME,
@Curs_Text NVARCHAR(50),
@Curs_TNAME NVARCHAR(50),
@Curs_OPOS NVARCHAR(50)

SET @TABLE_ACT='A_Actes_Table_Analyse'
SET @TABLE_Seq='Tmp_Type_Seq_Table'
SET @TABLE_UF='Listing_UF_V3'
SET @TABLE_Seq_NS='Tmp_Type_Sequence'
SET @TABLE_PD_UF='Tmp_UF_Dim_Table'
SET @J0DATASET= CONVERT(VARCHAR, '2018-01-01 00:00:00', 103)

--*******************   CREATE A CURSOR - BY LISTING ALL COLUMNS OF TABLES ****************************

SET @Query= 'DECLARE MyCursor CURSOR SCROLL FOR		
			SELECT CONCAT(TABLE_NAME,''.'', COLUMN_NAME) as Import_text, TABLE_NAME, ORDINAL_POSITION
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_NAME = N''Tmp_Type_Seq_Table'' AND COLUMN_NAME<>''id_A''

			ORDER BY TABLE_NAME, ORDINAL_POSITION'
EXECUTE(@Query)

SET @Col=''

OPEN MyCursor
FETCH NEXT FROM MyCursor INTO @Curs_TEXT , @Curs_TNAME, @Curs_OPOS

		WHILE @@FETCH_STATUS = 0
		BEGIN
			SET @Col+= ' , ' + @Curs_TEXT

			FETCH NEXT FROM MyCursor INTO  @Curs_TEXT , @Curs_TNAME, @Curs_OPOS
		END

CLOSE MyCursor
DEALLOCATE MyCursor

PRINT('COL = ' + @col)

SET @Query='
SELECT  
		Table_acte_sejours.[NIP]
	  ,''0'' as Type_Parcours 
	  ,Table_acte_sejours.[N_S]
	  ,Table_acte_sejours.[id_A]
      ,Table_acte_sejours.[DD_A]
	  ,Table_acte_sejours.[DF_A]
	  ,Table_PS.J0_V1 as J0_V1
	  ,Table_Seq_NS.J0_V3 as J0_V3
	  ,DATEDIFF(D,Table_PS.J0_V1,Table_acte_sejours.[DD_A]) as J_Parcours_V1
	  ,DATEDIFF(D,Table_PS.J0_V2,Table_acte_sejours.[DD_A]) as J_Parcours_V2
	  ,DATEDIFF(D,Table_Seq_NS.J0_V3,Table_acte_sejours.[DD_A]) as J_Parcours_V3
	  ,DATEDIFF(D,''' + CAST(@J0DATASET as nvarchar(20)) +''',Table_acte_sejours.[DD_A]) as J_DataSet
	  ,DATEDIFF(D,Table_acte_sejours.[DD_A],Table_acte_sejours.[DF_A])+1 as Duree
	   ' 
SET @Query+= @Col	  
	  /*
	  ,Table_Seq.*  -- 1ERE TABLE IMPORTEE 
	  ,Table_Pds_Dim_UF.* --Autant de colonne que de dimension d'UF, valeur=Poids_acte
	  ,Table_CAT_UF.* -- Autant de colonne que de type d'UF, valeur =1 pour Niveau détaillé, Somme des occurences pour niveau Séjour, Séquence ou parcours.
      ,Table_CAT_NGAP.* -- Autant de colonne que de type de Ref NGAP, valeur =1 pour Niveau détaillé, Somme des occurences pour niveau Séjour, Séquence ou parcours.
      ,Table_CAT_CCAM.* -- Autant de colonne que de type de Ref CCAM, valeur =1 pour Niveau détaillé, Somme des occurences pour niveau Séjour, Séquence ou parcours.
 	  ,Table_CAT_INX.* -- Autant de colonne que de type de Ref INX, valeur =1 pour Niveau détaillé, Somme des occurences pour niveau Séjour, Séquence ou parcours.
      ,Table_CAT_UFH.* -- Autant de colonne que de type de Ref UFH, valeur =1 pour Niveau détaillé, Somme des occurences pour niveau Séjour, Séquence ou parcours.
	  ,Table_CAT_Eq.* -- Autant de colonne que de type de Code_Equip, valeur =1 pour Niveau détaillé, Somme des occurences pour niveau Séjour, Séquence ou parcours.
	  */
SET @Query+=' 
  FROM [ICO_Activite].[dbo].[A_Actes_Table_Analyse] as Table_acte_sejours, --NIP, N_S, Dates,UF
	   --[ICO_Activite].[dbo].[Tmp_Type_Parcours] as Table_Parcours,
	   [ICO_Activite].[dbo].[Tmp_Type_Seq_Table] as Table_Seq,
	   [ICO_Activite].[dbo].[Tmp_Type_Sequence] as Table_Seq_NS,
	   [ICO_Activite].[dbo].[Tmp_UF_Dim_Table] as Table_Pds_Dim_UF,
	   [ICO_Activite].[dbo].[Listing_UF_V3] as Table_UF,
	   [ICO_Activite].[dbo].[Tmp_UF_Table] as Table_CAT_UF,
	   [ICO_Activite].[dbo].[Tmp_NGAP_Table] as Table_CAT_NGAP,
	   [ICO_Activite].[dbo].[Tmp_CCAM_Table] as Table_CAT_CCAM,
	   [ICO_Activite].[dbo].[Tmp_INX_Table] as Table_CAT_INX,
	   [ICO_Activite].[dbo].[Tmp_UFH_Table] as Table_CAT_UFH,
	   [ICO_Activite].[dbo].[Tmp_Code_Equip_Table] as Table_CAT_Eq,
	   [ICO_Activite].[dbo].[Tmp_PS_] as Table_PS

 WHERE	Table_acte_sejours.N_S=Table_Seq_NS.N_S AND
		Table_Seq_NS.id_Sequence=Table_Seq.id_Sequence AND
		Table_Pds_Dim_UF.idUFX_Ress_Equ=Table_UF.idUFX_Ress_Equ AND
		Table_acte_sejours.UFX=Table_UF.UFX_Code AND
		Table_acte_sejours.ID_A=Table_CAT_UF.id_A AND
		Table_acte_sejours.ID_A=Table_CAT_NGAP.id_A AND
		Table_acte_sejours.ID_A=Table_CAT_CCAM.id_A AND
		Table_acte_sejours.ID_A=Table_CAT_INX.id_A AND
		Table_acte_sejours.ID_A=Table_CAT_UFH.id_A AND
		Table_acte_sejours.N_S=Table_PS.N_S AND
		Table_acte_sejours.ID_A=Table_CAT_Eq.id_A --AND
		--Table_acte_sejours.NIP=''A197600310''

 ORDER BY Table_Seq.id_Sequence,J_Parcours_V1'

PRINT(@Query)
EXECUTE(@Query)