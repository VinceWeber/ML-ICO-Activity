USE [ICO_Activite]
GO
/****** Object:  StoredProcedure [dbo].[X_Delete_Preproc_A0_RenameCol]    Script Date: 29/07/2023 12:29:49 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
---PROCEDURE A0

---RENAME COLONNES DES TABLES ACTES


CREATE PROCEDURE [dbo].[X_Delete_Preproc_A0_RenameCol] @BDD nvarchar(50) 
									
								
AS
SET NOCOUNT ON;

DECLARE 
	--VARIABLES "INTERNES"
		@Query nvarchar(500)

SET @Query='EXEC sp_rename dbo.' + @BDD +'.idImport-Actes-CCAM, id_Acte, COLUMN;'
EXECUTE(@Query)
/*
SET @Query='EXEC sp_rename dbo.' + @BDD +'.NIP_original', 'NIP', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Ho_Num_Num_sejour', 'NS', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Date_début_acte', 'DDA', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Date_fin_acte', 'DFA', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Date-Debut_Mvt', 'DDM', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Heure_Debut_Mvt', 'HDM', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Date-Fin_Mvt', 'DFM', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Heure_Fin_Mvt', 'HFM', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.UFX_UFX_Code', 'UFX', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.UFX_Code_Lib', 'UFXCL', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Ressource_Med_INX_INX_Code', 'INX', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.INX_Code_Lib', 'INXCL', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.INX_Code_Spe', 'INXCS', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Ref_Actes_NGAP_AC_Code_NGAP', 'RNGAP', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.AC_Ref', 'AC_Ref', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.AC_Lib', 'AC_Lib', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Lib_spe', 'Lib_spe', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Ref_spe', 'Ref_spe', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Ref_Actes_CCAM_AC_Ref_CCAM', 'RCCAM', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.CCAM_AC_Lib', 'CCAM_AC_Lib', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.AC_Acti', 'AC_Acti', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.AC_Asso', 'AC_Asso', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Lc_Prix', 'Lc_Prix', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.mh_ufheber_??', 'UFH', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.mh_ufheber_lib_??', 'UFHL', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Prix_Acte', 'Prix_Acte', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Cout_Acte', 'Cout_Acte', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Statut', 'Statut', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Equipements_Code_Equipement', 'Code_Equip', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Site_idSite', 'Site', 'COLUMN';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename 'dbo.' + @BDD +'.Source', 'Source', 'COLUMN';'
EXECUTE(@Query)

*/
GO
