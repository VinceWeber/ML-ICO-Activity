---PROCEDURE A0

---RENAME COLONNES DES TABLES ACTES

USE [ICO_Activite]
GO

CREATE PROCEDURE dbo.Preproc_A0_RenameCol_V6 @BDD nvarchar(50) 
									
AS
SET NOCOUNT ON

DECLARE 
	--VARIABLES "INTERNES"
		@Query nvarchar(500)
		--@BDD nvarchar(50)

--SET @BDD='TEST_TABLE'

SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.idImport-Actes-CCAM'', ''ID_A'', ''COLUMN'';'
PRINT(@Query)
EXECUTE(@Query)

SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.NIP_original'', ''NIP'', ''COLUMN'';'
PRINT(@Query)
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Ho_Num_Num_sejour'', ''N_S'', ''COLUMN'';'
PRINT(@Query)
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Date_début_acte'', ''DD_A'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Date_fin_acte'', ''DF_A'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Date-Debut_Mvt'', ''DD_M'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Heure_Debut_Mvt'', ''HD_M'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Date-Fin_Mvt'', ''DF_M'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Heure_Fin_Mvt'', ''HF_M'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.UFX_UFX_Code'', ''UFX'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.UFX_Code_Lib'', ''UFX_CL'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Ressource_Med_INX_INX_Code'', ''INX'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.INX_Code_Lib'', ''INX_CL'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.INX_Code_Spe'', ''INX_CS'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Ref_Actes_NGAP_AC_Code_NGAP'', ''R_NGAP'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.AC_Ref'', ''AC_Ref'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.AC_Lib'', ''AC_Lib'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Lib_spe'', ''Lib_spe'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Ref_spe'', ''Ref_spe'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Ref_Actes_CCAM_AC_Ref_CCAM'', ''R_CCAM'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.CCAM_AC_Lib'', ''CCAM_AC_Lib'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.AC_Acti'', ''AC_Acti'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.AC_Asso'', ''AC_Asso'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Lc_Prix'', ''Lc_Prix'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.mh_ufheber_??'', ''UFH'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.mh_ufheber_lib_??'', ''UFH_L'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Prix_Acte'', ''Prix_Acte'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Cout_Acte'', ''Cout_Acte'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Statut'', ''Statut'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Equipements_Code_Equipement'', ''Code_Equip'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Site_idSite'', ''Site'', ''COLUMN'';'
EXECUTE(@Query)
SET @Query='EXEC sp_rename ' +'''dbo.' + @BDD +'.Source'', ''Source'', ''COLUMN'';'
EXECUTE(@Query)
