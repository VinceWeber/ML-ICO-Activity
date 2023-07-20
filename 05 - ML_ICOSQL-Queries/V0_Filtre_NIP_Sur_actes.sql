/****** Script for SelectTopNRows command from SSMS  ******/


CREATE PROCEDURE [dbo].[Preproc_A0_Filter_NIP_BY_DATE_AND_SITE] 
											@TableSortie nvarchar(50),
											@My_day int,
											@My_month int,
											@My_year int,
											@Site nvarchar(5)				
AS
SET NOCOUNT ON;

DECLARE
@Query as nvarchar(2000)
--@TABLE_A_EFF as nvarchar(50)
--SET  @TABLE_A_EFF='AAA_Tmp2_J0V12'

EXECUTE Delete_Table_if_exists @TableSortie

SET @Query='
	  SELECT [idImport-Actes-CCAM]
      ,[NIP_original]
      ,[Ho_Num_Num_sejour]
      ,[Date_début_acte]
      ,[Date_fin_acte]
      ,[Date-Debut_Mvt]
      ,[Heure_Debut_Mvt]
      ,[Date-Fin_Mvt]
      ,[Heure_Fin_Mvt]
      ,[UFX_UFX_Code]
      ,[UFX_Code_Lib]
      ,[Ressource_Med_INX_INX_Code]
      ,[INX_Code_Lib]
      ,[INX_Code_Spe]
      ,[Ref_Actes_NGAP_AC_Code_NGAP]
      ,[AC_Ref]
      ,[AC_Lib]
      ,[Lib_spe]
      ,[Ref_spe]
      ,[Ref_Actes_CCAM_AC_Ref_CCAM]
      ,[CCAM_AC_Lib]
      ,[AC_Acti]
      ,[AC_Asso]
      ,[Lc_Prix]
      ,[mh_ufheber_??]
      ,[mh_ufheber_lib_??]
      ,[Prix_Acte]
      ,[Cout_Acte]
      ,[Statut]
      ,[Equipements_Code_Equipement]
      ,[Site_idSite]
      ,[Source]
	  '
  SET @Query+=' INTO ' + @TableSortie +'
  '
  SET @Query+=' FROM [ICO_Activite].[dbo].[A_Actes_ICO_2018_2021_TRIMED] 
		WHERE [NIP_original] IN (
			SELECT DISTINCT [NIP_original]
			FROM [ICO_Activite].[dbo].[A_Actes_ICO_2018_2021_TRIMED]
			WHERE DAY(Date_début_acte)=' + CAST(@My_day as  nvarchar(10))
			
 SET @Query+=' AND MONTH(Date_début_acte)=' + CAST(@My_month as  nvarchar(10))
 SET @Query+=' AND YEAR(Date_début_acte)=' + CAST(@My_year as  nvarchar(10)) 
 SET @Query+=' AND Site_idSite=' + @Site +')'

 EXEC(@Query)