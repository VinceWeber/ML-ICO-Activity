USE [ICO_Activite]
GO
/****** Object:  StoredProcedure [dbo].[Preproc_A0_Filter_NIP_BY_2_DATES_AND_SITE]    Script Date: 29/07/2023 16:50:42 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Script for SelectTopNRows command from SSMS  ******/


CREATE PROCEDURE [dbo].[Preproc_A0_Filter_NIP_BY_2_DATES_AND_SITE] 
											@TableEntree nvarchar(50),
											@TableSortie nvarchar(50),
											@My_date1_deb_rech_NIP datetime,
											@My_date2_fin_rech_NIP datetime,
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
  SET @Query+=' FROM [ICO_Activite].[dbo].['+ @TableEntree + '] 
		WHERE [NIP_original] IN (
			SELECT DISTINCT [NIP_original]
			FROM [ICO_Activite].[dbo].[' + @TableEntree + ']
			WHERE Site_idSite=' + @Site + ' AND Date_début_acte BETWEEN '''+  + CONVERT(nvarchar(20), @My_date1_deb_rech_NIP , 120)	
 SET @Query+=''' AND ''' + CONVERT(nvarchar(20), @My_date2_fin_rech_NIP , 120) + ''' )'

 
 PRINT(@Query)
 EXEC(@Query)
GO
