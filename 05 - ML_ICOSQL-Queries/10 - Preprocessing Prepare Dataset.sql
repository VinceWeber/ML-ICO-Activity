USE [ICO_Activite]
GO
/****** Object:  StoredProcedure [dbo].[Preproc_B1_Prepare_Dataset]    Script Date: 11/07/2023 20:13:07 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[Preproc_B1_Prepare_Dataset] @TABLE_ACT nvarchar(50), @TABLE_UF nvarchar(50), @Summary nvarchar(5)
AS
SET NOCOUNT ON;

DECLARE
--@TABLE_ACT as nvarchar(50),
--@TABLE_UF as nvarchar(50),
@TABLE_SORTIE_A as nvarchar(50),
@TABLE_SORTIE_B  as nvarchar(50),
@DATE_DEBUT_DATASET as DATETIME,
@Query as nvarchar(500)
--@Seuil as BIGINT,

--@Query as nvarchar(2000)
--@Dim_Parcours as nvarchar(50),
--@Dim_Parcours2 as nvarchar(50)

--SET @TABLE_UF='Listing_UF_V3' 
--SET @TABLE_ACT='A_Actes_ICO_2018_2021_TRIMED'
SET @TABLE_SORTIE_A='Tmp_PS_'
SET @TABLE_SORTIE_B='Tmp_Type_Sequence'

-- EXPORTER UN FILTRE DE LA BASE ACTES ICO DANS UNE TABLE DE TRAVAIL
EXECUTE [dbo].[Delete_Table_if_exists] 'Tmp_A_Actes_Table_Analyse';
IF @Summary='YES'
	BEGIN
		SET @Query ='SELECT TOP (2000) * INTO dbo.Tmp_A_Actes_Table_Analyse FROM ' + @TABLE_ACT + ' WHERE Source=''NGAP'''
		SET @Query += ' UNION '
		SET @Query +='SELECT TOP (2000) * FROM ' + @TABLE_ACT + ' WHERE Source=''CCAM'''
		SET @Query += ' UNION '
		SET @Query +='SELECT TOP (2000) * FROM ' + @TABLE_ACT + ' WHERE Source=''MVT'''
	END
ELSE
	BEGIN
		SET @Query ='SELECT  * INTO dbo.Tmp_A_Actes_Table_Analyse FROM ' + @TABLE_ACT 
	END
PRINT(@QUERY)
EXEC(@QUERY)

SET @TABLE_ACT='Tmp_A_Actes_Table_Analyse'

-- RENOMMER LES COLONNES DE LA TABLE DE TRAVAIL
EXECUTE [dbo].[Preproc_A0_RenameCol_V6] @TABLE_ACT


--LANCER LE PREPROCESSING SUR LA TABLE DE TRAVAIL

EXECUTE [dbo].[Preproc_A1_V6]  @TABLE_ACT , @TABLE_UF , 'Tmp_A1_PS' --APPLY POIDS A CHAQUE ACTE FONCTION DE L'UF
EXECUTE [dbo].[Preproc_A2_V6]  @TABLE_ACT ,@TABLE_UF, 'Tmp_A2_J0V1V2' --CALCULE LE J0 FONCTION DU 1er ACTE ou Extrapolation du NIP
EXECUTE [dbo].[Preproc_A3_V6]  @TABLE_ACT , @TABLE_UF , 'Tmp_A3_DFSej' , 10  --CREE UNE DATE DE DEBUT ET FIN DE SEJOUR POUR TOUS LES ACTES
EXECUTE [dbo].[Preproc_A3_1_V6] @TABLE_ACT, @TABLE_UF , 'Tmp_A3_Soins' , 'Tmp_A3_SOS'  --DEFINI L'ACTE CARACTERISTIQUE (CELUI DONT LE POIDS EST PREPONDERENT DANS UN SEJOUR, PAR DIMENSION SOINS ET SOS)
EXECUTE [dbo].[Preproc_A4_V6] @TABLE_ACT , @TABLE_UF , @TABLE_SORTIE_A , 'Tmp_A1_PS' ,'Tmp_A2_J0V1V2' ,'Tmp_A3_DFSej' --FORMATAGE D'UNE TABLE DE SORTIE EN VUE D'UNE ROUTINE APPLICANT LES PHASES DU PARCOURS 

EXECUTE [dbo].[Delete_Table_if_exists] 'Tmp_A1_PS'
EXECUTE [dbo].[Delete_Table_if_exists] 'Tmp_A2_J0V1V2'
EXECUTE [dbo].[Delete_Table_if_exists] 'Tmp_A3_DFSej'
--EXECUTE [dbo].[Delete_Table_if_exists] 'Tmp_A3_Soins'
--EXECUTE [dbo].[Delete_Table_if_exists] 'Tmp_A3_SOS'

/* ******************* DEFINTITION TYPE DE SEJOUR / DECOMPOSTION DU PARCOURS EN SEQUENCES DE PARCOURS INIT / TRAIT / SUIVI_CT / SUIVI_LT ******************* 
dbo.J0V3V4_Type_SEjour @TableSource nvarchar(20) =NULL
										, @TableSortie nvarchar(20) =NULL
										--, @Seuil_PS float=NULL
										,@Date_Deb_DATATSET DATETIME
										,@Seuil_LT float
										,@Seuil_CT float
*/
SET @DATE_DEBUT_DATASET = CONVERT(VARCHAR, '2018-01-01 00:00:00', 103)
EXECUTE [dbo].[Preproc_B1_J0V3V4_Type_Sejour_V7] @TABLE_SORTIE_A , @TABLE_SORTIE_B , @DATE_DEBUT_DATASET, 80,60
