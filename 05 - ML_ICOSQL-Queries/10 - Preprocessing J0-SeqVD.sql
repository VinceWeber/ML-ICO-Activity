
USE [ICO_Activite]
GO


DECLARE
@TABLE_ACT as nvarchar(50),
@TABLE_UF as nvarchar(50),
@TABLE_SORTIE_A as nvarchar(50),
@TABLE_SORTIE_B  as nvarchar(50),
@DATE_DEBUT_DATASET as DATETIME,
@Query as nvarchar(500)
--@Seuil as BIGINT,

--@Query as nvarchar(2000)
--@Dim_Parcours as nvarchar(50),
--@Dim_Parcours2 as nvarchar(50)

SET @TABLE_UF='Listing_UF_V3' 
SET @TABLE_ACT='A_Actes_ICO_2018_2021_TRIMED'
SET @TABLE_SORTIE_A='Tmp_PS_'
SET @TABLE_SORTIE_B='Tmp_Type_Sequence'

-- EXPORTER UN FILTRE DE LA BASE ACTES ICO DANS UNE TABLE DE TRAVAIL
EXECUTE [dbo].[Delete_Table_if_exists] 'A_Actes_Table_Analyse';
SET @Query ='SELECT TOP (2000) * INTO dbo.A_Actes_Table_Analyse FROM ' + @TABLE_ACT + ' WHERE Source=''NGAP'''
SET @Query += ' UNION '
SET @Query +='SELECT TOP (2000) * FROM ' + @TABLE_ACT + ' WHERE Source=''CCAM'''
SET @Query += ' UNION '
SET @Query +='SELECT TOP (2000) * FROM ' + @TABLE_ACT + ' WHERE Source=''MVT'''
PRINT(@QUERY)
EXEC(@QUERY)

SET @TABLE_ACT='A_Actes_Table_Analyse'

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
EXECUTE [dbo].[Delete_Table_if_exists] 'Tmp_A3_Soins'
EXECUTE [dbo].[Delete_Table_if_exists] 'Tmp_A3_SOS'

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

--TRANSFORMATION DES CATEGORIES EN MATRICE DE 0 ET 1 (EN COLONNE LES CATEGORIES, 1 SI APPARTIENT, 0 SINON) + POIDS SUR UF

--EXECUTE [dbo].[Preproc_B2_Category_V6] @TABLE_ACT , 'Tmp_UF_Table' , 'id_A' , 'UFX'
--EXECUTE [dbo].[Preproc_B2_Category_V6] @TABLE_ACT , 'Tmp_INX_Table' , 'id_A' , 'INX'
--EXECUTE [dbo].[Preproc_B2_Category_V6] @TABLE_ACT , 'Tmp_NGAP_Table' , 'id_A' , 'R_NGAP'
--EXECUTE [dbo].[Preproc_B2_Category_V6] @TABLE_ACT , 'Tmp_CCAM_Table' , 'id_A' , 'R_CCAM'
--EXECUTE [dbo].[Preproc_B2_Category_V6] @TABLE_ACT , 'Tmp_UFH_Table' , 'id_A' , 'UFH'
--EXECUTE [dbo].[Preproc_B2_Category_V6] @TABLE_ACT , 'Tmp_Code_Equip_Table' , 'id_A' , 'Code_Equip'
--EXECUTE [dbo].[Preproc_B2_Category_V6] @TABLE_SORTIE_B , 'Tmp_Type_Seq_Table' , 'id_Sequence' , 'Type_Sequence'

--EXECUTE [dbo].[Preproc_B2_Category_UF_With_Weight_V7] @TABLE_ACT, @TABLE_UF, 'Tmp_UF_Dim_Table' 

