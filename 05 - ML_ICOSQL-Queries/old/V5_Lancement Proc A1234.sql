
USE [ICO_Activite]
GO


DECLARE
@TABLE_ACT as nvarchar(50),
@TABLE_UF as nvarchar(50),
@TABLE_SORTIE_A as nvarchar(50),
@TABLE_SORTIE_B  as nvarchar(50),
@DATE_DEBUT_DATASET as DATETIME
--@Seuil as BIGINT,

--@Query as nvarchar(2000)
--@Dim_Parcours as nvarchar(50),
--@Dim_Parcours2 as nvarchar(50)

SET @TABLE_UF='Listing_UF_V3' 
SET @TABLE_ACT='ACTES_TEST_NAMES'
SET @TABLE_SORTIE_A='Tmp_A1234_Export'

EXECUTE [dbo].[Preproc_A1]  @TABLE_ACT , @TABLE_UF , 'Tmp_A1_PS' --APPLY POIDS A CHAQUE ACTE FONCTION DE L'UF
EXECUTE [dbo].[Preproc_A2]  @TABLE_ACT ,@TABLE_UF, 'Tmp_A2_J0V1V2' --CALCULE LE J0 FONCTION DU 1er ACTE ou Extrapolation du NIP
EXECUTE [dbo].[Preproc_A3]  @TABLE_ACT , @TABLE_UF , 'Tmp_A3_DFSej' , 10  --CREE UNE DATE DE DEBUT ET FIN DE SEJOUR POUR TOUS LES ACTES
EXECUTE [dbo].[Preproc_A3_1] @TABLE_ACT, @TABLE_UF , 'Tmp_A3_Soins' , 'Tmp_A3_SOS'  --DEFINI L'ACTE CARACTERISTIQUE (CELUI DONT LE POIDS EST PREPONDERENT DANS UN SEJOUR, PAR DIMENSION SOINS ET SOS)
EXECUTE [dbo].[Preproc_A4] @TABLE_ACT , @TABLE_UF , @TABLE_SORTIE_A , 'Tmp_A1_PS' ,'Tmp_A2_J0V1V2' ,'Tmp_A3_DFSej' --FORMATAGE D'UNE TABLE DE SORTIE EN VUE D'UNE ROUTINE APPLICANT LES PHASES DU PARCOURS 

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
EXECUTE [dbo].[Preproc_B1_J0V3V4_Type_Sejour] @TABLE_SORTIE_A , @TABLE_SORTIE_B , @DATE_DEBUT_DATASET, 80,60

--TRANSFORMATION DES CATEGORIES EN MATRICE DE 0 ET 1 (EN COLONNE LES CATEGORIES, 1 SI APPARTIENT, 0 SINON)
EXECUTE [dbo].[Preproc_B2_Category] @TABLE_ACT , 'Tmp_UF_Table' , 'idImport-Actes-CCAM' , 'UFX_UFX_Code'

