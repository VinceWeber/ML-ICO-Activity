USE [ICO_Activite]
GO
/****** Object:  StoredProcedure [dbo].[Preproc_B1_Prepare_Dataset]    Script Date: 11/07/2023 20:13:07 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[Preproc_B2_Prepare_Dataset_Encoding] @TABLE_ACT nvarchar(50), @TABLE_UF nvarchar(50), @Summary nvarchar(5)
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

SET @TABLE_ACT='Tmp_A_Actes_Table_Analyse' -- A SUPPRIMER 

--TRANSFORMATION DES CATEGORIES EN MATRICE DE 0 ET 1 (EN COLONNE LES CATEGORIES, 1 SI APPARTIENT, 0 SINON) + POIDS SUR UF

		--ONE HOT ENCODING DES DIMENSIONS
			EXECUTE [dbo].[Preproc_B2_Category_V6] @TABLE_ACT , 'Tmp_UF_Table' , 'id_A' , 'UFX'
			EXECUTE [dbo].[Preproc_B2_Category_V6] @TABLE_ACT , 'Tmp_INX_Table' , 'id_A' , 'INX'
			EXECUTE [dbo].[Preproc_B2_Category_V6] @TABLE_ACT , 'Tmp_NGAP_Table' , 'id_A' , 'R_NGAP'
			EXECUTE [dbo].[Preproc_B2_Category_V6] @TABLE_ACT , 'Tmp_CCAM_Table' , 'id_A' , 'R_CCAM'
			EXECUTE [dbo].[Preproc_B2_Category_V6] @TABLE_ACT , 'Tmp_UFH_Table' , 'id_A' , 'UFH'
			EXECUTE [dbo].[Preproc_B2_Category_V6] @TABLE_ACT , 'Tmp_Code_Equip_Table' , 'id_A' , 'Code_Equip'
			EXECUTE [dbo].[Preproc_B2_Category_V6] @TABLE_SORTIE_B , 'Tmp_Type_Seq_Table' , 'id_Sequence' , 'Type_Sequence'
		
		--BINARY ENCODING DES DIMENSIONS
			--COPIER LES PRECEDENTS PROCEDURES POUR NE PAS GENERER X COLONNES, MAIS 1 SEULE, CONTENANT UNE STRING BINAIRE
			--REPARTIR DU DEBUT (LISTE DES DIFFERENTS ITEMS DE LA CATEGORIE



--BINARY ENCODING AGGREGATION PAR SEJOUR / SEQUENCE / PARCOURS PATIENT
	--METHODE 1 :  PAR PRESENCE OU ABSENCE DE LA CATEGORIE DANS L'AGGREGATION
	--METHODE 2 :  PAR SOMME DES OCCURENCES DE LA CATEGORIE DANS L'AGGREGATION
	
		--STEP 1: APPELER LA TABLE DE ONE HOT ENCODING EN Y AJOUTANT LA COLONNE DE L'AGGREGATEUR
			--COLONNES DE LA TABLE
				--ID_AGGREGATEUR			(SEJOUR)
				--ID_ELEMENT_A_AGGREGER		(ACTE)
				--COLONNES DE LA DIMENSION	(UF, INX, NGAP, CCAM, UFH, Code_Equip,...)

				--LISTER DANS UN CURSEUR L'ENSEMBLE DES COLONNES DE LA DIMENSION
				--SELECT MAX(COl_dim) SI METHODE 1 -> OU SUM SI METHODE 2
				--FROM Tmp_UF_Table, Table_acte
				--WHERE ID_acte
				--GROUP BY ID_AGGREGATEUR

				--ENVOYER LE RESULTAT DANS UNE 



--EXECUTE [dbo].[Preproc_B2_Category_UF_With_Weight_V7] @TABLE_ACT, @TABLE_UF, 'Tmp_UF_Dim_Table' 