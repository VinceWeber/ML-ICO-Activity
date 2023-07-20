USE [ICO_Activite]
GO
/****** Object:  StoredProcedure [dbo].[Preproc_B2_Prepare_Dataset_Encoding]    Script Date: 13/07/2023 10:02:54 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[Preproc_B4_Prepare_Dataset_Encoding] @TABLE_ACT nvarchar(50), @TABLE_UF nvarchar(50), @Summary nvarchar(5)
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

SET @TABLE_ACT='Tmp_A_Actes_Table_Analyse'

--BINARY ENCODING DES ACTES :
	--CAtégorie = CONCATENATION DES UF + REFCCAM/NGAP/UFH

	--STEP 1 : CREER UNE TABLE AVEC ID_ACTE+CLE
		
		EXECUTE Delete_Table_if_exists Tmp_Acte_Encoding
		SELECT ID_A, CONCAT(UFX,R_CCAM,R_NGAP,UFH) as Cle_Encode_acte INTO Tmp_Acte_Encoding FROM Tmp_A_Actes_Table_Analyse
	--STEP 2 : CREER LA TABLE DE BINARY ENCODING

		EXECUTE [dbo].[Preproc_B3_Binary_Encoding] Tmp_Acte_Encoding, 'Tmp_Acte_Binary_Table', 'Cle_Encode_acte'
	--STEP 3 : CREER UNE COLONNE AVEC ID_ACTE+CLE+ENCODING
		EXECUTE Delete_Table_if_exists Tmp_Acte_Encoded

		SELECT Tmp_Acte_Encoding.ID_A,Tmp_Acte_Binary_Table.[Row_Encoded], Tmp_Acte_Encoding.Cle_Encode_acte as Cle_Acte_Encoded
		INTO Tmp_Acte_Encoded
		FROM [ICO_Activite].[dbo].[Tmp_Acte_Encoding] as Tmp_Acte_Encoding,
			 [ICO_Activite].[dbo].[Tmp_Acte_Binary_Table] as Tmp_Acte_Binary_Table
		WHERE Tmp_acte_Encoding.Cle_Encode_acte=Tmp_acte_Binary_Table.Cle_Encode_acte
		

--BINARY ENCODING DES SEJOURS :
	--CAtégorie = CONCATENATION DES COLLECTIONS D'ACTES (Classés par DATE et UFX)

	--STEP 1 : CREER UNE TABLE AVEC ID_SEJOUR+CLE
		
		EXECUTE Delete_Table_if_exists Tmp_Sejour_Encoding
		SELECT N_S, STRING_AGG(table_Acte_Encoded.Cle_Acte_Encoded,'-') WITHIN GROUP (ORDER BY Table_sejour.DD_A asc, Table_sejour.UFX asc) as Cle_Encode_Sejour 
		
		INTO Tmp_Sejour_Encoding 
		
		FROM Tmp_A_Actes_Table_Analyse as Table_sejour, Tmp_Acte_Encoded as Table_Acte_Encoded
		WHERE Table_Acte_Encoded.ID_A=Table_sejour.ID_A
				-- AND AJOUTER FILTRE SUR LES CONDITIONS DE COLLECTION DE SEJOURS (POIDS > xx / NB ACTE > xx)
		GROUP BY N_S
		--CREATE INTERMED TABLE WITH ID_SEjour and AGGREGATE


	--STEP 2 : CREER LA TABLE DE BINARY ENCODING
	
		EXECUTE [dbo].[Preproc_B3_Binary_Encoding] Tmp_Sejour_Encoding, 'Tmp_Sejour_Binary_Table', 'Cle_Encode_Sejour'
	
	--STEP 3 : CREER UNE COLONNE AVEC ID_Sejour+CLE+ENCODING
		EXECUTE Delete_Table_if_exists Tmp_Sejour_Encoded
		
		SELECT Tmp_Sejour_Encoding.[N_S],Tmp_Sejour_Binary_Table.[Row_Encoded], Tmp_Sejour_Encoding.Cle_Encode_Sejour as Cle_Sejour_Encoded
		INTO Tmp_Sejour_Encoded
		FROM [ICO_Activite].[dbo].[Tmp_Sejour_Encoding] as Tmp_Sejour_Encoding,
			 [ICO_Activite].[dbo].[Tmp_Sejour_Binary_Table] as Tmp_Sejour_Binary_Table
		WHERE Tmp_Sejour_Encoding.Cle_Encode_Sejour=Tmp_Sejour_Binary_Table.Cle_Encode_Sejour
		
	
	
--BINARY ENCODING DES SEQUENCES :
	--Catégorie = CONCATENATION DES COLLECTIONS DE SEJOURS (Classés par DATE)

	--STEP 1 : CREER UNE TABLE AVEC ID_SEQUENCE+CLE
		
		EXECUTE Delete_Table_if_exists Tmp_Sequence_Encoding
		SELECT id_Sequence, STRING_AGG(CAST(table_Sejour_Encoded.Row_Encoded as int),'-') WITHIN GROUP (ORDER BY Table_sequence.Ddebsej asc) as Cle_Encode_Sequence 
		
		INTO Tmp_Sequence_Encoding 
		
		FROM Tmp_Type_Sequence as Table_sequence, Tmp_Sejour_Encoded as Table_sejour_Encoded
		WHERE Table_sejour_Encoded.N_S=Table_sequence.N_S
				-- AND AJOUTER FILTRE SUR LES CONDITIONS DE COLLECTION DE SEJOURS (POIDS > xx / NB ACTE > xx)
		GROUP BY id_Sequence
		--CREATE INTERMED TABLE WITH ID_SEjour and AGGREGATE

	--STEP 2 : CREER LA TABLE DE BINARY ENCODING
	
		EXECUTE [dbo].[Preproc_B3_Binary_Encoding] Tmp_Sequence_Encoding, 'Tmp_Sequence_Binary_Table', 'Cle_Encode_Sequence'

	--STEP 3 : CREER UNE COLONNE AVEC ID_Sequence+CLE+ENCODING
		EXECUTE Delete_Table_if_exists Tmp_Sequence_Encoded
		
		SELECT Tmp_Sequence_Encoding.[id_Sequence],Tmp_Sequence_Binary_Table.[Row_Encoded], Tmp_Sequence_Encoding.Cle_Encode_Sequence as Cle_Sequence_Encoded
		INTO Tmp_Sequence_Encoded
		FROM [ICO_Activite].[dbo].[Tmp_Sequence_Encoding] as Tmp_Sequence_Encoding,
			 [ICO_Activite].[dbo].[Tmp_Sequence_Binary_Table] as Tmp_Sequence_Binary_Table
		WHERE Tmp_Sequence_Encoding.Cle_Encode_Sequence=Tmp_Sequence_Binary_Table.Cle_Encode_Sequence




--BINARY ENCODING DES PARCOURS :
	--Catégorie = CONCATENATION DES COLLECTIONS DE SEQUENCES (Classés par DATE)

	--STEP 1 : CREER UNE TABLE AVEC ID_SEQUENCE+CLE
		
		EXECUTE Delete_Table_if_exists Tmp_Parcours_Encoding
		SELECT NIP, STRING_AGG(CAST(Table_Sequence_Encoded.Row_Encoded as int),'-') WITHIN GROUP (ORDER BY Table_Parcours.Ddebsej asc) as Cle_Encode_Parcours 
		
		INTO Tmp_Parcours_Encoding 
		
		FROM Tmp_Type_Sequence as Table_Parcours, Tmp_Sequence_Encoded as Table_Sequence_Encoded
		WHERE Table_Sequence_Encoded.id_Sequence=Table_Parcours.id_Sequence
				-- AND AJOUTER FILTRE SUR LES CONDITIONS DE COLLECTION DE SequenceS (POIDS > xx / NB ACTE > xx)
		GROUP BY NIP
		--CREATE INTERMED TABLE WITH ID_Sequence and AGGREGATE

	--STEP 2 : CREER LA TABLE DE BINARY ENCODING
	
		EXECUTE [dbo].[Preproc_B3_Binary_Encoding] Tmp_Parcours_Encoding, 'Tmp_Parcours_Binary_Table', 'Cle_Encode_Parcours'

	--STEP 3 : CREER UNE COLONNE AVEC ID_Parcours+CLE+ENCODING
		EXECUTE Delete_Table_if_exists Tmp_Parcours_Encoded
		
		SELECT Tmp_Parcours_Encoding.[NIP],Tmp_Parcours_Binary_Table.[Row_Encoded], Tmp_Parcours_Encoding.Cle_Encode_Parcours as Cle_Parcours_Encoded
		INTO Tmp_Parcours_Encoded
		FROM [ICO_Activite].[dbo].[Tmp_Parcours_Encoding] as Tmp_Parcours_Encoding,
			 [ICO_Activite].[dbo].[Tmp_Parcours_Binary_Table] as Tmp_Parcours_Binary_Table
		WHERE Tmp_Parcours_Encoding.Cle_Encode_Parcours=Tmp_Parcours_Binary_Table.Cle_Encode_Parcours