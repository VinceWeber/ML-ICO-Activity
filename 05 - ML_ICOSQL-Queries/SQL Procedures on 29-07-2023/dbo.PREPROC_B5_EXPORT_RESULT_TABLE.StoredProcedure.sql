USE [ICO_Activite]
GO
/****** Object:  StoredProcedure [dbo].[PREPROC_B5_EXPORT_RESULT_TABLE]    Script Date: 29/07/2023 16:50:42 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [dbo].[Preproc_B4_Prepare_Dataset_Encoding]    Script Date: 21/07/2023 17:23:24 ******/


CREATE PROCEDURE [dbo].[PREPROC_B5_EXPORT_RESULT_TABLE] @Table_Sortie nvarchar(max),@DateDeb_Dataset DATETIME
AS
SET NOCOUNT ON;

DECLARE
	@Datetext nvarchar(50),
	@Query nvarchar(max)

SET @Datetext=CONVERT(nvarchar(max),@DateDeb_Dataset,120)


--EXPORT A RESULT TABLE 

EXECUTE Delete_Table_if_exists @Table_Sortie
SET @Query='
SELECT Table_acte_sejours.[NIP]
	  --,Encod_parcours.Cle_Encode_Parcours 
	  ,Table_Seq_NS.id_Sequence
	  --,Encod_Seq.Cle_Encode_Sequence
	  ,Table_acte_sejours.[N_S]
	  --,Encod_Sej.Cle_Encode_Sejour
	  ,Table_acte_sejours.[ID_A]
	  --,Encod_acte.Cle_Encode_acte
      ,Table_acte_sejours.[DD_A]
	  ,Table_acte_sejours.[DF_A]
	  ,Table_PS.J0_V1 as J0_V1
	  ,Table_Seq_NS.J0_V3 as J0_V3
	  ,DATEDIFF(D,Table_PS.J0_V1,Table_acte_sejours.[DD_A]) as J_Parcours_V1
	  ,DATEDIFF(D,Table_PS.J0_V2,Table_acte_sejours.[DD_A]) as J_Parcours_V2
	  ,DATEDIFF(D,Table_Seq_NS.J0_V3,Table_acte_sejours.[DD_A]) as J_Parcours_V3
	  ,DATEDIFF(D,''' + @Datetext + ''',Table_acte_sejours.[DD_A]) as J_DataSet
	  ,DATEDIFF(D,Table_acte_sejours.[DD_A],Table_acte_sejours.[DF_A])+1 as Duree
	  ,Table_UF.Service as Service
	  ,Table_UF.Activite as Activite
	  ,Table_UF.Phase_Parcours as Phase
	  ,Table_UF.Dimension_Parcours as Dimension
	  ,Table_Seq_NS.Type_Sequence as Type_seq
	  ,Table_UF_Serv.*
	  ,Table_UF_Acti.*
	  ,Table_UF_Phase.*
	  ,Table_UF_DimPoids.*

INTO ' + @Table_Sortie + '

FROM  [ICO_Activite].[dbo].[Tmp_A_Actes_Table_Analyse] as Table_acte_sejours,
	   [ICO_Activite].[dbo].[Tmp_Type_Sequence] as Table_Seq_NS,
	   [ICO_Activite].[dbo].[Listing_UF_V3] as Table_UF,
	   [ICO_Activite].[dbo].[Tmp_PS_] as Table_PS,
	   [ICO_Activite].[dbo].[Tmp_UF_Service_OHE] as Table_UF_Serv,
	   [ICO_Activite].[dbo].[Tmp_UF_Activite_OHE] as Table_UF_Acti,
       [ICO_Activite].[dbo].[Tmp_UF_Phase_OHE] as Table_UF_Phase,
	   [ICO_Activite].[dbo].[Tmp_UF_DimPoids_OHE] as Table_UF_DimPoids
	   --,
	   --[ICO_Activite].[dbo].[Tmp_Sequence_Encoding] as Encod_Seq,
	   --[ICO_Activite].[dbo].[Tmp_Sejour_Encoding] as Encod_Sej,
	   --[ICO_Activite].[dbo].[Tmp_Acte_Encoding] as Encod_acte,
	   --[ICO_Activite].[dbo].[Tmp_Parcours_Encoding] as Encod_parcours

 WHERE  Table_acte_sejours.N_S=Table_PS.N_S AND
		Table_acte_sejours.N_S=Table_Seq_NS.N_S AND
		Table_UF.UFX_Code=Table_acte_sejours.UFX AND
		Table_UF_Serv.[idUFX_Ress_Equ-OHE-Se]=Table_UF.idUFX_Ress_Equ AND
		Table_UF_Acti.[idUFX_Ress_Equ-OHE-Ac]=Table_UF.idUFX_Ress_Equ AND
		Table_UF_Phase.[idUFX_Ress_Equ-OHE-Ph]=Table_UF.idUFX_Ress_Equ AND
		Table_UF_DimPoids.[idUFX_Ress_Equ]=Table_UF.idUFX_Ress_Equ --AND
		--AND
		--Encod_Seq.id_Sequence=Table_Seq_NS.id_Sequence AND
		--Encod_Sej.N_S=Table_acte_sejours.N_S AND
		--Encod_acte.ID_A=Table_acte_sejours.ID_A AND
		--Encod_parcours.NIP=Table_acte_sejours.NIP'

--PRINT(@Query)
EXEC (@Query)
GO
