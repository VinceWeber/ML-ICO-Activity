/****** Script for SelectTopNRows command from SSMS  ******/
SELECT Table_Actes.[NIP]
	  --,Table_Actes.[ID_A]
	  --,STRING_AGG(Table_Encoded_Actes.Cle_Acte_Encoded,'-')  WITHIN GROUP(ORDER BY CONCAT(UFX,R_CCAM,R_NGAP,UFH)  ASC) as Actes_Serie
	  --,Table_Actes.[N_S]
	  --,STRING_AGG(Table_Encoded_Sejours.Cle_Sejour_Encoded,'-')  WITHIN GROUP(ORDER BY DD_A  ASC) as Sejour_Serie
	  --,Table_Encoded_Sequence.id_Sequence
	  ,STRING_AGG(Table_Encoded_Sequence.Cle_Sequence_Encoded,'-')  WITHIN GROUP(ORDER BY DD_A  ASC) as Sequence_Serie
	  --,Table_Encoded_Sequence.Cle_Sequence_Encoded
	  --,Table_Encoded_Parcours.Cle_Parcours_Encoded
	  --,[DD_A]
      --,[UFX]
      --,[INX]
      --,[R_NGAP]
      --,[R_CCAM]
      --,[UFH]
      --,[Site]
  FROM [ICO_Activite].[dbo].[Tmp_A_Actes_Table_Analyse] as Table_Actes,
	   [ICO_Activite].[dbo].[Tmp_Acte_Encoded] as Table_Encoded_Actes,
	   [ICO_Activite].[dbo].[Tmp_Sejour_Encoded] as Table_Encoded_Sejours,
	   [ICO_Activite].[dbo].[Tmp_Sequence_Encoded] as Table_Encoded_Sequence,
	   [ICO_Activite].[dbo].[Tmp_Type_Sequence] as Table_Type_Seq,
	   [ICO_Activite].[dbo].[Tmp_Parcours_Encoded] as Table_Encoded_Parcours

  WHERE Table_Actes.ID_A=Table_Encoded_Actes.ID_A AND
		Table_Actes.N_S=Table_Encoded_Sejours.N_S AND
		Table_Actes.N_S=Table_Type_Seq.N_S AND
		Table_Type_Seq.id_Sequence=Table_Encoded_Sequence.id_Sequence AND
		Table_Actes.NIP=Table_Encoded_Parcours.NIP
		AND Table_Actes.NIP='A201906493'
		AND DD_A BETWEEN '2019-09-10' AND  '2019-09-10'
 GROUP BY Table_Actes.[NIP]