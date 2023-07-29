SELECT Table_acte_sejours.[NIP]
	  ,Encod_parcours.Cle_Encode_Parcours 
	  ,Table_Seq_NS.id_Sequence
	  ,Encod_Seq.Cle_Encode_Sequence
	  ,Table_acte_sejours.[N_S]
	  ,Encod_Sej.Cle_Encode_Sejour
	  ,Table_acte_sejours.[ID_A]
	  ,Encod_acte.Cle_Encode_acte
      ,Table_acte_sejours.[DD_A]
	  ,Table_acte_sejours.[DF_A]
	  ,Table_PS.J0_V1 as J0_V1
	  ,Table_Seq_NS.J0_V3 as J0_V3
	  ,DATEDIFF(D,Table_PS.J0_V1,Table_acte_sejours.[DD_A]) as J_Parcours_V1
	  ,DATEDIFF(D,Table_PS.J0_V2,Table_acte_sejours.[DD_A]) as J_Parcours_V2
	  ,DATEDIFF(D,Table_Seq_NS.J0_V3,Table_acte_sejours.[DD_A]) as J_Parcours_V3
	  ,DATEDIFF(D,'Jan  1 2018 12:00AM',Table_acte_sejours.[DD_A]) as J_DataSet
	  ,DATEDIFF(D,Table_acte_sejours.[DD_A],Table_acte_sejours.[DF_A])+1 as Duree
	  ,Table_UF.Service as Service
	  ,Table_UF.Activite as Activite
	  ,Table_UF.Phase_Parcours as Phase
	  ,Table_UF.Dimension_Parcours as Dimension
	  ,Table_Seq_NS.Type_Sequence as Type_seq
 
 FROM  [ICO_Activite].[dbo].[Tmp_A_Actes_Table_Analyse] as Table_acte_sejours,
	   [ICO_Activite].[dbo].[Tmp_Type_Sequence] as Table_Seq_NS,
	   [ICO_Activite].[dbo].[Listing_UF_V3] as Table_UF,
	   [ICO_Activite].[dbo].[Tmp_PS_] as Table_PS,
	   [ICO_Activite].[dbo].[Tmp_Sequence_Encoding] as Encod_Seq,
	   [ICO_Activite].[dbo].[Tmp_Sejour_Encoding] as Encod_Sej,
	   [ICO_Activite].[dbo].[Tmp_Acte_Encoding] as Encod_acte,
	   [ICO_Activite].[dbo].[Tmp_Parcours_Encoding] as Encod_parcours

 WHERE  Table_acte_sejours.N_S=Table_PS.N_S AND
		Table_acte_sejours.N_S=Table_Seq_NS.N_S AND
		Table_UF.UFX_Code=Table_acte_sejours.UFX AND
		Encod_Seq.id_Sequence=Table_Seq_NS.id_Sequence AND
		Encod_Sej.N_S=Table_acte_sejours.N_S AND
		Encod_acte.ID_A=Table_acte_sejours.ID_A AND
		Encod_parcours.NIP=Table_acte_sejours.NIP
