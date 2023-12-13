Req_Export_Acte="""SELECT Table_Acte.[NIP]
	  ,Table_Acte.[ID_A]
      ,Table_Acte.[N_S]
	  ,Table_Seq.[id_Sequence]

	  ,Table_PS.[J0_V1]
	  ,Table_PS.[J0_V2]
	  ,Table_Seq.[J0_V3]
	  ,Table_Seq.[J0_V4]

	  ,DATEDIFF(DAY,Table_PS.[J0_V1],Table_Acte.[DD_A]) As J_V1
	  ,DATEDIFF(DAY,Table_PS.[J0_V2],Table_Acte.[DD_A]) As J_V2
	  ,DATEDIFF(DAY,Table_Seq.[J0_V3],Table_Acte.[DD_A]) As J_V3
	  ,DATEDIFF(DAY,Table_Seq.[J0_V4],Table_Acte.[DD_A]) As J_V4

      ,Table_Acte.[DD_A]
      ,Table_Acte.[DF_A]
	  ,DATEDIFF(day,Table_Acte.[DD_A],Table_Acte.[DF_A])+1  as Duree_Acte_j

      ,Table_Acte.[DD_M]
	  ,Table_Acte.[HD_M]
      ,Table_Acte.[DF_M]
	  ,Table_Acte.[HF_M]

	  ,DATEDIFF(MINUTE,Table_Acte.[HD_M],Table_Acte.[HF_M]) as Duree_MVT_min

      ,Table_Acte.[UFX]
      ,Table_Acte.[INX]
      ,CONCAT(Table_Acte.[R_NGAP],Table_Acte.[R_CCAM],Table_Acte.[UFH]) as R_NGAP_CCAM_UFH
      ,Table_Acte.[Statut]
      ,Table_Acte.[Code_Equip]
	  ,Table_UF.Service
	  ,Table_UF.Phase_Parcours
	  ,Table_UF.Activite
      ,Table_Acte.[Site]
      ,Table_Acte.[Source]
	  ,Table_Seq.[Type_Sequence]

  FROM [ICO_Activite].[dbo].[Tmp_A_Actes_Table_Analyse] as Table_Acte,
	   [ICO_Activite].[dbo].[Tmp_PS_] as Table_PS,
	   [ICO_Activite].[dbo].[Listing_UF_V3] as Table_UF,
	   [ICO_Activite].[dbo].[Tmp_Type_Sequence] as Table_Seq

  WHERE Table_PS.N_S = Table_Acte.N_S 
		AND Table_UF.UFX_Code=Table_Acte.UFX
		AND Table_Seq.N_S=Table_Acte.N_S
		--AND Table_Acte.NIP='N201706912'
  ORDER BY Table_Acte.[DD_A]  """

Req_Export_Sejours=""" SELECT Table_Acte.[NIP]
	  --,Table_Acte.[ID_A]
      ,Table_Acte.[N_S]
	  ,Table_Seq.[id_Sequence]
	  ,Table_Seq.[Type_Sequence]

	  ,MIN(Table_PS.[J0_V1]) as J0_V1
	  ,MIN(Table_PS.[J0_V2]) as J0_V2
	  ,MIN(Table_Seq.[J0_V3]) as J0_V3
	  ,MIN(Table_Seq.[J0_V4]) as J0_V4

	  ,MIN(DATEDIFF(DAY,Table_PS.[J0_V1],Table_Acte.[DD_A])) As J_V1
	  ,MIN(DATEDIFF(DAY,Table_PS.[J0_V2],Table_Acte.[DD_A])) As J_V2
	  ,MIN(DATEDIFF(DAY,Table_Seq.[J0_V3],Table_Acte.[DD_A])) As J_V3
	  ,MIN(DATEDIFF(DAY,Table_Seq.[J0_V4],Table_Acte.[DD_A])) As J_V4

      ,MIN(Table_Acte.[DD_A]) as DD_S
      ,MIN(Table_Acte.[DF_A]) as DF_S
	  ,DATEDIFF(day,MIN(Table_Acte.[DD_A]),MAX(Table_Acte.[DF_A]))+1  as Duree_Sejour_j


	  ,Table_acte_C_soins.Id_A_Caracteristique
	  --,Table_Acte_caract.ID_A

	  ,Table_Acte_caract.[UFX] as C_UFX
      ,Table_Acte_caract.[INX] as C_INX
      ,CONCAT(Table_Acte_caract.[R_NGAP],Table_Acte_caract.[R_CCAM],Table_Acte_caract.[UFH]) as C_R_NGAP_CCAM_UFH
      ,Table_Acte_caract.[Statut] as C_Statut
      ,Table_Acte_caract.[Code_Equip] as C_Code_Equip
	  ,Table_UF_caract.Service as C_Service
	  ,Table_UF_caract.Phase_Parcours as C_Phase_Parcours
	  ,Table_UF_caract.Activite as C_Activite
      ,Table_Acte_caract.[Site] as C_Site
      ,Table_Acte_caract.[Source] as C_Source

      --,Table_Acte.[DD_M]
	  --,Table_Acte.[HD_M]
      --,Table_Acte.[DF_M]
	  --,Table_Acte.[HF_M]

	  --,DATEDIFF(MINUTE,Table_Acte.[HD_M],Table_Acte.[HF_M]) as Duree_MVT_min

  FROM [ICO_Activite].[dbo].[Tmp_A_Actes_Table_Analyse] as Table_Acte,
	   [ICO_Activite].[dbo].[Tmp_PS_] as Table_PS,
	   [ICO_Activite].[dbo].[Listing_UF_V3] as Table_UF,
	   [ICO_Activite].[dbo].[Tmp_Type_Sequence] as Table_Seq,
	   [ICO_Activite].[dbo].[Tmp_A3_Soins] as Table_acte_C_soins,
	   [ICO_Activite].[dbo].[Tmp_A_Actes_Table_Analyse] as Table_Acte_caract,
	   [ICO_Activite].[dbo].[Listing_UF_V3] as Table_UF_caract

  WHERE Table_PS.N_S = Table_Acte.N_S 
		AND Table_UF.UFX_Code=Table_Acte.UFX
		AND Table_Seq.N_S=Table_Acte.N_S
		AND Table_acte_C_soins.N_S = Table_Acte.N_S
		AND Table_acte_C_soins.Id_A_Caracteristique = Table_Acte_caract.ID_A
		AND Table_Acte_caract.UFX=Table_UF_caract.UFX_Code
		--AND Table_Acte.NIP='N201706912'
  GROUP BY Table_Acte.[NIP],Table_Seq.[id_Sequence],Table_Acte.[N_S],Table_acte_C_soins.Id_A_Caracteristique,Table_Seq.[Type_Sequence],
		   Table_Acte_caract.ID_A,Table_Acte_caract.[UFX],Table_Acte_caract.[INX],CONCAT(Table_Acte_caract.[R_NGAP],Table_Acte_caract.[R_CCAM],Table_Acte_caract.[UFH]),Table_Acte_caract.[Statut],Table_Acte_caract.[Code_Equip],
		   Table_UF_caract.Service,Table_UF_caract.Phase_Parcours,Table_UF_caract.Activite,Table_Acte_caract.[Site],Table_Acte_caract.[Source]
  ORDER BY DD_S
"""