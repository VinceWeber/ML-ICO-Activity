/****** Script for SelectTopNRows command from SSMS  ******/
SELECT [id_acte]
      ,[Site]
      ,[NIP]
      ,[Num_Sejour]
      ,[Ddebsej]
      ,[Dfinsej]
      ,[J0_V1]
      ,[J0_V2]
	  ,[J0_V3]
      ,[J0_V4]
      ,[J_Parcours_V1]
      ,[J_Parcours_V2]
      ,[J_Parcours_V3]
      ,[J_Parcours_V4]
      ,[Poids_Dim_Soins]
      ,[Poids_Dim_SOS]
      ,[Séquence_Parcours]
      ,[RessourceMedcode_refx]
      ,[INX_Code_Lib_refx]
      ,[INX_Code_Spe_refx]
      ,[UFX_Code]
      ,[UFX_Code_Lib]
      ,[Service]
      ,[Activite]
      ,[Info complémentaire]
      ,[Phase Parcours]
      ,[Dimension Parcours]
      ,[Poids de l'acte]
  FROM [ICOActivite].[dbo].[A_DataSet_Global]