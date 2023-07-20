/****** Script for SelectTopNRows command from SSMS  ******/

/*
	SIMPLE REQUETE D'EXPORT DE LA TABLE 
*/




SELECT [NIP_original]
      ,[Ho_Num_Num_sejour]
      ,[Ddebsej]
      ,[J0_V1]
      ,[J0_V2]
      ,[Annee_NIP]
      ,[Poids_Sejour_DS]
      ,[Poids_Sejour_DSOS]
  FROM [ICOActivite].[dbo].[AAA_Tmp5_Export_Pour_Trait_Donnée]
  ORDER BY NIP_original, Ddebsej