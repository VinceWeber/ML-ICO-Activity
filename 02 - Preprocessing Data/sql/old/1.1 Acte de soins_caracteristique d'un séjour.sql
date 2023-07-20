/****** Script for SelectTopNRows command from SSMS  ******/

/*
SELECT        A_DataSet_Global.id_acte, A_DataSet_Global.Num_Sejour, A_DataSet_Global.[Poids de l'acte], A_DataSet_Global.[Dimension Parcours], A_Temp_A_caracteristiq_sejour_dim_soins.Max_Poids_acte, 
                         A_DataSet_Global.UFX_Code, A_DataSet_Global.UFX_Code_Lib, A_DataSet_Global.Service, A_DataSet_Global.Activite
FROM            A_DataSet_Global INNER JOIN
                         A_Temp_A_caracteristiq_sejour_dim_soins ON A_DataSet_Global.Num_Sejour = A_Temp_A_caracteristiq_sejour_dim_soins.Num_Sejour
WHERE        (A_DataSet_Global.[Dimension Parcours] LIKE 'Soins' and [Poids de l'acte] = A_Temp_A_caracteristiq_sejour_dim_soins.Max_Poids_acte)
ORDER BY A_DataSet_Global.Num_Sejour, CAST(A_DataSet_Global.id_acte AS int)
*/
SELECT        min(A_DataSet_Global.id_acte) as Acte_Caracteristique_Soins, A_DataSet_Global.Num_Sejour                      
INTO A_Acte_caracteristique_soins_sejour
FROM            A_DataSet_Global INNER JOIN
                         A_Temp_A_caracteristiq_sejour_dim_soins ON A_DataSet_Global.Num_Sejour = A_Temp_A_caracteristiq_sejour_dim_soins.Num_Sejour
WHERE        (A_DataSet_Global.[Dimension Parcours] LIKE 'Soins' and [Poids de l'acte] = A_Temp_A_caracteristiq_sejour_dim_soins.Max_Poids_acte)
GROUP BY A_DataSet_Global.Num_Sejour
ORDER BY A_DataSet_Global.Num_Sejour

