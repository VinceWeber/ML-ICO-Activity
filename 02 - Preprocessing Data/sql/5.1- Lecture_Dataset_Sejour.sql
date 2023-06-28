SELECT        Site, NIP, Num_Sejour, MIN(Ddebsej) AS Ddebsej, MAX(Dfinsej) AS Dfinsej, MIN(J0_V1) AS J0_V1, MIN(J0_V2) AS J0_V2, MIN(J0_V3) AS J0_V3, MIN(J0_V4) AS J0_V4, MIN(J_Parcours_V1) AS J_Parcours_V1, 
                         MIN(J_Parcours_V2) AS J_Parcours_V2, MIN(J_Parcours_V3) AS J_Parcours_V3, MIN(J_Parcours_V4) AS J_Parcours_V4, MIN(Poids_Dim_Soins) AS asPoids_Dim_Soins, MIN(Poids_Dim_SOS) AS Poids_Dim_SOS, 
                         MIN(DISTINCT Séquence_Parcours) AS Séquence_Parcours
FROM            A_DataSet_Global
GROUP BY Site, NIP, Num_Sejour
ORDER BY Site, NIP, Num_Sejour