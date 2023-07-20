/****** Script for SelectTopNRows command from SSMS  ******/
SELECT        [A_Actes-ICO-2018-2021_Global0].NIP_original, [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour, A_Qualif_Sejour.Ddebsej, MIN(A_Date_J0.J0_V1) AS J0_V1, MIN(A_Date_J0.J0_V2) AS J0_V2, right(Left([A_Actes-ICO-2018-2021_Global0].NIP_original,5),4) as Annee_NIP,
                         MAX(A_Qualif_Sejour.Poids_Dim_Soins) AS poids_Séjour_soins, [A_Actes-ICO-2018-2021_Global0].Site_idSite
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         A_Date_J0 ON [A_Actes-ICO-2018-2021_Global0].NIP_original = A_Date_J0.NIP_original INNER JOIN
                         A_Qualif_Sejour ON [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour = A_Qualif_Sejour.Ho_Num_Num_sejour
GROUP BY [A_Actes-ICO-2018-2021_Global0].NIP_original, [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour, A_Qualif_Sejour.Ddebsej, [A_Actes-ICO-2018-2021_Global0].Site_idSite
ORDER BY [A_Actes-ICO-2018-2021_Global0].NIP_original, A_Qualif_Sejour.Ddebsej