SELECT   [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour, sum(A_Listing_UF.[Poids de l'acte]) as Poids_Sejour_DSOS
INTO A_Temp2_Qualif_Sejour_Poids_SOS
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         A_Listing_UF ON [A_Actes-ICO-2018-2021_Global0].UFX_UFX_Code = A_Listing_UF.UFX_Code
WHERE A_Listing_UF.[Dimension Parcours]='Soins Support'
GROUP BY [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour
ORDER BY [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour