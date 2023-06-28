
SELECT   [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour, sum(A_Listing_UF.[Poids de l'acte]) as Poids_Sejour_DS, sum(0) as Poids_Sejour_DSOS, A_Listing_UF.[Dimension Parcours] 

FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         A_Listing_UF ON [A_Actes-ICO-2018-2021_Global0].UFX_UFX_Code = A_Listing_UF.UFX_Code
WHERE A_Listing_UF.[Dimension Parcours]='Soins'
GROUP BY Ho_Num_Num_sejour,A_Listing_UF.[Dimension Parcours]

UNION

SELECT   [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour, sum(0)  as Poids_Sejour_DS ,sum(A_Listing_UF.[Poids de l'acte]) as Poids_Sejour_DSOS, A_Listing_UF.[Dimension Parcours]
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         A_Listing_UF ON [A_Actes-ICO-2018-2021_Global0].UFX_UFX_Code = A_Listing_UF.UFX_Code
WHERE A_Listing_UF.[Dimension Parcours]='Soins Support'
GROUP BY Ho_Num_Num_sejour,A_Listing_UF.[Dimension Parcours]

/* GROUP BY [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour, [A_Actes-ICO-2018-2021_Global0].NIP_original, A_Listing_UF.[Dimension Parcours]*/
ORDER BY [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour