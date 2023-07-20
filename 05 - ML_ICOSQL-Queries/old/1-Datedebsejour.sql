/****** Script for SelectTopNRows command from SSMS  ******/
SELECT        Ho_Num_Num_sejour, MIN(Date_début_acte) AS Ddebsej, MAX([Date-Fin_Mvt]) AS Dfinsej
INTO A_Temp4_Ddetfinsejour
FROM            [A_Actes-ICO-2018-2021_Global0]
GROUP BY Ho_Num_Num_sejour