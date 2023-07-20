/****** Script for SelectTopNRows command from SSMS  ******/
SELECT        NIP, [Phase Parcours], Service, min(Ddebsej) as Date_Sejour
INTO B_Rdth
FROM            A_DataSet_Global
WHERE [Phase Parcours] LIKE 'Trait%' and Service LIKE 'Rad%' and DATEDIFF ( DAY , '2021-12-01' , Ddebsej )>0
GROUP BY NIP, [Phase Parcours], Service
ORDER By NIP