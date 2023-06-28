/****** Script for SelectTopNRows command from SSMS  ******/
/*  REQUETE ORIGINALE
SELECT       Num_Sejour, Activite, Service, UFX_Code, UFX_Code_Lib, [Phase Parcours], [Dimension Parcours], max([Poids de l'acte]) as Max_Poids_acte
FROM            A_DataSet_Global
WHERE [Dimension Parcours] like 'Soins'
group by Num_Sejour,Activite, Service, UFX_Code, UFX_Code_Lib,[Phase Parcours],[Dimension Parcours],[Poids de l'acte]
Order by Num_Sejour
*/

SELECT       Num_Sejour,  max([Poids de l'acte]) as Max_Poids_acte
INTO A_Temp_A_caracteristiq_sejour_dim_soins_supp
FROM            A_DataSet_Global
WHERE [Dimension Parcours] like 'Soins Sup%'
group by Num_Sejour
Order by Num_Sejour

/* LANCE RCETTE REQUETE AUSSI POUR LES DIMENSIONS SOINS ET SOS" */
