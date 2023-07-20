/****** Script for SelectTopNRows command from SSMS  ******/
	SELECT [NIP_original]
		  ,[Date_début_acte] as J0_V1
		  ,'31-12-2099' as J0_V2
    INTO A_Temp4_J0V1_2
	FROM [ICOActivite].[dbo].[A_Actes-ICO-2018-2021_Global0]
 
UNION

	SELECT        [A_Actes-ICO-2018-2021_Global0].NIP_original, '31-12-2099' as J0_V1, [A_Actes-ICO-2018-2021_Global0].Date_début_acte AS J0_V2
	FROM          [A_Actes-ICO-2018-2021_Global0] INNER JOIN
							 A_Listing_UF ON [A_Actes-ICO-2018-2021_Global0].UFX_UFX_Code = A_Listing_UF.UFX_Code
	WHERE A_Listing_UF.[Poids de l'acte]=100
 
 
