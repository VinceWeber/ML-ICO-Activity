/****** Script for SelectTopNRows command from SSMS  ******/
SELECT [NIP_original]
      ,min([J0_V1]) as J0_V1
      ,min([J0_V2]) as J0_V2

  INTO A_Date_J0 
  FROM [ICOActivite].[dbo].[A_Temp4_J0V1_2]
  GROUP BY  [NIP_original]