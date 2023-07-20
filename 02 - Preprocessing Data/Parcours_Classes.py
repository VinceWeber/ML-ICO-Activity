

# This class module is dedicated for classes related to "parcours" 
# Caracteristiques_Dataset_Parcours
# Parcours
# Patient
# Sequence de soins
# Sejour
# Acte
# UF

from datetime import datetime
import FSQL_Classes as FSQLC

class Caracteristiques_Dataset_Parcours:
    
    

    output=False
    
    def __init__(self, name, date1, date2,site):
        self.set_attibutes(name,date1, date2,site)
    
    def get_x(self):
        Return_dict={'Name' : [self.name] ,'Date1' : [self.date1] ,'Date2' : [self.date2], 'Site' : [self.site], \
            'Total actes' : [self.Actes_Total], 'Encoded_Actes' : [self.Actes_Encoded], '% Encoded_Actes' : [self.Actes_percent_encoded], \
            'Total_sejours': [self.Sejours_Total], 'Encoded_Sejours' : [self.Sejours_Encoded], '% Encoded_Sejours' : [self.Sejours_percent_encoded], \
            'Total_sequence': [self.Sequence_Total], 'Encoded_Sequence' : [self.Sequence_Encoded], '% Encoded_Sequence' : [self.Sequence_percent_encoded], \
            'Total_NIP' : [self.NIP_Total]  }

        return Return_dict
    
    def __str__(self):
        Return_String=str(self.get_x())
        return Return_String
    def __repr__(self):
        return 'Dataset_Parcours( from ' + self.date1.strftime('%Y-%m-%d %H:%M:%S') + ', to ' + self.date2.strftime('%Y-%m-%d %H:%M:%S') + ', on site ' + self.site + ' )'


    def set_attibutes(self, name,date1, date2,my_site):
        self.name=name
        self.date1=date1
        self.date2=date2
        self.site=my_site
        
        Table_Liste_Actes = 'A_Actes_ICO_2018_2021_TRIMED' #INCRIT EN DUR DANS LA PROC SQL !!
        Table_Actes_filtres ='Tmp_Py_A_Actes_Export'
        Site=my_site
        
        My_filter_1rst_date=date1
        My_filter_2nd_date=date2

        Requete = ' EXECUTE Delete_Table_if_exists ' + Table_Actes_filtres
        Requete += ' EXECUTE Preproc_A0_Filter_NIP_BY_2_DATES_AND_SITE ' + Table_Liste_Actes + ','+ Table_Actes_filtres + ',\'' + My_filter_1rst_date.strftime('%Y-%m-%d %H:%M:%S') + '\',\'' + My_filter_2nd_date.strftime('%Y-%m-%d %H:%M:%S') + '\','  + Site
        if self.output:
            print(Requete)

        #print(Requete)
        
        print("STEP 1.1 : Filter NIP ON /n Site = " + Site + "/n Date1  = " + str(My_filter_1rst_date)  + " - Date2  = " + str(My_filter_2nd_date) + " - launched at " + str(datetime.now()))
        FSQLC.F_SQL_Execute(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)

        #Mise en forme du dataset (Création J0V1234, Date_sejour)
        print("STEP 1.2 : Prepare_Data_set - launched at " + str(datetime.now()))
        Requete = ' EXECUTE Preproc_B1_Prepare_Dataset ' + Table_Actes_filtres + ',' + 'Listing_UF_V3' + ',' + 'NO' #Table acte / Table_UF / Summary YES -> just first 2000 lines
        FSQLC.F_SQL_Execute(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)

        #Binary Encoding (UF,INX,NGAP,CCAM,UFH)
        print("STEP 1.3 : Prepare_Data_set - One Hot Encoding Categories - launched at " + str(datetime.now()))
        Requete = ' EXECUTE Preproc_B4_Prepare_Dataset_Encoding ' + Table_Actes_filtres + ',' + 'Listing_UF_V3' + ',' + 'NO' #Table acte / Table_UF / Summary YES -> just first 2000 lines
        FSQLC.F_SQL_Execute(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)


        Requete = 'SELECT COUNT([ID_A]) as Total FROM [ICO_Activite].[dbo].[Tmp_A_Actes_Table_Analyse]'
        Actes_Total=FSQLC.F_SQL_Requete(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)
        Requete = 'SELECT COUNT([Cle_Encode_acte]) as Total FROM [ICO_Activite].[dbo].[Tmp_Acte_Binary_Table]'
        Actes_Encoded=FSQLC.F_SQL_Requete(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)

        Actes_percent_encoded=float(Actes_Encoded.loc[0,'Total'])/float(Actes_Total.loc[0,'Total'])
        if self.output:
            print(str(Actes_Encoded.loc[0,'Total']) + " Actes encoded on  " + str(Actes_Total.loc[0,'Total']) + ' in total ' + str(int(Actes_percent_encoded*100)) +'%')

        self.Actes_Total=Actes_Total.loc[0,'Total']
        self.Actes_Encoded=Actes_Encoded.loc[0,'Total']
        self.Actes_percent_encoded=Actes_percent_encoded

            #2 NB of encoded Sejours / Total of Sejours

        Requete = 'SELECT COUNT(DISTINCT [N_S]) as Total FROM [ICO_Activite].[dbo].[Tmp_Sejour_Encoded]'
        Sejours_Total=FSQLC.F_SQL_Requete(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)
        Requete = 'SELECT COUNT([Cle_Encode_Sejour]) as Total FROM [ICO_Activite].[dbo].[Tmp_Sejour_Binary_Table]'
        Sejours_Encoded=FSQLC.F_SQL_Requete(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)

        Sejours_percent_encoded=float(Sejours_Encoded.loc[0,'Total'])/float(Sejours_Total.loc[0,'Total'])
        if self.output:
            print(str(Sejours_Encoded.loc[0,'Total']) + " Sejours encoded on  " + str(Sejours_Total.loc[0,'Total']) + ' in total ' + str(int(Sejours_percent_encoded*100)) +'%')

        self.Sejours_Total=Sejours_Total.loc[0,'Total']
        self.Sejours_Encoded=Sejours_Encoded.loc[0,'Total']
        self.Sejours_percent_encoded=Sejours_percent_encoded

            #3 NB of encoded Sequences / Total of Sequences

        Requete = 'SELECT COUNT(DISTINCT [id_Sequence]) as Total FROM [ICO_Activite].[dbo].[Tmp_Sequence_Encoded]'
        Sequence_Total=FSQLC.F_SQL_Requete(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)
        Requete = 'SELECT COUNT([Cle_Encode_Sequence]) as Total FROM [ICO_Activite].[dbo].[Tmp_Sequence_Binary_Table]'
        Sequence_Encoded=FSQLC.F_SQL_Requete(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)

        Sequence_percent_encoded=float(Sequence_Encoded.loc[0,'Total'])/float(Sequence_Total.loc[0,'Total'])
        if self.output:
            print(str(Sequence_Encoded.loc[0,'Total']) + " Sequences encoded on  " + str(Sequence_Total.loc[0,'Total']) + ' in total ' + str(int(Sequence_percent_encoded*100)) +'%')

        self.Sequence_Total=Sequence_Total.loc[0,'Total']
        self.Sequence_Encoded=Sequence_Encoded.loc[0,'Total']
        self.Sequence_percent_encoded=Sequence_percent_encoded
        
            #4 NB of Parcours / Total of NIP
        Requete = 'SELECT COUNT(DISTINCT [NIP]) as Total FROM [ICO_Activite].[dbo].[Tmp_A_Actes_Table_Analyse]'
        NIP_Total=FSQLC.F_SQL_Requete(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)
        self.NIP_Total=NIP_Total.loc[0,'Total']