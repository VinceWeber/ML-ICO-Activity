

# This class module is dedicated for classes related to "parcours" 
# Caracteristiques_Dataset_Parcours
# Parcours
# Patient
# Sequence de soins
# Sejour
# Acte
# UF

from datetime import datetime
#import FSQL_Classes as FSQLC
import Sql_Alchemy_Classes as AlSQL

class Caracteristiques_Dataset_Parcours:
    

    output=False
    
    def __init__(self, name, date1, date2,site,date_etude1,date_etude2):
        self.set_attibutes(name,date1, date2,site,date_etude1,date_etude2)
    
    def get_x(self):
        Return_dict={'Name' : [self.name] ,'Date1_filtreNIP' : [self.date1] ,'Date2_filtreNIP' : [self.date2],'Date3_filtre_Data_set' : [self.DebutDataset] , \
                     'Date4_filtre_Data_set' : [self.FinDataset], 'Site' : [self.site], \
            'DS_Total actes' : [self.Actes_Total], 'DS_Encoded_Actes' : [self.Actes_Encoded], 'DS_pct Encoded_Actes' : [self.Actes_percent_encoded], \
            'DS_Total_sejours': [self.Sejours_Total], 'DS_Encoded_Sejours' : [self.Sejours_Encoded], 'DS_pct Encoded_Sejours' : [self.Sejours_percent_encoded], \
            'DS_Total_sequence': [self.Sequence_Total], 'DS_Encoded_Sequence' : [self.Sequence_Encoded], 'DS_pct Encoded_Sequence' : [self.Sequence_percent_encoded], \
            'DS_Total_NIP' : [self.NIP_Total]  }

        return Return_dict

    def __str__(self):
        Return_String=str(self.get_x())
        return Return_String

    def __repr__(self):
        return 'Dataset_Parcours( from ' + self.date3.strftime('%Y-%m-%d %H:%M:%S') + ', to ' + self.date4.strftime('%Y-%m-%d %H:%M:%S') + ',\
              on site ' + self.site + ' , NIP Filtered between '+ self.date1.strftime('%Y-%m-%d %H:%M:%S') + ' and ' + self.date2.strftime('%Y-%m-%d %H:%M:%S') + ' )'


    def set_attibutes(self, name,date1, date2,my_site,date3,date4):
        self.name=name
        self.date1=date1
        self.date2=date2
        self.DebutDataset=date3
        self.FinDataset=date4
        self.site=my_site
        
        Table_Liste_Actes = 'A_Actes_ICO_2018_2021_V2_TRIMED' #INCRIT EN DUR DANS LA PROC SQL !!
        Table_Actes_filtres ='Tmp_Py_A_Actes_Export'
        Site=my_site
        
        My_filter_1rst_date=date1
        My_filter_2nd_date=date2
        Date1_Etude=date3
        Date2_Etude=date4

        #Requete = ' EXECUTE Delete_Table_if_exists ' + Table_Actes_filtres
        Requete = 'EXECUTE dbo.Delete_Table_if_exists ' + Table_Actes_filtres
            
        if self.output:
            print(Requete)

        #print(Requete)
        
        print("STEP 1.0 : Delete old Tables")
        #FSQLC.F_SQL_Execute(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)
        AlSQL.AlSQL_Execute(AlSQL.engine,Requete,self.output)
        
        print("STEP 1.1 : Filter NIP ON /n Site = " + Site + "/n Date1  = " + str(My_filter_1rst_date)  + " - Date2  = " + str(My_filter_2nd_date) + " - launched at " + str(datetime.now()))
        Requete = ' EXECUTE Preproc_A0_Filter_NIP_BY_2_DATES_AND_SITE_AND_DATASET_ON_2_DATES ' \
                    + Table_Liste_Actes \
                    + ','+ Table_Actes_filtres \
                    + ',\'' + My_filter_1rst_date.strftime('%Y-%m-%d %H:%M:%S') \
                    + '\',\'' + My_filter_2nd_date.strftime('%Y-%m-%d %H:%M:%S') \
                    + '\',\'' + Date1_Etude.strftime('%Y-%m-%d %H:%M:%S') \
                    + '\',\'' + Date2_Etude.strftime('%Y-%m-%d %H:%M:%S') \
                    + '\','  + Site 
        AlSQL.AlSQL_Execute(AlSQL.engine,Requete,self.output)


        #Mise en forme du dataset (Création J0V1234, Date_sejour)
        print("STEP 1.2 : Prepare_Data_set - launched at " + str(datetime.now()))
        Requete = ' EXECUTE Preproc_B1_Prepare_Dataset ' + Table_Actes_filtres + ',' + 'Listing_UF_V3' + ',' + 'NO' #Table acte / Table_UF / Summary YES -> just first 2000 lines
        if self.output:
            print(Requete)
        #FSQLC.F_SQL_Execute(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)
        AlSQL.AlSQL_Execute(AlSQL.engine,Requete,self.output)


        #Label Encoding (UF,INX,NGAP,CCAM,UFH)
        print("STEP 1.3 : Prepare_Data_set - Label Encoding Categories - launched at " + str(datetime.now()))
        Requete = ' EXECUTE Preproc_B4_Prepare_Dataset_Encoding_V2 ' + Table_Actes_filtres + ',' + 'Listing_UF_V3' + ',' + 'NO' #Table acte / Table_UF / Summary YES -> just first 2000 lines
        if self.output:
            print(Requete)
        #FSQLC.F_SQL_Execute(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)
        AlSQL.AlSQL_Execute(AlSQL.engine,Requete,self.output)


        #Result_Dataset 
        print("STEP 1.4 : Export_Data_set - launched at " + str(datetime.now()))
        Requete = ' EXECUTE PREPROC_B5_EXPORT_RESULT_TABLE \'Tmp_Carac_Actes\',\''+ Date1_Etude.strftime('%Y-%m-%d %H:%M:%S') + '\'' 
        if self.output:
            print(Requete)
        #FSQLC.F_SQL_Execute(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)
        AlSQL.AlSQL_Execute(AlSQL.engine,Requete,self.output)


        #Grouped_Result_Dataset 
        print("STEP 1.5 : Export_Grouped_Data_set - launched at " + str(datetime.now()))
        Requete = ' EXECUTE PREPROC_B6_EXPORT_REGROUP_TABLES \'Tmp_Carac_Actes\',\'Tmp_Group_Carac_\',\''+ Date1_Etude.strftime('%Y-%m-%d %H:%M:%S') + '\''
        if self.output:
            print(Requete)
        #FSQLC.F_SQL_Execute(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)
        AlSQL.AlSQL_Execute(AlSQL.engine,Requete,self.output)

        Requete = 'SELECT COUNT([ID_A]) as Total FROM [ICO_Activite].[dbo].[Tmp_A_Actes_Table_Analyse]'
        Actes_Total=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,self.output) #FSQLC.F_SQL_Requete(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)
        
        Requete = 'SELECT COUNT([Cle_Encode_acte]) as Total FROM [ICO_Activite].[dbo].[Tmp_Acte_Label_Table]'
        Actes_Encoded=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,self.output) #FSQLC.F_SQL_Requete(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)

        Actes_percent_encoded=float(Actes_Encoded.loc[0,'Total'])/float(Actes_Total.loc[0,'Total'])
        if self.output:
            print(str(Actes_Encoded.loc[0,'Total']) + " Actes encoded on  " + str(Actes_Total.loc[0,'Total']) + ' in total ' + str(int(Actes_percent_encoded*100)) +'%')

        self.Actes_Total=Actes_Total.loc[0,'Total']
        self.Actes_Encoded=Actes_Encoded.loc[0,'Total']
        self.Actes_percent_encoded=Actes_percent_encoded

            #2 NB of encoded Sejours / Total of Sejours

        Requete = 'SELECT COUNT(DISTINCT [N_S]) as Total FROM [ICO_Activite].[dbo].[Tmp_Sejour_Encoded]'
        Sejours_Total=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,self.output)
        Requete = 'SELECT COUNT([Cle_Encode_Sejour]) as Total FROM [ICO_Activite].[dbo].[Tmp_Sejour_Label_Table]'
        Sejours_Encoded=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,self.output) #FSQLC.F_SQL_Requete(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)

        Sejours_percent_encoded=float(Sejours_Encoded.loc[0,'Total'])/float(Sejours_Total.loc[0,'Total'])
        if self.output:
            print(str(Sejours_Encoded.loc[0,'Total']) + " Sejours encoded on  " + str(Sejours_Total.loc[0,'Total']) + ' in total ' + str(int(Sejours_percent_encoded*100)) +'%')

        self.Sejours_Total=Sejours_Total.loc[0,'Total']
        self.Sejours_Encoded=Sejours_Encoded.loc[0,'Total']
        self.Sejours_percent_encoded=Sejours_percent_encoded

            #3 NB of encoded Sequences / Total of Sequences

        Requete = 'SELECT COUNT(DISTINCT [id_Sequence]) as Total FROM [ICO_Activite].[dbo].[Tmp_Sequence_Encoded]'
        Sequence_Total=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,self.output) #FSQLC.F_SQL_Requete(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)
        Requete = 'SELECT COUNT([Cle_Encode_Sequence]) as Total FROM [ICO_Activite].[dbo].[Tmp_Sequence_Label_Table]'
        Sequence_Encoded=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,self.output) #FSQLC.F_SQL_Requete(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)

        Sequence_percent_encoded=float(Sequence_Encoded.loc[0,'Total'])/float(Sequence_Total.loc[0,'Total'])
        if self.output:
            print(str(Sequence_Encoded.loc[0,'Total']) + " Sequences encoded on  " + str(Sequence_Total.loc[0,'Total']) + ' in total ' + str(int(Sequence_percent_encoded*100)) +'%')

        self.Sequence_Total=Sequence_Total.loc[0,'Total']
        self.Sequence_Encoded=Sequence_Encoded.loc[0,'Total']
        self.Sequence_percent_encoded=Sequence_percent_encoded
        
            #4 NB of Parcours / Total of NIP
        Requete = 'SELECT COUNT(DISTINCT [NIP]) as Total FROM [ICO_Activite].[dbo].[Tmp_A_Actes_Table_Analyse]'
        NIP_Total=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,self.output) #FSQLC.F_SQL_Requete(FSQLC.cnxn,Requete,FSQLC.pyodbc,self.output)
        self.NIP_Total=NIP_Total.loc[0,'Total']


# Deleting (Calling destructor)
    def restart_DB():
        Requete = 'EXECUTE Delete_TmpTables'
        #FSQLC.F_SQL_Execute(FSQLC.cnxn,Requete,FSQLC.pyodbc)
        AlSQL.AlSQL_Execute(AlSQL.engine,Requete,self.output)
        print('Tmp_tables_Deleted, BDD is empty.')