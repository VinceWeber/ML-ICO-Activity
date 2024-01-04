#python script to perform multiple clusterings and store informations in mlflow and database

from datetime import datetime
import time
#import pyodbc
import sqlalchemy
#import sqlalchemy as msql
#import Connexion_bdd as Cx_bdd
import numpy as np
import pandas as pd
import Requetes_SQL as Req_SQL
import matplotlib.pyplot as plt
import seaborn as sns
#import dtw as dtw
#import tsfresh #TimeSeries Transformation library
import my_custom_func_TS_Clust_1 as Mcftsc
import my_custom_func_Clustering as McfC
import my_custom_func_Carepath_plotting as Mcfcp
import my_custom_func_config as Mcfconf
import my_custom_func_batch_follow as Mcfbf
import mlflow

from sklearn import feature_selection
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.model_selection import cross_validate, cross_val_score, train_test_split, KFold, RepeatedKFold, RepeatedStratifiedKFold
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor, DecisionTreeClassifier, export_graphviz
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn import metrics, tree

#SET THE RANDOM SET FOR SKLEARN
#np.random.seed(0)

#Project Classes
#import FSQL_Classes as FSQLC
import Sql_Alchemy_Classes as AlSQL
#import Parcours_Classes as PC

import os
current_directory = os.getcwd()
print("Current directory:", current_directory)

#Open config.csv file as a dict
file_path = current_directory + "\\07-Batch_configuration\\export_config.csv"
print("file_path:", file_path)

file_path = file_path.replace('\\\\', '\\')
print("file_path:", file_path)

print("Run : pd.read_csv(file_path, encoding='ISO-8859-1')")
config = pd.read_csv(file_path, encoding='ISO-8859-1')
#Add a function to chekc csv file
#print(Mcfbf.myprint('Import csv batch file succeed', 1, 1))


#total_index = len(config)