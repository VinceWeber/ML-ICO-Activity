# -*- coding: utf-8 -*-
"""
Created on Tue Aug 30 16:29:57 2022

@author: v-weber
"""

# Modules de fonctions spéciales

def MDFS_Import_Type_encodage_Fichier(Nom_fichier) #Retourne le type d'encodage d'un fichier , csv, pdf, etc..
    import chardet
    with open(Nom_fichier, 'rb') as rawdata:
        result = chardet.detect(rawdata.read(100000))
    return result