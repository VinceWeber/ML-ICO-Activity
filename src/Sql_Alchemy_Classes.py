import sqlalchemy
import pandas as pd

def AlSQL_Requete(engine, requete_sql, output=None):
    import warnings
    warnings.filterwarnings('ignore')

    try:
        df = pd.read_sql_query(requete_sql, engine)

        # Vous n'avez plus besoin de commit() car SQLalchemy gère cela automatiquement.
    except sqlalchemy.exc.SQLAlchemyError as ex:
        print('AlSQL_Requete - ERROR')
        print(ex)
        raise Exception('ALSQL_Requete - ERROR')
    else:
        if output:
            print('AlSQL_Requete - OK')
        return df


def AlSQL_Execute(engine, requete_sql, output=None):
    import warnings
    warnings.filterwarnings('ignore')

    try:
        # Use the execute() method of the engine to execute the SQL command
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text(requete_sql))
            #conn.commit()

    except sqlalchemy.exc.SQLAlchemyError as ex:
        print('ALSQL_Execution - ERROR')
        print(ex)
        raise Exception('ALSQL_Execution - ERROR')
    else:
        if output:
            print('AlSQL_Execution - OK')



# Initialisation des paramètres d'accès à la BDD Activité
try:
    server = '172.27.209.171'
    database = 'ICO_Activite'
    username = 'sa'
    password = 'vyNM~pgDxO>0[5+ryM>F'
    connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'

    # Créez la connexion à la base de données avec SQLalchemy
    engine = sqlalchemy.create_engine(connection_string, echo=True)

    # Vous n'avez plus besoin de créer un curseur, SQLalchemy s'en occupe

    # Votre code d'accès à la base de données en utilisant SQLalchemy
    #with engine.connect() as connection:
        # Exécutez vos requêtes ici
    #    result = connection.execute('SELECT * FROM MaTable')
    #    for row in result:
    #        print(row)

    df = pd.read_sql_query("SELECT TOP (10) [idUFX_Ress_Equ] FROM [ICO_Activite].[dbo].[Listing_UF_V3]", engine)


except sqlalchemy.exc.SQLAlchemyError as ex:
    print(f'Erreur de connexion à la BDD : {ex}')
else:
    print('Connexion BDD - OK')