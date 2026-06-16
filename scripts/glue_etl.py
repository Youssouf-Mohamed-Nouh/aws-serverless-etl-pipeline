import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# Initialisation
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. DÉTECTION AUTOMATIQUE DE LA TABLE
# On demande à AWS Glue de nous donner le nom de la table dans 'housing_db'
glue_client = boto3.client('glue', region_name='us-east-1')
tables = glue_client.get_tables(DatabaseName='housing_db')
# On prend le nom de la table (la dernière créée)
table_name = tables['TableList'][0]['Name']

# Lecture automatique via le Data Catalog
datasource = glueContext.create_dynamic_frame.from_catalog(
    database='housing_db', 
    table_name=table_name
)
df = datasource.toDF()

# ============================================================================
# 2. Transformation (Nettoyage & Winsorization à l'échelle Spark)
# ============================================================================

# 2.1 Renommage des colonnes (Exactement comme ton dictionnaire local)
renamed_columns = {
    'price': 'prix', 'area': 'superficie', 'bedrooms': 'chambre', 'bathrooms': 'salle_de_bain',
    'stories': 'etage', 'mainroad': 'route_principale', 'guestroom': 'chambre_invite',
    'basement': 'sous_sol', 'hotwaterheating': 'chauffe_eau', 'airconditioning': 'climatisation',
    'parking': 'stationnement', 'prefarea': 'zone_privilegie', 'furnishingstatus': 'meuble'
}
for old_col, new_col in renamed_columns.items():
    df = df.withColumnRenamed(old_col, new_col)

# 2.2 Remplacement des chaînes vides ou invalides par des vrais Nulls
valeurs_vides = ['', ' ', '?', 'NA', 'na', 'N/A', 'n/a', '.', 'None']
for col in df.columns:
    df = df.withColumn(col, F.when(F.col(col).isin(valeurs_vides), F.lit(None)).otherwise(F.col(col)))

# Suppression des lignes sans variables critiques
df = df.dropna(subset=['prix', 'superficie'])

# 2.3 Traitement des Outliers (Winsorization Spark)
# On applique ton calcul IQR pour clipper la variable critique 'superficie'
quantiles = df.approxQuantile('superficie', [0.25, 0.75], 0.05)
Q1, Q3 = quantiles[0], quantiles[1]
IQR = Q3 - Q1
borne_inf = Q1 - 1.5 * IQR
borne_sup = Q3 + 1.5 * IQR

# Clip (Winsorize) de la superficie à l'aide des fonctions natives Spark
df = df.withColumn(
    'superficie',
    F.when(F.col('superficie') < borne_inf, borne_inf)
     .when(F.col('superficie') > borne_sup, borne_sup)
     .otherwise(F.col('superficie'))
)

# Réorganisation des colonnes : prix à la fin comme dans ton script local
cols = [c for c in df.columns if c != 'prix'] + ['prix']
df_final = df.select(cols)

# 3. CHARGEMENT
# On écrit le résultat final
df.write.mode('overwrite').parquet('s3://ysf-data-space-2026/processed/housing_parquet/')

job.commit()