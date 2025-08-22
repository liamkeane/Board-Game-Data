from pyspark.sql import SparkSession
import re 
import logging

log = logging.getLogger(__name__)

class ETLProcessor:
    def __init__(self, spark,  db_properties, bgg_path, melissa_path):
        self.spark = spark
        self.db_properties = db_properties
        self.bgg_path = bgg_path
        self.melissa_path = melissa_path

    def extract(self):
        bgg_df = self.spark.read.csv(self.bgg_path, header=True, inferSchema=True)
        melissa_df = self.spark.read(self.melissa_path, header=True, inferSchema=True)
        return bgg_df, melissa_df
    
    def transform(self, bgg_df, melissa_df):
        for column in bgg_df:
            bgg_df = bgg_df.withColumnRenamed(column, self.clean_col_name(column))
        for column in melissa_df:
            melissa_df = melissa_df.withColumnRenamed(column, self.clean_col_name(column))

        # keep only bgg's "avg" and "rank"
        
        # drop "BGG Rank" and "Rating Average" from melissa
        # join dataframes to create master
        # create dfs for each tables
        
        # helper function? for the junction tables? (explode, then trim distinct)
        pass

    def load(self, game_df, rating_df, mechanic_df, game_mechanic_df, domain_df, game_domain_df):
        """
        i found this at https://medium.com/@suffyan.asad1/writing-to-databases-using-jdbc-in-apache-spark-9a0eb2ce1a
        .jdbc() function takes the following parameters:
        url: Connection URL
        table: Target table name
        properties: Optional dictionary
        mode: specify action if the target table already exists
        """
        

        
        # will just write final df to respective table
        pass

    def run(self):
        bgg_df, melissa_df = self.extract()
        bgg_df.show(25)
        melissa_df.show(25)
        # game, rating, mechanic, game_mechanic, domain, game_domain = self.transform(bgg_df, melissa_df)
        # self.load(game, rating, mechanic, game_mechanic, domain, game_domain)
    
    def clean_col_name(self, name):
        if name is None: return ""
        name = re.sub(r'[\s.]+', '_', name) # (m) replace spaces w underscore 

        # (both) insert underscore before any uppercase letter that follows a lowercase letter
        name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)

        # (m) insert underscore before any uppercase letter that is followed by a lowercase letter 
        name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)

        # strips any leading or trailing underscores
        name = name.strip('_')
        return name.lower() # (m) convert the entire string to lowercase


# #def transform(self, df):
#         #clean column names
#         df = df.select([self.clean_col_name(c) for c in df.columns])