from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import re 
import logging

log = logging.getLogger(__name__)

class ETLProcessor:
    def __init__(self, spark,  db_properties, bgg_path, melissa_path):
        self.spark = spark
        self.db_properties = db_properties
        self.bgg_path = bgg_path
        self.melissa_path = melissa_path

    def clean_col_name(self, name):
        if name is None: return ""
        name = re.sub(r'[\s.]+', '_', name) # (m) replace spaces w underscore 

        # (both) insert underscore before any uppercase letter that follows a lowercase letter
        name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)

        # (m) insert underscore before any uppercase letter that is followed by a lowercase letter 
        name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)

        # strips any leading or trailing underscores, just in case
        name = name.strip('_')
        return name.lower() # (m) convert the entire string to lowercase
    
    def extract(self):
        bgg_df = self.spark.read.csv(self.bgg_path, header=True, inferSchema=True)
        melissa_df = self.spark.read.csv(self.melissa_path, header=True, inferSchema=True)
        return bgg_df, melissa_df
    
    def transform(self, bgg_df, melissa_df):

        # 1. keep only bgg's "avg" and "rank"
        bgg_df = bgg_df.drop("abstracts_rank", "cgs_rank", "childrensgames_rank", "familygames_rank", "partygames_rank", "strategygames_rank", "thematic_rank", "wargames_rank")

        # 2. drop "BGG Rank" and "Rating Average" from melissa
        melissa_df = melissa_df.select("ID", "Mechanics", "Domains", "Complexity Average", "Owned Users", "Min Players", "Max Players", "Play Time")
        
        # clean column names
        for column in bgg_df.columns:
            bgg_df = bgg_df.withColumnRenamed(column, self.clean_col_name(column))
        for column in melissa_df.columns:
            melissa_df = melissa_df.withColumnRenamed(column, self.clean_col_name(column))
        
        # 3. join dataframes to create master
        master_df = bgg_df.join(melissa_df, bgg_df.id == melissa_df.id, "inner").drop(melissa_df.id)

        # 4. create dfs for each tables
        game_df = master_df.select(
            F.col("id").alias("game_id"),
            F.col("name"),
            F.col("year_published"),
            F.col("min_players"),
            F.col("max_players"),
            F.col("play_time".alias("avg_playtime")),
            F.col("min_age"),
            F.col("is_expansion").cast("boolean")
        )

        rating_df = master_df.select(
            F.col("id").alias("game_id"),
            F.col("users_rated"),
            F.col("average_rating"),
            F.col("rating_avearge"),
            F.col("bayes_average"),
            F.col("complexity_average"),
            F.col("owned_users")
        )
        # --- Logic for Mechanics ---
        game_mechanic_exploded = master_df.select(
            F.col("id").alias("game_id"),
            F.explode(F.split(F.col("mechanics"), ",")).alias("mechanic_name")
        ).withColumn("mechanic_name", F.trim(F.col("mechanic_name"))).filter(F.col("mechanic_name") != "").distinct()

        mechanic_df = game_mechanic_exploded.select("mechanic_name").distinct().withColumnRenamed("mechanic_name", "name").withColumn("mechanic_id", F.monotonically_increasing_id())
        game_mechanic_df = game_mechanic_exploded.join(mechanic_df, game_mechanic_exploded.mechanic_name == mechanic_df.name, "inner").select("game_id", "mechanic_id")

        # --- Logic for Domains ---
        game_domain_exploded = master_df.select(
            F.col("id").alias("game_id"),
            F.explode(F.split(F.col("domains"), ",")).alias("domain_name")
        ).withColumn("domain_name", F.trim(F.col("domain_name"))).filter(F.col("domain_name") != "").distinct()
        
        domain_df = game_domain_exploded.select("domain_name").distinct().withColumnRenamed("domain_name", "name").withColumn("domain_id", F.monotonically_increasing_id())
        game_domain_df = game_domain_exploded.join(domain_df, game_domain_exploded.domain_name == domain_df.name, "inner").select("game_id", "domain_id")

        return game_df, rating_df, mechanic_df, game_mechanic_df, domain_df, game_domain_df

    def load(self, game_df, rating_df, mechanic_df, game_mechanic_df, domain_df, game_domain_df):
        """
        i found this at https://medium.com/@suffyan.asad1/writing-to-databases-using-jdbc-in-apache-spark-9a0eb2ce1a
        .jdbc() function takes the following parameters:
        url: Connection URL
        table: Target table name
        properties: Optional dictionary
        mode: specify action if the target table already exists        
        """

        # 1. Load game table
        game_df.write.jdbc(url=self.db_url, table="game", properties=self.db_properties, mode="overwrite")

        # 2. Load mechanic table
        mechanic_df.write.jdbc(url=self.db_url, table='mechanic', mode='overwrite', properties=self.db_properties)

        # 3. Load domain table
        domain_df.write.jdbc(url=self.db_url, table='domain', mode='overwrite', properties=self.db_properties)

        # 4. Load rating table
        rating_df.write.jdbc(url=self.db_url, table='rating', mode='overwrite', properties=self.db_properties)

        # 5. Load game_mechanic junction table
        game_mechanic_df.write.jdbc(url=self.db_url, table='game_mechanic', mode='overwrite', properties=self.db_properties)

        # 6. Load game_domain junction table
        game_domain_df.write.jdbc(url=self.db_url, table='game_domain', mode='overwrite', properties=self.db_properties)
        
        log.info("Data successfully loaded into MySQL database.")

    def run(self):
        bgg_df, melissa_df = self.extract()
        # print(bgg_df.show(25))
        # print(melissa_df.show(25))
        game, rating, mechanic, game_mechanic, domain, game_domain = self.transform(bgg_df, melissa_df)
        self.load(game, rating, mechanic, game_mechanic, domain, game_domain) 
    


