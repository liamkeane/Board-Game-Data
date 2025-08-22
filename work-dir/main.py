from pyspark.sql import SparkSession
from etl import ETLProcessor

#paths to csv files
bgg_csv_path = "/opt/spark/input/bgg_ranks.csv"
melissa_csv_path = "/opt/spark/input/melissa-monfared-board-games.csv"

def main():
    spark = SparkSession.builder.appName("BoardGameETL").enableHiveSupport().getOrCreate()

    db_properties = {
        "url": spark.conf.get("spark.mysql.board_games_db.url"),
        "user": spark.conf.get("spark.mysql.board_games_db.user"),
        "password": spark.conf.get("spark.mysql.board_games_db.password"),
        "driver": "com.mysql.cj.jdbc.Driver",
    }
    
    processor = ETLProcessor(spark, db_properties, bgg_csv_path, melissa_csv_path)
    processor.run()

    spark.stop()

# if __name__ == "__main__":
main()
