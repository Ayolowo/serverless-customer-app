from decimal import Decimal
import requests
from io import BytesIO
import simplejson as json
import logging
from question import Question
import os
from zipfile import ZipFile
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException
from typing import List

app = FastAPI()
logger = logging.getLogger(__name__)

class Movies:
    def __init__(self, dyn_resource, movies_table: str):
        self.dyn_resource = dyn_resource
        self.table = self.dyn_resource.Table(movies_table)
        
    def exists(self, movies_table):
        """
        Determines whether a table exists. As a side effect, stores the table in
        a member variable.

        :param movies_table: The name of the table to check.
        :return: True when the table exists; otherwise, False.
        """
        try:
            table = self.dyn_resource.Table(movies_table)
            table.load()
            exists = True
        except ClientError as err:
            if err.response["Error"]["Code"] == "ResourceNotFoundException":
                exists = False
            else:
                logger.error(
                    "Couldn't check for existence of %s. Here's why: %s: %s",
                    movies_table,
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
                raise
        else:
            self.table = table
        return exists


    def create_table(self, movies_table: str):
        """
        Creates an Amazon DynamoDB table that can be used to store movie data.
        The table uses the title of the movie as the partition key and the
        release year as the sort key.

        :param TableName: The name of the table to create.
        :return(self.table): The newly created table.
        """
        try:
            self.table = self.dyn_resource.create_table(
                TableName=movies_table,
                KeySchema=[
                    {"AttributeName": "title", "KeyType": "HASH"},  # Partition key
                    {"AttributeName": "year", "KeyType": "RANGE"},  # Sort key
                ],
                AttributeDefinitions=[
                    {"AttributeName": "title", "AttributeType": "S"},
                    {"AttributeName": "year", "AttributeType": "N"},
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 10,
                    "WriteCapacityUnits": 10,
                },
            )
            self.table.wait_until_exists() #This line pauses code execution until DynamoDB has finished creating the table.
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s",
                movies_table,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return self.table

    def write_batch(self, movies):
        """
        Fills an Amazon DynamoDB table with the specified data, using the Boto3
        Table.batch_writer() function to put the items in the table.
        Inside the context manager, Table.batch_writer builds a list of
        requests. On exiting the context manager, Table.batch_writer starts sending
        batches of write requests to Amazon DynamoDB and automatically
        handles chunking, buffering, and retrying.

        :param movies: The data to put in the table. Each item must contain at least
                       the keys required by the schema that was specified when the
                       table was created i.e. title and year in this case.
        """
        try:
            with self.table.batch_writer() as writer:
                for movie in movies:
                    writer.put_item(Item=movie)
        except ClientError as err:
            logger.error(
                "Couldn't load data into table %s. Here's why: %s: %s",
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

    def get_all_movies(self) -> List[dict]:
        """To get all movie data within the table"""
        movies = []
        try:
            response = self.table.scan()
            movies.extend(response.get("Items", []))
            while "LastEvaluatedKey" in response:
                response = self.table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
                movies.extend(response.get("Items", []))
        except ClientError as err:
            logger.error("Error retrieving movies: %s", err)
            raise HTTPException(status_code=500, detail="Error retrieving movies.")
        return movies

    def query_movies(self, year: int) -> List[dict]:
        try:
            response = self.table.scan(
                FilterExpression=Key("year").eq(year)
            )
            return response.get("Items", [])
        except ClientError as err:
            logger.error("Error querying movies by year: %s", err)
            raise HTTPException(status_code=500, detail=f"Error querying movies by year: {err.response['Error']['Message']}")

    def delete_table(self):
        """
        Deletes the table.
        """
        try:
            self.table.delete()
            self.table = None
            print("Table deleted successfully.")
        except ClientError as err:
            logger.error(
                "Couldn't delete table. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        
def get_sample_movie_data(movie_file_name):    
    """
    Gets sample movie data, either from a local file or by first downloading it from
    the Amazon DynamoDB developer guide.

    :param movie_file_name: The local file name where the movie data is stored in JSON format.
    :return: The movie data as a dict.
    """
    if not os.path.isfile(movie_file_name):
        print(f"Downloading {movie_file_name}...")
        movie_content = requests.get(
            "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/samples/moviedata.zip"
        )
        movie_zip = ZipFile(BytesIO(movie_content.content))
        movie_zip.extractall()

    try:
        with open(movie_file_name) as movie_file:
            movie_data = json.load(movie_file, parse_float=Decimal)
    except FileNotFoundError:
        print(
            f"File {movie_file_name} not found. You must first download the file to "
        )
        raise
    else:
        # The below code returns only the first 250.
        return movie_data[:250]
        
def run_scenario(movies_table, movie_file_name, dyn_resource):
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    print("-" * 88)
    print("Welcome to the Amazon DynamoDB movies store.")
    print("-" * 88)

    movies = Movies(dyn_resource, movies_table)
    movies_exists = movies.exists(movies_table)
    if not movies_exists:
        print(f"\nCreating table {movies_table}...")
        movies.create_table(movies_table)
        print(f"\nCreated table {movies.table.name}.")
    else:
        print(f"\n{movies_table} exists...")

    if not movies_exists:
        movie_data = get_sample_movie_data(movie_file_name)
        print(f"\nReading data from '{movie_file_name}' into your table.")
        movies.write_batch(movie_data)
        print(f"\nWrote {len(movie_data)} movies into {movies_table}.")
    print("-" * 88)

    if Question.ask_question(f"\nDelete the table? (y/n) ", Question.is_yesno):
        movies.delete_table()
        print(f"Deleted {movies_table}.")
    else:
        print(
            "Don't forget to delete the table when you're done or you might incur "
            "charges on your account."
        )

    print("\nThanks for using my custom DynamDB table!")
    print("-" * 88)
        
# Define DynamoDB resource and Movies instance
dynamodb = boto3.resource("dynamodb")
movies_table = "Table-for-movies"
movies_instance = Movies(dynamodb, movies_table)

# Define API endpoints
@app.get("/movies", response_model=List[dict])
def get_all_movies():
    return movies_instance.get_all_movies()

@app.get("/movies/{year}", response_model=List[dict])
def get_movies_by_year(year: int):
    movies = movies_instance.query_movies(year)
    if not movies:
        raise HTTPException(status_code=404, detail="No movies found for the specified year.")
    return movies


if __name__ == "__main__":
    # Define the name of your JSON file here
    movie_file_name = 'moviedata.json'

    try:
        run_scenario("Table-for-movies", movie_file_name, boto3.resource("dynamodb"))
    except Exception as e:
        print(f"Something went wrong! Here's what: {e}")