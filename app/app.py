from decimal import Decimal
import requests
from io import BytesIO
import simplejson as json
import logging
import os
from pprint import pprint
from zipfile import ZipFile
import boto3
import boto3.dynamodb
import botocore
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from question import Question


# Used to track/log events that happen as your program runs
logger = logging.getLogger(__name__)

# Create S3 bucket for movie images
def create_bucket(movies_images, region='us-east-1'):
    try:
        if region is None:
            s3_client = boto3.resource('s3')
            s3_client.create_bucket(movies_images)
        else:
            s3_client = boto3.resource('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=movies_images, CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


# Retrieve the List of existing bucket names
s3 = boto3.client('s3')
response = s3.list_buckets()

# Output the bucket names
#print('Existing buckets:')
for bucket in response['Buckets']:
    print(f'  {bucket["Name"]}')

# To upload a file to the S3 bucket
def upload_file(file_name, movies_images, object_name=None):
    # If S3 Object_name isn't specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, movies_images, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True





########################################################################
class Movies:
    """Encapsulates an Amazon DynamoDB table of movie data.

    Example data structure for a movie record in this table:
        {
            "year": 1999,
            "title": "For Love of the Game",
            "info": {
                "directors": ["Sam Raimi"],
                "release_date": "1999-09-15T00:00:00Z",
                "rating": 6.3,
                "plot": "A washed up pitcher flashes through his career.",
                "rank": 4987,
                "running_time_secs": 8220,
                "actors": [
                    "Kevin Costner",
                    "Kelly Preston",
                    "John C. Reilly"
                ]
            }
        }
    """

    def __init__(self, dyn_resource):
        """
        :param dyn_resource: A Boto3 DynamoDB resource.
        """
        self.dyn_resource = dyn_resource
        # The table variable is set during the scenario in the call to
        # 'exists' if the table exists. Otherwise, it is set by 'create_table'.
        self.table = None

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

    def create_table(self, movies_table):
        """
        Creates an Amazon DynamoDB table that can be used to store movie data.
        The table uses the title of the movie as the partition key and the
        release year as the sort key.

        :param movies_table: The name of the table to create.
        :return: The newly created table.
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
                       table was created.
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
        
    def get_all_movies(self):
        """
        Gets movie data from the table for all movies.

        :return: The data about the requested movies.
        """
        movies = []
        try:
            response = self.table.scan()
            movies.extend(response.get("Items", []))
        
        # If there are more items to retrieve, continue scanning
            while "LastEvaluatedKey" in response:
                response = self.table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
                movies.extend(response.get("Items", []))    
       
        except ClientError as err:
            logger.error(
                "Couldn't get movie %s from table %s. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return movies 

    def query_movies(self, year):
        """
        Queries for movies that were released in the specified year.

        :param year: The year to query.
        :return: The list of movies that were released in the specified year.
        """
        try:
            response = self.table.scan(FilterExpression=Key("year").eq(year))
        except ClientError as err:
            logger.error(
                "Couldn't query for movies released in %s. Here's why: %s: %s",
                year,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return response["Items"]

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
            "run this demo. See the README for instructions."
        )
        raise
    else:
        # The file has over 4000 movies, the below code returns only the first 250.
        return movie_data[:250]


def run_scenario(movies_table, movie_file_name, dyn_resource):
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    print("-" * 88)
    print("Welcome to the Amazon DynamoDB getting started demo.")
    print("-" * 88)

    movies = Movies(dyn_resource)
    movies_exists = movies.exists(movies_table)
    if not movies_exists:
        print(f"\nCreating table {movies_table}...")
        movies.create_table(movies_table)
        print(f"\nCreated table {movies.table.name}.")
        
    # Movie management setup end
    if not movies_exists:
        movie_data = get_sample_movie_data(movie_file_name)
        print(f"\nReading data from '{movie_file_name}' into your table.")
        movies.write_batch(movie_data)
        print(f"\nWrote {len(movie_data)} movies into {movies_table}.")
    print("-" * 88)

# Get all movies in the table
    if Question.ask_question(
        f"Let's move on...do you want to get info about every movie? in '{movies_table}'? (y/n) ",
        Question.is_yesno,
    ):  
        all_movies = movies.get_all_movies()
        print("\nHere's what I found:")
        all_movies = json.dumps(all_movies, indent=4, sort_keys=False)
        all_movies = "\n" + all_movies + "\n"
        print(all_movies)
        # for movie in all_movies:
        #     movie = json.dumps(movie, indent=4, sort_keys=False)
        #     movie = "\n" + movie + "\n"
        #     print(movie)
    print("-" * 88)

    # To query movies by year
    ask_for_year = True
    while ask_for_year:
        year = Question.ask_question(
            f"\nLet's get a list of movies released in a given year. Enter a year between "
            f"1994 and 2008: ",
            Question.is_int,
            Question.in_range(1994, 2008),
        )
        releases = movies.query_movies(year)
        if releases:
            print(f"There were {len(releases)} movies released in {year}:")
            
            releases = json.dumps(releases, indent=4, sort_keys=False)
            releases = "\n" + releases + "\n"
            print(releases)
            ask_for_year = False
        else:
            print(f"I don't know about any movies released in {year}!")
            ask_for_year = Question.ask_question(
                "Try another year? (y/n) ", Question.is_yesno
            )
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

if __name__ == "__main__":
    # Define the name of your JSON file here
    movie_file_name = 'moviedata.json'

    try:
        run_scenario(
            "Table-for-movies", movie_file_name, boto3.resource("dynamodb")
        )
    except Exception as e:
        print(f"Something went wrong with the demo! Here's what: {e}")