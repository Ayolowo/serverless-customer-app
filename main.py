from decimal import Decimal
import requests
from io import BytesIO
import simplejson as json
import logging
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

    def get_all_movies(self) -> List[dict]:
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
            response = self.table.query(
                KeyConditionExpression=Key("year").eq(year)
            )
            return response.get("Items", [])
        except ClientError as err:
            logger.error("Error querying movies by year: %s", err)
            raise HTTPException(status_code=500, detail="Error querying movies by year.")

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
