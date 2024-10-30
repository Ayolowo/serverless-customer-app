# PRACTICE FILE FOR PYTHON CODING


# class Person:
#     def __init__(self, name, age):
#         self.name = name
#         self.age = age
    
#     def speak(self):
#         print("my name is {} and i'm {} years old.".format(self.name, self.age))

# class Student(Person):
#     def __init__(self, name, age, school):
#         super().__init__(name, age)
#         self.school = school

#     def speak(self):
#         print("my name is {} and i'm {} years old and I graduated from {}.".format(self.name, self.age, self.school))

# person = Person("Ayomide", 28)
# student = Student ("Ayomide", 28, "Bells")

# person.speak()
# student.speak()

#########################################
# Python tester

# import unittest

# def is_prime(n):
#     if n < 2:
#         return False
#     for i in range(2, n):
#         if n % i == 0:
#             return False
#     return True

# class TestIsPrime(unittest.TestCase):
#     def test_is_prime(self):
#         self.assertTrue(is_prime(2))
#         self.assertTrue(is_prime(3))
#         self.assertTrue(is_prime(5))
#         self.assertFalse(is_prime(4))

# if __name__ == '__main__':
#     unittest.main()

###################################
# Python regular expressions

# import re

# # number extraction from string
# text = 'my phone number is 23432-342'
# match = re.findall('\d+\.\d+|\d+', text)
# print(match)

#######################
# # email extraction

# text = 'my email is ayolowo9@gmail.com and stotube20@gmail.com'
# matches = re.findall(r'\S+@\S+', text)    
# print(matches)

#####################
# Date and time import

# from datetime import datetime, timedelta

# now = datetime.now()
# print(now)
#########################

# Build a Python Movie API leveraging cloud infra

# from flask import Flask

# app = Flask(__name__)

# @app.route('/')
# def hello_world():
#     return 'Hello, World!'

import logging
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

logger = logging.getLogger(__name__)
def query_movies(self, year):
        """
        Queries for movies that were released in the specified year.

        :param year: The year to query.
        :return: The list of movies that were released in the specified year.
        """
        try:
            response = self.table.query(KeyConditionExpression=Key("year").eq(year))
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

#################################

# DynamoDB table creation
"""
        Creates an Amazon DynamoDB table that can be used to store movie data.
        The table uses the titlr of the movie as the partition key and the
        releaseYear as the sort key.

        :param table_name: The name of the table to create.
        :return: The newly created table.
        """
dynamodb = boto3.resource('dynamodb')

table = dynamodb.create_table(
    TableName='users',
    KeySchema=[
        {
            'AttributeName': 'title',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'releaseYear',
            'KeyType': 'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'title',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'releaseYear',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'genre',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'coverUrl',
            'AttributeType': 'S'
        },
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)

# Wait until the table exists.
table.wait_until_exists()

# Print out some data about the table.
print(table.item_count)

# Place items into the dynamoDB table
item_1 = {'title': 'gemini man','releaseYear': 2004,'genre': 'Science Fiction, Action', 'coverUrl':'www.url.com'}
item_2 = {'title': 'deadpool', 'releaseYear': 2024, 'genre': 'Action, Blood, humour, Violence'}

items_to_add = [item_1, item_2]

with table.batch_wrtiter() as batch:
    for item in items_to_add:
        batch.put_item(Item=items_to_add)