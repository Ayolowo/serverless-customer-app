from fastapi import FastAPI, HTTPException
from collections import OrderedDict
from mangum import Mangum
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from typing import List
import boto3
import logging

# Initialize FastAPI AND Mangum to adapt FastAPI for AWS Lambda
app = FastAPI()
handler = Mangum(app)

# To log encountered errors
logger = logging.getLogger(__name__)

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb")
table_name = "Table-for-customers"


class Customers:     
    def __init__(self, dyn_resource, customers_table: str):
        self.dyn_resource = dyn_resource
        self.table = self.dyn_resource.Table(customers_table)
        
    
    def get_all_customers(self) -> List[OrderedDict]:
        """Retrieve all customers data."""
        customers = []
        try:
            response = self.table.scan()
            customers.extend(response.get("Items", []))
            while "LastEvaluatedKey" in response:
                response = self.table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
                customers.extend(response.get("Items", []))
            # Convert the list of dictionaries into OrderedDicts, maintaining order of keys
            ordered_customers = [
                OrderedDict([
                    ("customerId", customer["customerId"]),
                    ("name", customer["name"]),
                    ("email", customer["email"]),
                    ("phone", customer["phone"]),
                    ("address", customer["address"]),
                    ("transactionHistoryUrl", customer["transactionHistoryUrl"]),
                    ("createdAt", customer["createdAt"]),
                ])
                for customer in customers
            ]
            return ordered_customers
        except ClientError as err:
            logger.error(
                "Couldn't get customer data. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

    def query_customer(self, customerId: str) -> List[OrderedDict]:
        try:
            response = self.table.scan(FilterExpression=Key("customerId").eq(customerId))
            customers = response.get("Items", [])
            # Convert to OrderedDict to maintain field order
            ordered_customers = [
                OrderedDict([
                    ("customerId", customer["customerId"]),
                    ("name", customer["name"]),
                    ("email", customer["email"]),
                    ("phone", customer["phone"]),
                    ("address", customer["address"]),
                    ("transactionHistoryUrl", customer["transactionHistoryUrl"]),
                    ("createdAt", customer["createdAt"]),
                ])
                for customer in customers
            ]
            return ordered_customers
        except ClientError as err:
            logger.error(
                "Couldn't query for customer data in %s. Here's why: %s: %s",
                customerId,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

# Initialize customers instance
customers_instance = Customers(dynamodb, table_name)

# Define FastAPI routes

# @app.get("/")
# def root(event: dict, context: dict):
#     logger.info("Event received: %s", json.dumps(event))
#     return {"message": "Hello from FastAPI"}

@app.get("/customers", response_model=List[dict])
async def get_all_customers():
    return customers_instance.get_all_customers()

@app.get("/customers/{customerId}", response_model=List[dict])
async def query_customer(customerId: str):
    customers = customers_instance.query_customer(customerId)
    if not customers:
        raise HTTPException(status_code=404, detail="No customer details found for the specified customerId.")
    return customers