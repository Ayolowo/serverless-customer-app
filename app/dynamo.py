from decimal import Decimal
import simplejson as json
import logging
import boto3
import boto3.dynamodb
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from question import Question

# Used to track/log events that happen as your program runs
logger = logging.getLogger(__name__)

    
########################################################################
class Customers:
    """Encapsulates an Amazon DynamoDB table of customer data."""

    def __init__(self, dyn_resource):
        self.dyn_resource = dyn_resource
        self.table = None

    def exists(self, customers_table):
        try:
            table = self.dyn_resource.Table(customers_table)
            table.load()
            exists = True
        except ClientError as err:
            if err.response["Error"]["Code"] == "ResourceNotFoundException":
                exists = False
            else:
                logger.error(
                    "Couldn't check for existence of %s. Here's why: %s: %s",
                    customers_table,
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
                raise
        else:
            self.table = table
        return exists

    def create_table(self, customers_table):
        try:
            self.table = self.dyn_resource.create_table(
                TableName=customers_table,
                KeySchema=[
                    {"AttributeName": "name", "KeyType": "HASH"},  # Partition key
                    {"AttributeName": "customerId", "KeyType": "RANGE"},  # Sort key
                ],
                AttributeDefinitions=[
                    {"AttributeName": "name", "AttributeType": "S"},
                    {"AttributeName": "customerId", "AttributeType": "S"},
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 10,
                    "WriteCapacityUnits": 10,
                },
            )
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s",
                customers_table,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return self.table

    def write_batch(self, customers):
        try:
            with self.table.batch_writer() as writer:
                for customer in customers:
                    writer.put_item(Item=customer)
        except ClientError as err:
            logger.error(
                "Couldn't load data into table %s. Here's why: %s: %s",
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        
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

def load_customer_data(json_file):
    try:
        with open(json_file) as file:
            customer_data = json.load(file, parse_float=Decimal)
    except FileNotFoundError:
        print(f"File {json_file} not found.")
        raise
    else:
        return customer_data

def run_scenario(customers_table, json_file, dyn_resource):
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    
    print("-" * 88)
    print("Welcome to the {insert company name} customer data store.")
    print("-" * 88)

    customers = Customers(dyn_resource)
    table_exists = customers.exists(customers_table)

    if not table_exists:
        print(f"Creating table {customers_table}...")
        customers.create_table(customers_table)
        print(f"Created table {customers_table}.")
    else:
        print(f"{table_name} already exists...")
    
    if not table_exists:
        customer_data = load_customer_data(json_file)
        print(f"Writing data from '{json_file}' to the table...")
        customers.write_batch(customer_data)
        print(f"Wrote {len(customer_data)} customers into {customers_table}.")

    if Question.ask_question(f"\nDelete the table? (y/n) ", Question.is_yesno):
        customers.delete_table()
        print(f"Deleted {customers_table}.")
    else:
        print(
            "Don't forget to delete the table when you're done or you might incur "
            "charges on your account."
        )

    print("\nThanks for using my custom DynamDB table!")
    print("-" * 88)
    
if __name__ == "__main__":
    # Define the JSON file and table name
    json_file = "customerdata.json"
    table_name = "Table-for-customers"

    try:
        run_scenario(table_name, json_file, boto3.resource("dynamodb"))
    except Exception as e:
        print(f"An error occurred: {e}")