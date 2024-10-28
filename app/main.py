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