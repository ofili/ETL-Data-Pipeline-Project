# Import required objects
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base

# Create the engine
engine = create_engine("postgresql+psycopg2://student:student@localhost:5432/ladoldb")

# Initialize the session
session = Session(engine)

# Initialize the declarative base
Base = declarative_base()