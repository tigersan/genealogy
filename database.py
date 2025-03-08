"""
Database operations for Wolyn Genealogy Explorer using PostgreSQL on Supabase
"""
import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, Float, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from passlib.hash import pbkdf2_sha256

# Create a base class for our models
Base = declarative_base()

# Define our models
class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    username = Column(String(50), nullable=False, unique=True)
    password_hash = Column(String(100), nullable=False)
    
    def set_password(self, password):
        self.password_hash = pbkdf2_sha256.hash(password)
        
    def verify_password(self, password):
        return pbkdf2_sha256.verify(password, self.password_hash)

class Person(Base):
    __tablename__ = 'persons'
    
    id = Column(Integer, primary_key=True)
    first_name = Column(String(100))
    last_name = Column(String(100))
    birth_date = Column(Date, nullable=True)
    death_date = Column(Date, nullable=True)
    birth_place = Column(String(100), nullable=True)
    death_place = Column(String(100), nullable=True)
    
    # Relationships
    parents = relationship("Relationship", foreign_keys="Relationship.child_id", back_populates="child")
    children = relationship("Relationship", foreign_keys="Relationship.parent_id", back_populates="parent")
    spouses = relationship("Marriage", primaryjoin="or_(Person.id==Marriage.person1_id, Person.id==Marriage.person2_id)")
    
    events_birth = relationship("BirthEvent", back_populates="person")
    events_death = relationship("DeathEvent", back_populates="person")
    
    confidence = Column(Float, default=1.0)  # Confidence level in this person's identity
    
    def __repr__(self):
        return f"<Person {self.first_name} {self.last_name}>"

class Relationship(Base):
    __tablename__ = 'relationships'
    
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('persons.id'))
    child_id = Column(Integer, ForeignKey('persons.id'))
    
    # Define relationship type
    is_father = Column(Boolean, default=False)
    
    # Confidence in this relationship
    confidence = Column(Float, default=1.0)
    
    # Relations for easy navigation
    parent = relationship("Person", foreign_keys=[parent_id], back_populates="children")
    child = relationship("Person", foreign_keys=[child_id], back_populates="parents")

class Marriage(Base):
    __tablename__ = 'marriages'
    
    id = Column(Integer, primary_key=True)
    person1_id = Column(Integer, ForeignKey('persons.id'))
    person2_id = Column(Integer, ForeignKey('persons.id'))
    marriage_date = Column(Date, nullable=True)
    marriage_place = Column(String(100), nullable=True)
    
    # Confidence in this marriage
    confidence = Column(Float, default=1.0)
    
    # Event reference if available
    event_id = Column(Integer, ForeignKey('marriage_events.id'), nullable=True)
    
    person1 = relationship("Person", foreign_keys=[person1_id])
    person2 = relationship("Person", foreign_keys=[person2_id])
    event = relationship("MarriageEvent")

# Event models to store raw scraped data
class BirthEvent(Base):
    __tablename__ = 'birth_events'
    
    id = Column(Integer, primary_key=True)
    day = Column(Integer, nullable=True)
    month = Column(Integer, nullable=True)
    year = Column(Integer, nullable=True)
    parish = Column(String(100), nullable=True)
    first_name = Column(String(100))
    last_name = Column(String(100))
    location = Column(String(100), nullable=True)
    father_first_name = Column(String(100), nullable=True)
    mother_first_name = Column(String(100), nullable=True)
    mother_maiden_name = Column(String(100), nullable=True)
    godparents_notes = Column(Text, nullable=True)
    signature = Column(String(50), nullable=True)
    page = Column(String(20), nullable=True)
    position = Column(String(20), nullable=True)
    archive = Column(String(50), nullable=True)
    scan_number = Column(String(20), nullable=True)
    index_author = Column(String(20), nullable=True)
    scan_url = Column(String(255), nullable=True)
    
    # Link to person record
    person_id = Column(Integer, ForeignKey('persons.id'), nullable=True)
    person = relationship("Person", back_populates="events_birth")
    
    raw_html = Column(Text, nullable=True)  # Store original HTML for reference

class DeathEvent(Base):
    __tablename__ = 'death_events'
    
    id = Column(Integer, primary_key=True)
    day = Column(Integer, nullable=True)
    month = Column(Integer, nullable=True)
    year = Column(Integer, nullable=True)
    parish = Column(String(100), nullable=True)
    first_name = Column(String(100))
    last_name = Column(String(100))
    age = Column(Integer, nullable=True)
    location = Column(String(100), nullable=True)
    about_deceased_and_family = Column(Text, nullable=True)
    signature = Column(String(50), nullable=True)
    page = Column(String(20), nullable=True)
    position = Column(String(20), nullable=True)
    archive = Column(String(50), nullable=True)
    scan_number = Column(String(20), nullable=True)
    index_author = Column(String(20), nullable=True)
    scan_url = Column(String(255), nullable=True)
    
    # Link to person record
    person_id = Column(Integer, ForeignKey('persons.id'), nullable=True)
    person = relationship("Person", back_populates="events_death")
    
    raw_html = Column(Text, nullable=True)  # Store original HTML for reference

class MarriageEvent(Base):
    __tablename__ = 'marriage_events'
    
    id = Column(Integer, primary_key=True)
    day = Column(Integer, nullable=True)
    month = Column(Integer, nullable=True)
    year = Column(Integer, nullable=True)
    parish = Column(String(100), nullable=True)
    
    # Groom information
    groom_first_name = Column(String(100))
    groom_last_name = Column(String(100))
    groom_location = Column(String(100), nullable=True)
    groom_age = Column(Integer, nullable=True)
    groom_father_first_name = Column(String(100), nullable=True)
    groom_mother_first_name = Column(String(100), nullable=True)
    groom_mother_maiden_name = Column(String(100), nullable=True)
    
    # Bride information
    bride_first_name = Column(String(100))
    bride_last_name = Column(String(100))
    bride_location = Column(String(100), nullable=True)
    bride_age = Column(Integer, nullable=True)
    bride_father_first_name = Column(String(100), nullable=True)
    bride_mother_first_name = Column(String(100), nullable=True)
    bride_mother_maiden_name = Column(String(100), nullable=True)
    
    witnesses_notes = Column(Text, nullable=True)
    signature = Column(String(50), nullable=True)
    page = Column(String(20), nullable=True)
    position = Column(String(20), nullable=True)
    archive = Column(String(50), nullable=True)
    scan_number = Column(String(20), nullable=True)
    index_author = Column(String(20), nullable=True)
    scan_url = Column(String(255), nullable=True)
    
    raw_html = Column(Text, nullable=True)  # Store original HTML for reference

class FamilyTree(Base):
    __tablename__ = 'family_trees'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    root_person_id = Column(Integer, ForeignKey('persons.id'))
    created_at = Column(Date)
    updated_at = Column(Date)
    user_id = Column(Integer, ForeignKey('users.id'))
    
    root_person = relationship("Person")

class CensusEntry(Base):
    __tablename__ = 'census_entries'
    
    id = Column(Integer, primary_key=True)
    household_number = Column(String(20), nullable=True)
    male_number = Column(String(20), nullable=True)
    female_number = Column(String(20), nullable=True)
    full_name = Column(String(255))
    male_age = Column(Integer, nullable=True)
    female_age = Column(Integer, nullable=True)
    parish = Column(String(100), nullable=True)
    location = Column(String(100), nullable=True)
    year = Column(Integer, nullable=True)
    archive = Column(String(50), nullable=True)
    index_author = Column(String(20), nullable=True)
    signature = Column(String(50), nullable=True)
    page = Column(String(20), nullable=True)
    scan_number = Column(String(20), nullable=True)
    notes = Column(Text, nullable=True)
    
    # Link to person record
    person_id = Column(Integer, ForeignKey('persons.id'), nullable=True)
    
    raw_html = Column(Text, nullable=True)  # Store original HTML for reference

class Database:
    def __init__(self):
        """Initialize the database connection to Supabase PostgreSQL"""
        # Get connection details from Streamlit secrets
        try:
            db_url = st.secrets["supabase_db"]["db_url"]
            
            # Create the engine with specific PostgreSQL options
            self.engine = create_engine(db_url)
            self.Session = sessionmaker(bind=self.engine)
            
            # Create a session
            self.session = self.Session()
            
            # Check connection
            self.session.execute("SELECT 1")
            
            # Create tables if they don't exist
            Base.metadata.create_all(self.engine)
            
        except Exception as e:
            st.error(f"Database connection error: {str(e)}")
            raise
    
    def close(self):
        """Close the database session"""
        if hasattr(self, 'session'):
            self.session.close()
    
    # User operations
    def add_user(self, username, password):
        """Add a new user to the database"""
        user = User(username=username)
        user.set_password(password)
        self.session.add(user)
        self.session.commit()
        return user
    
    def verify_user(self, username, password):
        """Verify a user's credentials"""
        user = self.session.query(User).filter_by(username=username).first()
        if user and user.verify_password(password):
            return user
        return None
    
    # Person operations
    def add_person(self, first_name, last_name, birth_date=None, death_date=None, 
                   birth_place=None, death_place=None, confidence=1.0):
        """Add a new person to the database"""
        person = Person(
            first_name=first_name,
            last_name=last_name,
            birth_date=birth_date,
            death_date=death_date,
            birth_place=birth_place,
            death_place=death_place,
            confidence=confidence
        )
        self.session.add(person)
        self.session.commit()
        return person
    
    def add_relationship(self, parent_id, child_id, is_father=False, confidence=1.0):
        """Add a parent-child relationship"""
        relationship = Relationship(
            parent_id=parent_id,
            child_id=child_id,
            is_father=is_father,
            confidence=confidence
        )
        self.session.add(relationship)
        self.session.commit()
        return relationship
    
    def add_marriage(self, person1_id, person2_id, marriage_date=None, 
                     marriage_place=None, confidence=1.0, event_id=None):
        """Add a marriage relationship"""
        marriage = Marriage(
            person1_id=person1_id,
            person2_id=person2_id,
            marriage_date=marriage_date,
            marriage_place=marriage_place,
            confidence=confidence,
            event_id=event_id
        )
        self.session.add(marriage)
        self.session.commit()
        return marriage
    
    # Event operations
    def add_birth_event(self, **kwargs):
        """Add a birth event"""
        birth_event = BirthEvent(**kwargs)
        self.session.add(birth_event)
        self.session.commit()
        return birth_event
    
    def add_death_event(self, **kwargs):
        """Add a death event"""
        death_event = DeathEvent(**kwargs)
        self.session.add(death_event)
        self.session.commit()
        return death_event
    
    def add_marriage_event(self, **kwargs):
        """Add a marriage event"""
        marriage_event = MarriageEvent(**kwargs)
        self.session.add(marriage_event)
        self.session.commit()
        return marriage_event
    
    def add_census_entry(self, **kwargs):
        """Add a census entry"""
        census_entry = CensusEntry(**kwargs)
        self.session.add(census_entry)
        self.session.commit()
        return census_entry
    
    # Query operations
    def get_person_by_id(self, person_id):
        """Get a person by ID"""
        return self.session.query(Person).filter_by(id=person_id).first()
    
    def find_persons_by_name(self, first_name=None, last_name=None):
        """Find persons by name"""
        query = self.session.query(Person)
        
        if first_name:
            query = query.filter(Person.first_name.ilike(f"%{first_name}%"))
        
        if last_name:
            query = query.filter(Person.last_name.ilike(f"%{last_name}%"))
        
        return query.all()
    
    def get_family_tree(self, person_id, generations=3):
        """Get family tree data for a person"""
        # This would be a more complex query to get ancestors and descendants
        # For simplicity, we'll just get the immediate family
        person = self.get_person_by_id(person_id)
        if not person:
            return None
        
        # Get parents
        parents = []
        for rel in person.parents:
            parents.append(rel.parent)
        
        # Get children
        children = []
        for rel in person.children:
            children.append(rel.child)
        
        # Get spouses
        spouses = []
        for marriage in person.spouses:
            if marriage.person1_id == person_id:
                spouses.append(self.get_person_by_id(marriage.person2_id))
            else:
                spouses.append(self.get_person_by_id(marriage.person1_id))
        
        return {
            'person': person,
            'parents': parents,
            'children': children,
            'spouses': spouses
        }
    
    def get_all_birth_events(self):
        """Get all birth events"""
        return self.session.query(BirthEvent).all()
    
    def get_all_death_events(self):
        """Get all death events"""
        return self.session.query(DeathEvent).all()
    
    def get_all_marriage_events(self):
        """Get all marriage events"""
        return self.session.query(MarriageEvent).all()
    
    def get_all_census_entries(self):
        """Get all census entries"""
        return self.session.query(CensusEntry).all()

# Create a function to initialize the database instance
def init_database():
    """Initialize and return a database instance"""
    try:
        return Database()
    except Exception as e:
        st.error(f"Failed to initialize database: {str(e)}")
        return None

# Global database instance - will be initialized in app.py
db = None