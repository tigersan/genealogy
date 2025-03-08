"""
Simple database operations for Wolyn Genealogy Explorer using Supabase
"""
import os
import streamlit as st
import datetime
import logging
import json
from supabase import create_client

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        """Initialize the Supabase client"""
        try:
            # Get Supabase credentials from Streamlit secrets or environment variables
            if hasattr(st, 'secrets') and 'SUPABASE_URL' in st.secrets:
                url = st.secrets['SUPABASE_URL']
                key = st.secrets['SUPABASE_KEY']
            else:
                url = os.environ.get('SUPABASE_URL')
                key = os.environ.get('SUPABASE_KEY')
            
            if not url or not key:
                raise ValueError("Supabase URL and key must be provided in environment variables or Streamlit secrets")
                
            # Initialize Supabase client
            self.supabase = create_client(url, key)
            
            # Initialize database tables if they don't exist
            self._init_tables()
            
            logger.info("Successfully connected to Supabase")
            
        except Exception as e:
            logger.error(f"Error connecting to Supabase: {e}")
            raise
    
    def _init_tables(self):
        """Initialize database tables if they don't exist"""
        try:
            # Check if tables exist by querying persons table
            self.supabase.table("persons").select("count", count="exact").execute()
            logger.info("Tables already exist")
        except Exception:
            # Tables don't exist, create them
            logger.info("Creating tables")
            
            # Execute SQL to create tables
            sql = """
            -- Users table
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                password_hash VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Persons table
            CREATE TABLE IF NOT EXISTS persons (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100), 
                birth_date DATE,
                death_date DATE,
                birth_place VARCHAR(100),
                death_place VARCHAR(100),
                confidence FLOAT DEFAULT 1.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Relationships table
            CREATE TABLE IF NOT EXISTS relationships (
                id SERIAL PRIMARY KEY,
                parent_id INTEGER REFERENCES persons(id) ON DELETE CASCADE,
                child_id INTEGER REFERENCES persons(id) ON DELETE CASCADE,
                is_father BOOLEAN DEFAULT FALSE,
                confidence FLOAT DEFAULT 1.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Marriages table
            CREATE TABLE IF NOT EXISTS marriages (
                id SERIAL PRIMARY KEY,
                person1_id INTEGER REFERENCES persons(id) ON DELETE CASCADE,
                person2_id INTEGER REFERENCES persons(id) ON DELETE CASCADE,
                marriage_date DATE,
                marriage_place VARCHAR(100),
                confidence FLOAT DEFAULT 1.0,
                event_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Birth events table
            CREATE TABLE IF NOT EXISTS birth_events (
                id SERIAL PRIMARY KEY,
                day INTEGER,
                month INTEGER,
                year INTEGER,
                parish VARCHAR(100),
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                location VARCHAR(100),
                father_first_name VARCHAR(100),
                mother_first_name VARCHAR(100),
                mother_maiden_name VARCHAR(100),
                godparents_notes TEXT,
                signature VARCHAR(50),
                page VARCHAR(20),
                position VARCHAR(20),
                archive VARCHAR(50),
                scan_number VARCHAR(20),
                index_author VARCHAR(20),
                scan_url VARCHAR(255),
                person_id INTEGER REFERENCES persons(id) ON DELETE SET NULL,
                raw_html TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Death events table
            CREATE TABLE IF NOT EXISTS death_events (
                id SERIAL PRIMARY KEY,
                day INTEGER,
                month INTEGER,
                year INTEGER,
                parish VARCHAR(100),
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                age INTEGER,
                location VARCHAR(100),
                about_deceased_and_family TEXT,
                signature VARCHAR(50),
                page VARCHAR(20),
                position VARCHAR(20),
                archive VARCHAR(50),
                scan_number VARCHAR(20),
                index_author VARCHAR(20),
                scan_url VARCHAR(255),
                person_id INTEGER REFERENCES persons(id) ON DELETE SET NULL,
                raw_html TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Marriage events table
            CREATE TABLE IF NOT EXISTS marriage_events (
                id SERIAL PRIMARY KEY,
                day INTEGER,
                month INTEGER,
                year INTEGER,
                parish VARCHAR(100),
                groom_first_name VARCHAR(100),
                groom_last_name VARCHAR(100),
                groom_location VARCHAR(100),
                groom_age INTEGER,
                groom_father_first_name VARCHAR(100),
                groom_mother_first_name VARCHAR(100),
                groom_mother_maiden_name VARCHAR(100),
                bride_first_name VARCHAR(100),
                bride_last_name VARCHAR(100),
                bride_location VARCHAR(100),
                bride_age INTEGER,
                bride_father_first_name VARCHAR(100),
                bride_mother_first_name VARCHAR(100),
                bride_mother_maiden_name VARCHAR(100),
                witnesses_notes TEXT,
                signature VARCHAR(50),
                page VARCHAR(20),
                position VARCHAR(20),
                archive VARCHAR(50),
                scan_number VARCHAR(20),
                index_author VARCHAR(20),
                scan_url VARCHAR(255),
                raw_html TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Census entries table
            CREATE TABLE IF NOT EXISTS census_entries (
                id SERIAL PRIMARY KEY,
                household_number VARCHAR(20),
                male_number VARCHAR(20),
                female_number VARCHAR(20),
                full_name VARCHAR(255),
                male_age INTEGER,
                female_age INTEGER,
                parish VARCHAR(100),
                location VARCHAR(100),
                year INTEGER,
                archive VARCHAR(50),
                index_author VARCHAR(20),
                signature VARCHAR(50),
                page VARCHAR(20),
                scan_number VARCHAR(20),
                notes TEXT,
                person_id INTEGER REFERENCES persons(id) ON DELETE SET NULL,
                raw_html TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Family trees table
            CREATE TABLE IF NOT EXISTS family_trees (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                root_person_id INTEGER REFERENCES persons(id) ON DELETE CASCADE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                user_id INTEGER REFERENCES users(id) ON DELETE SET NULL
            );
            
            -- Create indexes for performance
            CREATE INDEX IF NOT EXISTS idx_persons_names ON persons(first_name, last_name);
            CREATE INDEX IF NOT EXISTS idx_relationships_parent ON relationships(parent_id);
            CREATE INDEX IF NOT EXISTS idx_relationships_child ON relationships(child_id);
            CREATE INDEX IF NOT EXISTS idx_marriages_persons ON marriages(person1_id, person2_id);
            CREATE INDEX IF NOT EXISTS idx_birth_events_person ON birth_events(person_id);
            CREATE INDEX IF NOT EXISTS idx_death_events_person ON death_events(person_id);
            """
            
            self.supabase.sql(sql).execute()
            logger.info("Tables created successfully")
    
    # User operations
    def add_user(self, username, password):
        """Add a new user to the database"""
        from passlib.hash import pbkdf2_sha256
        
        # Hash password
        password_hash = pbkdf2_sha256.hash(password)
        
        # Insert user into users table
        try:
            data = {
                "username": username,
                "password_hash": password_hash
            }
            result = self.supabase.table("users").insert(data).execute()
            return result.data[0]
        except Exception as e:
            logger.error(f"Error adding user: {e}")
            raise e
    
    def verify_user(self, username, password):
        """Verify a user's credentials"""
        from passlib.hash import pbkdf2_sha256
        
        try:
            # Find user by username
            result = self.supabase.table("users").select("*").eq("username", username).execute()
            
            if not result.data:
                return None
            
            user_data = result.data[0]
            
            # Verify password
            if pbkdf2_sha256.verify(password, user_data["password_hash"]):
                return user_data
            return None
        except Exception as e:
            logger.error(f"Error verifying user: {e}")
            return None
    
    # Person operations
    def add_person(self, first_name, last_name, birth_date=None, death_date=None, 
                   birth_place=None, death_place=None, confidence=1.0):
        """Add a new person to the database"""
        # Prepare date fields in proper format
        if birth_date:
            if isinstance(birth_date, datetime.date):
                birth_date = birth_date.isoformat()
        
        if death_date:
            if isinstance(death_date, datetime.date):
                death_date = death_date.isoformat()
        
        # Insert person
        try:
            data = {
                "first_name": first_name,
                "last_name": last_name,
                "birth_date": birth_date,
                "death_date": death_date,
                "birth_place": birth_place,
                "death_place": death_place,
                "confidence": confidence
            }
            result = self.supabase.table("persons").insert(data).execute()
            return result.data[0]
        except Exception as e:
            logger.error(f"Error adding person: {e}")
            raise e
    
    def add_relationship(self, parent_id, child_id, is_father=False, confidence=1.0):
        """Add a parent-child relationship"""
        try:
            data = {
                "parent_id": parent_id,
                "child_id": child_id,
                "is_father": is_father,
                "confidence": confidence
            }
            result = self.supabase.table("relationships").insert(data).execute()
            return result.data[0]
        except Exception as e:
            logger.error(f"Error adding relationship: {e}")
            raise e
    
    def add_marriage(self, person1_id, person2_id, marriage_date=None, 
                     marriage_place=None, confidence=1.0, event_id=None):
        """Add a marriage relationship"""
        # Prepare date field
        if marriage_date:
            if isinstance(marriage_date, datetime.date):
                marriage_date = marriage_date.isoformat()
        
        try:
            data = {
                "person1_id": person1_id,
                "person2_id": person2_id,
                "marriage_date": marriage_date,
                "marriage_place": marriage_place,
                "confidence": confidence,
                "event_id": event_id
            }
            result = self.supabase.table("marriages").insert(data).execute()
            return result.data[0]
        except Exception as e:
            logger.error(f"Error adding marriage: {e}")
            raise e
    
    # Event operations
    def add_birth_event(self, **kwargs):
        """Add a birth event"""
        try:
            # Filter out None values to avoid SQL errors
            data = {k: v for k, v in kwargs.items() if v is not None}
            result = self.supabase.table("birth_events").insert(data).execute()
            return result.data[0]
        except Exception as e:
            logger.error(f"Error adding birth event: {e}")
            raise e
    
    def add_death_event(self, **kwargs):
        """Add a death event"""
        try:
            # Filter out None values to avoid SQL errors
            data = {k: v for k, v in kwargs.items() if v is not None}
            result = self.supabase.table("death_events").insert(data).execute()
            return result.data[0]
        except Exception as e:
            logger.error(f"Error adding death event: {e}")
            raise e
    
    def add_marriage_event(self, **kwargs):
        """Add a marriage event"""
        try:
            # Filter out None values to avoid SQL errors
            data = {k: v for k, v in kwargs.items() if v is not None}
            result = self.supabase.table("marriage_events").insert(data).execute()
            return result.data[0]
        except Exception as e:
            logger.error(f"Error adding marriage event: {e}")
            raise e
    
    def add_census_entry(self, **kwargs):
        """Add a census entry"""
        try:
            # Filter out None values to avoid SQL errors
            data = {k: v for k, v in kwargs.items() if v is not None}
            result = self.supabase.table("census_entries").insert(data).execute()
            return result.data[0]
        except Exception as e:
            logger.error(f"Error adding census entry: {e}")
            raise e
    
    # Query operations
    def get_person_by_id(self, person_id):
        """Get a person by ID"""
        try:
            result = self.supabase.table("persons").select("*").eq("id", person_id).execute()
            if not result.data:
                return None
            
            person_data = result.data[0]
            
            # Get relationships and other data
            person_data['parents'] = self._get_parent_relationships(person_id)
            person_data['children'] = self._get_child_relationships(person_id)
            person_data['spouses'] = self._get_spouses(person_id)
            person_data['events_birth'] = self._get_birth_events(person_id)
            person_data['events_death'] = self._get_death_events(person_id)
            
            return Person(**person_data)
        except Exception as e:
            logger.error(f"Error getting person: {e}")
            return None
    
    def _get_parent_relationships(self, child_id):
        """Get parent relationships for a person"""
        try:
            result = self.supabase.table("relationships") \
                .select("*, parent:persons(*)") \
                .eq("child_id", child_id) \
                .execute()
            
            return result.data
        except Exception as e:
            logger.error(f"Error getting parent relationships: {e}")
            return []
    
    def _get_child_relationships(self, parent_id):
        """Get child relationships for a person"""
        try:
            result = self.supabase.table("relationships") \
                .select("*, child:persons(*)") \
                .eq("parent_id", parent_id) \
                .execute()
            
            return result.data
        except Exception as e:
            logger.error(f"Error getting child relationships: {e}")
            return []
    
    def _get_spouses(self, person_id):
        """Get marriages for a person"""
        try:
            # Get marriages where person is person1
            marriages1 = self.supabase.table("marriages") \
                .select("*, person2:persons(*)") \
                .eq("person1_id", person_id) \
                .execute()
            
            # Get marriages where person is person2
            marriages2 = self.supabase.table("marriages") \
                .select("*, person1:persons(*)") \
                .eq("person2_id", person_id) \
                .execute()
            
            return marriages1.data + marriages2.data
        except Exception as e:
            logger.error(f"Error getting marriages: {e}")
            return []
    
    def _get_birth_events(self, person_id):
        """Get birth events for a person"""
        try:
            result = self.supabase.table("birth_events") \
                .select("*") \
                .eq("person_id", person_id) \
                .execute()
            
            return result.data
        except Exception as e:
            logger.error(f"Error getting birth events: {e}")
            return []
    
    def _get_death_events(self, person_id):
        """Get death events for a person"""
        try:
            result = self.supabase.table("death_events") \
                .select("*") \
                .eq("person_id", person_id) \
                .execute()
            
            return result.data
        except Exception as e:
            logger.error(f"Error getting death events: {e}")
            return []
    
    def find_persons_by_name(self, first_name=None, last_name=None):
        """Find persons by name"""
        try:
            query = self.supabase.table("persons").select("*")
            
            if first_name:
                # Use ILIKE for case-insensitive matching
                query = query.ilike("first_name", f"%{first_name}%")
            
            if last_name:
                # Use ILIKE for case-insensitive matching
                query = query.ilike("last_name", f"%{last_name}%")
            
            result = query.execute()
            
            # Convert to Person objects
            persons = [Person(**data) for data in result.data]
            return persons
        except Exception as e:
            logger.error(f"Error finding persons: {e}")
            return []
    
    def get_family_tree(self, person_id, generations=3):
        """Get family tree data for a person"""
        person = self.get_person_by_id(person_id)
        if not person:
            return None
        
        # Get parents
        parents = []
        for rel in person.parents:
            if 'parent' in rel and rel['parent']:
                parents.append(Person(**rel['parent']))
        
        # Get children
        children = []
        for rel in person.children:
            if 'child' in rel and rel['child']:
                children.append(Person(**rel['child']))
        
        # Get spouses
        spouses = []
        for marriage in person.spouses:
            if person_id == marriage.get('person1_id') and 'person2' in marriage:
                spouses.append(Person(**marriage['person2']))
            elif person_id == marriage.get('person2_id') and 'person1' in marriage:
                spouses.append(Person(**marriage['person1']))
        
        return {
            'person': person,
            'parents': parents,
            'children': children,
            'spouses': spouses
        }
    
    def get_all_birth_events(self):
        """Get all birth events"""
        try:
            result = self.supabase.table("birth_events").select("*").execute()
            return result.data
        except Exception as e:
            logger.error(f"Error getting all birth events: {e}")
            return []
    
    def get_all_death_events(self):
        """Get all death events"""
        try:
            result = self.supabase.table("death_events").select("*").execute()
            return result.data
        except Exception as e:
            logger.error(f"Error getting all death events: {e}")
            return []
    
    def get_all_marriage_events(self):
        """Get all marriage events"""
        try:
            result = self.supabase.table("marriage_events").select("*").execute()
            return result.data
        except Exception as e:
            logger.error(f"Error getting all marriage events: {e}")
            return []
    
    def get_all_census_entries(self):
        """Get all census entries"""
        try:
            result = self.supabase.table("census_entries").select("*").execute()
            return result.data
        except Exception as e:
            logger.error(f"Error getting all census entries: {e}")
            return []
    
    def session(self):
        """
        Mock SQLAlchemy session for compatibility with existing code
        """
        return self

    def query(self, cls):
        """
        Mock SQLAlchemy query for compatibility with existing code
        """
        if cls == Person:
            return PersonQuery(self)
        if cls == Relationship:
            return RelationshipQuery(self)
        if cls == Marriage:
            return MarriageQuery(self)
        return None
    
    def commit(self):
        """Mock commit for compatibility"""
        pass
    
    def close(self):
        """Mock close for compatibility"""
        pass


# Mock query classes for compatibility
class PersonQuery:
    def __init__(self, db):
        self.db = db
    
    def all(self):
        """Get all persons"""
        try:
            result = self.db.supabase.table("persons").select("*").execute()
            return [Person(**data) for data in result.data]
        except Exception as e:
            logger.error(f"Error querying persons: {e}")
            return []
    
    def filter_by(self, **kwargs):
        """Filter persons by exact match"""
        try:
            query = self.db.supabase.table("persons").select("*")
            for key, value in kwargs.items():
                query = query.eq(key, value)
            result = query.execute()
            return [Person(**data) for data in result.data]
        except Exception as e:
            logger.error(f"Error filtering persons: {e}")
            return []
    
    def first(self):
        """Get first result"""
        try:
            result = self.db.supabase.table("persons").select("*").limit(1).execute()
            if result.data:
                return Person(**result.data[0])
            return None
        except Exception as e:
            logger.error(f"Error getting first person: {e}")
            return None


class RelationshipQuery:
    def __init__(self, db):
        self.db = db
    
    def all(self):
        """Get all relationships"""
        try:
            result = self.db.supabase.table("relationships").select("*").execute()
            relationships = []
            for data in result.data:
                rel = Relationship(**data)
                relationships.append(rel)
            return relationships
        except Exception as e:
            logger.error(f"Error querying relationships: {e}")
            return []
    
    def filter_by(self, **kwargs):
        """Filter relationships by exact match"""
        try:
            query = self.db.supabase.table("relationships").select("*")
            for key, value in kwargs.items():
                query = query.eq(key, value)
            result = query.execute()
            return [Relationship(**data) for data in result.data]
        except Exception as e:
            logger.error(f"Error filtering relationships: {e}")
            return []
    
    def first(self):
        """Get first result"""
        try:
            result = self.filter_by(**kwargs).limit(1).execute()
            if result.data:
                return Relationship(**result.data[0])
            return None
        except Exception as e:
            logger.error(f"Error getting first relationship: {e}")
            return None


class MarriageQuery:
    def __init__(self, db):
        self.db = db
    
    def all(self):
        """Get all marriages"""
        try:
            result = self.db.supabase.table("marriages").select("*").execute()
            return [Marriage(**data) for data in result.data]
        except Exception as e:
            logger.error(f"Error querying marriages: {e}")
            return []
    
    def filter(self, condition):
        """More complex filtering for marriages"""
        # This is a simplified implementation for compatibility
        try:
            if "person1_id" in str(condition) and "person2_id" in str(condition):
                # Assume we're looking for a specific marriage between two people
                result = self.db.supabase.table("marriages").select("*").execute()
                filtered = []
                for data in result.data:
                    marriage = Marriage(**data)
                    # Very basic filtering logic - this would need to be expanded for real use
                    if (marriage.person1_id == kwargs.get('person1_id') and 
                        marriage.person2_id == kwargs.get('person2_id')):
                        filtered.append(marriage)
                    elif (marriage.person1_id == kwargs.get('person2_id') and 
                          marriage.person2_id == kwargs.get('person1_id')):
                        filtered.append(marriage)
                return filtered
            return []
        except Exception as e:
            logger.error(f"Error filtering marriages: {e}")
            return []
    
    def first(self):
        """Get first result"""
        try:
            result = self.db.supabase.table("marriages").select("*").limit(1).execute()
            if result.data:
                return Marriage(**result.data[0])
            return None
        except Exception as e:
            logger.error(f"Error getting first marriage: {e}")
            return None


# Model classes
class Person:
    """Person model to simulate SQLAlchemy model"""
    def __init__(self, id=None, first_name=None, last_name=None, birth_date=None,
                 death_date=None, birth_place=None, death_place=None, confidence=1.0,
                 parents=None, children=None, spouses=None, events_birth=None, events_death=None,
                 **kwargs):
        self.id = id
        self.first_name = first_name
        self.last_name = last_name
        self.birth_date = self._parse_date(birth_date)
        self.death_date = self._parse_date(death_date)
        self.birth_place = birth_place
        self.death_place = death_place
        self.confidence = confidence
        
        # Relationships
        self.parents = parents or []
        self.children = children or []
        self.spouses = spouses or []
        self.events_birth = events_birth or []
        self.events_death = events_death or []
        
        # Needed for compatibility with tree builder
        self.gender = None
    
    def _parse_date(self, date_value):
        """Parse date from string or keep as is if already a date object"""
        if not date_value:
            return None
        
        if isinstance(date_value, datetime.date):
            return date_value
        
        try:
            if isinstance(date_value, str):
                return datetime.date.fromisoformat(date_value)
        except ValueError:
            pass
        
        return None
    
    def __repr__(self):
        return f"<Person {self.first_name} {self.last_name}>"


class Relationship:
    """Relationship model to simulate SQLAlchemy model"""
    def __init__(self, id=None, parent_id=None, child_id=None, is_father=False, confidence=1.0,
                 parent=None, child=None, **kwargs):
        self.id = id
        self.parent_id = parent_id
        self.child_id = child_id
        self.is_father = bool(is_father)
        self.confidence = confidence
        self.parent = parent
        self.child = child


class Marriage:
    """Marriage model to simulate SQLAlchemy model"""
    def __init__(self, id=None, person1_id=None, person2_id=None, marriage_date=None,
                 marriage_place=None, confidence=1.0, event_id=None, person1=None, person2=None,
                 event=None, **kwargs):
        self.id = id
        self.person1_id = person1_id
        self.person2_id = person2_id
        self.marriage_date = self._parse_date(marriage_date)
        self.marriage_place = marriage_place
        self.confidence = confidence
        self.event_id = event_id
        self.person1 = person1
        self.person2 = person2
        self.event = event
    
    def _parse_date(self, date_value):
        """Parse date from string or keep as is if already a date object"""
        if not date_value:
            return None
        
        if isinstance(date_value, datetime.date):
            return date_value
        
        try:
            if isinstance(date_value, str):
                return datetime.date.fromisoformat(date_value)
        except ValueError:
            pass
        
        return None


# Create a global instance only when imported
db = None

def get_db():
    """Get or create the database instance"""
    global db
    if db is None:
        db = Database()
    return db