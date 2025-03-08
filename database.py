"""
Database operations for Wolyn Genealogy Explorer using Supabase
"""
import streamlit as st
import pandas as pd
from datetime import datetime
from passlib.hash import pbkdf2_sha256
from supabase import create_client

# Model classes to maintain compatibility with existing code
# These act as data container classes rather than SQLAlchemy models
class Person:
    def __init__(self, id=None, first_name=None, last_name=None, birth_date=None, 
                 death_date=None, birth_place=None, death_place=None, confidence=1.0, **kwargs):
        self.id = id
        self.first_name = first_name
        self.last_name = last_name
        self.birth_date = birth_date
        self.death_date = death_date
        self.birth_place = birth_place
        self.death_place = death_place
        self.confidence = confidence
        
        # These will be populated later when needed
        self.parents = []
        self.children = []
        self.spouses = []
        self.events_birth = []
        self.events_death = []
    
    def __repr__(self):
        return f"<Person {self.first_name} {self.last_name}>"

class Relationship:
    def __init__(self, id=None, parent_id=None, child_id=None, is_father=False, 
                 confidence=1.0, **kwargs):
        self.id = id
        self.parent_id = parent_id
        self.child_id = child_id
        self.is_father = is_father
        self.confidence = confidence
        
        # These will be populated later when needed
        self.parent = None
        self.child = None

class Marriage:
    def __init__(self, id=None, person1_id=None, person2_id=None, marriage_date=None, 
                 marriage_place=None, confidence=1.0, event_id=None, **kwargs):
        self.id = id
        self.person1_id = person1_id
        self.person2_id = person2_id
        self.marriage_date = marriage_date
        self.marriage_place = marriage_place
        self.confidence = confidence
        self.event_id = event_id
        
        # These will be populated later when needed
        self.person1 = None
        self.person2 = None
        self.event = None

class BirthEvent:
    def __init__(self, id=None, day=None, month=None, year=None, parish=None, 
                 first_name=None, last_name=None, location=None, father_first_name=None, 
                 mother_first_name=None, mother_maiden_name=None, godparents_notes=None, 
                 signature=None, page=None, position=None, archive=None, scan_number=None, 
                 index_author=None, scan_url=None, person_id=None, raw_html=None, **kwargs):
        self.id = id
        self.day = day
        self.month = month
        self.year = year
        self.parish = parish
        self.first_name = first_name
        self.last_name = last_name
        self.location = location
        self.father_first_name = father_first_name
        self.mother_first_name = mother_first_name
        self.mother_maiden_name = mother_maiden_name
        self.godparents_notes = godparents_notes
        self.signature = signature
        self.page = page
        self.position = position
        self.archive = archive
        self.scan_number = scan_number
        self.index_author = index_author
        self.scan_url = scan_url
        self.person_id = person_id
        self.raw_html = raw_html
        
        # Will be populated later when needed
        self.person = None

class DeathEvent:
    def __init__(self, id=None, day=None, month=None, year=None, parish=None, 
                 first_name=None, last_name=None, age=None, location=None, 
                 about_deceased_and_family=None, signature=None, page=None, 
                 position=None, archive=None, scan_number=None, index_author=None, 
                 scan_url=None, person_id=None, raw_html=None, **kwargs):
        self.id = id
        self.day = day
        self.month = month
        self.year = year
        self.parish = parish
        self.first_name = first_name
        self.last_name = last_name
        self.age = age
        self.location = location
        self.about_deceased_and_family = about_deceased_and_family
        self.signature = signature
        self.page = page
        self.position = position
        self.archive = archive
        self.scan_number = scan_number
        self.index_author = index_author
        self.scan_url = scan_url
        self.person_id = person_id
        self.raw_html = raw_html
        
        # Will be populated later when needed
        self.person = None

class MarriageEvent:
    def __init__(self, id=None, day=None, month=None, year=None, parish=None, 
                 groom_first_name=None, groom_last_name=None, groom_location=None, 
                 groom_age=None, groom_father_first_name=None, groom_mother_first_name=None, 
                 groom_mother_maiden_name=None, bride_first_name=None, bride_last_name=None, 
                 bride_location=None, bride_age=None, bride_father_first_name=None, 
                 bride_mother_first_name=None, bride_mother_maiden_name=None, 
                 witnesses_notes=None, signature=None, page=None, position=None, 
                 archive=None, scan_number=None, index_author=None, scan_url=None, 
                 raw_html=None, **kwargs):
        self.id = id
        self.day = day
        self.month = month
        self.year = year
        self.parish = parish
        self.groom_first_name = groom_first_name
        self.groom_last_name = groom_last_name
        self.groom_location = groom_location
        self.groom_age = groom_age
        self.groom_father_first_name = groom_father_first_name
        self.groom_mother_first_name = groom_mother_first_name
        self.groom_mother_maiden_name = groom_mother_maiden_name
        self.bride_first_name = bride_first_name
        self.bride_last_name = bride_last_name
        self.bride_location = bride_location
        self.bride_age = bride_age
        self.bride_father_first_name = bride_father_first_name
        self.bride_mother_first_name = bride_mother_first_name
        self.bride_mother_maiden_name = bride_mother_maiden_name
        self.witnesses_notes = witnesses_notes
        self.signature = signature
        self.page = page
        self.position = position
        self.archive = archive
        self.scan_number = scan_number
        self.index_author = index_author
        self.scan_url = scan_url
        self.raw_html = raw_html

class CensusEntry:
    def __init__(self, id=None, household_number=None, male_number=None, female_number=None, 
                 full_name=None, male_age=None, female_age=None, parish=None, location=None, 
                 year=None, archive=None, index_author=None, signature=None, page=None, 
                 scan_number=None, notes=None, person_id=None, raw_html=None, **kwargs):
        self.id = id
        self.household_number = household_number
        self.male_number = male_number
        self.female_number = female_number
        self.full_name = full_name
        self.male_age = male_age
        self.female_age = female_age
        self.parish = parish
        self.location = location
        self.year = year
        self.archive = archive
        self.index_author = index_author
        self.signature = signature
        self.page = page
        self.scan_number = scan_number
        self.notes = notes
        self.person_id = person_id
        self.raw_html = raw_html

class User:
    def __init__(self, id=None, username=None, password_hash=None, **kwargs):
        self.id = id
        self.username = username
        self.password_hash = password_hash
    
    def set_password(self, password):
        self.password_hash = pbkdf2_sha256.hash(password)
        
    def verify_password(self, password):
        return pbkdf2_sha256.verify(password, self.password_hash)

class Database:
    def __init__(self):
        """Initialize the database connection to Supabase"""
        try:
            # Get Supabase URL and key from Streamlit secrets
            self.url = st.secrets["supabase"]["url"]
            self.key = st.secrets["supabase"]["key"]
            
            # Create Supabase client
            self.supabase = create_client(self.url, self.key)
            
            # Test connection
            self._initialize_tables()
            
        except Exception as e:
            st.error(f"Database connection error: {str(e)}")
            raise
    
    def _initialize_tables(self):
        """Initialize database tables if they don't exist"""
        try:
            # Check if tables exist, if not create them
            # This would normally be done in SQL migrations but we'll implement a simplified version
            
            # Test if persons table exists by querying it
            self.supabase.table("persons").select("id").limit(1).execute()
            
        except Exception as e:
            # If tables don't exist, we would create them
            # This would normally be done through Supabase migrations or SQL
            st.warning("Tables may not be properly set up in Supabase. Please ensure the schema is created.")
            # We don't automatically create tables with Supabase client as it requires SQL execution
    
    def close(self):
        """Close the database connection (no action needed for Supabase client)"""
        pass
    
    # User operations
    def add_user(self, username, password):
        """Add a new user to the database"""
        try:
            # Create User object
            user = User(username=username)
            user.set_password(password)
            
            # Check if username already exists
            existing = self.supabase.table('users').select('*').eq('username', username).execute()
            if existing.data and len(existing.data) > 0:
                return None  # Username already exists
            
            # Insert user into Supabase
            user_data = {
                'username': user.username,
                'password_hash': user.password_hash
            }
            
            result = self.supabase.table('users').insert(user_data).execute()
            
            if result.data and len(result.data) > 0:
                user.id = result.data[0]['id']
                return user
            
            return None
        except Exception as e:
            st.error(f"Error adding user: {str(e)}")
            return None
    
    def verify_user(self, username, password):
        """Verify a user's credentials"""
        try:
            result = self.supabase.table('users').select('*').eq('username', username).execute()
            
            if result.data and len(result.data) > 0:
                user_data = result.data[0]
                user = User(**user_data)
                
                if user.verify_password(password):
                    return user
            
            return None
        except Exception as e:
            st.error(f"Error verifying user: {str(e)}")
            return None
    
    # Person operations
    def add_person(self, first_name, last_name, birth_date=None, death_date=None, 
                   birth_place=None, death_place=None, confidence=1.0):
        """Add a new person to the database"""
        try:
            # Format dates properly for Supabase
            birth_date_str = birth_date.isoformat() if birth_date else None
            death_date_str = death_date.isoformat() if death_date else None
            
            # Prepare person data
            person_data = {
                'first_name': first_name,
                'last_name': last_name,
                'birth_date': birth_date_str,
                'death_date': death_date_str,
                'birth_place': birth_place,
                'death_place': death_place,
                'confidence': confidence
            }
            
            # Insert person into Supabase
            result = self.supabase.table('persons').insert(person_data).execute()
            
            if result.data and len(result.data) > 0:
                # Create Person object from result
                person_dict = result.data[0]
                # Convert date strings to datetime objects
                if person_dict.get('birth_date'):
                    try:
                        person_dict['birth_date'] = datetime.fromisoformat(person_dict['birth_date'])
                    except ValueError:
                        person_dict['birth_date'] = None
                if person_dict.get('death_date'):
                    try:
                        person_dict['death_date'] = datetime.fromisoformat(person_dict['death_date'])
                    except ValueError:
                        person_dict['death_date'] = None
                        
                person = Person(**person_dict)
                return person
            
            return None
        except Exception as e:
            st.error(f"Error adding person: {str(e)}")
            return None
    
    def add_relationship(self, parent_id, child_id, is_father=False, confidence=1.0):
        """Add a parent-child relationship"""
        try:
            # Check if relationship already exists
            existing = self.supabase.table('relationships').select('*')\
                .eq('parent_id', parent_id).eq('child_id', child_id).execute()
            
            if existing.data and len(existing.data) > 0:
                # Relationship already exists, return it
                rel_dict = existing.data[0]
                relationship = Relationship(**rel_dict)
                return relationship
            
            # Prepare relationship data
            relationship_data = {
                'parent_id': parent_id,
                'child_id': child_id,
                'is_father': is_father,
                'confidence': confidence
            }
            
            # Insert relationship into Supabase
            result = self.supabase.table('relationships').insert(relationship_data).execute()
            
            if result.data and len(result.data) > 0:
                # Create Relationship object from result
                rel_dict = result.data[0]
                relationship = Relationship(**rel_dict)
                return relationship
            
            return None
        except Exception as e:
            st.error(f"Error adding relationship: {str(e)}")
            return None
    
    def add_marriage(self, person1_id, person2_id, marriage_date=None, 
                     marriage_place=None, confidence=1.0, event_id=None):
        """Add a marriage relationship"""
        try:
            # Format date properly for Supabase
            marriage_date_str = marriage_date.isoformat() if marriage_date else None
            
            # Check if marriage already exists
            existing = self.supabase.table('marriages').select('*')\
                .or(f"person1_id.eq.{person1_id},and(person2_id.eq.{person2_id}),person1_id.eq.{person2_id},and(person2_id.eq.{person1_id})").execute()
            
            if existing.data and len(existing.data) > 0:
                # Marriage already exists, return it
                marriage_dict = existing.data[0]
                if marriage_dict.get('marriage_date'):
                    try:
                        marriage_dict['marriage_date'] = datetime.fromisoformat(marriage_dict['marriage_date'])
                    except ValueError:
                        marriage_dict['marriage_date'] = None
                        
                marriage = Marriage(**marriage_dict)
                return marriage
            
            # Prepare marriage data
            marriage_data = {
                'person1_id': person1_id,
                'person2_id': person2_id,
                'marriage_date': marriage_date_str,
                'marriage_place': marriage_place,
                'confidence': confidence,
                'event_id': event_id
            }
            
            # Insert marriage into Supabase
            result = self.supabase.table('marriages').insert(marriage_data).execute()
            
            if result.data and len(result.data) > 0:
                # Create Marriage object from result
                marriage_dict = result.data[0]
                if marriage_dict.get('marriage_date'):
                    try:
                        marriage_dict['marriage_date'] = datetime.fromisoformat(marriage_dict['marriage_date'])
                    except ValueError:
                        marriage_dict['marriage_date'] = None
                        
                marriage = Marriage(**marriage_dict)
                return marriage
            
            return None
        except Exception as e:
            st.error(f"Error adding marriage: {str(e)}")
            return None
    
    # Event operations
    def add_birth_event(self, **kwargs):
        """Add a birth event"""
        try:
            # Filter out any keys that aren't in the model
            birth_event_data = {k: v for k, v in kwargs.items() if hasattr(BirthEvent, k)}
            
            # Insert birth event into Supabase
            result = self.supabase.table('birth_events').insert(birth_event_data).execute()
            
            if result.data and len(result.data) > 0:
                # Create BirthEvent object from result
                birth_event_dict = result.data[0]
                birth_event = BirthEvent(**birth_event_dict)
                return birth_event
            
            return None
        except Exception as e:
            st.error(f"Error adding birth event: {str(e)}")
            return None
    
    def add_death_event(self, **kwargs):
        """Add a death event"""
        try:
            # Filter out any keys that aren't in the model
            death_event_data = {k: v for k, v in kwargs.items() if hasattr(DeathEvent, k)}
            
            # Insert death event into Supabase
            result = self.supabase.table('death_events').insert(death_event_data).execute()
            
            if result.data and len(result.data) > 0:
                # Create DeathEvent object from result
                death_event_dict = result.data[0]
                death_event = DeathEvent(**death_event_dict)
                return death_event
            
            return None
        except Exception as e:
            st.error(f"Error adding death event: {str(e)}")
            return None
    
    def add_marriage_event(self, **kwargs):
        """Add a marriage event"""
        try:
            # Filter out any keys that aren't in the model
            marriage_event_data = {k: v for k, v in kwargs.items() if hasattr(MarriageEvent, k)}
            
            # Insert marriage event into Supabase
            result = self.supabase.table('marriage_events').insert(marriage_event_data).execute()
            
            if result.data and len(result.data) > 0:
                # Create MarriageEvent object from result
                marriage_event_dict = result.data[0]
                marriage_event = MarriageEvent(**marriage_event_dict)
                return marriage_event
            
            return None
        except Exception as e:
            st.error(f"Error adding marriage event: {str(e)}")
            return None
    
    def add_census_entry(self, **kwargs):
        """Add a census entry"""
        try:
            # Filter out any keys that aren't in the model
            census_entry_data = {k: v for k, v in kwargs.items() if hasattr(CensusEntry, k)}
            
            # Insert census entry into Supabase
            result = self.supabase.table('census_entries').insert(census_entry_data).execute()
            
            if result.data and len(result.data) > 0:
                # Create CensusEntry object from result
                census_entry_dict = result.data[0]
                census_entry