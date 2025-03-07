"""
Family tree builder for Wolyn Genealogy Explorer
"""
import re
import datetime
import pandas as pd
import networkx as nx
import logging
from Levenshtein import distance
import matplotlib.pyplot as plt
from database import Person, Relationship, Marriage, BirthEvent, DeathEvent, MarriageEvent

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TreeBuilder:
    """
    Build family trees by analyzing genealogical events
    """
    def __init__(self, db):
        """
        Initialize the tree builder.
        
        Args:
            db: Database instance for storing and retrieving data
        """
        self.db = db
        # Gender constants
        self.MALE = 'M'
        self.FEMALE = 'F'
        self.UNKNOWN = 'U'
    
    def import_scraped_data(self, data):
        """
        Import scraped data and create person records.
        
        Args:
            data (dict): Dictionary with births, deaths, marriages, and census data
            
        Returns:
            dict: Statistics about imported data
        """
        stats = {
            'births_imported': 0,
            'deaths_imported': 0,
            'marriages_imported': 0,
            'census_imported': 0,
            'persons_created': 0,
            'relationships_created': 0,
            'marriages_created': 0
        }
        
        # Process births
        for birth in data.get('births', []):
            # Create birth event record
            birth_event = self.db.add_birth_event(**birth)
            stats['births_imported'] += 1
            
            # Create or update person record
            person = self._create_or_update_person_from_birth(birth_event)
            if person:
                stats['persons_created'] += 1
                
                # Create parent relationships if parent information available
                if birth['father_first_name']:
                    father = self._find_or_create_parent(
                        birth['father_first_name'], 
                        None,  # We don't always have father's last name (typically same as child's)
                        birth['last_name'],  # Use child's last name as default
                        self.MALE, 
                        birth['location']
                    )
                    if father:
                        self.db.add_relationship(father.id, person.id, is_father=True)
                        stats['relationships_created'] += 1
                
                if birth['mother_first_name']:
                    mother = self._find_or_create_parent(
                        birth['mother_first_name'], 
                        birth['mother_maiden_name'], 
                        None,
                        self.FEMALE, 
                        birth['location']
                    )
                    if mother:
                        self.db.add_relationship(mother.id, person.id, is_father=False)
                        stats['relationships_created'] += 1
        
        # Process deaths
        for death in data.get('deaths', []):
            # Create death event record
            death_event = self.db.add_death_event(**death)
            stats['deaths_imported'] += 1
            
            # Create or update person record
            person = self._create_or_update_person_from_death(death_event)
            if person:
                stats['persons_created'] += 1
                
                # Extract family information from notes if available
                if death['about_deceased_and_family']:
                    self._process_death_notes(person, death['about_deceased_and_family'], death['location'])
        
        # Process marriages
        for marriage in data.get('marriages', []):
            # Create marriage event record
            marriage_event = self.db.add_marriage_event(**marriage)
            stats['marriages_imported'] += 1
            
            # Create or update person records for bride and groom
            groom = self._create_or_update_person_from_marriage(
                marriage_event, 
                is_groom=True
            )
            
            bride = self._create_or_update_person_from_marriage(
                marriage_event, 
                is_groom=False
            )
            
            if groom and bride:
                # Create marriage relationship
                marriage_date = None
                if marriage['year']:
                    try:
                        marriage_date = datetime.date(
                            marriage['year'], 
                            marriage['month'] or 1, 
                            marriage['day'] or 1
                        )
                    except ValueError:
                        pass
                
                self.db.add_marriage(
                    groom.id, 
                    bride.id, 
                    marriage_date=marriage_date,
                    marriage_place=marriage['parish'],
                    event_id=marriage_event.id
                )
                stats['marriages_created'] += 1
                
                # Create relationships with parents if available
                # Groom's parents
                if marriage['groom_father_first_name']:
                    father = self._find_or_create_parent(
                        marriage['groom_father_first_name'], 
                        None,  # We don't always have father's last name (typically same as child's)
                        marriage['groom_last_name'],  # Use groom's last name as default
                        self.MALE, 
                        marriage['groom_location']
                    )
                    if father:
                        self.db.add_relationship(father.id, groom.id, is_father=True)
                        stats['relationships_created'] += 1
                
                if marriage['groom_mother_first_name']:
                    mother = self._find_or_create_parent(
                        marriage['groom_mother_first_name'], 
                        marriage['groom_mother_maiden_name'], 
                        None,
                        self.FEMALE, 
                        marriage['groom_location']
                    )
                    if mother:
                        self.db.add_relationship(mother.id, groom.id, is_father=False)
                        stats['relationships_created'] += 1
                
                # Bride's parents
                if marriage['bride_father_first_name']:
                    father = self._find_or_create_parent(
                        marriage['bride_father_first_name'], 
                        None,  # We don't always have father's last name (typically same as child's)
                        marriage['bride_last_name'],  # Use bride's last name as default
                        self.MALE, 
                        marriage['bride_location']
                    )
                    if father:
                        self.db.add_relationship(father.id, bride.id, is_father=True)
                        stats['relationships_created'] += 1
                
                if marriage['bride_mother_first_name']:
                    mother = self._find_or_create_parent(
                        marriage['bride_mother_first_name'], 
                        marriage['bride_mother_maiden_name'], 
                        None,
                        self.FEMALE, 
                        marriage['bride_location']
                    )
                    if mother:
                        self.db.add_relationship(mother.id, bride.id, is_father=False)
                        stats['relationships_created'] += 1
        
        # Process census
        for census in data.get('census', []):
            # Create census entry
            census_entry = self.db.add_census_entry(**census)
            stats['census_imported'] += 1
            
            # Extract name and create/update person
            name_parts = self._parse_census_name(census['full_name'])
            if name_parts:
                first_name, last_name, maiden_name, gender = name_parts
                
                # Determine age based on gender
                age = None
                if gender == self.MALE and census['male_age']:
                    age = census['male_age']
                elif gender == self.FEMALE and census['female_age']:
                    age = census['female_age']
                
                # Estimate birth year
                birth_year = None
                if age and census['year']:
                    birth_year = census['year'] - age
                
                # Create or update person
                person = self._find_or_create_person(
                    first_name=first_name,
                    last_name=last_name,
                    birth_year=birth_year,
                    location=census['location'],
                    confidence=0.8  # Lower confidence for census records
                )
                
                if person:
                    stats['persons_created'] += 1
        
        return stats
    
    def build_trees(self):
        """
        Build family trees from imported data.
        
        Returns:
            list: List of tree dictionaries with nodes and edges
        """
        # Get all persons
        persons = self.db.session.query(Person).all()
        
        # Create a directed graph
        G = nx.DiGraph()
        
        # Add all persons as nodes
        for person in persons:
            G.add_node(person.id, 
                     name=f"{person.first_name} {person.last_name}",
                     birth_date=person.birth_date,
                     death_date=person.death_date,
                     birth_place=person.birth_place,
                     death_place=person.death_place)
        
        # Add relationships as edges
        relationships = self.db.session.query(Relationship).all()
        for rel in relationships:
            G.add_edge(rel.parent_id, rel.child_id, 
                     relationship="parent", 
                     is_father=rel.is_father)
        
        # Add marriages as undirected edges
        marriages = self.db.session.query(Marriage).all()
        for marriage in marriages:
            G.add_edge(marriage.person1_id, marriage.person2_id, 
                     relationship="spouse", 
                     marriage_date=marriage.marriage_date,
                     marriage_place=marriage.marriage_place)
            G.add_edge(marriage.person2_id, marriage.person1_id, 
                     relationship="spouse", 
                     marriage_date=marriage.marriage_date,
                     marriage_place=marriage.marriage_place)
        
        # Find connected components (these are the separate family trees)
        connected_components = list(nx.weakly_connected_components(G))
        
        trees = []
        for i, component in enumerate(connected_components):
            # Create a subgraph for each component
            subgraph = G.subgraph(component)
            
            # Convert to a format suitable for visualization
            tree = {
                'id': i,
                'name': f"Family Tree {i+1}",
                'nodes': [],
                'edges': []
            }
            
            # Add nodes
            for node_id in subgraph.nodes():
                node_data = subgraph.nodes[node_id]
                person = self.db.get_person_by_id(node_id)
                
                # Determine gender if possible
                gender = self.UNKNOWN
                if person:
                    # Look at relationships to guess gender
                    for rel in person.parents:
                        if rel.is_father:
                            gender = self.MALE
                            break
                        else:
                            gender = self.FEMALE
                            break
                
                tree['nodes'].append({
                    'id': node_id,
                    'name': node_data['name'],
                    'birth_date': node_data.get('birth_date'),
                    'death_date': node_data.get('death_date'),
                    'birth_place': node_data.get('birth_place'),
                    'death_place': node_data.get('death_place'),
                    'gender': gender
                })
            
            # Add edges
            for u, v, data in subgraph.edges(data=True):
                tree['edges'].append({
                    'source': u,
                    'target': v,
                    'relationship': data['relationship']
                })
            
            trees.append(tree)
        
        return trees
    
    def find_matches_by_name(self, first_name, last_name, threshold=0.8):
        """
        Find persons by fuzzy name matching.
        
        Args:
            first_name (str): First name to search for
            last_name (str): Last name to search for
            threshold (float): Similarity threshold (0-1) for matching
            
        Returns:
            list: Matching person records
        """
        # Get all persons
        persons = self.db.session.query(Person).all()
        
        matches = []
        for person in persons:
            # Calculate similarity scores
            first_name_sim = self._name_similarity(first_name, person.first_name)
            last_name_sim = self._name_similarity(last_name, person.last_name)
            
            # Average the scores
            avg_sim = (first_name_sim + last_name_sim) / 2
            
            if avg_sim >= threshold:
                matches.append({
                    'person': person,
                    'similarity': avg_sim
                })
        
        # Sort by similarity score
        matches.sort(key=lambda x: x['similarity'], reverse=True)
        
        return matches
    
    def get_ancestors(self, person_id, generations=3):
        """
        Get ancestors of a person.
        
        Args:
            person_id (int): Person ID
            generations (int): Number of generations to include
            
        Returns:
            dict: Tree data with nodes and edges
        """
        person = self.db.get_person_by_id(person_id)
        if not person:
            return None
        
        # Create a tree
        tree = {
            'nodes': [],
            'edges': []
        }
        
        # Add the root person
        tree['nodes'].append({
            'id': person.id,
            'name': f"{person.first_name} {person.last_name}",
            'birth_date': person.birth_date,
            'death_date': person.death_date,
            'birth_place': person.birth_place,
            'death_place': person.death_place,
            'gender': self.UNKNOWN
        })
        
        # Recursively add ancestors
        self._add_ancestors(person, tree, generations, current_gen=0)
        
        return tree
    
    def get_descendants(self, person_id, generations=3):
        """
        Get descendants of a person.
        
        Args:
            person_id (int): Person ID
            generations (int): Number of generations to include
            
        Returns:
            dict: Tree data with nodes and edges
        """
        person = self.db.get_person_by_id(person_id)
        if not person:
            return None
        
        # Create a tree
        tree = {
            'nodes': [],
            'edges': []
        }
        
        # Add the root person
        tree['nodes'].append({
            'id': person.id,
            'name': f"{person.first_name} {person.last_name}",
            'birth_date': person.birth_date,
            'death_date': person.death_date,
            'birth_place': person.birth_place,
            'death_place': person.death_place,
            'gender': self.UNKNOWN
        })
        
        # Recursively add descendants
        self._add_descendants(person, tree, generations, current_gen=0)
        
        return tree
    
    def merge_persons(self, person1_id, person2_id):
        """
        Merge two person records.
        
        Args:
            person1_id (int): Person 1 ID (to keep)
            person2_id (int): Person 2 ID (to merge into Person 1)
            
        Returns:
            Person: Merged person record
        """
        person1 = self.db.get_person_by_id(person1_id)
        person2 = self.db.get_person_by_id(person2_id)
        
        if not person1 or not person2:
            return None
        
        # Merge birth/death dates and places if missing in person1
        if not person1.birth_date and person2.birth_date:
            person1.birth_date = person2.birth_date
        
        if not person1.birth_place and person2.birth_place:
            person1.birth_place = person2.birth_place
        
        if not person1.death_date and person2.death_date:
            person1.death_date = person2.death_date
        
        if not person1.death_place and person2.death_place:
            person1.death_place = person2.death_place
        
        # Merge relationships
        for rel in person2.parents:
            # Check if this relationship already exists
            exists = self.db.session.query(Relationship).filter_by(
                parent_id=rel.parent_id,
                child_id=person1.id
            ).first()
            
            if not exists:
                self.db.add_relationship(
                    rel.parent_id, 
                    person1.id, 
                    is_father=rel.is_father,
                    confidence=rel.confidence
                )
        
        for rel in person2.children:
            # Check if this relationship already exists
            exists = self.db.session.query(Relationship).filter_by(
                parent_id=person1.id,
                child_id=rel.child_id
            ).first()
            
            if not exists:
                self.db.add_relationship(
                    person1.id, 
                    rel.child_id, 
                    is_father=rel.is_father,
                    confidence=rel.confidence
                )
        
        # Merge marriages
        for marriage in person2.spouses:
            other_person_id = marriage.person1_id if marriage.person1_id != person2.id else marriage.person2_id
            
            # Check if this marriage already exists
            exists = self.db.session.query(Marriage).filter(
                ((Marriage.person1_id == person1.id) & (Marriage.person2_id == other_person_id)) |
                ((Marriage.person1_id == other_person_id) & (Marriage.person2_id == person1.id))
            ).first()
            
            if not exists:
                self.db.add_marriage(
                    person1.id, 
                    other_person_id, 
                    marriage_date=marriage.marriage_date,
                    marriage_place=marriage.marriage_place,
                    confidence=marriage.confidence,
                    event_id=marriage.event_id
                )
        
        # Update events to point to person1
        for event in person2.events_birth:
            event.person_id = person1.id
        
        for event in person2.events_death:
            event.person_id = person1.id
        
        # Delete person2
        self.db.session.delete(person2)
        self.db.session.commit()
        
        return person1
    
    def visualize_tree(self, tree_data, filename=None):
        """
        Visualize a family tree.
        
        Args:
            tree_data (dict): Tree data with nodes and edges
            filename (str): Output filename (optional)
            
        Returns:
            plt.Figure: Matplotlib figure object
        """
        G = nx.DiGraph()
        
        # Add nodes
        for node in tree_data['nodes']:
            G.add_node(node['id'], **node)
        
        # Add edges
        for edge in tree_data['edges']:
            G.add_edge(edge['source'], edge['target'], **edge)
        
        # Get position layout
        pos = nx.spring_layout(G, k=0.3*1/np.sqrt(len(G.nodes())), iterations=20)
        
        # Create figure
        plt.figure(figsize=(12, 8))
        
        # Draw nodes
        nx.draw_networkx_nodes(G, pos, 
                             node_color='skyblue', 
                             node_size=700, 
                             alpha=0.8)
        
        # Draw edges
        parent_edges = [(u, v) for u, v, d in G.edges(data=True) if d.get('relationship') == 'parent']
        spouse_edges = [(u, v) for u, v, d in G.edges(data=True) if d.get('relationship') == 'spouse']
        
        nx.draw_networkx_edges(G, pos, 
                             edgelist=parent_edges,
                             edge_color='r',
                             arrows=True,
                             width=1.5)
        
        nx.draw_networkx_edges(G, pos, 
                             edgelist=spouse_edges,
                             edge_color='b',
                             style='dashed',
                             arrows=False,
                             width=1.5)
        
        # Draw labels
        labels = {node['id']: node['name'] for node in tree_data['nodes']}
        nx.draw_networkx_labels(G, pos, labels=labels, font_size=10)
        
        plt.axis('off')
        plt.tight_layout()
        
        if filename:
            plt.savefig(filename)
        
        return plt.gcf()
    
    def _create_or_update_person_from_birth(self, birth_event):
        """
        Create or update a person record from a birth event.
        
        Args:
            birth_event (BirthEvent): Birth event record
            
        Returns:
            Person: Created or updated person record
        """
        # Check if we already have a person linked to this event
        if birth_event.person_id:
            return self.db.get_person_by_id(birth_event.person_id)
        
        # Try to find an existing person by name and approximate birth date
        birth_date = None
        if birth_event.year:
            try:
                birth_date = datetime.date(
                    birth_event.year, 
                    birth_event.month or 1, 
                    birth_event.day or 1
                )
            except ValueError:
                pass
        
        # Look for potential matches
        similar_persons = self.find_matches_by_name(
            first_name=birth_event.first_name,
            last_name=birth_event.last_name,
            threshold=0.8
        )
        
        # Check if any of the matches have a compatible birth date
        for match in similar_persons:
            person = match['person']
            
            # If person already has a birth date, check if it's close to our event
            if person.birth_date and birth_date:
                date_diff = abs((person.birth_date - birth_date).days)
                # Allow up to 30 days difference (handling baptism vs birth date)
                if date_diff <= 30:
                    # Update the person with any new information
                    if not person.birth_place and birth_event.location:
                        person.birth_place = birth_event.location
                    
                    # Link the event to this person
                    birth_event.person_id = person.id
                    self.db.session.commit()
                    
                    return person
            
            # If person doesn't have a birth date, but has a compatible birth place
            elif not person.birth_date and person.birth_place == birth_event.location:
                # Update with birth date and link event
                person.birth_date = birth_date
                birth_event.person_id = person.id
                self.db.session.commit()
                
                return person
        
        # If no suitable match found, create a new person
        person = self.db.add_person(
            first_name=birth_event.first_name,
            last_name=birth_event.last_name,
            birth_date=birth_date,
            birth_place=birth_event.location
        )
        
        # Link the event to this person
        birth_event.person_id = person.id
        self.db.session.commit()
        
        return person
    
    def _create_or_update_person_from_death(self, death_event):
        """
        Create or update a person record from a death event.
        
        Args:
            death_event (DeathEvent): Death event record
            
        Returns:
            Person: Created or updated person record
        """
        # Check if we already have a person linked to this event
        if death_event.person_id:
            return self.db.get_person_by_id(death_event.person_id)
        
        # Calculate approximate birth year based on age at death
        birth_year = None
        if death_event.age and death_event.year:
            birth_year = death_event.year - death_event.age
        
        # Try to find an existing person by name and approximate birth year
        death_date = None
        if death_event.year:
            try:
                death_date = datetime.date(
                    death_event.year, 
                    death_event.month or 1, 
                    death_event.day or 1
                )
            except ValueError:
                pass
        
        # Look for potential matches
        similar_persons = self.find_matches_by_name(
            first_name=death_event.first_name,
            last_name=death_event.last_name,
            threshold=0.8
        )
        
        # Check if any of the matches have a compatible birth year
        for match in similar_persons:
            person = match['person']
            
            # If person has a birth date, check if the birth year is compatible
            if person.birth_date and birth_year:
                year_diff = abs(person.birth_date.year - birth_year)
                # Allow up to 5 years difference (approximate age reporting)
                if year_diff <= 5:
                    # Update the person with death information
                    person.death_date = death_date
                    person.death_place = death_event.location
                    
                    # Link the event to this person
                    death_event.person_id = person.id
                    self.db.session.commit()
                    
                    return person
            
            # If person doesn't have a birth date or death date yet
            elif not person.death_date:
                # Update with death information and link event
                person.death_date = death_date
                person.death_place = death_event.location
                
                # If we can estimate birth year from age at death
                if birth_year and not person.birth_date:
                    # Set approximate birth date (January 1 of estimated year)
                    try:
                        person.birth_date = datetime.date(birth_year, 1, 1)
                    except ValueError:
                        pass
                
                death_event.person_id = person.id
                self.db.session.commit()
                
                return person
        
        # If no suitable match found, create a new person
        birth_date = None
        if birth_year:
            try:
                birth_date = datetime.date(birth_year, 1, 1)  # Approximate birth date
            except ValueError:
                pass
        
        person = self.db.add_person(
            first_name=death_event.first_name,
            last_name=death_event.last_name,
            birth_date=birth_date,
            death_date=death_date,
            death_place=death_event.location
        )
        
        # Link the event to this person
        death_event.person_id = person.id
        self.db.session.commit()
        
        return person
    
    def _create_or_update_person_from_marriage(self, marriage_event, is_groom=True):
        """
        Create or update a person record from a marriage event.
        
        Args:
            marriage_event (MarriageEvent): Marriage event record
            is_groom (bool): Whether to create/update the groom (True) or bride (False)
            
        Returns:
            Person: Created or updated person record
        """
        # Get the relevant fields based on whether this is groom or bride
        if is_groom:
            first_name = marriage_event.groom_first_name
            last_name = marriage_event.groom_last_name
            age = marriage_event.groom_age
            location = marriage_event.groom_location
        else:
            first_name = marriage_event.bride_first_name
            last_name = marriage_event.bride_last_name
            age = marriage_event.bride_age
            location = marriage_event.bride_location
        
        # Calculate approximate birth year based on age at marriage
        birth_year = None
        if age and marriage_event.year:
            birth_year = marriage_event.year - age
        
        # Look for potential matches
        similar_persons = self.find_matches_by_name(
            first_name=first_name,
            last_name=last_name,
            threshold=0.8
        )
        
        # Check if any of the matches have a compatible birth year
        for match in similar_persons:
            person = match['person']
            
            # If person has a birth date, check if the birth year is compatible
            if person.birth_date and birth_year:
                year_diff = abs(person.birth_date.year - birth_year)
                # Allow up to 5 years difference (approximate age reporting)
                if year_diff <= 5:
                    return person
            
            # If person doesn't have a birth date yet
            elif not person.birth_date and birth_year:
                # Update with approximate birth date
                try:
                    person.birth_date = datetime.date(birth_year, 1, 1)
                    self.db.session.commit()
                except ValueError:
                    pass
                
                return person
            
            # If no birth year information, check location for additional confirmation
            elif person.birth_place == location:
                return person
        
        # If no suitable match found, create a new person
        birth_date = None
        if birth_year:
            try:
                birth_date = datetime.date(birth_year, 1, 1)  # Approximate birth date
            except ValueError:
                pass
        
        person = self.db.add_person(
            first_name=first_name,
            last_name=last_name,
            birth_date=birth_date,
            birth_place=location
        )
        
        return person
    
    def _find_or_create_parent(self, first_name, last_name, child_last_name, gender, location):
        """
        Find or create a parent record.
        
        Args:
            first_name (str): First name
            last_name (str): Last name (can be None)
            child_last_name (str): Child's last name (fallback for father's last name)
            gender (str): Gender (MALE or FEMALE)
            location (str): Location
            
        Returns:
            Person: Found or created person record
        """
        # If it's a father and no last name provided, use child's last name
        if gender == self.MALE and not last_name and child_last_name:
            last_name = child_last_name
        
        # If we still don't have a last name, we can't reliably identify the person
        if not last_name:
            return None
        
        # Look for potential matches
        similar_persons = self.find_matches_by_name(
            first_name=first_name,
            last_name=last_name,
            threshold=0.8
        )
        
        # Check if any of the matches are compatible
        for match in similar_persons:
            person = match['person']
            
            # If location matches, it's likely the same person
            if person.birth_place == location or person.death_place == location:
                return person
        
        # If no suitable match found, create a new person
        person = self.db.add_person(
            first_name=first_name,
            last_name=last_name,
            birth_place=location
        )
        
        return person
    
    def _process_death_notes(self, person, notes, location):
        """
        Process death notes to extract family information.
        
        Args:
            person (Person): Person record
            notes (str): Notes about deceased and family
            location (str): Location
            
        Returns:
            None
        """
        # Extract spouse, children, and other family members from notes
        # Common patterns in death notes:
        # - "żona Adama" - wife of Adam
        # - "mąż Marianny" - husband of Marianna
        # - "syn/córka Józefa i Marianny" - son/daughter of Józef and Marianna
        # - "wdowa po Adamie" - widow of Adam
        # - "dzieci: Jan, Maria" - children: Jan, Maria
        
        # Extract spouse information
        spouse_match = re.search(r'(?:żona|mąż) ([A-Za-zżźćńółęąśŻŹĆĄŚĘŁÓŃ]+)', notes)
        if spouse_match:
            spouse_name = spouse_match.group(1)
            # Check if person is male or female based on relationship description
            is_male = 'mąż' in spouse_match.group(0)
            
            # Create spouse
            if is_male:
                spouse = self._find_or_create_parent(
                    spouse_name,
                    None,  # We don't know spouse's last name
                    None,
                    self.FEMALE,
                    location
                )
            else:
                spouse = self._find_or_create_parent(
                    spouse_name,
                    person.last_name,  # For husband, use person's last name
                    None,
                    self.MALE,
                    location
                )
            
            if spouse:
                # Create marriage relationship
                self.db.add_marriage(
                    person.id, 
                    spouse.id, 
                    marriage_place=location,
                    confidence=0.7  # Lower confidence for relationships inferred from notes
                )
        
        # Extract parents information
        parents_match = re.search(r'(?:syn|córka) ([A-Za-zżźćńółęąśŻŹĆĄŚĘŁÓŃ]+)(?: i ([A-Za-zżźćńółęąśŻŹĆĄŚĘŁÓŃ]+))?', notes)
        if parents_match:
            father_name = parents_match.group(1)
            mother_name = parents_match.group(2) if parents_match.group(2) else None
            
            # Create father
            if father_name:
                father = self._find_or_create_parent(
                    father_name,
                    person.last_name,  # For father, use person's last name
                    None,
                    self.MALE,
                    location
                )
                
                if father:
                    self.db.add_relationship(
                        father.id, 
                        person.id, 
                        is_father=True,
                        confidence=0.7
                    )
            
            # Create mother
            if mother_name:
                mother = self._find_or_create_parent(
                    mother_name,
                    None,  # We don't know mother's maiden name
                    None,
                    self.FEMALE,
                    location
                )
                
                if mother:
                    self.db.add_relationship(
                        mother.id, 
                        person.id, 
                        is_father=False,
                        confidence=0.7
                    )
        
        # Extract children information
        children_match = re.search(r'(?:dzieci|synowie|córki|dzieci): ([A-Za-zżźćńółęąśŻŹĆĄŚĘŁÓŃ, ]+)', notes)
        if children_match:
            children_list = children_match.group(1).split(',')
            for child_name in children_list:
                child_name = child_name.strip()
                if not child_name:
                    continue
                
                # Create child
                child = self._find_or_create_parent(
                    child_name,
                    person.last_name if 'syn' in notes else None,  # Use person's last name for sons
                    None,
                    self.UNKNOWN,  # We don't know child's gender
                    location
                )
                
                if child:
                    # Create parent-child relationship
                    self.db.add_relationship(
                        person.id, 
                        child.id, 
                        is_father='syn' in notes,  # If 'syn' in notes, person is father
                        confidence=0.7
                    )
    
    def _parse_census_name(self, full_name):
        """
        Parse census name to extract components.
        
        Args:
            full_name (str): Full name from census record
            
        Returns:
            tuple: (first_name, last_name, maiden_name, gender)
        """
        # Common patterns in census names:
        # - "Jan Kowalski" - male
        # - "Marianna Kowalska" - female
        # - "Marianna z Kowalskich" - female, maiden name Kowalska
        # - "żona Adama - Ewa z Nowaków" - wife of Adam, maiden name Nowak
        
        # Extract maiden name if present
        maiden_name = None
        maiden_match = re.search(r'z ([A-Za-zżźćńółęąśŻŹĆĄŚĘŁÓŃ]+)', full_name)
        if maiden_match:
            maiden_name = maiden_match.group(1)
        
        # Try to determine gender
        gender = self.UNKNOWN
        if 'żona' in full_name or 'córka' in full_name:
            gender = self.FEMALE
        elif 'mąż' in full_name or 'syn' in full_name:
            gender = self.MALE
        
        # Extract first and last name
        # This is a simplified approach and may not work for all cases
        name_match = re.search(r'([A-Za-zżźćńółęąśŻŹĆĄŚĘŁÓŃ]+) ([A-Za-zżźćńółęąśŻŹĆĄŚĘŁÓŃ]+)', full_name)
        if name_match:
            first_name = name_match.group(1)
            last_name = name_match.group(2)
            
            # Adjust for gender endings in Polish surnames
            if gender == self.FEMALE and not last_name.endswith('a') and last_name.endswith('i'):
                last_name = last_name[:-1] + 'a'
            
            return first_name, last_name, maiden_name, gender
        
        # If no clear pattern, just use the full name as first name
        return full_name, None, maiden_name, gender
    
    def _name_similarity(self, name1, name2):
        """
        Calculate similarity between two names.
        
        Args:
            name1 (str): First name
            name2 (str): Second name
            
        Returns:
            float: Similarity score (0-1)
        """
        if not name1 or not name2:
            return 0.0
        
        # Normalize names for comparison
        name1 = self._normalize_name(name1)
        name2 = self._normalize_name(name2)
        
        # Exact match
        if name1 == name2:
            return 1.0
        
        # Handle diminutive forms and spelling variations
        # Common Polish diminutives
        diminutives = {
            'adam': ['adaś', 'adek'],
            'jan': ['janek', 'jaś', 'jasiek'],
            'józef': ['józek', 'józio'],
            'marianna': ['marysia', 'maryna', 'mania'],
            'katarzyna': ['kasia', 'kasieńka', 'kaśka'],
            'anna': ['ania', 'anka', 'anusia'],
            'magdalena': ['magda', 'madzia'],
            'stanisław': ['staś', 'staszek'],
            'wacław': ['wacek'],
            'zofia': ['zosia', 'zośka']
        }
        
        # Check if either name is a diminutive form of the other
        if name1 in diminutives and name2 in diminutives[name1]:
            return 0.9
        if name2 in diminutives and name1 in diminutives[name2]:
            return 0.9
        
        # Calculate Levenshtein distance similarity
        max_len = max(len(name1), len(name2))
        if max_len == 0:
            return 0.0
        
        dist = distance(name1, name2)
        similarity = 1.0 - (dist / max_len)
        
        return similarity
    
    def _normalize_name(self, name):
        """
        Normalize a name for comparison.
        
        Args:
            name (str): Name to normalize
            
        Returns:
            str: Normalized name
        """
        if not name:
            return ""
        
        # Convert to lowercase and remove diacritics
        name = name.lower()
        
        # Replace Polish special characters
        replacements = {
            'ą': 'a', 'ć': 'c', 'ę': 'e', 'ł': 'l', 'ń': 'n',
            'ó': 'o', 'ś': 's', 'ż': 'z', 'ź': 'z'
        }
        
        for char, replacement in replacements.items():
            name = name.replace(char, replacement)
        
        return name
    
    def _add_ancestors(self, person, tree, max_generations, current_gen):
        """
        Recursively add ancestors to a tree.
        
        Args:
            person (Person): Person record
            tree (dict): Tree data
            max_generations (int): Maximum generations to include
            current_gen (int): Current generation
            
        Returns:
            None
        """
        if current_gen >= max_generations:
            return
        
        # Get parents
        for rel in person.parents:
            parent = rel.parent
            
            # Check if parent is already in the tree
            if not any(node['id'] == parent.id for node in tree['nodes']):
                # Add parent
                tree['nodes'].append({
                    'id': parent.id,
                    'name': f"{parent.first_name} {parent.last_name}",
                    'birth_date': parent.birth_date,
                    'death_date': parent.death_date,
                    'birth_place': parent.birth_place,
                    'death_place': parent.death_place,
                    'gender': self.MALE if rel.is_father else self.FEMALE
                })
            
            # Add edge from parent to person
            tree['edges'].append({
                'source': parent.id,
                'target': person.id,
                'relationship': 'parent'
            })
            
            # Recursively add parent's ancestors
            self._add_ancestors(parent, tree, max_generations, current_gen + 1)
            
            # Add parent's spouses (other than the one who is already a parent of this person)
            for marriage in parent.spouses:
                other_id = marriage.person1_id if marriage.person1_id != parent.id else marriage.person2_id
                
                # Skip if this is another parent of the person
                is_other_parent = False
                for other_rel in person.parents:
                    if other_rel.parent.id == other_id:
                        is_other_parent = True
                        break
                
                if is_other_parent:
                    continue
                
                # Get the spouse
                spouse = self.db.get_person_by_id(other_id)
                
                # Check if spouse is already in the tree
                if not any(node['id'] == spouse.id for node in tree['nodes']):
                    # Add spouse
                    tree['nodes'].append({
                        'id': spouse.id,
                        'name': f"{spouse.first_name} {spouse.last_name}",
                        'birth_date': spouse.birth_date,
                        'death_date': spouse.death_date,
                        'birth_place': spouse.birth_place,
                        'death_place': spouse.death_place,
                        'gender': self.FEMALE if rel.is_father else self.MALE
                    })
                
                # Add edge between spouses
                tree['edges'].append({
                    'source': parent.id,
                    'target': spouse.id,
                    'relationship': 'spouse'
                })
                tree['edges'].append({
                    'source': spouse.id,
                    'target': parent.id,
                    'relationship': 'spouse'
                })
    
    def _add_descendants(self, person, tree, max_generations, current_gen):
        """
        Recursively add descendants to a tree.
        
        Args:
            person (Person): Person record
            tree (dict): Tree data
            max_generations (int): Maximum generations to include
            current_gen (int): Current generation
            
        Returns:
            None
        """
        if current_gen >= max_generations:
            return
        
        # Get children
        for rel in person.children:
            child = rel.child
            
            # Check if child is already in the tree
            if not any(node['id'] == child.id for node in tree['nodes']):
                # Add child
                tree['nodes'].append({
                    'id': child.id,
                    'name': f"{child.first_name} {child.last_name}",
                    'birth_date': child.birth_date,
                    'death_date': child.death_date,
                    'birth_place': child.birth_place,
                    'death_place': child.death_place,
                    'gender': self.UNKNOWN  # We don't know the gender from this relationship
                })
            
            # Add edge from person to child
            tree['edges'].append({
                'source': person.id,
                'target': child.id,
                'relationship': 'parent'
            })
            
            # Add child's other parent if available
            for child_rel in child.parents:
                if child_rel.parent.id != person.id:
                    other_parent = child_rel.parent
                    
                    # Check if other parent is already in the tree
                    if not any(node['id'] == other_parent.id for node in tree['nodes']):
                        # Add other parent
                        tree['nodes'].append({
                            'id': other_parent.id,
                            'name': f"{other_parent.first_name} {other_parent.last_name}",
                            'birth_date': other_parent.birth_date,
                            'death_date': other_parent.death_date,
                            'birth_place': other_parent.birth_place,
                            'death_place': other_parent.death_place,
                            'gender': self.MALE if child_rel.is_father else self.FEMALE
                        })
                    
                    # Add edge from other parent to child
                    tree['edges'].append({
                        'source': other_parent.id,
                        'target': child.id,
                        'relationship': 'parent'
                    })
                    
                    # Add edge between parents
                    tree['edges'].append({
                        'source': person.id,
                        'target': other_parent.id,
                        'relationship': 'spouse'
                    })
                    tree['edges'].append({
                        'source': other_parent.id,
                        'target': person.id,
                        'relationship': 'spouse'
                    })
            
            # Recursively add child's descendants
            self._add_descendants(child, tree, max_generations, current_gen + 1)
