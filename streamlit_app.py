"""
Wolyn Genealogy Explorer - Main Application
"""
import os
import streamlit as st
import pandas as pd
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
import re
import time
import tempfile
from datetime import datetime
from io import BytesIO
from PIL import Image
import base64
from pyvis.network import Network
import json
import logging
from streamlit_agraph import agraph, Node, Edge, Config

# Import our modules
from scraper import WolynScraper
from database import db, Person, Relationship, Marriage
from tree_builder import TreeBuilder
from auth import init_auth, login_form, logout

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Wolyn Genealogy Explorer",
    page_icon="👪",
    layout="wide",
    initial_sidebar_state="expanded"
)


# App header and navigation
def show_header():
    """Show the app header and navigation."""
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col1:
        st.image("https://wolyn-metryki.pl/Wolyn/flaga_en.png", width=100)
    
    with col2:
        st.title("Wolyn Genealogy Explorer")
        st.markdown("Discover your family roots from the Wolyn (Volhynia) region")
    
    with col3:
        if st.session_state.authenticated:
            st.write(f"Logged in as: {st.session_state.username}")
            if st.button("Logout"):
                logout()
                st.experimental_rerun()


# Initialize app state
def init_app():
    """Initialize the app state."""
    # Initialize the database connection
    from database import init_database, db as db_instance
    global db
    
    if 'db' not in st.session_state:
        db = init_database()
        st.session_state.db = db
    else:
        db = st.session_state.db
    
    # Check if database was initialized successfully
    if not db:
        st.error("Failed to connect to Supabase database. Please check your credentials.")
        st.stop()
    
    # Initialize other components
    init_auth()
    
    if 'scraper' not in st.session_state:
        st.session_state.scraper = WolynScraper()
    
    if 'tree_builder' not in st.session_state:
        st.session_state.tree_builder = TreeBuilder(db, st.session_state.scraper)
    
    if 'current_view' not in st.session_state:
        st.session_state.current_view = 'search'
    
    if 'search_results' not in st.session_state:
        st.session_state.search_results = None
    
    if 'selected_person' not in st.session_state:
        st.session_state.selected_person = None
    
    if 'family_trees' not in st.session_state:
        st.session_state.family_trees = None
    
    if 'discovery_status' not in st.session_state:
        st.session_state.discovery_status = None

# Sidebar navigation
def show_sidebar():
    """Show the sidebar with navigation options."""
    st.sidebar.title("Navigation")
    
    if st.sidebar.button("Search Records"):
        set_view('search')
    
    if st.sidebar.button("View Family Trees"):
        set_view('trees')
        # Load family trees
        with st.spinner("Loading family trees..."):
            st.session_state.family_trees = st.session_state.tree_builder.build_trees()
    
    if st.sidebar.button("Person Profiles"):
        set_view('profiles')
    
    if st.sidebar.button("Data Import/Export"):
        set_view('data')
    
    if st.sidebar.button("Settings"):
        set_view('settings')
    
    st.sidebar.markdown("---")
    st.sidebar.title("About")
    st.sidebar.info(
        """
        This application helps you explore genealogical data from the 
        [wolyn-metryki.pl](https://wolyn-metryki.pl) website and build 
        your family tree.
        
        Data from the website is owned and maintained by its creators.
        """
    )

# Search view
def show_search_view():
    """Show the search interface."""
    st.header("Search Genealogical Records")
    
    with st.form("search_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            first_name = st.text_input("First Name")
            last_name = st.text_input("Last Name")
            location = st.text_input("Location")
        
        with col2:
            parish = st.text_input("Parish")
            start_year = st.number_input("Start Year", min_value=1800, max_value=2000, value=1800)
            end_year = st.number_input("End Year", min_value=1800, max_value=2000, value=1900)
        
        submitted = st.form_submit_button("Search")
        
        if submitted:
            with st.spinner("Searching records..."):
                results = st.session_state.scraper.search(
                    first_name=first_name,
                    last_name=last_name,
                    location=location,
                    parish=parish,
                    start_year=start_year,
                    end_year=end_year
                )
                
                st.session_state.search_results = results
    
    # Show search results if available
    if st.session_state.search_results:
        show_search_results(st.session_state.search_results)

def show_search_results(results):
    """Show search results."""
    st.subheader("Search Results")
    
    # Count results
    births_count = len(results.get('births', []))
    deaths_count = len(results.get('deaths', []))
    marriages_count = len(results.get('marriages', []))
    census_count = len(results.get('census', []))
    
    st.write(f"Found {births_count} births, {deaths_count} deaths, {marriages_count} marriages, and {census_count} census records.")
    
    # Show tabs for different result types
    tabs = st.tabs(["Births", "Deaths", "Marriages", "Census"])
    
    # Births tab
    with tabs[0]:
        if births_count > 0:
            df_births = pd.DataFrame(results['births'])
            
            # Reformat dates
            for col in ['day', 'month', 'year']:
                if col in df_births.columns:
                    df_births[col] = df_births[col].astype(str)
            
            # Create date column
            df_births['date'] = df_births['day'] + '/' + df_births['month'] + '/' + df_births['year']
            
            # Select columns to display
            display_cols = ['date', 'first_name', 'last_name', 'location', 'father_first_name', 
                          'mother_first_name', 'mother_maiden_name', 'parish']
            
            # Filter for columns that exist
            display_cols = [col for col in display_cols if col in df_births.columns]
            
            st.dataframe(df_births[display_cols], use_container_width=True)
            
            # Import button
            if st.button("Import Birth Records"):
                with st.spinner("Importing records..."):
                    stats = st.session_state.tree_builder.import_scraped_data({'births': results['births']})
                    st.success(f"Imported {stats['births_imported']} birth records and created {stats['persons_created']} person records.")
        else:
            st.info("No birth records found.")
    
    # Deaths tab
    with tabs[1]:
        if deaths_count > 0:
            df_deaths = pd.DataFrame(results['deaths'])
            
            # Reformat dates
            for col in ['day', 'month', 'year']:
                if col in df_deaths.columns:
                    df_deaths[col] = df_deaths[col].astype(str)
            
            # Create date column
            df_deaths['date'] = df_deaths['day'] + '/' + df_deaths['month'] + '/' + df_deaths['year']
            
            # Select columns to display
            display_cols = ['date', 'first_name', 'last_name', 'age', 'location', 
                          'about_deceased_and_family', 'parish']
            
            # Filter for columns that exist
            display_cols = [col for col in display_cols if col in df_deaths.columns]
            
            st.dataframe(df_deaths[display_cols], use_container_width=True)
            
            # Import button
            if st.button("Import Death Records"):
                with st.spinner("Importing records..."):
                    stats = st.session_state.tree_builder.import_scraped_data({'deaths': results['deaths']})
                    st.success(f"Imported {stats['deaths_imported']} death records and created {stats['persons_created']} person records.")
        else:
            st.info("No death records found.")
    
    # Marriages tab
    with tabs[2]:
        if marriages_count > 0:
            df_marriages = pd.DataFrame(results['marriages'])
            
            # Reformat dates
            for col in ['day', 'month', 'year']:
                if col in df_marriages.columns:
                    df_marriages[col] = df_marriages[col].astype(str)
            
            # Create date column
            df_marriages['date'] = df_marriages['day'] + '/' + df_marriages['month'] + '/' + df_marriages['year']
            
            # Select columns to display
            display_cols = ['date', 'groom_first_name', 'groom_last_name', 'groom_age', 
                          'bride_first_name', 'bride_last_name', 'bride_age', 
                          'parish']
            
            # Filter for columns that exist
            display_cols = [col for col in display_cols if col in df_marriages.columns]
            
            st.dataframe(df_marriages[display_cols], use_container_width=True)
            
            # Import button
            if st.button("Import Marriage Records"):
                with st.spinner("Importing records..."):
                    stats = st.session_state.tree_builder.import_scraped_data({'marriages': results['marriages']})
                    st.success(f"Imported {stats['marriages_imported']} marriage records and created {stats['persons_created']} person records.")
        else:
            st.info("No marriage records found.")
    
    # Census tab
    with tabs[3]:
        if census_count > 0:
            df_census = pd.DataFrame(results['census'])
            
            # Select columns to display
            display_cols = ['full_name', 'male_age', 'female_age', 'household_number', 
                          'location', 'year', 'parish']
            
            # Filter for columns that exist
            display_cols = [col for col in display_cols if col in df_census.columns]
            
            st.dataframe(df_census[display_cols], use_container_width=True)
            
            # Import button
            if st.button("Import Census Records"):
                with st.spinner("Importing records..."):
                    stats = st.session_state.tree_builder.import_scraped_data({'census': results['census']})
                    st.success(f"Imported {stats['census_imported']} census records and created {stats['persons_created']} person records.")
        else:
            st.info("No census records found.")
    
    # Option to import all records
    if births_count > 0 or deaths_count > 0 or marriages_count > 0 or census_count > 0:
        if st.button("Import All Records"):
            with st.spinner("Importing all records..."):
                stats = st.session_state.tree_builder.import_scraped_data(results)
                st.success(f"Imported {stats['births_imported']} births, {stats['deaths_imported']} deaths, {stats['marriages_imported']} marriages, and {stats['census_imported']} census records. Created {stats['persons_created']} person records.")

# Trees view
def show_trees_view():
    """Show the family trees view."""
    st.header("Family Trees")
    
    if not st.session_state.family_trees:
        st.warning("No family trees found. Please build trees first.")
        
        if st.button("Build Trees"):
            with st.spinner("Building family trees..."):
                st.session_state.family_trees = st.session_state.tree_builder.build_trees()
    else:
        # Display tree selection
        tree_names = [f"Family Tree {i+1} ({len(tree['nodes'])} members)" for i, tree in enumerate(st.session_state.family_trees)]
        selected_tree_idx = st.selectbox("Select Family Tree", range(len(tree_names)), format_func=lambda x: tree_names[x])
        
        if selected_tree_idx is not None:
            selected_tree = st.session_state.family_trees[selected_tree_idx]
            
            # Show tree details
            st.subheader(f"Family Tree {selected_tree_idx + 1}")
            st.write(f"Members: {len(selected_tree['nodes'])}")
            
            # Visualize tree
            if len(selected_tree['nodes']) > 0:
                st.write("Family Tree Visualization:")
                
                # Choose visualization method based on tree size
                if len(selected_tree['nodes']) <= 50:
                    # Interactive graph for smaller trees
                    visualize_tree_interactive(selected_tree)
                else:
                    # Static image for larger trees
                    visualize_tree_static(selected_tree)
                
                # Show table of tree members
                st.write("Tree Members:")
                
                df_members = pd.DataFrame([
                    {
                        'ID': node['id'],
                        'Name': node['name'],
                        'Birth': node.get('birth_date', 'Unknown'),
                        'Death': node.get('death_date', 'Unknown'),
                        'Birth Place': node.get('birth_place', 'Unknown'),
                        'Death Place': node.get('death_place', 'Unknown')
                    }
                    for node in selected_tree['nodes']
                ])
                
                st.dataframe(df_members, use_container_width=True)
                
                # Allow selecting a person for detailed view
                selected_person_id = st.selectbox("Select Person to View", 
                                               options=[node['id'] for node in selected_tree['nodes']],
                                               format_func=lambda x: next((node['name'] for node in selected_tree['nodes'] if node['id'] == x), str(x)))
                
                if st.button("View Person"):
                    st.session_state.selected_person = db.get_person_by_id(selected_person_id)
                    set_view('person')

def visualize_tree_interactive(tree_data):
    """
    Create an interactive tree visualization.
    
    Args:
        tree_data (dict): Tree data with nodes and edges
    """
    nodes = []
    edges = []
    
    # Process nodes
    for node in tree_data['nodes']:
        # Determine color based on gender
        color = "#1E88E5"  # Default blue
        if node.get('gender') == 'M':
            color = "#1E88E5"  # Blue for male
        elif node.get('gender') == 'F':
            color = "#D81B60"  # Pink for female
        
        # Create label with name and dates if available
        label = node['name']
        birth = node.get('birth_date')
        death = node.get('death_date')
        
        if birth or death:
            label += "\n"
            if birth:
                if isinstance(birth, datetime):
                    label += f"b. {birth.year}"
                else:
                    label += f"b. {birth}"
            if death:
                if isinstance(death, datetime):
                    label += f" d. {death.year}"
                else:
                    label += f" d. {death}"
        
        nodes.append(Node(id=node['id'], 
                        label=label,
                        color=color,
                        size=20))
    
    # Process edges
    for edge in tree_data['edges']:
        # Determine color and style based on relationship
        color = "#888888"  # Default gray
        if edge.get('relationship') == 'parent':
            color = "#4CAF50"  # Green for parent
        elif edge.get('relationship') == 'spouse':
            color = "#FF9800"  # Orange for spouse
        
        edges.append(Edge(source=edge['source'],
                        target=edge['target'],
                        color=color))
    
    # Create config
    config = Config(width=800,
                  height=600,
                  directed=True,
                  physics=True,
                  hierarchical=False)
    
    # Render graph
    agraph(nodes=nodes, edges=edges, config=config)

def visualize_tree_static(tree_data):
    """
    Create a static tree visualization.
    
    Args:
        tree_data (dict): Tree data with nodes and edges
    """
    # Create a directed graph
    G = nx.DiGraph()
    
    # Add nodes
    for node in tree_data['nodes']:
        G.add_node(node['id'], **node)
    
    # Add edges
    for edge in tree_data['edges']:
        G.add_edge(edge['source'], edge['target'], **edge)
    
    # Create figure
    plt.figure(figsize=(12, 8))
    
    # Get position layout
    pos = nx.spring_layout(G, k=0.3*1/np.sqrt(len(G.nodes())), iterations=20)
    
    # Draw nodes with colors based on gender
    node_colors = []
    for node_id in G.nodes():
        node = G.nodes[node_id]
        if node.get('gender') == 'M':
            node_colors.append('skyblue')
        elif node.get('gender') == 'F':
            node_colors.append('lightpink')
        else:
            node_colors.append('lightgray')
    
    nx.draw_networkx_nodes(G, pos, 
                         node_color=node_colors, 
                         node_size=500, 
                         alpha=0.8)
    
    # Draw edges
    parent_edges = [(u, v) for u, v, d in G.edges(data=True) if d.get('relationship') == 'parent']
    spouse_edges = [(u, v) for u, v, d in G.edges(data=True) if d.get('relationship') == 'spouse']
    
    nx.draw_networkx_edges(G, pos, 
                         edgelist=parent_edges,
                         edge_color='green',
                         arrows=True,
                         width=1.5)
    
    nx.draw_networkx_edges(G, pos, 
                         edgelist=spouse_edges,
                         edge_color='orange',
                         style='dashed',
                         arrows=False,
                         width=1.5)
    
    # Draw labels - just names to keep it readable
    labels = {}
    for node_id in G.nodes():
        node = G.nodes[node_id]
        name = node.get('name', str(node_id))
        # Shorten name to first name + initial for readability in large trees
        name_parts = name.split()
        if len(name_parts) > 1:
            labels[node_id] = f"{name_parts[0]} {name_parts[1][0]}."
        else:
            labels[node_id] = name
    
    nx.draw_networkx_labels(G, pos, labels=labels, font_size=8)
    
    plt.axis('off')
    plt.tight_layout()
    
    # Display the plot
    st.pyplot(plt)

# Person view
def show_person_view():
    """Show detailed view of a selected person."""
    if not st.session_state.selected_person:
        st.warning("No person selected. Please select a person from the family tree.")
        return
    
    person = st.session_state.selected_person
    
    st.header(f"{person.first_name} {person.last_name}")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("Personal Information")
        
        info_table = [
            ["First Name", person.first_name],
            ["Last Name", person.last_name],
            ["Birth Date", person.birth_date or "Unknown"],
            ["Birth Place", person.birth_place or "Unknown"],
            ["Death Date", person.death_date or "Unknown"],
            ["Death Place", person.death_place or "Unknown"]
        ]
        
        # Format as markdown table
        st.markdown("| Field | Value |")
        st.markdown("| --- | --- |")
        for row in info_table:
            st.markdown(f"| {row[0]} | {row[1]} |")
    
    with col2:
        # Get family information
        family_tree = db.get_family_tree(person.id)
        
        if family_tree:
            st.subheader("Family")
            
            # Parents
            if family_tree['parents']:
                st.write("**Parents:**")
                for parent in family_tree['parents']:
                    st.write(f"- {parent.first_name} {parent.last_name}")
            
            # Spouses
            if family_tree['spouses']:
                st.write("**Spouses:**")
                for spouse in family_tree['spouses']:
                    st.write(f"- {spouse.first_name} {spouse.last_name}")
            
            # Children
            if family_tree['children']:
                st.write("**Children:**")
                for child in family_tree['children']:
                    st.write(f"- {child.first_name} {child.last_name}")
    
    # Events
    st.subheader("Events")
    
    # Birth events
    if person.events_birth:
        st.write("**Birth Records:**")
        for event in person.events_birth:
            birth_date = f"{event.day}/{event.month}/{event.year}" if event.day and event.month and event.year else "Unknown date"
            st.write(f"- Born on {birth_date} in {event.location or 'Unknown location'}")
            if event.father_first_name or event.mother_first_name:
                parents_info = []
                if event.father_first_name:
                    parents_info.append(f"Father: {event.father_first_name}")
                if event.mother_first_name:
                    mother_name = f"{event.mother_first_name}"
                    if event.mother_maiden_name:
                        mother_name += f" (née {event.mother_maiden_name})"
                    parents_info.append(f"Mother: {mother_name}")
                
                st.write(f"  {', '.join(parents_info)}")
    
    # Death events
    if person.events_death:
        st.write("**Death Records:**")
        for event in person.events_death:
            death_date = f"{event.day}/{event.month}/{event.year}" if event.day and event.month and event.year else "Unknown date"
            st.write(f"- Died on {death_date} at age {event.age or 'unknown'} in {event.location or 'Unknown location'}")
            if event.about_deceased_and_family:
                st.write(f"  Notes: {event.about_deceased_and_family}")
    
    # View ancestors and descendants
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("View Ancestors"):
            with st.spinner("Loading ancestors..."):
                ancestors_tree = st.session_state.tree_builder.get_ancestors(person.id)
                if ancestors_tree:
                    st.subheader("Ancestors")
                    visualize_tree_interactive(ancestors_tree)
                else:
                    st.info("No ancestors found.")
    
    with col2:
        if st.button("View Descendants"):
            with st.spinner("Loading descendants..."):
                descendants_tree = st.session_state.tree_builder.get_descendants(person.id)
                if descendants_tree:
                    st.subheader("Descendants")
                    visualize_tree_interactive(descendants_tree)
                else:
                    st.info("No descendants found.")

# Profiles view
def show_profiles_view():
    """Show person profiles."""
    st.header("Person Profiles")
    
    # Search for persons
    first_name = st.text_input("First Name")
    last_name = st.text_input("Last Name")
    
    if st.button("Search Persons"):
        with st.spinner("Searching..."):
            results = db.find_persons_by_name(first_name, last_name)
            
            if results:
                st.write(f"Found {len(results)} persons:")
                
                # Create a dataframe
                df_persons = pd.DataFrame([
                    {
                        'ID': person.id,
                        'First Name': person.first_name,
                        'Last Name': person.last_name,
                        'Birth Date': person.birth_date,
                        'Death Date': person.death_date,
                        'Birth Place': person.birth_place,
                        'Death Place': person.death_place
                    }
                    for person in results
                ])
                
                st.dataframe(df_persons, use_container_width=True)
                
                # Select person to view
                selected_id = st.selectbox("Select Person to View", 
                                        options=[person.id for person in results],
                                        format_func=lambda x: next((f"{person.first_name} {person.last_name}" for person in results if person.id == x), str(x)))
                
                if st.button("View Selected Person"):
                    person = db.get_person_by_id(selected_id)
                    if person:
                        st.session_state.selected_person = person
                        set_view('person')
            else:
                st.info("No persons found with that name.")
    
    # Option to view all persons
    if st.button("View All Persons"):
        with st.spinner("Loading all persons..."):
            all_persons = db.session.query(Person).all()
            
            if all_persons:
                st.write(f"Found {len(all_persons)} persons:")
                
                # Create a dataframe
                df_all_persons = pd.DataFrame([
                    {
                        'ID': person.id,
                        'First Name': person.first_name,
                        'Last Name': person.last_name,
                        'Birth Date': person.birth_date,
                        'Death Date': person.death_date,
                        'Birth Place': person.birth_place,
                        'Death Place': person.death_place
                    }
                    for person in all_persons
                ])
                
                st.dataframe(df_all_persons, use_container_width=True)
                
                # Select person to view
                selected_id = st.selectbox("Select Person", 
                                        options=[person.id for person in all_persons],
                                        format_func=lambda x: next((f"{person.first_name} {person.last_name}" for person in all_persons if person.id == x), str(x)))
                
                if st.button("View Person"):
                    person = db.get_person_by_id(selected_id)
                    if person:
                        st.session_state.selected_person = person
                        set_view('person')

# Data view
def show_data_view():
    """Show data import/export view."""
    st.header("Data Import/Export")
    
    tab1, tab2 = st.tabs(["Import", "Export"])
    
    # Import tab
    with tab1:
        st.subheader("Import Data")
        
        # Option to scrape data directly
        st.write("**Scrape Data from wolyn-metryki.pl**")
        
        with st.form("scrape_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                last_name = st.text_input("Last Name (required)")
                first_name = st.text_input("First Name (optional)")
            
            with col2:
                location = st.text_input("Location (optional)")
                parish = st.text_input("Parish (optional)")
            
            col3, col4 = st.columns(2)
            
            with col3:
                start_year = st.number_input("Start Year", min_value=1800, max_value=2000, value=1800)
            
            with col4:
                end_year = st.number_input("End Year", min_value=1800, max_value=2000, value=1900)
            
            submitted = st.form_submit_button("Scrape Data")
            
            if submitted:
                if not last_name:
                    st.error("Last name is required.")
                else:
                    with st.spinner("Scraping data..."):
                        results = st.session_state.scraper.search(
                            first_name=first_name,
                            last_name=last_name,
                            location=location,
                            parish=parish,
                            start_year=start_year,
                            end_year=end_year
                        )
                        
                        # Show results
                        births_count = len(results.get('births', []))
                        deaths_count = len(results.get('deaths', []))
                        marriages_count = len(results.get('marriages', []))
                        census_count = len(results.get('census', []))
                        
                        st.success(f"Scraped {births_count} births, {deaths_count} deaths, {marriages_count} marriages, and {census_count} census records.")
                        
                        # Option to import
                        if st.button("Import Scraped Data"):
                            with st.spinner("Importing scraped data..."):
                                stats = st.session_state.tree_builder.import_scraped_data(results)
                                st.success(f"Imported {stats['births_imported']} births, {stats['deaths_imported']} deaths, {stats['marriages_imported']} marriages, and {stats['census_imported']} census records. Created {stats['persons_created']} person records.")
        
        # Option to upload CSV
        st.write("**Upload Data File**")
        
        uploaded_file = st.file_uploader("Upload CSV or Excel file", type=['csv', 'xlsx', 'xls'])
        
        if uploaded_file is not None:
            try:
                # Determine file type
                file_type = uploaded_file.name.split('.')[-1].lower()
                
                if file_type == 'csv':
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                st.write("Preview of uploaded data:")
                st.dataframe(df.head(), use_container_width=True)
                
                # Determine data type
                data_type = st.selectbox("Select data type", 
                                      options=["births", "deaths", "marriages", "census"])
                
                # Map columns
                st.write("Map columns to database fields:")
                
                if data_type == "births":
                    col_mapping = {}
                    for col in df.columns:
                        field = st.selectbox(f"Map '{col}' to", 
                                          options=["(ignore)", "day", "month", "year", "first_name", 
                                                 "last_name", "location", "father_first_name", 
                                                 "mother_first_name", "mother_maiden_name"])
                        if field != "(ignore)":
                            col_mapping[col] = field
                    
                    if st.button("Import Birth Data"):
                        with st.spinner("Importing data..."):
                            # Convert data
                            births = []
                            for _, row in df.iterrows():
                                birth = {}
                                for col, field in col_mapping.items():
                                    birth[field] = row[col]
                                births.append(birth)
                            
                            # Import
                            stats = st.session_state.tree_builder.import_scraped_data({'births': births})
                            st.success(f"Imported {stats['births_imported']} birth records and created {stats['persons_created']} person records.")
                
                elif data_type == "deaths":
                    col_mapping = {}
                    for col in df.columns:
                        field = st.selectbox(f"Map '{col}' to", 
                                          options=["(ignore)", "day", "month", "year", "first_name", 
                                                 "last_name", "age", "location", "about_deceased_and_family"])
                        if field != "(ignore)":
                            col_mapping[col] = field
                    
                    if st.button("Import Death Data"):
                        with st.spinner("Importing data..."):
                            # Convert data
                            deaths = []
                            for _, row in df.iterrows():
                                death = {}
                                for col, field in col_mapping.items():
                                    death[field] = row[col]
                                deaths.append(death)
                            
                            # Import
                            stats = st.session_state.tree_builder.import_scraped_data({'deaths': deaths})
                            st.success(f"Imported {stats['deaths_imported']} death records and created {stats['persons_created']} person records.")
                
                elif data_type == "marriages":
                    col_mapping = {}
                    for col in df.columns:
                        field = st.selectbox(f"Map '{col}' to", 
                                          options=["(ignore)", "day", "month", "year", "groom_first_name", 
                                                 "groom_last_name", "groom_age", "bride_first_name", 
                                                 "bride_last_name", "bride_age", "parish"])
                        if field != "(ignore)":
                            col_mapping[col] = field
                    
                    if st.button("Import Marriage Data"):
                        with st.spinner("Importing data..."):
                            # Convert data
                            marriages = []
                            for _, row in df.iterrows():
                                marriage = {}
                                for col, field in col_mapping.items():
                                    marriage[field] = row[col]
                                marriages.append(marriage)
                            
                            # Import
                            stats = st.session_state.tree_builder.import_scraped_data({'marriages': marriages})
                            st.success(f"Imported {stats['marriages_imported']} marriage records and created {stats['persons_created']} person records.")
                
                elif data_type == "census":
                    col_mapping = {}
                    for col in df.columns:
                        field = st.selectbox(f"Map '{col}' to", 
                                          options=["(ignore)", "full_name", "male_age", "female_age", 
                                                 "household_number", "location", "year"])
                        if field != "(ignore)":
                            col_mapping[col] = field
                    
                    if st.button("Import Census Data"):
                        with st.spinner("Importing data..."):
                            # Convert data
                            census = []
                            for _, row in df.iterrows():
                                entry = {}
                                for col, field in col_mapping.items():
                                    entry[field] = row[col]
                                census.append(entry)
                            
                            # Import
                            stats = st.session_state.tree_builder.import_scraped_data({'census': census})
                            st.success(f"Imported {stats['census_imported']} census records and created {stats['persons_created']} person records.")
            
            except Exception as e:
                st.error(f"Error processing file: {e}")
    
    # Export tab
    with tab2:
        st.subheader("Export Data")
        
        # Option to export all data
        if st.button("Export All Data"):
            with st.spinner("Preparing data export..."):
                # Get all data
                persons = db.session.query(Person).all()
                relationships = db.session.query(Relationship).all()
                marriages = db.session.query(Marriage).all()
                
                # Create dataframes
                df_persons = pd.DataFrame([
                    {
                        'ID': person.id,
                        'First Name': person.first_name,
                        'Last Name': person.last_name,
                        'Birth Date': person.birth_date,
                        'Death Date': person.death_date,
                        'Birth Place': person.birth_place,
                        'Death Place': person.death_place
                    }
                    for person in persons
                ])
                
                df_relationships = pd.DataFrame([
                    {
                        'ID': rel.id,
                        'Parent ID': rel.parent_id,
                        'Child ID': rel.child_id,
                        'Is Father': rel.is_father,
                        'Confidence': rel.confidence
                    }
                    for rel in relationships
                ])
                
                df_marriages = pd.DataFrame([
                    {
                        'ID': marriage.id,
                        'Person 1 ID': marriage.person1_id,
                        'Person 2 ID': marriage.person2_id,
                        'Marriage Date': marriage.marriage_date,
                        'Marriage Place': marriage.marriage_place,
                        'Confidence': marriage.confidence
                    }
                    for marriage in marriages
                ])
                
                # Create Excel file
                output = BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_persons.to_excel(writer, sheet_name='Persons', index=False)
                    df_relationships.to_excel(writer, sheet_name='Relationships', index=False)
                    df_marriages.to_excel(writer, sheet_name='Marriages', index=False)
                
                # Provide download link
                output.seek(0)
                b64 = base64.b64encode(output.read()).decode()
                href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="wolyn_genealogy_data.xlsx">Download Excel file</a>'
                st.markdown(href, unsafe_allow_html=True)
        
        # Option to export family trees as JSON
        if st.button("Export Family Trees as JSON"):
            with st.spinner("Preparing family trees export..."):
                if not st.session_state.family_trees:
                    st.session_state.family_trees = st.session_state.tree_builder.build_trees()
                
                # Convert to JSON
                trees_json = json.dumps(st.session_state.family_trees, default=str, indent=2)
                
                # Provide download link
                b64 = base64.b64encode(trees_json.encode()).decode()
                href = f'<a href="data:application/json;base64,{b64}" download="wolyn_family_trees.json">Download JSON file</a>'
                st.markdown(href, unsafe_allow_html=True)
        
        # Option to export GEDCOM
        if st.button("Export GEDCOM"):
            with st.spinner("Preparing GEDCOM export..."):
                # Create GEDCOM content
                gedcom = create_gedcom()
                
                # Provide download link
                b64 = base64.b64encode(gedcom.encode()).decode()
                href = f'<a href="data:text/plain;base64,{b64}" download="wolyn_genealogy.ged">Download GEDCOM file</a>'
                st.markdown(href, unsafe_allow_html=True)

def create_gedcom():
    """
    Create a GEDCOM file.
    
    Returns:
        str: GEDCOM content
    """
    # Get all data
    persons = db.session.query(Person).all()
    relationships = db.session.query(Relationship).all()
    marriages = db.session.query(Marriage).all()
    
    # Create GEDCOM header
    gedcom = "0 HEAD\n"
    gedcom += "1 CHAR UTF-8\n"
    gedcom += "1 GEDC\n"
    gedcom += "2 VERS 5.5.1\n"
    gedcom += "2 FORM LINEAGE-LINKED\n"
    gedcom += "1 SOUR Wolyn Genealogy Explorer\n"
    gedcom += "2 VERS 1.0\n"
    gedcom += "1 DATE " + datetime.now().strftime("%d %b %Y") + "\n"
    gedcom += "1 SUBM @SUBM@\n"
    gedcom += "0 @SUBM@ SUBM\n"
    gedcom += "1 NAME " + (st.session_state.username or "User") + "\n"
    
    # Add persons
    for person in persons:
        gedcom += f"0 @I{person.id}@ INDI\n"
        gedcom += f"1 NAME {person.first_name} /{person.last_name}/\n"
        
        if person.birth_date:
            gedcom += "1 BIRT\n"
            gedcom += f"2 DATE {person.birth_date}\n"
            if person.birth_place:
                gedcom += f"2 PLAC {person.birth_place}\n"
        
        if person.death_date:
            gedcom += "1 DEAT\n"
            gedcom += f"2 DATE {person.death_date}\n"
            if person.death_place:
                gedcom += f"2 PLAC {person.death_place}\n"
    
    # Add families
    family_id = 1
    families = {}
    
    # Process marriages
    for marriage in marriages:
        family_key = f"{min(marriage.person1_id, marriage.person2_id)}_{max(marriage.person1_id, marriage.person2_id)}"
        
        if family_key not in families:
            families[family_key] = family_id
            
            gedcom += f"0 @F{family_id}@ FAM\n"
            gedcom += f"1 HUSB @I{marriage.person1_id}@\n"
            gedcom += f"1 WIFE @I{marriage.person2_id}@\n"
            
            if marriage.marriage_date:
                gedcom += "1 MARR\n"
                gedcom += f"2 DATE {marriage.marriage_date}\n"
                if marriage.marriage_place:
                    gedcom += f"2 PLAC {marriage.marriage_place}\n"
            
            family_id += 1
    
    # Process parent-child relationships
    # Group children by parents
    parent_children = {}
    for rel in relationships:
        parent_key = f"{rel.parent_id}"
        if parent_key not in parent_children:
            parent_children[parent_key] = []
        
        parent_children[parent_key].append(rel.child_id)
    
    # Find family ID for each parent pair
    for person in persons:
        # Get parents
        father_id = None
        mother_id = None
        
        for rel in person.parents:
            if rel.is_father:
                father_id = rel.parent_id
            else:
                mother_id = rel.parent_id
        
        if father_id and mother_id:
            # Check if this family exists
            family_key = f"{min(father_id, mother_id)}_{max(father_id, mother_id)}"
            
            if family_key in families:
                # Add child to existing family
                gedcom += f"1 CHIL @I{person.id}@\n"
            else:
                # Create new family
                families[family_key] = family_id
                
                gedcom += f"0 @F{family_id}@ FAM\n"
                gedcom += f"1 HUSB @I{father_id}@\n"
                gedcom += f"1 WIFE @I{mother_id}@\n"
                gedcom += f"1 CHIL @I{person.id}@\n"
                
                family_id += 1
        elif father_id:
            # Single father
            gedcom += f"0 @F{family_id}@ FAM\n"
            gedcom += f"1 HUSB @I{father_id}@\n"
            gedcom += f"1 CHIL @I{person.id}@\n"
            
            family_id += 1
        elif mother_id:
            # Single mother
            gedcom += f"0 @F{family_id}@ FAM\n"
            gedcom += f"1 WIFE @I{mother_id}@\n"
            gedcom += f"1 CHIL @I{person.id}@\n"
            
            family_id += 1
    
    # End GEDCOM file
    gedcom += "0 TRLR\n"
    
    return gedcom

# Settings view
def show_settings_view():
    """Show settings."""
    st.header("Settings")
    
    st.subheader("Tree Builder Settings")
    
    # Name similarity threshold
    similarity_threshold = st.slider(
        "Name Similarity Threshold",
        min_value=0.5,
        max_value=1.0,
        value=0.8,
        step=0.05,
        help="Threshold for considering names as similar when building trees."
    )
    
    # Age validation
    age_validation = st.checkbox(
        "Enable Age Validation",
        value=True,
        help="Validate parent-child relationships based on age."
    )
    
    # Father age range
    father_min_age = st.number_input("Minimum Father Age at Child Birth", value=16, min_value=10, max_value=100)
    father_max_age = st.number_input("Maximum Father Age at Child Birth", value=80, min_value=16, max_value=100)
    
    # Mother age range
    mother_min_age = st.number_input("Minimum Mother Age at Child Birth", value=16, min_value=10, max_value=100)
    mother_max_age = st.number_input("Maximum Mother Age at Child Birth", value=45, min_value=16, max_value=100)
    
    # Location matching
    location_matching = st.checkbox(
        "Consider Location for Matching",
        value=True,
        help="Use location information when matching persons."
    )
    
    if st.button("Save Settings"):
        # Store settings in session state
        st.session_state.settings = {
            'similarity_threshold': similarity_threshold,
            'age_validation': age_validation,
            'father_min_age': father_min_age,
            'father_max_age': father_max_age,
            'mother_min_age': mother_min_age,
            'mother_max_age': mother_max_age,
            'location_matching': location_matching
        }
        
        st.success("Settings saved!")
    
    st.subheader("Database Management")
    
    # Option to rebuild trees
    if st.button("Rebuild All Trees"):
        with st.spinner("Rebuilding family trees..."):
            st.session_state.family_trees = st.session_state.tree_builder.build_trees()
            st.success("Family trees rebuilt successfully!")
    
    # Option to clear database
    st.warning("Danger Zone")
    
    if st.button("Clear All Data"):
        if st.text_input("Type 'DELETE' to confirm") == "DELETE":
            with st.spinner("Clearing all data..."):
                # Delete all records
                db.session.query(Marriage).delete()
                db.session.query(Relationship).delete()
                db.session.query(Person).delete()
                db.session.commit()
                
                # Reset session state
                st.session_state.family_trees = None
                st.session_state.selected_person = None
                
                st.success("All data cleared successfully!")

# Main app
def main():
    """Main application entry point."""
    # Initialize app
    init_app()
    
    # Show login screen if not authenticated
    if not st.session_state.authenticated:
        if login_form():
            st.experimental_rerun()
    else:
        # Show header
        show_header()
        
        # Show sidebar
        show_sidebar()
        
        # Show current view
        if st.session_state.current_view == 'search':
            show_search_view()
        elif st.session_state.current_view == 'trees':
            show_trees_view()
        elif st.session_state.current_view == 'person':
            show_person_view()
        elif st.session_state.current_view == 'profiles':
            show_profiles_view()
        elif st.session_state.current_view == 'data':
            show_data_view()
        elif st.session_state.current_view == 'settings':
            show_settings_view()

if __name__ == "__main__":
    main()
