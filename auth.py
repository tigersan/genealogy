"""
Authentication functionality for Wolyn Genealogy Explorer
"""
import os
import sqlite3
import hashlib
import secrets
import streamlit as st
from database import User, db

def init_auth():
    """
    Initialize the authentication system.
    """
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.username = None

def login_form():
    """
    Display the login form.
    
    Returns:
        bool: True if login successful, False otherwise
    """
    st.title("Login")
    
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("Login"):
            if validate_credentials(username, password):
                st.session_state.authenticated = True
                st.session_state.username = username
                st.success("Login successful!")
                return True
            else:
                st.error("Invalid username or password")
    
    with col2:
        if st.button("Register"):
            st.session_state.show_register = True
    
    if st.session_state.get('show_register', False):
        register_form()
    
    return False

def register_form():
    """
    Display the registration form.
    """
    st.subheader("Register New Account")
    
    new_username = st.text_input("New Username")
    new_password = st.text_input("New Password", type="password")
    confirm_password = st.text_input("Confirm Password", type="password")
    
    if st.button("Create Account"):
        if new_password != confirm_password:
            st.error("Passwords do not match")
            return
        
        if register_user(new_username, new_password):
            st.success("Account created! You can now log in.")
            st.session_state.show_register = False
        else:
            st.error("Username already exists or other error occurred")

def validate_credentials(username, password):
    """
    Validate user credentials.
    
    Args:
        username (str): Username
        password (str): Password
        
    Returns:
        bool: True if credentials are valid, False otherwise
    """
    user = db.verify_user(username, password)
    return user is not None

def register_user(username, password):
    """
    Register a new user.
    
    Args:
        username (str): Username
        password (str): Password
        
    Returns:
        bool: True if registration successful, False otherwise
    """
    # Check if username already exists
    existing_user = db.session.query(User).filter_by(username=username).first()
    if existing_user:
        return False
    
    try:
        # Create new user
        db.add_user(username, password)
        return True
    except Exception as e:
        print(f"Error registering user: {e}")
        return False

def logout():
    """
    Log out the current user.
    """
    st.session_state.authenticated = False
    st.session_state.username = None
    # Clear any other session state if needed
    for key in list(st.session_state.keys()):
        if key not in ['authenticated', 'username']:
            del st.session_state[key]
