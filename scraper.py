"""
Web scraper for wolyn-metryki.pl genealogy data.
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
import time
import logging
from urllib.parse import quote, urlencode

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WolynScraper:
    """
    Scraper for wolyn-metryki.pl genealogy data.
    """
    BASE_URL = "https://wolyn-metryki.pl/Wolyn/index.php"
    
    def __init__(self, timeout=10, delay=1):
        """
        Initialize the scraper.
        
        Args:
            timeout (int): Request timeout in seconds
            delay (int): Delay between requests in seconds to avoid overloading the server
        """
        self.timeout = timeout
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7',
        })
    
    def search(self, first_name=None, last_name=None, location=None, parish=None, 
              start_year=None, end_year=None):
        """
        Search for genealogy records.
        
        Args:
            first_name (str): First name to search for
            last_name (str): Last name to search for
            location (str): Location name to search for
            parish (str): Parish name to search for
            start_year (int): Start year for search range
            end_year (int): End year for search range
            
        Returns:
            dict: Dictionary containing scraped data tables
        """
        # Build query parameters
        params = {
            'imie_szuk': first_name or '',
            'nazw_szuk': last_name or '',
            'miej_szuk': location or '',
            'para_szuk': parish or '',
            'rok_start_szuk': start_year or '',
            'rok_koniec_szuk': end_year or ''
        }
        
        # Encode properly for Polish characters
        encoded_params = {}
        for key, value in params.items():
            if value:
                # Handle Polish encoding properly
                encoded_params[key] = value
        
        url = f"{self.BASE_URL}?{urlencode(encoded_params)}"
        logger.info(f"Searching with URL: {url}")
        
        # Make request
        response = self.session.get(url, timeout=self.timeout)
        if response.status_code != 200:
            logger.error(f"Error: Status code {response.status_code}")
            return None
        
        # Ensure proper encoding
        response.encoding = 'ISO-8859-2'
        html_content = response.text
        
        # Parse response
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Check if we have results
        results_text = soup.find(text=re.compile(r'Znaleziono \d+ wynik'))
        if not results_text:
            logger.warning("No results found")
            return {'births': [], 'deaths': [], 'marriages': [], 'census': []}
        
        # Extract number of results
        match = re.search(r'Znaleziono (\d+) wynik', results_text)
        if match:
            num_results = int(match.group(1))
            logger.info(f"Found {num_results} results")
        
        # Parse tables - there should be up to 4 tables:
        # 1. Deaths (Zgony)
        # 2. Births (Urodzenia)
        # 3. Marriages (Śluby)
        # 4. Census (Spisy)
        
        # Find all tables
        tables = soup.find_all('table')
        
        # Initialize result containers
        births = []
        deaths = []
        marriages = []
        census = []
        
        # Process each table
        for table in tables:
            # Check table header to determine type
            headers = [th.get_text().strip() for th in table.find_all('th')]
            
            if not headers:
                continue
                
            # Try to determine table type by headers
            table_type = self._identify_table_type(headers)
            
            # Process rows based on table type
            rows = table.find_all('tr')[1:]  # Skip header row
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 3:  # Skip rows with too few cells
                    continue
                    
                # Extract cell text and URLs
                cell_data = []
                for cell in cells:
                    # Get text
                    text = cell.get_text().strip()
                    
                    # Check for links
                    links = cell.find_all('a')
                    url = None
                    if links:
                        url = links[0].get('href')
                    
                    cell_data.append({
                        'text': text,
                        'url': url
                    })
                
                # Process based on table type
                if table_type == 'deaths':
                    deaths.append(self._process_death_row(cell_data, row))
                elif table_type == 'births':
                    births.append(self._process_birth_row(cell_data, row))
                elif table_type == 'marriages':
                    marriages.append(self._process_marriage_row(cell_data, row))
                elif table_type == 'census':
                    census.append(self._process_census_row(cell_data, row))
        
        # Return all collected data
        return {
            'births': births,
            'deaths': deaths,
            'marriages': marriages,
            'census': census
        }
    
    def _identify_table_type(self, headers):
        """
        Identify table type based on headers.
        
        Args:
            headers (list): List of table headers
            
        Returns:
            str: Table type ('births', 'deaths', 'marriages', 'census')
        """
        # Common headers for each table type
        death_headers = ['Dzień', 'Miesiąc', 'Rok', 'Parafia', 'Imię', 'Nazwisko', 'Lat']
        birth_headers = ['Dzień', 'Miesiąc', 'Rok', 'Parafia', 'Imiona', 'Nazwisko', 'Miejscowość', 'Imię Ojca', 'Imię Matki']
        marriage_headers = ['Dzień', 'Miesiąc', 'Rok', 'Parafia', 'Imię p. Młodego', 'Nazwisko p. Młodego']
        census_headers = ['Nr Gosp', 'Nr M', 'Nr K', 'Personalia', 'Wiek M', 'Wiek K']
        
        # Check if headers match any of the patterns
        if self._check_headers_match(headers, death_headers):
            return 'deaths'
        elif self._check_headers_match(headers, birth_headers):
            return 'births'
        elif self._check_headers_match(headers, marriage_headers):
            return 'marriages'
        elif self._check_headers_match(headers, census_headers):
            return 'census'
        
        # Default if unknown
        return 'unknown'
    
    def _check_headers_match(self, headers, pattern):
        """
        Check if headers match a pattern.
        
        Args:
            headers (list): List of table headers
            pattern (list): List of expected headers
            
        Returns:
            bool: True if headers match pattern
        """
        # Need at least the first few headers to match
        min_match_count = min(len(pattern), len(headers))
        headers_simple = [h.lower().replace('ą', 'a').replace('ę', 'e')
                         .replace('ó', 'o').replace('ś', 's')
                         .replace('ł', 'l').replace('ż', 'z')
                         .replace('ź', 'z').replace('ć', 'c')
                         .replace('ń', 'n') for h in headers[:min_match_count]]
        pattern_simple = [p.lower().replace('ą', 'a').replace('ę', 'e')
                         .replace('ó', 'o').replace('ś', 's')
                         .replace('ł', 'l').replace('ż', 'z')
                         .replace('ź', 'z').replace('ć', 'c')
                         .replace('ń', 'n') for p in pattern[:min_match_count]]
        
        # Count matches
        matches = sum(1 for h, p in zip(headers_simple, pattern_simple) if h in p or p in h)
        
        # Consider it a match if at least half of the headers match
        return matches >= min_match_count // 2
    
    def _process_death_row(self, cell_data, row_html):
        """
        Process a death record row.
        
        Args:
            cell_data (list): List of cell data dictionaries
            row_html (bs4.element.Tag): Original row HTML
            
        Returns:
            dict: Parsed death record
        """
        # Extract data from cells
        death = {
            'day': int(cell_data[0]['text']) if cell_data[0]['text'].isdigit() else None,
            'month': int(cell_data[1]['text']) if cell_data[1]['text'].isdigit() else None,
            'year': int(cell_data[2]['text']) if cell_data[2]['text'].isdigit() else None,
            'parish': cell_data[3]['text'],
            'first_name': cell_data[4]['text'],
            'last_name': cell_data[5]['text'],
            'age': int(cell_data[6]['text']) if cell_data[6]['text'].isdigit() else None,
            'location': cell_data[7]['text'] if len(cell_data) > 7 else None,
            'about_deceased_and_family': cell_data[8]['text'] if len(cell_data) > 8 else None,
            'signature': cell_data[9]['text'] if len(cell_data) > 9 else None,
            'page': cell_data[10]['text'] if len(cell_data) > 10 else None,
            'position': cell_data[11]['text'] if len(cell_data) > 11 else None,
            'archive': cell_data[12]['text'] if len(cell_data) > 12 else None,
            'scan_number': cell_data[13]['text'] if len(cell_data) > 13 else None,
            'index_author': cell_data[14]['text'] if len(cell_data) > 14 else None,
            'scan_url': cell_data[15]['url'] if len(cell_data) > 15 and cell_data[15]['url'] else None,
            'raw_html': str(row_html)
        }
        return death
    
    def _process_birth_row(self, cell_data, row_html):
        """
        Process a birth record row.
        
        Args:
            cell_data (list): List of cell data dictionaries
            row_html (bs4.element.Tag): Original row HTML
            
        Returns:
            dict: Parsed birth record
        """
        # Extract data from cells
        birth = {
            'day': int(cell_data[0]['text']) if cell_data[0]['text'].isdigit() else None,
            'month': int(cell_data[1]['text']) if cell_data[1]['text'].isdigit() else None,
            'year': int(cell_data[2]['text']) if cell_data[2]['text'].isdigit() else None,
            'parish': cell_data[3]['text'],
            'first_name': cell_data[4]['text'],
            'last_name': cell_data[5]['text'],
            'location': cell_data[6]['text'] if len(cell_data) > 6 else None,
            'father_first_name': cell_data[7]['text'] if len(cell_data) > 7 else None,
            'mother_first_name': cell_data[8]['text'] if len(cell_data) > 8 else None,
            'mother_maiden_name': cell_data[9]['text'] if len(cell_data) > 9 else None,
            'godparents_notes': cell_data[10]['text'] if len(cell_data) > 10 else None,
            'signature': cell_data[11]['text'] if len(cell_data) > 11 else None,
            'page': cell_data[12]['text'] if len(cell_data) > 12 else None,
            'position': cell_data[13]['text'] if len(cell_data) > 13 else None,
            'archive': cell_data[14]['text'] if len(cell_data) > 14 else None,
            'scan_number': cell_data[15]['text'] if len(cell_data) > 15 else None,
            'index_author': cell_data[16]['text'] if len(cell_data) > 16 else None,
            'scan_url': cell_data[17]['url'] if len(cell_data) > 17 and cell_data[17]['url'] else None,
            'raw_html': str(row_html)
        }
        return birth
    
    def _process_marriage_row(self, cell_data, row_html):
        """
        Process a marriage record row.
        
        Args:
            cell_data (list): List of cell data dictionaries
            row_html (bs4.element.Tag): Original row HTML
            
        Returns:
            dict: Parsed marriage record
        """
        # Extract data from cells
        marriage = {
            'day': int(cell_data[0]['text']) if cell_data[0]['text'].isdigit() else None,
            'month': int(cell_data[1]['text']) if cell_data[1]['text'].isdigit() else None,
            'year': int(cell_data[2]['text']) if cell_data[2]['text'].isdigit() else None,
            'parish': cell_data[3]['text'],
            'groom_first_name': cell_data[4]['text'],
            'groom_last_name': cell_data[5]['text'],
            'groom_location': cell_data[6]['text'] if len(cell_data) > 6 else None,
            'groom_age': cell_data[7]['text'].replace('w', '') if len(cell_data) > 7 and cell_data[7]['text'] else None,
            'groom_father_first_name': cell_data[8]['text'] if len(cell_data) > 8 else None,
            'groom_mother_first_name': cell_data[9]['text'] if len(cell_data) > 9 else None,
            'groom_mother_maiden_name': cell_data[10]['text'] if len(cell_data) > 10 else None,
            'bride_first_name': cell_data[11]['text'] if len(cell_data) > 11 else None,
            'bride_last_name': cell_data[12]['text'] if len(cell_data) > 12 else None,
            'bride_location': cell_data[13]['text'] if len(cell_data) > 13 else None,
            'bride_age': cell_data[14]['text'].replace('w', '') if len(cell_data) > 14 and cell_data[14]['text'] else None,
            'bride_father_first_name': cell_data[15]['text'] if len(cell_data) > 15 else None,
            'bride_mother_first_name': cell_data[16]['text'] if len(cell_data) > 16 else None,
            'bride_mother_maiden_name': cell_data[17]['text'] if len(cell_data) > 17 else None,
            'witnesses_notes': cell_data[18]['text'] if len(cell_data) > 18 else None,
            'signature': cell_data[19]['text'] if len(cell_data) > 19 else None,
            'page': cell_data[20]['text'] if len(cell_data) > 20 else None,
            'position': cell_data[21]['text'] if len(cell_data) > 21 else None,
            'archive': cell_data[22]['text'] if len(cell_data) > 22 else None,
            'scan_number': cell_data[23]['text'] if len(cell_data) > 23 else None,
            'index_author': cell_data[24]['text'] if len(cell_data) > 24 else None,
            'scan_url': cell_data[25]['url'] if len(cell_data) > 25 and cell_data[25]['url'] else None,
            'raw_html': str(row_html)
        }
        
        # Convert ages to integers if possible
        try:
            if marriage['groom_age'] and marriage['groom_age'].isdigit():
                marriage['groom_age'] = int(marriage['groom_age'])
            else:
                marriage['groom_age'] = None
        except:
            marriage['groom_age'] = None
            
        try:
            if marriage['bride_age'] and marriage['bride_age'].isdigit():
                marriage['bride_age'] = int(marriage['bride_age'])
            else:
                marriage['bride_age'] = None
        except:
            marriage['bride_age'] = None
            
        return marriage
    
    def _process_census_row(self, cell_data, row_html):
        """
        Process a census record row.
        
        Args:
            cell_data (list): List of cell data dictionaries
            row_html (bs4.element.Tag): Original row HTML
            
        Returns:
            dict: Parsed census record
        """
        # Extract data from cells
        census = {
            'household_number': cell_data[0]['text'],
            'male_number': cell_data[1]['text'] if len(cell_data) > 1 else None,
            'female_number': cell_data[2]['text'] if len(cell_data) > 2 else None,
            'full_name': cell_data[3]['text'] if len(cell_data) > 3 else None,
            'male_age': int(cell_data[4]['text']) if len(cell_data) > 4 and cell_data[4]['text'].isdigit() else None,
            'female_age': int(cell_data[5]['text']) if len(cell_data) > 5 and cell_data[5]['text'].isdigit() else None,
            'parish': cell_data[6]['text'] if len(cell_data) > 6 else None,
            'location': cell_data[7]['text'] if len(cell_data) > 7 else None,
            'year': int(cell_data[8]['text']) if len(cell_data) > 8 and cell_data[8]['text'].isdigit() else None,
            'archive': cell_data[9]['text'] if len(cell_data) > 9 else None,
            'index_author': cell_data[10]['text'] if len(cell_data) > 10 else None,
            'signature': cell_data[11]['text'] if len(cell_data) > 11 else None,
            'page': cell_data[12]['text'] if len(cell_data) > 12 else None,
            'scan_number': cell_data[13]['text'] if len(cell_data) > 13 else None,
            'notes': cell_data[14]['text'] if len(cell_data) > 14 else None,
            'raw_html': str(row_html)
        }
        return census
