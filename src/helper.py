import spacy
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import re
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any
from collections import defaultdict

logger = logging.getLogger(__name__)

class TextToSQLConverter:
    def __init__(self, db_schema: Optional[str] = None):
        logger.info("Initializing Enhanced TextToSQLConverter")
        self.nlp = spacy.load("en_core_web_sm")
        self.db_schema = db_schema
        
        # Enhanced table relationships with clear join paths
        self.table_relationships = {
            'employees': {
                'departments': {'type': 'many_to_one', 'keys': ['department', 'name']},
                'projects': {'type': 'many_to_many', 'through': 'departments', 'keys': ['department', 'name']}
            },
            'departments': {
                'employees': {'type': 'one_to_many', 'keys': ['name', 'department']},
                'projects': {'type': 'one_to_many', 'keys': ['department_id', 'department_id']}
            },
            'projects': {
                'departments': {'type': 'many_to_one', 'keys': ['department_id', 'department_id']},
                'employees': {'type': 'many_to_many', 'through': 'departments', 'keys': ['department_id', 'department_id']}
            }
        }
        
        # Enhanced table columns with detailed metadata
        self.table_columns = {
            'employees': {
                'employee_id': {'type': 'INTEGER', 'primary_key': True},
                'name': {'type': 'TEXT', 'nullable': False},
                'department': {'type': 'TEXT', 'nullable': False},
                'salary': {'type': 'REAL', 'nullable': False},
                'hire_date': {'type': 'DATE', 'nullable': False},
                'role': {'type': 'TEXT', 'nullable': True},
                'email': {'type': 'TEXT', 'unique': True}
            },
            'departments': {
                'department_id': {'type': 'INTEGER', 'primary_key': True},
                'name': {'type': 'TEXT', 'nullable': False, 'unique': True},
                'budget': {'type': 'REAL', 'nullable': False},
                'location': {'type': 'TEXT', 'nullable': True},
                'head_id': {'type': 'INTEGER', 'foreign_key': 'employees.employee_id'}
            },
            'projects': {
                'project_id': {'type': 'INTEGER', 'primary_key': True},
                'name': {'type': 'TEXT', 'nullable': False},
                'department_id': {'type': 'INTEGER', 'foreign_key': 'departments.department_id'},
                'start_date': {'type': 'DATE', 'nullable': False},
                'end_date': {'type': 'DATE', 'nullable': True},
                'budget': {'type': 'REAL', 'nullable': False},
                'status': {'type': 'TEXT', 'check': ['Planning', 'Active', 'Completed', 'On Hold']}
            }
        }
        
        # Common natural language patterns
        self.patterns = {
            'select': {
                'keywords': ['show', 'list', 'display', 'get', 'find', 'what', 'which', 'who'],
                'regex': r'(?:show|list|display|get|find|what|which|who)\s+(?:are|is|the)?'
            },
            'aggregate': {
                'avg': ['average', 'mean', 'typical'],
                'sum': ['total', 'sum', 'combined'],
                'count': ['count', 'number of', 'how many'],
                'max': ['highest', 'maximum', 'most', 'top'],
                'min': ['lowest', 'minimum', 'least', 'bottom']
            },
            'conditions': {
                'equals': ['is', 'equals', 'equal to', '='],
                'greater': ['greater than', 'more than', 'over', 'above', '>'],
                'less': ['less than', 'under', 'below', '<'],
                'between': ['between', 'from', 'range'],
                'like': ['like', 'contains', 'similar to']
            },
            'order': {
                'asc': ['ascending', 'increasing', 'smallest first'],
                'desc': ['descending', 'decreasing', 'largest first', 'highest first']
            }
        }

    def convert_to_sql(self, text: str) -> str:
        """Convert natural language query to SQL."""
        try:
            # Parse query intent and components
            query_components = self._parse_query(text.lower())
            
            # Build SQL query
            sql = self._build_sql_query(query_components)
            
            logger.info(f"Generated SQL query: {sql}")
            return sql
            
        except Exception as e:
            logger.error(f"Error converting text to SQL: {str(e)}")
            raise ValueError(f"Failed to convert text to SQL: {str(e)}")

    def _parse_query(self, text: str) -> Dict[str, Any]:
        """Parse natural language query into structured components."""
        doc = self.nlp(text)
        
        components = {
            'select': {'tables': set(), 'columns': set()},
            'joins': [],
            'where': [],
            'group_by': [],
            'having': [],
            'order_by': [],
            'limit': None,
            'aggregates': []
        }
        
        # Extract main entities (tables and columns)
        self._extract_entities(doc, components)
        
        # Extract conditions
        self._extract_conditions(doc, components)
        
        # Extract aggregations and grouping
        self._extract_aggregations(doc, components)
        
        # Extract ordering
        self._extract_ordering(doc, components)
        
        # Extract limits
        self._extract_limits(doc, components)
        
        return components

    def _extract_entities(self, doc, components):
        """Extract table and column references from the query."""
        # Find table references
        for token in doc:
            # Check for table names
            for table in self.table_columns.keys():
                if table.lower() in token.text.lower() or token.text.lower() in table.lower():
                    components['select']['tables'].add(table)
                    
            # Check for column references
            for table in self.table_columns:
                for column in self.table_columns[table]:
                    if column.lower() in token.text.lower():
                        components['select']['columns'].add(f"{table}.{column}")
                        components['select']['tables'].add(table)
        
        # If no specific columns are mentioned, add all columns from mentioned tables
        if not components['select']['columns']:
            for table in components['select']['tables']:
                for column in self.table_columns[table]:
                    components['select']['columns'].add(f"{table}.{column}")

    def _extract_conditions(self, doc, components):
        """Extract WHERE conditions from the query."""
        for token in doc:
            if token.dep_ in ['prep', 'prep_in'] and token.text in ['in', 'with', 'where']:
                condition = self._parse_condition_phrase(token)
                if condition:
                    components['where'].append(condition)

    def _parse_condition_phrase(self, token) -> Optional[Dict[str, str]]:
        """Parse a condition phrase into a structured condition."""
        condition = {'column': None, 'operator': None, 'value': None}
        
        # Get the full phrase following the condition token
        phrase = ' '.join([t.text for t in token.rights])
        
        # Try to match conditions patterns
        for op_type, patterns in self.patterns['conditions'].items():
            for pattern in patterns:
                if pattern in phrase:
                    condition['operator'] = op_type
                    parts = phrase.split(pattern)
                    if len(parts) == 2:
                        condition['column'] = parts[0].strip()
                        condition['value'] = parts[1].strip()
                        return condition
        
        return None

    def _extract_aggregations(self, doc, components):
        """Extract aggregation functions and GROUP BY clauses."""
        text = doc.text.lower()
        
        # Check for aggregation functions
        for agg_type, patterns in self.patterns['aggregate'].items():
            for pattern in patterns:
                if pattern in text:
                    # Find the column being aggregated
                    for table in self.table_columns:
                        for column in self.table_columns[table]:
                            if column.lower() in text:
                                components['aggregates'].append({
                                    'function': agg_type.upper(),
                                    'column': f"{table}.{column}"
                                })
        
        # Check for GROUP BY
        group_matches = re.findall(r'by\s+(\w+)', text)
        for match in group_matches:
            for table in self.table_columns:
                if match in self.table_columns[table]:
                    components['group_by'].append(f"{table}.{match}")

    def _extract_ordering(self, doc, components):
        """Extract ORDER BY clauses."""
        text = doc.text.lower()
        
        # Check for ordering indicators
        order_match = re.search(r'(?:order|sort)(?:ed)?\s+by\s+(\w+)\s*(desc(?:ending)?|asc(?:ending)?)?', text)
        if order_match:
            column = order_match.group(1)
            direction = 'DESC' if order_match.group(2) and 'desc' in order_match.group(2) else 'ASC'
            
            # Find the table for this column
            for table in self.table_columns:
                if column in self.table_columns[table]:
                    components['order_by'].append({
                        'column': f"{table}.{column}",
                        'direction': direction
                    })

    def _extract_limits(self, doc, components):
        """Extract LIMIT clause."""
        text = doc.text.lower()
        
        # Look for numeric limits
        limit_match = re.search(r'(?:top|first|limit)\s+(\d+)', text)
        if limit_match:
            components['limit'] = int(limit_match.group(1))

    def _build_sql_query(self, components: Dict[str, Any]) -> str:
        """Build the final SQL query from the parsed components."""
        # Start with SELECT clause
        select_items = []
        if components['aggregates']:
            for agg in components['aggregates']:
                select_items.append(f"{agg['function']}({agg['column']}) as {agg['function'].lower()}_{agg['column'].split('.')[-1]}")
        else:
            select_items.extend(components['select']['columns'])
        
        query = f"SELECT {', '.join(select_items)}\n"
        
        # Add FROM clause with joins
        tables = list(components['select']['tables'])
        query += f"FROM {tables[0]}\n"
        
        # Add any necessary joins
        if len(tables) > 1:
            for i in range(1, len(tables)):
                join_info = self.table_relationships[tables[0]][tables[i]]
                query += f"JOIN {tables[i]} ON {tables[0]}.{join_info['keys'][0]} = {tables[i]}.{join_info['keys'][1]}\n"
        
        # Add WHERE clause
        if components['where']:
            conditions = []
            for condition in components['where']:
                if condition['operator'] == 'equals':
                    conditions.append(f"{condition['column']} = '{condition['value']}'")
                elif condition['operator'] == 'greater':
                    conditions.append(f"{condition['column']} > {condition['value']}")
                elif condition['operator'] == 'less':
                    conditions.append(f"{condition['column']} < {condition['value']}")
                elif condition['operator'] == 'between':
                    values = condition['value'].split('and')
                    conditions.append(f"{condition['column']} BETWEEN {values[0].strip()} AND {values[1].strip()}")
                elif condition['operator'] == 'like':
                    conditions.append(f"{condition['column']} LIKE '%{condition['value']}%'")
            
            if conditions:
                query += f"WHERE {' AND '.join(conditions)}\n"
        
        # Add GROUP BY clause
        if components['group_by']:
            query += f"GROUP BY {', '.join(components['group_by'])}\n"
        
        # Add HAVING clause
        if components['having']:
            query += f"HAVING {' AND '.join(components['having'])}\n"
        
        # Add ORDER BY clause
        if components['order_by']:
            order_clauses = [f"{item['column']} {item['direction']}" for item in components['order_by']]
            query += f"ORDER BY {', '.join(order_clauses)}\n"
        
        # Add LIMIT clause
        if components['limit']:
            query += f"LIMIT {components['limit']}"
        
        return query.strip()