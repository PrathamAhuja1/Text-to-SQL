import sqlite3
import pandas as pd
import os

class DatabaseManager:
    def __init__(self):
        self.db_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'company.db')
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.create_database()
    
    def create_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables with comprehensive schema
        cursor.executescript("""
            CREATE TABLE IF NOT EXISTS employees (
                employee_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                department TEXT NOT NULL,
                salary REAL NOT NULL,
                hire_date DATE NOT NULL,
                role TEXT,
                email TEXT UNIQUE,
                CONSTRAINT salary_check CHECK (salary > 0)
            );
            
            CREATE TABLE IF NOT EXISTS departments (
                department_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                budget REAL NOT NULL,
                location TEXT,
                head_id INTEGER,
                FOREIGN KEY (head_id) REFERENCES employees(employee_id)
            );
            
            CREATE TABLE IF NOT EXISTS projects (
                project_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                department_id INTEGER,
                start_date DATE NOT NULL,
                end_date DATE,
                budget REAL NOT NULL,
                status TEXT CHECK(status IN ('Planning', 'Active', 'Completed', 'On Hold')),
                FOREIGN KEY (department_id) REFERENCES departments(department_id)
            );
        """)
        
        # Insert comprehensive sample data
        cursor.executemany("INSERT OR IGNORE INTO employees VALUES (?, ?, ?, ?, ?, ?, ?)", [
            (1, 'John Smith', 'Engineering', 85000, '2024-01-15', 'Senior Engineer', 'john.smith@company.com'),
            (2, 'Emma Davis', 'Marketing', 75000, '2024-02-01', 'Marketing Manager', 'emma.davis@company.com'),
            (3, 'Michael Chen', 'Engineering', 90000, '2024-01-10', 'Lead Engineer', 'michael.chen@company.com'),
            (4, 'Sarah Johnson', 'Sales', 70000, '2024-03-01', 'Sales Representative', 'sarah.j@company.com'),
            (5, 'David Wilson', 'Engineering', 82000, '2024-02-15', 'Software Engineer', 'david.w@company.com')
        ])
        
        cursor.executemany("INSERT OR IGNORE INTO departments VALUES (?, ?, ?, ?, ?)", [
            (1, 'Engineering', 1000000, 'Building A', 3),
            (2, 'Marketing', 500000, 'Building B', 2),
            (3, 'Sales', 750000, 'Building C', 4)
        ])
        
        cursor.executemany("INSERT OR IGNORE INTO projects VALUES (?, ?, ?, ?, ?, ?, ?)", [
            (1, 'Website Redesign', 2, '2024-01-01', '2024-06-30', 100000, 'Active'),
            (2, 'Mobile App', 1, '2024-02-01', '2024-12-31', 200000, 'Active'),
            (3, 'CRM Integration', 3, '2024-03-01', '2024-09-30', 150000, 'Planning')
        ])
        
        conn.commit()
        conn.close()
    
    def execute_query(self, query):
        conn = sqlite3.connect(self.db_path)
        try:
            result = pd.read_sql_query(query, conn)
            return result
        finally:
            conn.close()
    
    def get_schema(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        schema = []
        for table in ['employees', 'departments', 'projects']:
            cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
            schema.append(cursor.fetchone()[0] + ";")
        
        conn.close()
        return "\n\n".join(schema)