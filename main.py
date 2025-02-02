import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import logging
from src.helper import TextToSQLConverter
from src.database import DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def configure_page():
    st.set_page_config(
        layout="wide",
        page_title="SQL Magic ✨",
        page_icon="🪄",
        initial_sidebar_state="expanded"
    )
    
    st.markdown("""
        <style>
        .stApp {
            background: linear-gradient(135deg, #f5f7fa 0%, #f8f9fa 100%);
        }
        
        .main-header {
            font-size: 3rem;
            font-weight: 700;
            text-align: center;
            color: #1e293b;
            margin-bottom: 1rem;
            padding: 1rem 0;
        }
        
        .example-query-btn {
            background-color: #ffffff;
            border: 1px solid #e2e8f0;
            border-radius: 8px;
            padding: 0.75rem 1rem;
            margin: 0.5rem 0;
            text-align: left;
            transition: all 0.2s ease;
            cursor: pointer;
            width: 100%;
        }
        
        .example-query-btn:hover {
            background-color: #f8fafc;
            border-color: #94a3b8;
            transform: translateY(-2px);
        }
        
        .query-area {
            background: white;
            border: 1px solid #e2e8f0;
            border-radius: 10px;
            padding: 1rem;
            margin: 1rem 0;
        }
        
        .results-container {
            background: white;
            border-radius: 10px;
            padding: 1.5rem;
            margin: 1rem 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }
        
        .metric-card {
            background: white;
            border-radius: 10px;
            padding: 1rem;
            text-align: center;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            border: 1px solid #e2e8f0;
        }
        
        .sql-code {
            background: #1e293b;
            color: #e2e8f0;
            border-radius: 8px;
            padding: 1rem;
            font-family: 'Courier New', monospace;
        }
        </style>
    """, unsafe_allow_html=True)

def execute_query(query_text: str, converter: TextToSQLConverter, db_manager: DatabaseManager):
    try:
        with st.spinner("🪄 Converting to SQL..."):
            sql_query = converter.convert_to_sql(query_text)
            
        st.markdown("### 📝 Generated SQL")
        st.code(sql_query, language="sql")
        
        with st.spinner("⚡ Executing query..."):
            results = db_manager.execute_query(sql_query)
            
        st.markdown("### 📊 Results")
        st.dataframe(
            results.style.background_gradient(
                cmap='Blues',
                subset=results.select_dtypes(include=['float64', 'int64']).columns
            ),
            use_container_width=True
        )
        
        # Create visualization
        fig = create_visualization(results)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
            
        # Save to history
        if 'query_history' not in st.session_state:
            st.session_state.query_history = []
        st.session_state.query_history.append({
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'query': query_text,
            'sql': sql_query,
            'success': True
        })
        
    except Exception as e:
        st.error(f"🚫 Error: {str(e)}")
        if 'query_history' not in st.session_state:
            st.session_state.query_history = []
        st.session_state.query_history.append({
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'query': query_text,
            'error': str(e),
            'success': False
        })

def create_visualization(df: pd.DataFrame) -> go.Figure:
    if len(df) == 0:
        return None
        
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    if len(numeric_cols) > 0:
        if len(df) > 10:  # Line chart for time series or large datasets
            fig = px.line(
                df,
                y=numeric_cols,
                markers=True,
                template="plotly_white",
                title="📊 Data Visualization"
            )
        else:  # Bar chart for smaller datasets
            fig = px.bar(
                df,
                y=numeric_cols,
                template="plotly_white",
                title="📊 Data Visualization",
                barmode='group'
            )
        
        fig.update_layout(
            height=500,
            hovermode="x unified",
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="right",
                x=0.99,
                bgcolor="rgba(255, 255, 255, 0.8)"
            )
        )
        return fig
    return None

def main():
    configure_page()
    
    st.markdown('<h1 class="main-header">✨ SQL Magic Assistant</h1>', unsafe_allow_html=True)
    
    # Initialize components
    db_manager = DatabaseManager()
    converter = TextToSQLConverter(db_manager.get_schema())
    
    # Dashboard layout
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        st.markdown("### 🔍 Ask Your Question")
        query_input = st.text_area(
            "",
            placeholder="Example: Show me the top 5 highest paid employees in Engineering",
            key="query_input",
            height=100,
            help="💡 Type your question naturally - I'll convert it to SQL!"
        )
        
        if st.button("🚀 Generate & Execute Query", type="primary", use_container_width=True):
            if query_input:
                execute_query(query_input, converter, db_manager)
            else:
                st.warning("🤔 Please enter a question first!")
    
    with col2:
        st.markdown("### ⭐ Example Queries")
        example_queries = [
            ("Show all employees in Marketing department", "List all employees working in Marketing"),
            ("Calculate average salary by department", "What's the average salary in each department?"),
            ("List active projects with budgets over 100000", "Show me all active projects with budgets exceeding $100,000"),
            ("Find top 5 highest paid employees", "Who are our top 5 highest-paid employees?"),
            ("Show departments and their total project budgets", "List all departments with their total project budgets"),
            ("List projects starting this month", "Which projects are starting this month?"),
            ("Show department heads and their team sizes", "List all department heads and how many employees they manage")
        ]
        
        for query_text, description in example_queries:
            if st.button(f"💡 {description}", key=f"example_{hash(query_text)}", use_container_width=True):
                execute_query(query_text, converter, db_manager)
    
    with col3:
        st.markdown("### 📜 Query History")
        if 'query_history' in st.session_state and st.session_state.query_history:
            for item in reversed(st.session_state.query_history[-5:]):
                with st.expander(f"🕒 {item['timestamp']}", expanded=False):
                    st.write("Question:", item['query'])
                    if item['success']:
                        st.code(item['sql'], language="sql")
                    else:
                        st.error(f"Error: {item['error']}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"🚨 Application Error: {str(e)}")
        logger.error(f"Application error: {str(e)}", exc_info=True)