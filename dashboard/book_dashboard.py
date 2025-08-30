import dash
from dash import dcc, html, Input, Output, dash_table
import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os

# Initialize Dash app
app = dash.Dash(__name__)
server = app.server  # Required for deployment

# AWS configuration - use environment variables for deployment
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION = 'us-east-1'

def query_athena(query):
    """Query Athena and return results as DataFrame"""
    try:
        athena = boto3.client(
            'athena',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        
        response = athena.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                'OutputLocation': 's3://googlebooks-scraping-data/athena-results/'
            },
            WorkGroup='primary'
        )
        
        query_execution_id = response['QueryExecutionId']
        
        # Wait for query to complete
        while True:
            response = athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED']:
                break
            elif status in ['FAILED', 'CANCELLED']:
                raise Exception(f"Query failed with status: {status}")
        
        # Get results
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        
        # Download results from S3
        result_key = f"athena-results/{query_execution_id}.csv"
        
        obj = s3.get_object(Bucket='googlebooks-scraping-data', Key=result_key)
        df = pd.read_csv(obj['Body'])
        
        return df
        
    except Exception as e:
        print(f"Error querying Athena: {e}")
        # Return sample data for development/testing
        return pd.DataFrame({
            'title': ['Sample Book 1', 'Sample Book 2'],
            'primary_author': ['Author A', 'Author B'],
            'publication_year': [2020, 2021],
            'avg_rating': [4.2, 3.8],
            'primary_category': ['Fiction', 'Non-fiction']
        })

# Load data (with error handling for development)
try:
    book_data = query_athena("""
        SELECT title, primary_author, publisher, publication_year, 
               avg_rating, ratings_count, primary_category, list_price, currency
        FROM books_db.stg_books_flattening 
        ORDER BY publication_year DESC
        LIMIT 500
    """)
except:
    # Fallback sample data for testing
    book_data = pd.DataFrame({
        'title': ['The Great Gatsby', 'To Kill a Mockingbird', 'Pride and Prejudice'],
        'primary_author': ['F. Scott Fitzgerald', 'Harper Lee', 'Jane Austen'],
        'publisher': ['Scribner', 'J.B. Lippincott & Co.', 'T. Egerton'],
        'publication_year': [1925, 1960, 1813],
        'avg_rating': [3.9, 4.3, 4.1],
        'primary_category': ['Fiction', 'Fiction', 'Romance'],
        'list_price': [12.99, 14.99, 9.99],
        'currency': ['USD', 'USD', 'USD']
    })

# Calculate summary metrics
total_books = len(book_data)
ratings_count = book_data['ratings_count'].sum()
avg_rating = book_data['avg_rating'].mean()
latest_year = book_data['publication_year'].max()

# Dashboard layout
app.layout = html.Div([
    # Header
    html.Div([
        html.H1("Book Analytics Dashboard", 
                style={'text-align': 'center', 'color': '#2c3e50', 'margin-bottom': '30px'})
    ]),
    
    # Summary cards
    html.Div([
        html.Div([
            html.H3(f"{total_books:,}", style={'margin': '0', 'color': '#3498db'}),
            html.P("Total Books", style={'margin': '5px 0'})
        ], className='summary-card'),
        
        html.Div([
            html.H3(f"{avg_rating:.1f}", style={'margin': '0', 'color': '#e74c3c'}),
            html.P("Average Rating", style={'margin': '5px 0'})
        ], className='summary-card'),
        
        html.Div([
            html.H3(f"{latest_year}", style={'margin': '0', 'color': '#27ae60'}),
            html.P("Latest Publication", style={'margin': '5px 0'})
        ], className='summary-card')
    ], style={'display': 'flex', 'justify-content': 'space-around', 'margin-bottom': '30px'}),
    
    # Charts section
    html.Div([
        # Publication year trend
        html.Div([
            dcc.Graph(
                figure=px.line(
                    book_data.groupby('publication_year').size().reset_index(name='count'),
                    x='publication_year', 
                    y='count',
                    title='Books by Publication Year',
                    labels={'count': 'Number of Books', 'publication_year': 'Year'}
                ).update_layout(height=400)
            )
        ], style={'width': '50%', 'display': 'inline-block'}),
        
        # Top publishers
        html.Div([
            dcc.Graph(
                figure=px.bar(
                    book_data['publisher'].value_counts().head(8).reset_index(),
                    x='count', 
                    y='publisher',
                    orientation='h',
                    title='Top Publishers',
                    labels={'count': 'Number of Books', 'publisher': 'Publisher'}
                ).update_layout(height=400)
            )
        ], style={'width': '50%', 'display': 'inline-block'})
    ]),
    
    html.Div([
        # Category distribution
        html.Div([
            dcc.Graph(
                figure=px.pie(
                    book_data['primary_category'].value_counts().reset_index(),
                    values='count',
                    names='primary_category',
                    title='Books by Category'
                ).update_layout(height=400)
            )
        ], style={'width': '50%', 'display': 'inline-block'}),
        
        # Rating distribution
        html.Div([
            dcc.Graph(
                figure=px.histogram(
                    book_data,
                    x='avg_rating',
                    nbins=10,
                    title='Rating Distribution',
                    labels={'avg_rating': 'Average Rating', 'count': 'Number of Books'}
                ).update_layout(height=400)
            )
        ], style={'width': '50%', 'display': 'inline-block'})
    ]),
    
    # Data table
    html.Div([
        html.H3("Book Details", style={'margin-top': '30px'}),
        dash_table.DataTable(
            data=book_data.head(10).to_dict('records'),
            columns=[
                {'name': 'Title', 'id': 'title'},
                {'name': 'Author', 'id': 'primary_author'},
                {'name': 'Publisher', 'id': 'publisher'},
                {'name': 'Year', 'id': 'publication_year'},
                {'name': 'Rating', 'id': 'avg_rating', 'type': 'numeric', 'format': {'specifier': '.1f'}},
                {'name': 'Ratings Count', 'id': 'ratings_count', 'type': 'numeric', 'format': {'specifier': 'd'}},
                {'name': 'Category', 'id': 'primary_category'}
            ],
            style_cell={'textAlign': 'left', 'padding': '10px'},
            style_header={'backgroundColor': '#3498db', 'color': 'white', 'fontWeight': 'bold'},
            style_data_conditional=[
                {
                    'if': {'filter_query': '{avg_rating} > 4'},
                    'backgroundColor': '#d5f4e6',
                }
            ]
        )
    ])
], style={'max-width': '1200px', 'margin': '0 auto', 'padding': '20px'})

# Add CSS styling
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            .summary-card {
                background: white;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                text-align: center;
                width: 200px;
            }
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: #f8f9fa;
                margin: 0;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

if __name__ == '__main__':
    app.run(debug=True)