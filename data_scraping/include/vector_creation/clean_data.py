import pandas as pd
import re
import numpy as np


def clean_data(df):
    """Process and clean the book DataFrame"""
    df_filtered = df.copy()
    cols_to_keep = ['id', 'title', 'primary_author', 'description', 'publisher', 'published_date', 'page_count', 'primary_category', 'avg_rating', 'ratings_count', 'thumbnail_url', 'preview_link', 'list_price', 'buy_link']
    df_filtered = df_filtered[cols_to_keep]

    # Replace avg_rating with NaN where ratings_count is 0
    df_filtered['avg_rating'] = np.where(
        df_filtered['ratings_count'] == 0, np.nan,
        df_filtered['avg_rating']
    )

    # Columns to keep as-is
    exclude_cols = ['authors', 'id', 'description', 'title', 'avg_rating', 'ratings_count']

    # Convert page_count before the loop
    df_filtered['page_count'] = df_filtered['page_count'].astype('Int64')

    for col in df_filtered.columns:
        if col in exclude_cols:
            continue

        if pd.api.types.is_integer_dtype(df_filtered[col]):
            # Direct assignment instead of inplace=True
            df_filtered[col] = df_filtered[col].replace(0, pd.NA)
        elif pd.api.types.is_float_dtype(df_filtered[col]):
            df_filtered[col] = df_filtered[col].replace(0, np.nan)
        elif pd.api.types.is_string_dtype(df_filtered[col]) or pd.api.types.is_object_dtype(df_filtered[col]):
            df_filtered[col] = df_filtered[col].replace('', None)

    df_filtered['description'] = df_filtered['description'].apply(_clean_book_description)

    return df_filtered

def _clean_book_description(text):
    """Clean book description text"""
    if pd.isna(text) or text is None:
        return ""
    text = str(text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'http[s]?://\S+', '', text)
    text = re.sub(r'[^\w\s\.\,\!\?\'\"]', ' ', text)
    text = re.sub(r'["\']{2,}', '"', text)  # Multiple quotes to single
    text = re.sub(r'\s+([\.!?])', r'\1', text)  # Fix spacing before punctuation
    text = re.sub(r'\s+', ' ', text)
    return text.strip()
