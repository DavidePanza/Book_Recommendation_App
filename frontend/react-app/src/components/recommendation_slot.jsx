// recommendation_slot.jsx
import React, { useState } from "react";

const RecommendationSlot = ({ book, index, styles }) => {
  const [expanded, setExpanded] = useState(false);
  
  return (
    <div style={styles.recommendationSlot}>
      <div style={styles.recommendationContent}>
        {book.thumbnail_url && (
          <img 
            src={book.thumbnail_url} 
            alt={book.title}
            style={styles.bookImage}
          />
        )}
        
        <div style={styles.bookInfo}>
          <h4 style={styles.bookTitle}>{book.title}</h4>
          <p style={styles.bookAuthor}>
            <strong>by {book.primary_author}</strong>
          </p>
          
          <p style={styles.bookDescription}>
            {expanded ? book.description : `${book.description?.substring(0, 150)}...`}
            {book.description?.length > 150 && (
              <button 
                onClick={() => setExpanded(!expanded)}
                style={styles.expandButton}
              >
                {expanded ? 'Show less' : 'Read more'}
              </button>
            )}
          </p>
          
          <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end'}}>
            <div style={styles.bookMeta}>
              <span>📅 {book.published_date}</span>
              {book.page_count && <span>📄 {book.page_count} pages</span>}
              {book.primary_category && <span>🏷️ {book.primary_category}</span>}
              {book.avg_rating && (
                <span>⭐ {book.avg_rating}{book.ratings_count && ` (${book.ratings_count} ratings)`}</span>
              )}
            </div>
            
            {book.preview_link && (
              <a 
                href={book.preview_link} 
                target="_blank" 
                rel="noopener noreferrer"
                style={{
                  color: '#3498db',
                  textDecoration: 'none',
                  fontSize: '14px',
                  fontWeight: '500',
                  padding: '6px 12px',
                  border: '1px solid #3498db',
                  borderRadius: '4px',
                  transition: 'all 0.2s'
                }}
                onMouseEnter={(e) => {
                  e.target.style.backgroundColor = '#3498db';
                  e.target.style.color = 'white';
                }}
                onMouseLeave={(e) => {
                  e.target.style.backgroundColor = 'transparent';
                  e.target.style.color = '#3498db';
                }}
              >
                📖 Preview
              </a>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default RecommendationSlot;