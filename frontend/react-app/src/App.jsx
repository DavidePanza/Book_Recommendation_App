import React, { useState } from 'react';
import RecommendationSlot from './components/recommendation_slot.jsx';
const API_BASE_URL = import.meta.env.VITE_API_URL || window.location.origin;

const BookRecommenderApp = () => {
  const [user_query, setUserQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [recommendations, setRecommendations] = useState([]);
  const [projectInfo_isexpanded, setProjectInfoExpanded] = useState(false);

  const fetchData = async () => {
    setLoading(true);
    setError('');

      try {
        const response = await fetch(`${API_BASE_URL}/api/search`, {
          method: 'POST',
          headers: { 
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ 
            query: user_query,
            limit: 5 
          })
        });

      if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
      
      const data = await response.json();
      setRecommendations(data.results || []); 
      
    } catch (err) {
      setError(`Failed to fetch recommendations: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  // Centralized styles object
  const styles = {
    container: { 
      minHeight: '100vh', 
      backgroundColor: '#f8f9fa', 
      padding: '20px', 
      fontFamily: 'Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif'
    },
    card: { 
      backgroundColor: 'white', 
      padding: '24px', 
      borderRadius: '12px', 
      boxShadow: '0 4px 6px rgba(0,0,0,0.1)', 
      marginBottom: '24px', 
      maxWidth: '1000px', 
      margin: '0 auto 24px auto',
      border: '1px solid #e9ecef'
    },
    overview: {
      textAlign: 'center',
      color: '#2c3e50',
      backgroundColor: 'transparent',
      border: 'none',
      outline: 'none',
      resize: 'none',
      fontSize: '18px',
      fontWeight: '400',
      lineHeight: '1.5',
      marginBottom: '20px',
      width: '100%',
      maxWidth: '800px',
      margin: '0 auto 40px auto',
      display: 'block',
      fontFamily: 'inherit',
      cursor: 'default'
    },
    // Updated button container styles
    buttonContainer: {
      display: 'flex',
      justifyContent: 'center',
      gap: '40px',
      maxWidth: '800px',
      margin: '0 auto',
      flexWrap: 'wrap' // Allow wrapping on smaller screens
    },
    buttonSection: {
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      minWidth: '160px'
    },
    // New style for buttons in the horizontal layout
    horizontalButton: {
      padding: '10px 16px',
      border: '2px solid #e9ecef',
      borderRadius: '8px',
      fontSize: '14px',
      backgroundColor: 'white',
      cursor: 'pointer',
      transition: 'all 0.2s',
      outline: 'none',
      fontFamily: 'Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif',
      whiteSpace: 'nowrap',
      minWidth: '140px'
    },
    infobutton: {
      padding: '10px 12px', 
      border: '2px solid #e9ecef', 
      borderRadius: '8px', 
      fontSize: '14px',
      width: '100%',
      maxWidth: '140px',
      margin: '0 auto 20px auto',
      display: 'block',
      transition: 'border-color 0.2s',
      outline: 'none',
      fontFamily: 'Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif'
    },
    detailsContent: {
      backgroundColor: '#f8f9fa',
      border: '1px solid #e9ecef',
      borderRadius: '8px',
      padding: '20px',
      marginTop: '15px',
      maxWidth: '400px', // Reduced width so it doesn't span too wide
      lineHeight: '1.6'
    },
    detailsText: {
      margin: '0 0 12px 0',
      color: '#2c3e50',
      fontSize: '14px'
    },
    input: { 
      padding: '12px 16px', 
      border: '2px solid #e9ecef', 
      borderRadius: '8px', 
      fontSize: '16px',
      width: '100%',
      maxWidth: '600px',
      margin: '0 auto 20px auto',
      display: 'block',
      transition: 'border-color 0.2s',
      outline: 'none',
      fontFamily: 'inherit'
    },
    button: {
      padding: '12px 24px',
      backgroundColor: '#3498db',
      color: 'white',
      border: 'none',
      borderRadius: '8px',
      fontSize: '16px',
      fontWeight: '600',
      cursor: 'pointer',
      transition: 'background-color 0.2s',
      display: 'block',
      margin: '0 auto 30px auto'
    },
    buttonDisabled: {
      backgroundColor: '#bdc3c7',
      cursor: 'not-allowed'
    },
    error: {
      color: '#e74c3c',
      backgroundColor: '#fdf2f2',
      padding: '12px',
      borderRadius: '8px',
      margin: '0 auto 20px auto',
      maxWidth: '600px',
      textAlign: 'center',
      border: '1px solid #f5c6cb'
    },
    recommendationsHeader: {
      textAlign: 'center',
      color: '#2c3e50',
      marginBottom: '30px',
      fontSize: '1.5rem',
      fontWeight: '500'
    },
    // Styles for RecommendationSlot component
    recommendationSlot: {
      border: '1px solid #e9ecef',
      borderRadius: '12px',
      margin: '15px auto',
      padding: '24px',
      maxWidth: '1000px',
      minHeight: '200px',
      boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
      backgroundColor: 'white',
      transition: 'transform 0.2s, box-shadow 0.2s'
    },
    recommendationContent: {
      display: 'flex',
      alignItems: 'flex-start',
      gap: '20px'
    },
    bookImage: {
      width: '90px',
      height: '135px',
      objectFit: 'cover',
      borderRadius: '8px',
      boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
    },
    bookInfo: {
      flex: 1
    },
    bookTitle: {
      margin: '0 0 8px 0',
      color: '#2c3e50',
      fontSize: '1.2rem',
      fontWeight: '600',
      lineHeight: '1.3'
    },
    bookAuthor: {
      margin: '4px 0 12px 0',
      color: '#7f8c8d',
      fontSize: '16px'
    },
    bookDescription: {
      margin: '8px 0 15px 0',
      fontSize: '14px',
      lineHeight: '1.5',
      color: '#2c3e50'
    },
    expandButton: {
      border: 'none',
      background: 'none',
      color: '#3498db',
      cursor: 'pointer',
      marginLeft: '5px',
      fontSize: '14px',
      fontWeight: '500'
    },
    bookMeta: {
      display: 'flex',
      gap: '20px',
      fontSize: '13px',
      color: '#666',
      marginTop: '12px'
    }
  };

  return (
    <div style={styles.container}>
      <h1 style={{textAlign: 'center', color: '#2c3e50', marginBottom: '50px', fontSize: '2.5rem', fontWeight: '600'}}>
        Book Recommender App
      </h1>

      <textarea
        value="This App allows you to find books which fit your interests and preferences. 
        Just enter a description of the book you're looking for."
        readOnly
        rows={3}
        style={styles.overview}
      />

      <div style={styles.buttonContainer}>
        {/* Left side - existing button */}
        <div style={styles.buttonSection}>
          <button 
            onClick={() => setProjectInfoExpanded(!projectInfo_isexpanded)}
            style={styles.horizontalButton}
          >
            {projectInfo_isexpanded ? "Hide Details" : "Show Details"}
          </button>

          {projectInfo_isexpanded && (
            <div style={styles.detailsContent}>
              <p style={styles.detailsText}>
                <strong>How it works:</strong> 
                <br />
                Our book recommendation system uses advanced search algorithms to match your description with thousands of books in our database.
                <br />
                An embedding vector is calculated from your input using a pre-trained model (all-MiniLM-L6-v2 from Hugging Face), and similar books are retrieved based on this vector.
                <br />
                The books informations are retrieved from Google Books API and stored in a LanceDB vector database for efficient searching.
                <br />
                The 5 most similar books are then presented as recommendations.
              </p>
              <p style={styles.detailsText}>
                <strong>Tips for better results:</strong> 
                <br />
                Be specific about genres, themes, or elements you're looking for. For example: "mystery novel set in Victorian London" or "sci-fi with strong female protagonist."
              </p>
              <p style={{...styles.detailsText, margin: 0}}>
                <strong>Features:</strong> 
                <br />
                View book covers, read descriptions, see publication details, and discover your next favorite read!
              </p>
            </div>
          )}
        </div>

        {/* Right side - new dashboard button */}
        <div style={styles.buttonSection}>
          <button 
            onClick={() => window.open('https://book-recommendation-dashboard.onrender.com', '_blank')}
            style={styles.horizontalButton}
          >
            View Book Statistics
          </button>
        </div>
      </div>

      <br />
      <hr
        style={{
          border: "none",
          borderTop: "2px solid #ccc",
          margin: "20px 340px",
        }}
      />

      <br />
      <br />

      <textarea
        value={user_query}
        onChange={(e) => setUserQuery(e.target.value)}
        placeholder="Enter your book description here..."
        rows={4}
        style={styles.input}
      />

      <button 
        onClick={fetchData} 
        disabled={loading} 
        style={{
          ...styles.button,
          ...(loading ? styles.buttonDisabled : {})
        }}
      >
        {loading ? 'Searching...' : 'Search Books'}
      </button>

      {error && <div style={styles.error}>{error}</div>}

      {recommendations.length > 0 && (
        <div>
          <h3 style={styles.recommendationsHeader}>
            Recommendations ({recommendations.length})
          </h3>
          {recommendations.map((book, index) => (
            <RecommendationSlot 
              key={book.id} 
              book={book} 
              index={index}
              styles={styles}
            />
          ))}
        </div>
      )}
    </div>
  );
}

export default BookRecommenderApp;