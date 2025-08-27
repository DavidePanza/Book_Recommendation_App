import requests

def _setting_api_params(author, start_index=0, language='en', country='US', max_results=40):
    params = {
        'q': f'inauthor:{author}',
        'startIndex': start_index,
        'langRestrict': language,
        'country': country,
        'maxResults': max_results,
        'printType': 'books',
        'fields': '''totalItems,items(
                id,
                volumeInfo(
                    title,subtitle,authors,publisher,publishedDate,description,
                    pageCount,mainCategory,categories,averageRating,
                    ratingsCount,language,previewLink,
                    imageLinks(thumbnail)
                ),
                saleInfo(
                    country,saleability,
                    listPrice(amount,currencyCode),
                    buyLink
                )
                    )'''.replace('\n', '').replace(' ', ''),
                    #'key': API_KEY,
            }
    
    return params

def get_filtered_books(author, iterations=3):
    """
    Get a list of filtered book IDs for a specific author with enhanced deduplication.
    """
    # total items is unreliable, using an euristic of 3 iterations to gather more results per author
    iteration = 0
    filtered_books = []
    while iteration < iterations:
        print(f"Fetching books for {author}, iteration {iteration + 1}")
        url = f"https://www.googleapis.com/books/v1/volumes"
        params = _setting_api_params(author, start_index=iteration * 40)
        response = requests.get(url, params=params)
        books = response.json().get('items', [])
        
        # Get the current iteration length
        current_iteration_length = len(books)

        # Filter books
        books[:] = [book for book in books if 
                    book['volumeInfo'].get('authors', []) == [author]
                    and book['volumeInfo'].get('language', '') == 'en'
                    and book['volumeInfo'].get('title', '') != ''
                    and len(book['volumeInfo'].get('description', '')) > 100
                    and book['id']
                    ]
        
        filtered_books.extend(books)

        if current_iteration_length < 40:
            break
        iteration += 1

    return filtered_books