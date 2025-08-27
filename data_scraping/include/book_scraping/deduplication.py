def _deduplicate_books_by_shortest_title(books, verbose=False):
    """
    Deduplicate books by their shortest title.
    """
    titles = [book['volumeInfo'].get('title', '').strip().lower() for book in books]

    to_remove = []
    for idx_outer, title in enumerate(titles):
        if idx_outer in to_remove:
            continue
        # this checks for duplicates and skips if none found
        temp_no_outer = titles[:idx_outer] + titles[idx_outer+1:]
        if any(title in temp_no_outer[idx_temp] for idx_temp in range(len(temp_no_outer))):
            if verbose:
                print(f'Duplicate found: {title}')
            for idx_inner in range(len(titles)):
                if idx_inner == idx_outer:
                    continue
                if titles[idx_outer] in titles[idx_inner]:
                    if verbose:
                        print(f'  "{titles[idx_inner]}" contains "{titles[idx_outer]}"')
                    if ((len(titles[idx_outer]) < len(titles[idx_inner])) and (len(titles[idx_outer]) > 3)) or (len(titles[idx_outer]) == len(titles[idx_inner])):
                        to_remove.append(idx_inner)
                        continue
        else:
            if verbose:
                print('No duplicate found')

    return [books[idx] for idx in range(len(books)) if idx not in to_remove]


def _calculate_text_overlap(text1, text2, word_threshold=5):
    """
    Calculate what percentage of text1 appears in text2 using sliding window approach.
    """
    words1 = text1.lower().split()
    words2_text = ' '.join(text2.lower().split())
    
    if len(words1) < word_threshold:
        return 0.0
    
    matches = 0
    i = 0
    while i <= len(words1) - word_threshold:
        window = ' '.join(words1[i:i + word_threshold])
        if window in words2_text:
            matches += word_threshold
            i += word_threshold  # Skip ahead properly
        else:
            i += 1
    
    return min(matches / len(words1), 1.0)  # Cap at 100%


def _find_similar_descriptions(books, similarity_threshold=0.8, min_description_length=50):
    """
    Find books with descriptions where 80%+ of text appears in another description.
    Returns indices of books to remove.
    """
    descriptions = []
    valid_indices = []
    
    # Extract descriptions and track valid indices
    for idx, book in enumerate(books):
        desc = book.get('volumeInfo', {}).get('description', '').strip()
        if len(desc) >= min_description_length:
            descriptions.append(desc)
            valid_indices.append(idx)
    
    to_remove = set()
    
    for i, desc1 in enumerate(descriptions):
        if valid_indices[i] in to_remove:
            continue
            
        for j, desc2 in enumerate(descriptions):
            if i != j and valid_indices[j] not in to_remove:
                # Check if desc1 is mostly contained in desc2
                overlap1_in_2 = _calculate_text_overlap(desc1, desc2)
                overlap2_in_1 = _calculate_text_overlap(desc2, desc1)

                if overlap1_in_2 >= similarity_threshold or overlap2_in_1 >= similarity_threshold:
                    # High overlap detected - remove both books
                    max_overlap = max(overlap1_in_2, overlap2_in_1)
                    to_remove.add(valid_indices[i])
                    to_remove.add(valid_indices[j])
                    print(f"Removing both books {valid_indices[i]} and {valid_indices[j]}: {max_overlap:.1%} overlap")
                    break
    
    return list(to_remove)


def _deduplicate_by_description_similarity(books, similarity_threshold=0.8):
    """
    Remove books with highly similar descriptions.
    """
    indices_to_remove = _find_similar_descriptions(books, similarity_threshold)
    return [book for idx, book in enumerate(books) if idx not in indices_to_remove]


def deduplicate_books(books, verbose=False):
    """
    Enhanced deduplication combining title and description similarity.
    """
    # First deduplicate by titles
    books = _deduplicate_books_by_shortest_title(books, verbose)

    if verbose:
        print(f"\nAfter title deduplication: {len(books)} books")
    
    # Then deduplicate by description similarity
    books = _deduplicate_by_description_similarity(books, similarity_threshold=0.8)
    
    if verbose:
        print(f"After description deduplication: {len(books)} books")
    
    return books