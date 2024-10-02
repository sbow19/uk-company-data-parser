def split_into_chunks(lst, n):
    """Split a list into n chunks."""
    if n <= 0:
        raise ValueError("Number of chunks must be greater than 0")
    chunk_size = len(lst) // n
    chunks = []
    
    for i in range(n):
        start_index = i * chunk_size
        # Handle the last chunk to include any remaining elements
        end_index = start_index + chunk_size if i < n - 1 else len(lst)
        chunks.append(lst[start_index:end_index])
        
    return chunks
