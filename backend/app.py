import pandas as pd
import numpy as np
import math
from tasks import app
from scipy.sparse import csr_matrix

def create_matrix(df):
    N = len(df['userId'].unique())
    M = len(df['bookId'].unique())
    
    # Map Ids to indices
    user_mapper = dict(zip(np.unique(df["userId"]), list(range(N))))
    book_mapper = dict(zip(np.unique(df["bookId"]), list(range(M))))
    
    # Map indices to IDs
    user_inv_mapper = dict(zip(list(range(N)), np.unique(df["userId"])))
    book_inv_mapper = dict(zip(list(range(M)), np.unique(df["bookId"])))
    
    user_index = [user_mapper[i] for i in df['userId']]
    book_index = [book_mapper[i] for i in df['bookId']]

    X = csr_matrix((df["rating"], (book_index, user_index)), shape=(M, N))

    X_dense = X.toarray()  # Convert sparse matrix to a dense numpy array
    X_flat = [float(x) for x in X_dense.flatten()]
    book_mapper = {int(key): int(value) if isinstance(value, np.int64) else value for key, value in book_mapper.items()}
    book_inv_mapper = {int(key): int(value) if isinstance(value, np.int64) else value for key, value in book_inv_mapper.items()}
    
    return X, X_flat, user_mapper, book_mapper, user_inv_mapper, book_inv_mapper

def combine_results(first_half_similarity, second_half_similarity):
    combined_similarity = first_half_similarity + second_half_similarity
    return combined_similarity

def recommend_books(books, ratings, book_id, k):
    if book_id not in ratings['bookId'].values:
        return "Book ID not found"

    book_titles = dict(zip(books['bookId'], books['title']))
    book_title = book_titles[book_id]

    mid_index = len(ratings) // 2
    first_half = ratings.iloc[:mid_index]
    second_half = ratings.iloc[mid_index:]

    X, X_flat, user_mapper, book_mapper, user_inv_mapper, book_inv_mapper = create_matrix(first_half)
    first_half_result = app.send_task('tasks.process_first_half', args=[book_mapper, book_inv_mapper, book_id, X_flat, math.floor(k/2), X.shape]).get()

    X, X_flat, user_mapper, book_mapper, user_inv_mapper, book_inv_mapper = create_matrix(second_half)
    second_half_result = app.send_task('tasks.process_second_half', args=[book_mapper, book_inv_mapper, book_id, X_flat, math.ceil(k/2), X.shape]).get()

    all_results = combine_results(first_half_result, second_half_result)
    recommend_books = [{"bookId": i, "title": book_titles[i]} for i in all_results]

    return book_title, recommend_books