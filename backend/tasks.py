import pandas as pd
import numpy as np
from celery import Celery
from scipy.sparse import csr_matrix

# Konfigurasi Celery dengan Redis yang berjalan di Laptop 1
app = Celery('recommender_system', broker='redis://localhost:6379/0')

app.conf.update(
    result_backend='redis://localhost:6379/0',
    task_serializer='pickle', 
    result_serializer='pickle',
    accept_content=['pickle'],
    broker_connection_retry_on_startup=True,
    worker_pool='solo',
)

def process_user_subset(ratings_csv, user_ids):
    """Memproses subset user untuk membuat bagian matriks."""
    try:
        ratings = pd.read_csv(ratings_csv)
    except FileNotFoundError:
        print(f"Error: File not found at {ratings_csv}")
        return None, {}, {}, {}, {}
    except pd.errors.ParserError:
        print(f"Error: Could not parse CSV file at {ratings_csv}. Check the format.")
        return None, {}, {}, {}, {}
    except Exception as e:
        print(f"An unexpected error occurred while reading the CSV: {e}")
        return None, {}, {}, {}, {}

    subset_ratings = ratings[ratings['userId'].isin(user_ids)]

    if subset_ratings.empty:
        return None, {}, {}, {}, {}

    M = len(subset_ratings['bookId'].unique())
    N = len(subset_ratings['userId'].unique())

    # Map Ids to indices (Lokal untuk subset)
    user_mapper_subset = dict(zip(np.unique(subset_ratings["userId"]), list(range(N))))
    movie_mapper_subset = dict(zip(np.unique(subset_ratings["bookId"]), list(range(M))))

    # Map indices to IDs (Lokal untuk subset)
    user_inv_mapper_subset = dict(zip(list(range(N)), np.unique(subset_ratings["userId"])))
    movie_inv_mapper_subset = dict(zip(list(range(M)), np.unique(subset_ratings["bookId"])))

    user_index_subset = [user_mapper_subset[i] for i in subset_ratings['userId']]
    movie_index_subset = [movie_mapper_subset[i] for i in subset_ratings['bookId']]

    X_subset = csr_matrix((subset_ratings["rating"], (movie_index_subset, user_index_subset)), shape=(M, N))

    X_subset_coo = X_subset.tocoo()

    return X_subset_coo, user_mapper_subset, movie_mapper_subset, user_inv_mapper_subset, movie_inv_mapper_subset

@app.task
def create_matrix_first_task(ratings_csv, user_ids):
    """Task Celery untuk memproses sebagian matriks."""
    return process_user_subset(ratings_csv, user_ids)

@app.task
def create_matrix_second_task(ratings_csv, user_ids):
    """Task Celery untuk memproses sebagian matriks."""
    return process_user_subset(ratings_csv, user_ids)