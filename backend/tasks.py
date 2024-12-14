import numpy as np
from celery import Celery
from sklearn.neighbors import NearestNeighbors

# Konfigurasi Celery dengan Redis yang berjalan di Laptop 1
app = Celery('recommender_system', broker='redis://172.16.14.163:6379/0')

app.conf.update(
    result_backend='redis://172.16.14.163:6379/0',
    task_serializer='json',
    accept_content=['json'],
    broker_connection_retry_on_startup=True,
    worker_pool='solo',
)

@app.task
def process_first_half(book_mapper, book_inv_mapper, book_id, X, k, shape, metric='cosine', show_distance=False):
    neighbour_ids = []
    book_id = str(book_id)
    n_rows, n_cols = shape  # Get the original dimensions of X
    X = np.array(X).reshape(n_rows, n_cols)

    # Check if book_id exists in book_mapper
    if book_id not in book_mapper:
        return neighbour_ids
    
    book_ind = book_mapper[book_id]
    book_vec = X[book_ind]
    k+=1
    kNN = NearestNeighbors(n_neighbors=k, algorithm="brute", metric=metric)
    kNN.fit(X)
    book_vec = book_vec.reshape(1,-1)
    neighbour = kNN.kneighbors(book_vec, return_distance=show_distance)
    for i in range(0,k):
        n = str(neighbour.item(i))
        neighbour_ids.append(book_inv_mapper[n])
    neighbour_ids.pop(0)
    return neighbour_ids

@app.task
def process_second_half(book_mapper, book_inv_mapper, book_id, X, k, shape, metric='cosine', show_distance=False):
    neighbour_ids = []
    book_id = str(book_id)
    n_rows, n_cols = shape  # Get the original dimensions of X
    X = np.array(X).reshape(n_rows, n_cols)

    # Check if book_id exists in book_mapper
    if book_id not in book_mapper:
        return neighbour_ids
    
    book_ind = book_mapper[book_id]
    book_vec = X[book_ind]
    k+=1
    kNN = NearestNeighbors(n_neighbors=k, algorithm="brute", metric=metric)
    kNN.fit(X)
    book_vec = book_vec.reshape(1,-1)
    neighbour = kNN.kneighbors(book_vec, return_distance=show_distance)
    for i in range(0,k):
        n = str(neighbour.item(i))
        neighbour_ids.append(book_inv_mapper[n])
    neighbour_ids.pop(0)
    return neighbour_ids