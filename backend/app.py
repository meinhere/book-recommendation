import pandas as pd
import numpy as np
from tasks import app
from sklearn.neighbors import NearestNeighbors
from scipy.sparse import csr_matrix

def find_similarity(book_mapper, book_inv_mapper, book_id, X, k, metric='cosine', show_distance=False):
    neighbour_ids = []

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
        n = neighbour.item(i)
        neighbour_ids.append(book_inv_mapper[n])
    neighbour_ids.pop(0)
    return neighbour_ids


def combine_matrices(results, original_books_ids):
    """Menggabungkan hasil dari task-task Celery dan mapper."""
    all_data = []
    all_rows = []
    all_cols = []

    master_books_mapper = {books_id: index for index, books_id in enumerate(original_books_ids)}
    master_user_offset = 0
    master_user_mapper = {}
    master_books_mapper_final = {}
    master_user_inv_mapper = {}
    master_books_inv_mapper = {}

    for result in results:
        X_serializable, user_mapper_subset, books_mapper_subset, user_inv_mapper_subset, books_inv_mapper_subset = result

        # Rekonstruksi array NumPy dari list
        data = X_serializable.data
        row = X_serializable.row
        col = X_serializable.col

        for i in range(len(data)):
            master_books_index = master_books_mapper[int(list(books_mapper_subset.keys())[row[i]])]
            all_data.append(data[i])
            all_rows.append(master_books_index)
            all_cols.append(int(col[i]) + master_user_offset)

        # Gabungkan mapper
        for user_id, local_user_index in user_mapper_subset.items():
            master_user_mapper[int(user_id)] = int(local_user_index) + master_user_offset
        for books_id, local_books_index in books_mapper_subset.items():
            # Pastikan books id ada di master mapper, jika tidak ada, tambahkan.
            if int(books_id) not in master_books_mapper_final:
                master_books_mapper_final[int(books_id)] = master_books_mapper[int(books_id)]

        for local_user_index, user_id in user_inv_mapper_subset.items():
            master_user_inv_mapper[int(local_user_index) + master_user_offset] = int(user_id)
        for local_books_index, books_id in books_inv_mapper_subset.items():
            if master_books_mapper[int(books_id)] not in master_books_inv_mapper:
                master_books_inv_mapper[master_books_mapper[int(books_id)]] = int(books_id)

        master_user_offset += len(user_mapper_subset)
    
    if not all_data:
        return None, {}, {}, {}, {}

    M = len(original_books_ids)
    N = master_user_offset

    combined_matrix = csr_matrix((all_data, (all_rows, all_cols)), shape=(M, N))
    return combined_matrix, master_user_mapper, master_books_mapper_final, master_user_inv_mapper, master_books_inv_mapper

def recommend_books(books_file, ratings_file, book_id, k):
    books = pd.read_csv(books_file)
    ratings = pd.read_csv(ratings_file)

    if book_id not in ratings['bookId'].values:
        return "Book ID not found"
    
    book_titles = dict(zip(books['bookId'], books['title']))
    book_title = book_titles[book_id]

    all_user_ids = ratings['userId'].unique()
    num_users = len(all_user_ids)
    midpoint = num_users // 2

    user_ids_1 = all_user_ids[:midpoint]
    user_ids_2 = all_user_ids[midpoint:]
    original_books_ids = ratings['bookId'].unique()

    # Memulai task secara paralel
    task1 = app.send_task("tasks.create_matrix_first_task", args=[ratings_file, user_ids_1])
    task2 = app.send_task("tasks.create_matrix_second_task", args=[ratings_file, user_ids_2])

    results = [task1.get(), task2.get()]

    X, user_mapper, book_mapper, user_inv_mapper, book_inv_mapper = combine_matrices(results, original_books_ids)

    all_results = find_similarity(book_mapper, book_inv_mapper, book_id, X, k)
    
    recommend_books = [{"bookId": i, "title": book_titles[i]} for i in all_results]

    return book_title, recommend_books