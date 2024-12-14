import csv
import random

# Generate ratings
ratings = []
user_ids = list(range(1, 51))
book_ids = list(range(101, 601))

for user_id in user_ids:
  rated_books = random.sample(book_ids, 400)
  for book_id in rated_books:
    rating = random.randint(1, 5)
    ratings.append([user_id, book_id, rating])

# Write to CSV
with open('dataset/ratings.csv', 'a', newline='') as file:
  writer = csv.writer(file)
  writer.writerows(ratings)