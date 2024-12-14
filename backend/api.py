from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
from app import recommend_books

app = Flask(__name__)
CORS(app=app, resources={r"*": {"origins": "*"}})

@app.route('/')
def index():
    return jsonify({"message": "Books Recommendation API"}), 200

@app.route('/books', methods=['GET'])
def books():
    books_df = pd.read_csv('dataset/books.csv').to_dict('records')
    return jsonify({"message": "Books successfully fetched", "data": books_df}), 200

@app.route('/ratings', methods=['GET'])
def ratings():
    ratings_df = pd.read_csv('dataset/ratings.csv').to_dict('records')
    return jsonify({"message": "Books successfully fetched", "data": ratings_df}), 200

@app.route('/recommend', methods=['POST'])
def recommend():
    if not request.json:
        return jsonify({"message": "Book ID and K parameter are required"}), 400

    if 'book_id' not in request.json:
        return jsonify({"message": "Book ID is required"}), 400

    if 'k' not in request.json:
        return jsonify({"message": "K parameter is required"}), 400

    books_df = pd.read_csv('dataset/books.csv')
    ratings_df = pd.read_csv('dataset/ratings.csv')

    book_id = int(request.json['book_id'])
    k = int(request.json['k'])

    print("----------------------> request json", request.json)

    book_title, result = recommend_books(books_df, ratings_df, book_id, k)
    return jsonify({"message": "Books recommendation successfully fetched by book title: " + book_title, "data": result}), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)