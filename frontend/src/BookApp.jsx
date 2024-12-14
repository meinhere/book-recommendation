import React, { useState, useEffect } from "react";

// BookSelector Component
const BookSelector = ({ books, onBookChange }) => {
  return (
    <div className="book-selector">
      <label htmlFor="bookSelect">Pilih Buku:</label>
      <select id="bookSelect" onChange={onBookChange}>
        <option value="0">Semua Buku</option>
        {books.map((book) => (
          <option key={book[0]} value={book[0]}>
            {book[1]}
          </option>
        ))}
      </select>
    </div>
  );
};

// Input Component
const BookInput = ({ label, value, onChange }) => {
  return (
    <div className="book-input">
      <label className="form-label" htmlFor="k">
        {label}
      </label>
      <input
        className="form-input"
        type="number"
        min="1"
        max="10"
        name="k"
        id="k"
        value={value}
        onChange={onChange}
      />
    </div>
  );
};

// ButtonSearch Component
const ButtonSearch = ({ onClick, children }) => {
  return (
    <button className="button-submit" onClick={onClick}>
      {children}
    </button>
  );
};

// BookList Component
const BookList = ({ books }) => {
  if (books.length === 0) {
    return null;
  }

  return (
    <div className="book-list">
      <h2>Hasil Rekomendasi Buku:</h2>
      <ul>
        {books.map((book) => (
          <li key={book[0]} className="book-item">
            <span className="book-title">Judul Buku: {book[1]}</span>
          </li>
        ))}
      </ul>
    </div>
  );
};

// --- BookApp Component ---
const BookApp = () => {
  const [books, setBooks] = useState([]);
  const [inputValue, setInputValue] = useState(1);
  const [selectedBook, setSelectedBook] = useState("0");
  const [listRecommendations, setListRecommendations] = useState([]);

  useEffect(() => {
    const fetchBooks = async () => {
      try {
        const response = await fetch("http://localhost:5000/books");
        const data = await response.json();
        const formattedBooks = data.data.map((book) => [
          book.bookId,
          book.title,
        ]);
        setBooks(formattedBooks);
      } catch (error) {
        console.error("Error fetching books:", error);
      }
    };

    fetchBooks();
  }, []);

  const fetchRecommendations = async (inputValue, selectedBook) => {
    try {
      if (selectedBook === "0") {
        return;
      }

      const response = await fetch("http://localhost:5000/recommend", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          book_id: selectedBook,
          k: inputValue,
        }),
      });

      const data = await response.json();
      const formattedBooks = data.data.map((book) => [book.bookId, book.title]);
      setListRecommendations(formattedBooks);
    } catch (error) {
      console.error("Error fetching recommendations:", error);
    }
  };

  const handleInputChange = (event) => {
    setInputValue(event.target.value);
  };

  const handleBookChange = (event) => {
    setSelectedBook(event.target.value);
  };

  const handleSubmit = () => {
    console.log("Selected Book:", selectedBook);
    console.log("Input Value:", inputValue);

    fetchRecommendations(inputValue, selectedBook);
  };

  return (
    <div className="app-container">
      <h1>Rekomendasi Buku</h1>
      <BookSelector books={books} onBookChange={handleBookChange} />
      <BookInput
        label="Jumlah Rekomendasi Buku"
        value={inputValue}
        onChange={handleInputChange}
      />
      <ButtonSearch onClick={handleSubmit}>Cari Rekomendasi</ButtonSearch>
      <BookList books={listRecommendations} />
    </div>
  );
};

export default BookApp;
