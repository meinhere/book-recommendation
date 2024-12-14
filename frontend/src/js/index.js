const fetchBooks = async (url) => {
  try {
    const response = await fetch(url);
    const data = await response.json();
    const formattedBooks = data.data.map((book) => [book.bookId, book.title]);
    return formattedBooks;
  } catch (error) {
    console.error("Error fetching books:", error);
  }
};

const fetchRecommendations = async (url, inputValue, selectedBook) => {
  if (selectedBook === "0") {
    return;
  }

  try {
    const response = await fetch(url, {
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
    return formattedBooks;
  } catch (error) {
    console.error("Error fetching recommendations:", error);
  }
};

export { fetchBooks, fetchRecommendations };
