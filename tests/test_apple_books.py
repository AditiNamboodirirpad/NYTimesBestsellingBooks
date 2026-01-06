from src.api_clients.apple_books_client import fetch_apple_book

data = fetch_apple_book(
    isbn="9780385548984",
    title="The Widow",
    author="John Grisham"
)

print(data)
