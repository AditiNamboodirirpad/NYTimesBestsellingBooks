from src.api_clients.nyt_client import get_best_list

data = get_best_list("hardcover-fiction")

print("Results:", len(data["results"]))
print(data["results"])

