import requests
import json
from tqdm import tqdm
from dotenv import load_dotenv
import os
import time

load_dotenv()

API_KEY = os.getenv("STEAM_API_KEY")

BASE = "https://api.steampowered.com"

def load_seeds(path="seeds.txt"):
    with open(path) as f:
        return [line.strip() for line in f]


def get_friends(steam_id):

    url = f"{BASE}/ISteamUser/GetFriendList/v1/"

    params = {
        "key": API_KEY,
        "steamid": steam_id,
        "relationship": "friend"
    }

    r = requests.get(url, params=params)

    if r.status_code != 200:
        return []

    data = r.json()

    if "friendslist" not in data:
        return []

    return [f["steamid"] for f in data["friendslist"]["friends"]]


def get_owned_games(steam_id):

    url = f"{BASE}/IPlayerService/GetOwnedGames/v1/"

    params = {
        "key": API_KEY,
        "steamid": steam_id,
        "include_appinfo": True
    }

    r = requests.get(url, params=params)

    if r.status_code != 200:
        return []

    data = r.json()

    if "response" not in data or "games" not in data["response"]:
        return []

    return data["response"]["games"]


def expand_network(seed_players, depth=1):

    visited = set(seed_players)
    frontier = set(seed_players)

    edges = []

    for _ in range(depth):

        new_frontier = set()

        for player in tqdm(frontier, desc="Expanding network"):

            friends = get_friends(player)

            for f in friends:
                edges.append((player, f))

                if f not in visited:
                    visited.add(f)
                    new_frontier.add(f)

            time.sleep(0.2)

        frontier = new_frontier

    return visited, edges


def collect_games(players):

    player_games = {}

    for p in tqdm(players, desc="Collecting games"):

        games = get_owned_games(p)

        player_games[p] = games

        time.sleep(0.2)

    return player_games


def save_json(data, path):

    with open(path, "w") as f:
        json.dump(data, f)


def main():

    seeds = load_seeds()

    print("Seeds:", len(seeds))

    players, friend_edges = expand_network(seeds, depth=1)

    print("Players collected:", len(players))

    games = collect_games(players)

    os.makedirs("data", exist_ok=True)

    save_json(list(players), "data/players.json")
    save_json(friend_edges, "data/friends.json")
    save_json(games, "data/games.json")

    print("Dataset saved")


if __name__ == "__main__":
    main()