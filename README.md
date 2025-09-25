# Tweeter Simulation (Distributed System)

This project implements a **Twitter-like engine and simulator** in Erlang.  
It models users, tweets, hashtags, mentions, subscriptions, live updates, and re-tweets, all running on lightweight Erlang processes.  

The simulator spawns a configurable number of users, assigns them followers with a Zipf distribution, and generates random actions (tweets, queries, connects/disconnects, re-tweets) until a request limit is reached.

---

## What’s in the code

- `tweeter.erl` — main implementation:  
  - `start_server/0` spins up the server process and starts listening for client requests.  
  - `start_client/2` registers a client (user), handles commands like connect, disconnect, tweet, retweet, follow, query, and relays live updates.  
  - `start_simulation/3` runs the simulator with given parameters. It spawns users, connects them, assigns followers, and loops through random actions until the configured number of requests is met.

- `hashtags.txt` — sample hashtags injected into tweets.  
- `loremip.txt` — pool of placeholder text used to generate random tweet bodies.  
- `Report.pdf` — documentation of the simulation, its design, and performance.

---

## Requirements

- Erlang/OTP installed.
- `tweeter.erl`, `hashtags.txt`, and `loremip.txt` in the same directory.

---

## Quick start

### 1. Compile:
   ```
   c(tweeter).
   ```

### 2. Run simulation:
```
tweeter:start_simulation(<START_USER>, <END_USER>, <TOTAL_REQUESTS>).
```
##### <START_USER>: First user number, e.g., 1 creates user1.

##### <END_USER>: Last user number, e.g., 50 creates user1 … user50.

##### <TOTAL_REQUESTS>: Total requests to run before stopping.

### Example:
```
tweeter:start_simulation(1, 50, 5000).
```

### Core features

- Register: Each username must be unique.

- Connect/Disconnect: Live users receive updates, disconnected ones don’t.

- Send Tweets: Tweets may include hashtags and mentions. Stored in maps for efficient lookup.

- Follow/Subscribe: Users can follow others to receive their tweets.

- Query: Retrieve tweets by usernames, hashtags, or mentions.

- Re-tweet: Users can re-share existing tweets.

- Live updates: Mentions and subscriber tweets are pushed live to connected users

### Simulator behavior

- Spawns the server and multiple clients (userN).

- Followers are assigned using a Zipf distribution (popular users get more followers).

- Actions randomly chosen: connect, disconnect, tweet, retweet, query.

- Tweets built from random lorem text with random mentions and hashtags

### Sample runs

50 users, 5000 requests
Runs smoothly; latency grows moderately with load.

500 users, 5000 requests
Noticeable increase in latency. Improvement idea: delegate responses to a separate process

### Example output

[Server] Starting
[user1] Successfully registered...
[Simulator1] Invoking send_tweet on user17.
[user17] Tweet "Lorem Ipsum ... #CR7 @user3" invoked.
[user3] [Live Update] New mention from user17 on tweet "Lorem Ipsum ... #CR7 @user3"
...
[Simulator1] Average Tweet time: 12.35 microseconds, Total Tweet Requests: 231
