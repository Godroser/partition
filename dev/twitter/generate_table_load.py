import random

def generate_tweets_sql(num_users=500000, tweets_per_user=5, output_file="insert_tweets.sql"):
    tweet_id = 1
    with open(output_file, "w") as f:
        f.write("-- Bulk insert tweets\n")
        f.write("INSERT INTO tweets (tweet_id, user_id, content) VALUES\n")
        lines = []
        for user_id in range(1, num_users + 1):
            for _ in range(tweets_per_user):
                content = f"'Tweet {tweet_id} from user_{user_id}'"
                lines.append(f"({tweet_id}, {user_id}, {content})")
                tweet_id += 1
        f.write(",\n".join(lines))
        f.write(";\n")
    print(f"Generated {tweet_id - 1} tweets into '{output_file}'.")

def generate_follows_sql(num_users=1000000, avg_follows=20, output_file="insert_follows.sql"):
    follows = set()
    with open(output_file, "w") as f:
        f.write("-- Bulk insert follows\n")
        f.write("INSERT INTO follows (follower_id, followee_id) VALUES\n")
        for user_id in range(1, num_users + 1):
            num_follows = random.randint(avg_follows - 5, avg_follows + 5)
            for _ in range(num_follows):
                followee_id = random.randint(1, num_users)
                if followee_id != user_id:
                    follows.add((user_id, followee_id))
        lines = [f"({follower}, {followee})" for (follower, followee) in follows]
        f.write(",\n".join(lines))
        f.write(";\n")
    print(f"Generated {len(follows)} follow relationships into '{output_file}'.")

def generate_users_sql(num_users=500000, output_file="insert_users.sql"):
    with open(output_file, "w") as f:
        f.write("-- Bulk insert users\n")
        f.write("INSERT INTO users (user_id, name) VALUES\n")
        for i in range(1, num_users + 1):
            name = f"'user_{i}'"
            line = f"({i}, {name})"
            if i < num_users:
                line += ","
            else:
                line += ";"
            f.write(line + "\n")
    print(f"Generated {num_users} user insert statements into '{output_file}'.")

# 调用函数
generate_users_sql()    

generate_follows_sql()

generate_tweets_sql()

# CREATE TABLE users (
#   user_id    BIGINT PRIMARY KEY,
#   name       VARCHAR(255) NOT NULL
# );

# CREATE TABLE tweets (
#   tweet_id   BIGINT PRIMARY KEY,
#   user_id    BIGINT NOT NULL,
#   content    TEXT NOT NULL
# );

# CREATE TABLE follows (
#   follower_id BIGINT NOT NULL,
#   followee_id BIGINT NOT NULL
# );

# mysql -h 10.77.110.144 -u root -P 4000 twitter < /data3/dzh/project/grep/dev/twitter/insert_follows.sql