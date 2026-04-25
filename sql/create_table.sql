CREATE TABLE IF NOT EXISTS raw_posts (
    id INT PRIMARY KEY,
    user_id INT,
    title TEXT,
    body TEXT
);