CREATE TABLE IF NOT EXISTS questions (
    id integer PRIMARY KEY,
    question text NOT NULL,
    answer text NOT NULL,
    created_at timestamp with time zone NOT NULL
);
