DROP DATABASE IF EXISTS board_games_db;
CREATE DATABASE board_games_db;

USE board_games_db;

-- Core table for static game information
CREATE TABLE game (
    game_id INT PRIMARY KEY NOT NULL, -- Using the ID from the CSV as the primary key
    name VARCHAR(255) NOT NULL,
    year_published INT,
    min_players INT,
    max_players INT,
    avg_playtime INT,
    min_age INT,
    is_expansion BOOLEAN
);

-- Table for user-generated ratings and popularity metrics
CREATE TABLE rating (
    rating_id INT PRIMARY KEY AUTO_INCREMENT,
    game_id INT NOT NULL UNIQUE, -- Ensures a one-to-one relationship with a game
    users_rated INT,
    rating_average DECIMAL(10, 8),
    bayes_average DECIMAL(10, 8),
    complexity_average DECIMAL(10, 8),
    owned_users INT,
    CONSTRAINT fk_rating_game
        FOREIGN KEY (game_id) REFERENCES game(game_id)
);

-- Table for all BGG ranking categories
-- CREATE TABLE rank (
--     rank_id INT PRIMARY KEY AUTO_INCREMENT,
--     game_id INT NOT NULL UNIQUE,
--     bgg_rank INT,
--     abstracts_rank INT,
--     cgs_rank INT,
--     childrensgames_rank INT,
--     familygames_rank INT,
--     partygames_rank INT,
--     strategygames_rank INT,
--     thematic_rank INT,
--     wargames_rank INT,
--     CONSTRAINT fk_rank_game
--         FOREIGN KEY (game_id) REFERENCES game(game_id)
-- );

-- Lookup table for unique mechanic names
CREATE TABLE mechanic (
    mechanic_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL UNIQUE
);

-- Lookup table for unique domain (category) names
CREATE TABLE domain (
    domain_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL UNIQUE
);

-- Junction table to link games to their mechanics (many-to-many)
CREATE TABLE game_mechanic (
    game_id INT NOT NULL,
    mechanic_id INT NOT NULL,
    PRIMARY KEY (game_id, mechanic_id),
    FOREIGN KEY (game_id) REFERENCES game(game_id),
    FOREIGN KEY (mechanic_id) REFERENCES mechanic(mechanic_id)
);

-- Junction table to link games to their domains (many-to-many)
CREATE TABLE game_domain (
    game_id INT NOT NULL,
    domain_id INT NOT NULL,
    PRIMARY KEY (game_id, domain_id),
    FOREIGN KEY (game_id) REFERENCES game(game_id),
    FOREIGN KEY (domain_id) REFERENCES domain(domain_id)
);
