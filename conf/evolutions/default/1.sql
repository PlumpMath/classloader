# Tasks schema

# --- !Ups

CREATE TABLE Company (
  id BIGSERIAL PRIMARY KEY,
  name VARCHAR(255),
  enabled BOOLEAN
);

# --- !Downs
DROP TABLE Company;
