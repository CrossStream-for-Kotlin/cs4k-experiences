
create table if not exists Games(
    id int generated always as identity primary key,
    board jsonb not null
);

create table if not exists Waiting(
    player varchar(16) primary key,
    gameId int unique
);