/* DDL to create new tables to accomodate WNBA/G League data */

/* TEAM
lg is null for NBA, W for WNBA, G for G League
this should be the only place lg needs to be */

/* PLAYER 
note for code - changed player_name to player*/

/* SEASON
alt_season_desc is for WNBA, since their season is one calendar year
write code to dynamically assigned desc */

/* GAME
_t and _p for team and player */

/* BOX
_t and _p for team and player */

/* SHTG
_t and _p for team and player */

create table if not exists team (
    team_id int, 
    team varchar(3),
    team_name varchar(80),
    lg varchar(4),
    primary key (team_id)
);

create table if not exists player (
    player_id int,
    player varchar(255),
    team_id int,
    active boolean, 
    lg varchar(4),
    primary key (player_id),
    foreign key (team_id) references team (team_id)
);

create table if not exists season (
    season_id int,
    season varchar(13),
    season_desc varchar(50),
    wseason varchar(8),
    wseason_desc varchar(50),
    primary key (season_id)
);

create table if not exists game (
    game_id int,
    season_id int,
    lg varchar(8),
    game_date date, 
    matchup varchar(12),
    final varchar(18),
    ot tinyint,
    primary key (game_id),
    foreign key (season_id) references season (season_id)
);


create table if not exists t_game (
    game_id int,
    season_id int, 
    team_id int, 
    game_date date, 
    matchup varchar(12), 
    pts int,
    diff int,
    wl varchar(1), 
    loc varchar(1),
    ot tinyint,
    primary key (game_id),
    foreign key (game_id) references game (game_id),
    foreign key (season_id) references season (season_id),
    foreign key (team_id) references team (team_id)
); 

create table if not exists p_game (
    game_id int,
    season_id int,
    team_id int, 
    player_id int,
    game_date date, 
    matchup varchar(12), 
    mins int,
    pts int,
    diff int,
    wl varchar(1), 
    loc varchar(1),
    ot tinyint,
    primary key (game_id),
    foreign key (game_id) references game (game_id),
    foreign key (season_id) references season (season_id),
    foreign key (team_id) references team (team_id),
    foreign key (player_id) references player (player_id)
); 

create table if not exists t_box (
    game_id int, 
    season_id int,
    team_id int,
    mins tinyint,
    pts tinyint, 
    ast tinyint, 
    reb tinyint, 
    stl tinyint, 
    blk tinyint, 
    oreb tinyint,
    dreb tinyint,
    pm tinyint,
    tov tinyint,
    pf tinyint,
    primary key (game_id, team_id),
    foreign key (game_id) references t_game (game_id),
    foreign key (season_id) references season (season_id),
    foreign key (team_id) references team (team_id)
);

create table if not exists p_box (
    game_id int, 
    season_id int,
    team_id int,
    player_id int,
    mins tinyint,
    pts tinyint, 
    ast tinyint, 
    reb tinyint, 
    stl tinyint, 
    blk tinyint, 
    oreb tinyint,
    dreb tinyint,
    pm tinyint,
    tov tinyint,
    pf tinyint,
    primary key (game_id, player_id),
    foreign key (game_id) references p_game (game_id),
    foreign key (season_id) references season (season_id),
    foreign key (team_id) references team (team_id),
    foreign key (player_id) references player (player_id)
);

create table if not exists t_shtg (
    game_id int, 
    season_id int,
    team_id int,
    fgm tinyint, 
    fga tinyint,
    fg3m tinyint, 
    fg3a tinyint, 
    ftm tinyint,
    fta tinyint,
    fg_pct decimal(10,2),
    fg3_pct decimal(10,2),
    ft_pct decimal(10,2),
    primary key (game_id, team_id),
    foreign key (game_id) references t_game (game_id),
    foreign key (season_id) references season (season_id),
    foreign key (team_id) references team (team_id)
);

create table if not exists p_shtg (
    game_id int, 
    season_id int,
    team_id int,
    player_id int,
    fgm tinyint, 
    fga tinyint,
    fg3m tinyint, 
    fg3a tinyint, 
    ftm tinyint,
    fta tinyint,
    fg_pct decimal(10,2),
    fg3_pct decimal(10,2),
    ft_pct decimal(10,2),
    primary key (game_id, player_id),
    foreign key (game_id) references p_game (game_id),
    foreign key (season_id) references season (season_id),
    foreign key (team_id) references team (team_id),
    foreign key (player_id) references player (player_id)
);

drop table t_box;
drop table p_box;
drop table t_shtg;
drop table p_shtg;
drop table t_game;
drop table p_game;
drop table game;
drop table season;