create database demodb;

use demodb;

create table
    stock_prices_stage(
        ticker varchar(30),
        as_of_date date,
        open_price double,
        high_price double,
        low_price double,
        close_price double
    );

create table
    stock_prices(
        id int not null AUTO_INCREMENT,
        ticker varchar(30),
        as_of_date date,
        open_price double,
        high_price double,
        low_price double,
        close_price double,
        created_at timestamp default now(),
        updated_at timestamp default now(),
        primary key (id)
    );

create index ids_stockprices on stock_prices(ticker, as_of_date);

create index
    ids_stockpricestage on stock_prices_stage(ticker, as_of_date);