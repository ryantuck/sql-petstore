create table customers (
    id integer,
    first text,
    last text,
    city text,
    is_member boolean
);

create table pets (
    id integer,
    name text,
    species text,
    weight integer,
    age integer,
    owner_id integer
);

create table items (
    id integer,
    category text,
    name text,
    price float(2),
    manufacturer_id integer
);

create table manufacturers (
    id integer,
    name text,
    location text
);

create table transactions (
    id integer,
    customer_id integer,
    date date
);

create table transaction_items (
    item_id integer,
    quantity integer,
    transaction_id integer
);
