-- © ketteQ, Inc.

CREATE SCHEMA plan;
SET SEARCH_PATH = plan;

CREATE TABLE plan.calendar (
    id int8 NOT NULL,
    xuid text NOT NULL,
    "name" text NOT NULL,
    CONSTRAINT calendar_id_pk PRIMARY KEY (id),
    CONSTRAINT calendar_xuid_index UNIQUE (xuid),
    CONSTRAINT calendar_name_index UNIQUE (name)
);

CREATE TABLE plan.calendar_date (
    id int8 GENERATED ALWAYS AS IDENTITY NOT NULL,
    calendar_id int8 NOT NULL,
    "date" DATE NOT NULL,
    CONSTRAINT calendar_date_id_pk PRIMARY KEY (id)
);

ALTER TABLE plan.calendar_date
    ADD CONSTRAINT calendar_date_calendar_id_fk
        FOREIGN KEY (calendar_id) REFERENCES plan.calendar(id)
            ON DELETE CASCADE;

CREATE TABLE plan.data_date (
    "date" DATE NOT NULL
);

--

INSERT INTO plan.data_date ("date")
VALUES
    (NOW());

INSERT INTO plan.calendar (id, "name", xuid)
VALUES
    (1, 'month', 'month'),
    (2, 'quarter', 'quarter'),
    (3, 'year', 'year');

INSERT INTO plan.calendar_date (calendar_id, "date")
VALUES
    (1, '2024-01-01'),
    (1, '2024-02-01'),
    (1, '2024-03-01'),
    (1, '2024-04-01'),
    (1, '2024-05-01'),
    (1, '2024-06-01'),
    (2, '2024-01-01'),
    (2, '2024-04-01'),
    (2, '2024-07-01'),
    (2, '2024-10-01'),
    (2, '2025-01-01'),
    (2, '2025-04-01'),
    (2, '2025-07-01'),
    (2, '2025-10-01'),
    (3, '2024-01-01'),
    (3, '2025-01-01'),
    (3, '2026-01-01'),
    (3, '2027-01-01');

--
SELECT pg_sleep(1);