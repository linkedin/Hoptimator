
!set maxWidth 80
!schemas
!table

-- built-in bounded tables
SELECT * FROM DATAGEN.PERSON;
SELECT * FROM DATAGEN.COMPANY;

-- test mermaid and yaml commands
!mermaid insert into RAWKAFKA."test-sink" SELECT AGE AS PAYLOAD, NAME AS KEY FROM DATAGEN.PERSON
!yaml insert into RAWKAFKA."test-sink" SELECT AGE AS PAYLOAD, NAME AS KEY FROM DATAGEN.PERSON

-- test insert into command
!insert into RAWKAFKA."test-sink" SELECT AGE AS PAYLOAD, NAME AS KEY FROM DATAGEN.PERSON
SELECT * FROM RAWKAFKA."test-sink" LIMIT 5;

-- read from sample subscription "names"
SELECT * FROM RAWKAFKA."names" LIMIT 1;

