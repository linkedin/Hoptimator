
!set maxWidth 80
!table
!schemas

-- built-in bounded tables
SELECT * FROM DATAGEN.PERSON;
SELECT * FROM DATAGEN.COMPANY;

-- MySQL CDC tables
SELECT * FROM INVENTORY."products_on_hand" LIMIT 1;

-- test testing command
!test SELECT * FROM INVENTORY."products_on_hand" LIMIT 4;

-- test checkexpect command
-- !checkexpect 943fe SELECT * FROM DATAGEN.COMPANY;
-- !checkexpect 52 SELECT * FROM DATAGEN.PERSON;

-- MySQL CDC -> Kafka
SELECT * FROM RAWKAFKA."products" LIMIT 1;

-- test insert into command
!insert into RAWKAFKA."test-sink" SELECT AGE AS PAYLOAD, NAME AS KEY FROM DATAGEN.PERSON
SELECT * FROM RAWKAFKA."test-sink" LIMIT 5;

