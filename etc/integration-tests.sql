
!set maxWidth 80
!table
!schemas

-- built-in bounded tables
SELECT * FROM DATAGEN.PERSON;
SELECT * FROM DATAGEN.COMPANY;

-- MySQL CDC tables
SELECT * FROM INVENTORY."products_on_hand" LIMIT 1;

-- Test check command
!check not empty SELECT * FROM INVENTORY."products_on_hand";

-- MySQL CDC -> Kafka
SELECT * FROM RAWKAFKA."products" LIMIT 1;

-- test insert into command
!insert into RAWKAFKA."test-sink" SELECT AGE AS PAYLOAD, NAME AS KEY FROM DATAGEN.PERSON
SELECT * FROM RAWKAFKA."test-sink" LIMIT 5;

